from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import email
from email.message import EmailMessage, Message
from email.utils import make_msgid, parseaddr
import imaplib
import os
from pathlib import Path
import smtplib
import time
from typing import Any

from action_tracking.data.db import connect, init_db
from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    GlobalSettingsRepository,
    SettingsRepository,
)
from action_tracking.services.normalize import normalize_key


def parse_email_to_draft(message: Message) -> tuple[dict[str, Any] | None, list[str]]:
    body = _extract_body(message)
    if not body:
        return None, ["Brak treści wiadomości."]

    lines = [line.rstrip() for line in body.splitlines()]
    header_index = next(
        (idx for idx, line in enumerate(lines) if line.strip().upper() == "ACTION_DRAFT"),
        None,
    )
    if header_index is None:
        return None, ["Nie znaleziono nagłówka ACTION_DRAFT."]

    payload: dict[str, Any] = {}
    description_lines: list[str] = []
    in_description = False
    for line in lines[header_index + 1 :]:
        if in_description:
            description_lines.append(line)
            continue
        if not line.strip():
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        if key == "description":
            in_description = True
            if value:
                description_lines.append(value)
            continue
        payload[key] = value

    payload["description"] = "\n".join(description_lines).strip()

    errors: list[str] = []
    title = (payload.get("title") or "").strip()
    description = (payload.get("description") or "").strip()
    if not title:
        errors.append("Brak pola Title.")
    if not description:
        errors.append("Brak pola Description.")
    return payload, errors


def process_inbox(con) -> dict[str, int]:
    config = _load_config()
    inbox = config["imap_folder"]
    reply_mode = config["reply_mode"]
    allowed_domain = config.get("allowed_sender_domain")

    action_repo = ActionRepository(con)
    champion_repo = ChampionRepository(con)
    settings_repo = SettingsRepository(con)
    rules_repo = GlobalSettingsRepository(con)

    active_categories = [row["category_label"] for row in rules_repo.get_category_rules()]
    if not active_categories:
        active_categories = [
            row["name"] for row in settings_repo.list_action_categories(active_only=True)
        ]
    category_map = {normalize_key(name): name for name in active_categories}

    champions = champion_repo.list_champions()

    counts = {"processed": 0, "created": 0, "rejected": 0, "duplicates": 0}

    with _imap_connect(config) as imap:
        imap.select(inbox)
        status, data = imap.search(None, "UNSEEN")
        if status != "OK":
            raise RuntimeError("Nie udało się wyszukać wiadomości w skrzynce.")
        message_ids = data[0].split()
        for msg_id in message_ids:
            status, msg_data = imap.fetch(msg_id, "(RFC822)")
            if status != "OK" or not msg_data:
                continue
            raw_message = msg_data[0][1]
            message = email.message_from_bytes(raw_message)
            sender_name, sender_email = parseaddr(message.get("From", ""))
            sender_email = sender_email.strip()

            errors: list[str] = []
            if not sender_email:
                errors.append("Brak adresu nadawcy.")
            if allowed_domain and sender_email:
                domain = allowed_domain.lstrip("@").lower()
                if not sender_email.lower().endswith(f"@{domain}"):
                    errors.append("Niedozwolona domena nadawcy.")

            message_id = (message.get("Message-ID") or "").strip()
            if not message_id:
                errors.append("Brak Message-ID.")

            payload, parse_errors = parse_email_to_draft(message)
            errors.extend(parse_errors)

            if message_id and action_repo.get_action_by_source_message_id(message_id):
                _send_reply(
                    config,
                    sender_email,
                    message.get("Subject", ""),
                    _build_reply(
                        reply_mode,
                        "already_processed",
                        [],
                    ),
                )
                _mark_seen(imap, msg_id)
                counts["duplicates"] += 1
                counts["processed"] += 1
                continue

            champion_id, champion_errors = _resolve_champion(
                champions, sender_email, payload.get("champion") if payload else None
            )
            errors.extend(champion_errors)

            category = None
            if payload and payload.get("category"):
                raw_category = payload["category"]
                category = category_map.get(normalize_key(raw_category))
                if not category:
                    errors.append("Nieprawidłowa kategoria akcji.")

            due_date = None
            if payload and payload.get("due"):
                due_raw = payload["due"]
                try:
                    due_date = date.fromisoformat(due_raw).isoformat()
                except ValueError:
                    errors.append("Nieprawidłowy format pola Due (wymagany ISO).")

            if errors:
                _send_reply(
                    config,
                    sender_email,
                    message.get("Subject", ""),
                    _build_reply(reply_mode, "rejected", errors),
                )
                _mark_seen(imap, msg_id)
                counts["rejected"] += 1
                counts["processed"] += 1
                continue

            now = datetime.now(timezone.utc).isoformat()
            try:
                action_repo.create_draft(
                    {
                        "title": payload["title"],
                        "description": payload["description"],
                        "owner_champion_id": champion_id,
                        "category": category,
                        "due_date": due_date,
                        "source": "email",
                        "source_message_id": message_id,
                        "submitted_by_email": sender_email,
                        "submitted_at": now,
                        "created_at": now,
                    }
                )
            except Exception as exc:
                _send_reply(
                    config,
                    sender_email,
                    message.get("Subject", ""),
                    _build_reply(reply_mode, "rejected", [str(exc)]),
                )
                _mark_seen(imap, msg_id)
                counts["rejected"] += 1
                counts["processed"] += 1
                continue

            _send_reply(
                config,
                sender_email,
                message.get("Subject", ""),
                _build_reply(reply_mode, "accepted", []),
            )
            _mark_seen(imap, msg_id)
            counts["created"] += 1
            counts["processed"] += 1

    return counts


def _resolve_champion(
    champions: list[dict[str, Any]],
    sender_email: str,
    champion_text: str | None,
) -> tuple[str | None, list[str]]:
    errors: list[str] = []
    normalized_email = sender_email.lower().strip() if sender_email else ""
    champions_by_email = {
        (row.get("email") or "").lower(): row["id"] for row in champions if row.get("email")
    }
    if normalized_email and normalized_email in champions_by_email:
        return champions_by_email[normalized_email], []

    if not champion_text:
        return None, ["Nie udało się dopasować championa po adresie email."]

    candidate = champion_text.strip().lower()
    if candidate in champions_by_email:
        return champions_by_email[candidate], []

    labels = []
    for row in champions:
        label = row.get("display_name") or row.get("name") or row.get("email") or row["id"]
        labels.append((label, row["id"]))

    exact_matches = [champion_id for label, champion_id in labels if label.lower() == candidate]
    if len(exact_matches) == 1:
        return exact_matches[0], []
    if len(exact_matches) > 1:
        errors.append("Wskazany champion jest niejednoznaczny.")
        return None, errors

    scored = []
    for label, champion_id in labels:
        ratio = _fuzzy_ratio(candidate, label.lower())
        scored.append((ratio, label, champion_id))
    scored.sort(reverse=True)
    if not scored or scored[0][0] < 0.75:
        errors.append("Nie udało się dopasować championa po nazwie.")
        return None, errors

    best_ratio = scored[0][0]
    close_matches = [label for ratio, label, _ in scored if ratio >= best_ratio - 0.05]
    if len(close_matches) > 1:
        errors.append(
            "Champion jest niejednoznaczny. Możliwe dopasowania: "
            + ", ".join(close_matches[:3])
        )
        return None, errors

    return scored[0][2], []


def _fuzzy_ratio(a: str, b: str) -> float:
    import difflib

    return difflib.SequenceMatcher(None, a, b).ratio()


def _build_reply(mode: str, status: str, errors: list[str]) -> str:
    if status == "accepted":
        return "Dziękujemy! Draft akcji został zarejestrowany i czeka na uzupełnienie."
    if status == "already_processed":
        return "Ta wiadomość została już przetworzona. Draft akcji jest w systemie."
    if mode == "generic":
        return "Nie udało się przetworzyć tej wiadomości. Skontaktuj się z administratorem."
    details = "\n".join(f"- {error}" for error in errors) if errors else ""
    return (
        "Nie udało się utworzyć draftu akcji z tej wiadomości.\n\n"
        "Powody:\n"
        f"{details}"
    )


def _extract_body(message: Message) -> str:
    if message.is_multipart():
        for part in message.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                charset = part.get_content_charset() or "utf-8"
                try:
                    return part.get_payload(decode=True).decode(charset, errors="replace")
                except (AttributeError, LookupError):
                    continue
        return ""
    charset = message.get_content_charset() or "utf-8"
    payload = message.get_payload(decode=True)
    if payload is None:
        return message.get_payload() or ""
    return payload.decode(charset, errors="replace")


def _load_config() -> dict[str, Any]:
    imap_host = os.getenv("ACTION_TRACKING_EMAIL_IMAP_HOST")
    imap_port = int(os.getenv("ACTION_TRACKING_EMAIL_IMAP_PORT", "993"))
    imap_user = os.getenv("ACTION_TRACKING_EMAIL_IMAP_USER")
    imap_pass = os.getenv("ACTION_TRACKING_EMAIL_IMAP_PASS")
    smtp_host = os.getenv("ACTION_TRACKING_EMAIL_SMTP_HOST")
    smtp_port = int(os.getenv("ACTION_TRACKING_EMAIL_SMTP_PORT", "587"))
    smtp_user = os.getenv("ACTION_TRACKING_EMAIL_SMTP_USER")
    smtp_pass = os.getenv("ACTION_TRACKING_EMAIL_SMTP_PASS")
    imap_folder = os.getenv("ACTION_TRACKING_EMAIL_INBOX_FOLDER", "INBOX")
    allowed_sender_domain = os.getenv("ACTION_TRACKING_EMAIL_ALLOWED_SENDER_DOMAIN")
    reply_mode = os.getenv("ACTION_TRACKING_EMAIL_REPLY_MODE", "strict").lower()

    missing = [
        name
        for name, value in {
            "ACTION_TRACKING_EMAIL_IMAP_HOST": imap_host,
            "ACTION_TRACKING_EMAIL_IMAP_USER": imap_user,
            "ACTION_TRACKING_EMAIL_IMAP_PASS": imap_pass,
            "ACTION_TRACKING_EMAIL_SMTP_HOST": smtp_host,
            "ACTION_TRACKING_EMAIL_SMTP_USER": smtp_user,
            "ACTION_TRACKING_EMAIL_SMTP_PASS": smtp_pass,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Brak konfiguracji: {', '.join(missing)}")

    return {
        "imap_host": imap_host,
        "imap_port": imap_port,
        "imap_user": imap_user,
        "imap_pass": imap_pass,
        "imap_folder": imap_folder,
        "smtp_host": smtp_host,
        "smtp_port": smtp_port,
        "smtp_user": smtp_user,
        "smtp_pass": smtp_pass,
        "allowed_sender_domain": allowed_sender_domain,
        "reply_mode": reply_mode,
    }


def _imap_connect(config: dict[str, Any]) -> imaplib.IMAP4_SSL:
    imap = imaplib.IMAP4_SSL(config["imap_host"], config["imap_port"])
    imap.login(config["imap_user"], config["imap_pass"])
    return imap


def _smtp_connect(config: dict[str, Any]) -> smtplib.SMTP:
    if config["smtp_port"] == 465:
        smtp = smtplib.SMTP_SSL(config["smtp_host"], config["smtp_port"])
    else:
        smtp = smtplib.SMTP(config["smtp_host"], config["smtp_port"])
        smtp.starttls()
    smtp.login(config["smtp_user"], config["smtp_pass"])
    return smtp


def _send_reply(config: dict[str, Any], recipient: str, subject: str, body: str) -> None:
    if not recipient:
        return
    msg = EmailMessage()
    msg["Subject"] = f"Re: {subject}" if subject else "Re: ACTION_DRAFT"
    msg["From"] = config["smtp_user"]
    msg["To"] = recipient
    msg["Message-ID"] = make_msgid()
    msg.set_content(body)
    with _smtp_connect(config) as smtp:
        smtp.send_message(msg)


def _mark_seen(imap: imaplib.IMAP4_SSL, msg_id: bytes) -> None:
    imap.store(msg_id, "+FLAGS", "\\Seen")


def _get_db_connection() -> Any:
    data_dir = Path(os.getenv("ACTION_TRACKING_DATA_DIR", "./data"))
    db_path = Path(os.getenv("ACTION_TRACKING_DB_PATH", data_dir / "app.db"))
    con = connect(db_path)
    init_db(con)
    return con


def main() -> None:
    parser = argparse.ArgumentParser(description="Email ingest for action drafts.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--once", action="store_true", help="Process inbox once and exit.")
    group.add_argument("--loop", action="store_true", help="Process inbox in a loop.")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval in seconds.")
    args = parser.parse_args()

    run_once = args.once or not args.loop
    con = _get_db_connection()

    if run_once:
        counts = process_inbox(con)
        print(f"Processed: {counts}")
        return

    while True:
        counts = process_inbox(con)
        print(f"Processed: {counts}")
        time.sleep(max(args.interval, 5))


if __name__ == "__main__":
    main()
