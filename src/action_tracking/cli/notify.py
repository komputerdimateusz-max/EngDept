from __future__ import annotations

import argparse
from datetime import date, datetime
import logging
import os
from pathlib import Path
from typing import Any

from action_tracking.data.db import connect, init_db
from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    NotificationRepository,
)
from action_tracking.integrations.email_sender import build_email, send_email, smtp_config_status
from action_tracking.services.notifications import build_daily_digest, build_overdue_alert

LOGGER = logging.getLogger(__name__)


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _notifications_enabled() -> bool:
    return _parse_bool(os.getenv("ACTION_TRACKING_EMAIL_NOTIFICATIONS_ENABLED"), default=False)


def _parse_today(value: str | None) -> date:
    if not value:
        return date.today()
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("Nieprawidłowy format daty --today (YYYY-MM-DD).") from exc


def _get_db_connection() -> Any:
    data_dir = Path(os.getenv("ACTION_TRACKING_DATA_DIR", "./data"))
    db_path = Path(os.getenv("ACTION_TRACKING_DB_PATH", data_dir / "app.db"))
    con = connect(db_path)
    init_db(con)
    return con


def _collect_action_ids(actions: list[dict[str, Any]]) -> list[str]:
    return [row.get("id") for row in actions if row.get("id")]


def _run_daily(con, today: date, dry_run: bool) -> dict[str, int]:
    action_repo = ActionRepository(con)
    champion_repo = ChampionRepository(con)
    notification_repo = NotificationRepository(con)

    counts = {
        "sent": 0,
        "skipped_no_email": 0,
        "skipped_no_actions": 0,
        "skipped_duplicate": 0,
        "dry_run": 0,
        "errors": 0,
    }

    champions = champion_repo.list_champions()
    for champion in champions:
        if not champion.get("active"):
            continue
        email = (champion.get("email") or "").strip()
        if not email:
            counts["skipped_no_email"] += 1
            continue

        open_actions = action_repo.list_open_actions_for_owner(champion["id"], today=today)
        overdue_actions = action_repo.list_overdue_actions_for_owner(
            champion["id"], cutoff_date=today
        )

        if not open_actions and not overdue_actions:
            counts["skipped_no_actions"] += 1
            continue

        unique_key = f"DAILY_DIGEST:{today.isoformat()}:{email}"
        if notification_repo.was_sent(unique_key):
            counts["skipped_duplicate"] += 1
            continue

        subject, body = build_daily_digest(champion, open_actions, overdue_actions)

        if dry_run:
            counts["dry_run"] += 1
            print(f"--- {email} ---")
            print(subject)
            print(body)
            print("")
            continue

        message = build_email(subject, email, body)
        if send_email(message):
            payload = {
                "open_count": len(open_actions),
                "overdue_count": len(overdue_actions),
                "action_ids": _collect_action_ids(open_actions + overdue_actions),
            }
            notification_repo.log_sent(
                "DAILY_DIGEST",
                email,
                None,
                payload,
                unique_key,
            )
            counts["sent"] += 1
        else:
            counts["errors"] += 1

    return counts


def _run_overdue(con, today: date, dry_run: bool) -> dict[str, int]:
    action_repo = ActionRepository(con)
    champion_repo = ChampionRepository(con)
    notification_repo = NotificationRepository(con)

    counts = {
        "sent": 0,
        "skipped_no_email": 0,
        "skipped_no_actions": 0,
        "skipped_duplicate": 0,
        "dry_run": 0,
        "errors": 0,
    }

    champions = champion_repo.list_champions()
    for champion in champions:
        if not champion.get("active"):
            continue
        email = (champion.get("email") or "").strip()
        if not email:
            counts["skipped_no_email"] += 1
            continue

        overdue_actions = action_repo.list_overdue_actions_for_owner(
            champion["id"], cutoff_date=today
        )
        if not overdue_actions:
            counts["skipped_no_actions"] += 1
            continue

        unique_key = f"OVERDUE_ALERT:{today.isoformat()}:{email}"
        if notification_repo.was_sent(unique_key):
            counts["skipped_duplicate"] += 1
            continue

        subject, body = build_overdue_alert(champion, overdue_actions)

        if dry_run:
            counts["dry_run"] += 1
            print(f"--- {email} ---")
            print(subject)
            print(body)
            print("")
            continue

        message = build_email(subject, email, body)
        if send_email(message):
            payload = {
                "overdue_count": len(overdue_actions),
                "action_ids": _collect_action_ids(overdue_actions),
            }
            notification_repo.log_sent(
                "OVERDUE_ALERT",
                email,
                None,
                payload,
                unique_key,
            )
            counts["sent"] += 1
        else:
            counts["errors"] += 1

    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Send action notification emails.")
    parser.add_argument("mode", choices=["daily", "overdue"], help="Notification type.")
    parser.add_argument("--dry-run", action="store_true", help="Print emails without sending.")
    parser.add_argument("--today", help="Override today date (YYYY-MM-DD).")
    args = parser.parse_args()

    if not _notifications_enabled():
        LOGGER.info("Email notifications disabled (ACTION_TRACKING_EMAIL_NOTIFICATIONS_ENABLED=false).")
        return

    today = _parse_today(args.today)

    if not args.dry_run:
        status = smtp_config_status()
        if not status["configured"]:
            LOGGER.error("SMTP not configured: %s", ", ".join(status["missing"]))
            return

    con = _get_db_connection()

    if args.mode == "daily":
        counts = _run_daily(con, today, args.dry_run)
    else:
        counts = _run_overdue(con, today, args.dry_run)

    LOGGER.info("Summary: %s", counts)


if __name__ == "__main__":
    main()
