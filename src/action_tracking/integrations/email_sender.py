from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage
from typing import Any

LOGGER = logging.getLogger(__name__)


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_smtp_config() -> tuple[dict[str, Any] | None, list[str]]:
    host = os.getenv("ACTION_TRACKING_SMTP_HOST")
    port = int(os.getenv("ACTION_TRACKING_SMTP_PORT", "587"))
    user = os.getenv("ACTION_TRACKING_SMTP_USER")
    password = os.getenv("ACTION_TRACKING_SMTP_PASS")
    from_address = os.getenv("ACTION_TRACKING_SMTP_FROM") or user
    tls = _parse_bool(os.getenv("ACTION_TRACKING_SMTP_TLS"), default=True)

    missing = [name for name, value in {"ACTION_TRACKING_SMTP_HOST": host}.items() if not value]
    if not from_address:
        missing.append("ACTION_TRACKING_SMTP_FROM")

    if missing:
        return None, missing

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "from_address": from_address,
        "tls": tls,
    }, []


def smtp_config_status() -> dict[str, Any]:
    config, missing = _load_smtp_config()
    return {
        "configured": config is not None,
        "missing": missing,
        "config": config,
    }


def build_email(subject: str, to_email: str, body_text: str) -> EmailMessage:
    config, _ = _load_smtp_config()
    from_address = (config or {}).get("from_address") or "no-reply@localhost"
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = from_address
    message["To"] = to_email
    message.set_content(body_text)
    return message


def send_email(message: EmailMessage) -> bool:
    config, missing = _load_smtp_config()
    if not config:
        LOGGER.error("SMTP config missing: %s", ", ".join(missing))
        return False

    try:
        if config["tls"] and config["port"] == 465:
            smtp: smtplib.SMTP = smtplib.SMTP_SSL(config["host"], config["port"])
        else:
            smtp = smtplib.SMTP(config["host"], config["port"])
            if config["tls"]:
                smtp.starttls()
        if config.get("user"):
            smtp.login(config["user"], config.get("password") or "")
        smtp.send_message(message)
        smtp.quit()
        return True
    except (smtplib.SMTPException, OSError) as exc:
        LOGGER.error("SMTP send failed: %s", exc)
        return False
