from __future__ import annotations

from datetime import date, datetime
from typing import Any

from action_tracking.services.effectiveness import parse_date


def _parse_action_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            pass
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            pass
    return parse_date(value)


def action_marker_date_with_source(
    action: dict[str, Any],
) -> tuple[date | None, str | None]:
    """Resolve marker date priority: due_date > closed_at > created_at."""
    for field in ("due_date", "closed_at", "created_at"):
        parsed = _parse_action_date(action.get(field))
        if parsed:
            return parsed, field
    return None, None


def action_marker_date(action: dict[str, Any]) -> date | None:
    return action_marker_date_with_source(action)[0]


def action_marker_fields(action: dict[str, Any]) -> dict[str, str | None]:
    def _format(value: Any) -> str | None:
        parsed = _parse_action_date(value)
        return parsed.isoformat() if parsed else None

    return {
        "due_date": _format(action.get("due_date")),
        "closed_at": _format(action.get("closed_at")),
        "created_at": _format(action.get("created_at")),
    }
