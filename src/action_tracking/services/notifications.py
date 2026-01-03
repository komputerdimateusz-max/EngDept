from __future__ import annotations

from datetime import date, datetime


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            return None


def _format_action_line(action: dict[str, str], today: date) -> str:
    title = action.get("title") or "(bez tytułu)"
    project = action.get("project_name") or "Bez projektu"
    due_date = _parse_date(action.get("due_date"))
    due_label = due_date.isoformat() if due_date else "Brak terminu"
    category = action.get("category") or "Brak kategorii"
    priority = action.get("priority") or "med"
    status = action.get("status") or "open"
    days_overdue = ""
    if due_date and due_date < today:
        days_overdue = f" | { (today - due_date).days } dni po terminie"
    return (
        f"- {title} ({project}) | Termin: {due_label}{days_overdue}"
        f" | Kategoria: {category} | Priorytet: {priority} | Status: {status}"
    )


def build_daily_digest(
    owner: dict[str, str],
    actions_open: list[dict[str, str]],
    actions_overdue: list[dict[str, str]],
) -> tuple[str, str]:
    today = date.today()
    owner_name = owner.get("display_name") or owner.get("email") or "Champion"
    subject = f"Action Tracker: Daily digest ({today.isoformat()})"

    lines = [f"Cześć {owner_name},", "", "Podsumowanie akcji:"]
    lines.append(f"- Otwarte: {len(actions_open)}")
    lines.append(f"- Przeterminowane: {len(actions_overdue)}")
    lines.append("")

    if actions_open:
        lines.append("Otwarte akcje:")
        for action in actions_open:
            lines.append(_format_action_line(action, today))
    else:
        lines.append("Otwarte akcje: brak.")

    lines.append("")
    if actions_overdue:
        lines.append("Przeterminowane akcje:")
        for action in actions_overdue:
            lines.append(_format_action_line(action, today))
    else:
        lines.append("Przeterminowane akcje: brak.")

    lines.append("")
    lines.append("Szczegóły znajdziesz w aplikacji: Action Tracker → Akcje.")
    return subject, "\n".join(lines)


def build_overdue_alert(
    owner: dict[str, str],
    newly_overdue_actions: list[dict[str, str]],
) -> tuple[str, str]:
    today = date.today()
    owner_name = owner.get("display_name") or owner.get("email") or "Champion"
    subject = f"Action Tracker: Overdue alert ({today.isoformat()})"

    lines = [
        f"Cześć {owner_name},",
        "",
        f"Masz {len(newly_overdue_actions)} przeterminowanych akcji:",
        "",
    ]
    if newly_overdue_actions:
        for action in newly_overdue_actions:
            lines.append(_format_action_line(action, today))
    else:
        lines.append("Brak przeterminowanych akcji.")

    lines.append("")
    lines.append("Szczegóły znajdziesz w aplikacji: Action Tracker → Akcje.")
    return subject, "\n".join(lines)
