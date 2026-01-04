from __future__ import annotations

import json
from datetime import date, datetime
import sqlite3
from typing import Any

import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    ProjectRepository,
)


FIELD_LABELS = {
    "first_name": "Imię",
    "last_name": "Nazwisko",
    "email": "Adres email",
    "hire_date": "Data zatrudnienia",
    "position": "Stanowisko",
    "active": "Aktywny",
}


def _format_value(field: str, value: Any) -> str:
    if value is None:
        return "—"
    if field == "active":
        return "Tak" if int(value) == 1 else "Nie"
    return str(value)


def _format_changes(event_type: str, changes: dict[str, Any]) -> str:
    if event_type == "UPDATE":
        parts = []
        for field, payload in changes.items():
            label = FIELD_LABELS.get(field, field)
            before = _format_value(field, payload.get("from"))
            after = _format_value(field, payload.get("to"))
            parts.append(f"{label}: {before} → {after}")
        return "; ".join(parts) if parts else "Brak zmian."
    parts = []
    for field, value in changes.items():
        label = FIELD_LABELS.get(field, field)
        parts.append(f"{label}: {_format_value(field, value)}")
    return "; ".join(parts) if parts else "Brak danych."


def _format_project_type_counts(type_counts: dict[str, int]) -> str:
    return " | ".join(
        [
            f"SL: {type_counts.get('SL', 0)}",
            f"RL: {type_counts.get('RL', 0)}",
            f"FL: {type_counts.get('FL', 0)}",
            f"Other: {type_counts.get('Other', 0)}",
        ]
    )


def _parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        try:
            return datetime.fromisoformat(str(value)).date()
        except ValueError:
            return None


def render(con: sqlite3.Connection) -> None:
    repo = ChampionRepository(con)
    project_repo = ProjectRepository(con)
    action_repo = ActionRepository(con)

    projects = project_repo.list_projects()
    project_names = {project["id"]: project.get("name") or project["id"] for project in projects}
    projects_by_id = {project["id"]: project for project in projects}

    champions = repo.list_champions()
    champions_by_id = {champion["id"]: champion for champion in champions}

    selected_focus = st.selectbox(
        "Wybierz championa",
        ["(brak)"] + [champion["id"] for champion in champions],
        index=0,
        format_func=lambda cid: "(brak)"
        if cid == "(brak)"
        else champions_by_id[cid]["display_name"],
    )

    st.header("Champions")

    if selected_focus and selected_focus != "(brak)":
        assigned_ids = repo.get_assigned_projects_with_fallback(selected_focus)
        champion_projects = [
            projects_by_id[pid] for pid in assigned_ids if pid in projects_by_id
        ]

        st.subheader("Projekty championa")
        project_rows = []
        for project in champion_projects:
            project_rows.append(
                {
                    "Projekt": project.get("name") or project.get("id") or "—",
                    "Typ": project.get("type") or "—",
                    "Status": project.get("status") or "—",
                    "Work Center": project.get("work_center") or "—",
                }
            )
        if project_rows:
            st.dataframe(project_rows, use_container_width=True)
        else:
            st.caption("Brak przypisanych projektów.")

        st.subheader("Akcje otwarte / opóźnione")
        all_actions = action_repo.list_actions(
            champion_id=selected_focus,
            is_draft=False,
        )
        open_actions = [
            action
            for action in all_actions
            if (action.get("status") or "").lower() not in {"done", "cancelled"}
        ]
        today = date.today()

        def _open_sort_key(action: dict[str, Any]) -> tuple[int, date]:
            due = _parse_date(action.get("due_date"))
            is_overdue = bool(due and due < today)
            created = _parse_date(action.get("created_at")) or date.max
            return (0 if is_overdue else 1, created)

        open_actions_sorted = sorted(open_actions, key=_open_sort_key)
        open_rows = []
        for action in open_actions_sorted:
            project_name = action.get("project_name") or project_names.get(
                action.get("project_id"), "—"
            )
            open_rows.append(
                {
                    "Tytuł": action.get("title") or "—",
                    "Projekt": project_name,
                    "Status": action.get("status") or "—",
                    "Termin": action.get("due_date") or "—",
                    "Data utworzenia": action.get("created_at") or "—",
                }
            )
        if open_rows:
            st.dataframe(open_rows, use_container_width=True)
        else:
            st.caption("Brak otwartych akcji.")

        st.subheader("Akcje zamknięte")
        with st.expander("Akcje zamknięte", expanded=False):
            closed_actions = [
                action
                for action in all_actions
                if (action.get("status") or "").lower() in {"done", "cancelled"}
            ]
            closed_rows = []
            for action in closed_actions:
                project_name = action.get("project_name") or project_names.get(
                    action.get("project_id"), "—"
                )
                closed_rows.append(
                    {
                        "Tytuł": action.get("title") or "—",
                        "Projekt": project_name,
                        "Status": action.get("status") or "—",
                        "Termin": action.get("due_date") or "—",
                        "Data zamknięcia": action.get("closed_at") or "—",
                    }
                )
            if closed_rows:
                st.dataframe(closed_rows, use_container_width=True)
            else:
                st.caption("Brak zamkniętych akcji.")

    table_rows = []
    for champion in champions:
        assigned_ids = repo.get_assigned_projects_with_fallback(champion["id"])
        assigned_projects = [projects_by_id[pid] for pid in assigned_ids if pid in projects_by_id]
        type_counts = {"SL": 0, "RL": 0, "FL": 0, "Other": 0}
        for project in assigned_projects:
            raw_type = (project.get("type") or "").strip().upper()
            if raw_type in {"SL", "RL", "FL"}:
                type_counts[raw_type] += 1
            else:
                type_counts["Other"] += 1
        table_rows.append(
            {
                "Imię": champion["first_name"],
                "Nazwisko": champion["last_name"],
                "Adres email": champion["email"],
                "Data zatrudnienia": champion["hire_date"],
                "Stanowisko": champion["position"],
                "Aktywny": "Tak" if int(champion["active"]) == 1 else "Nie",
                "Liczba projektów": len(assigned_ids),
                "Typy projektów": _format_project_type_counts(type_counts),
            }
        )

    st.subheader("Lista championów")
    st.caption(f"Liczba championów: {len(champions)}")
    st.dataframe(table_rows, use_container_width=True)

    st.subheader("Dodaj / Edytuj champion")
    champion_options = ["(nowy)"] + [
        champion["id"] for champion in champions
    ]
    selected_id = st.selectbox(
        "Wybierz championa do edycji",
        champion_options,
        format_func=lambda cid: "(nowy)"
        if cid == "(nowy)"
        else f"{champions_by_id[cid]['first_name']} {champions_by_id[cid]['last_name']}",
    )
    editing = selected_id != "(nowy)"
    selected = champions_by_id.get(selected_id) if editing else {}

    assigned_default = repo.get_assigned_projects_with_fallback(selected_id) if editing else []
    hire_date_value = None
    if selected.get("hire_date"):
        hire_date_value = date.fromisoformat(selected["hire_date"])

    with st.form("champion_form"):
        first_name = st.text_input("Imię", value=selected.get("first_name", ""))
        last_name = st.text_input("Nazwisko", value=selected.get("last_name", ""))
        email = st.text_input("Adres email", value=selected.get("email", "") or "")
        no_hire_date = st.checkbox(
            "Brak daty zatrudnienia",
            value=hire_date_value is None,
        )
        hire_date = st.date_input(
            "Data zatrudnienia",
            value=hire_date_value or date.today(),
            disabled=no_hire_date,
        )
        position = st.text_input("Stanowisko", value=selected.get("position", "") or "")
        active = st.checkbox(
            "Aktywny",
            value=bool(selected.get("active", 1)),
        )
        assigned_projects = st.multiselect(
            "Przypisane projekty",
            options=[project["id"] for project in projects],
            default=assigned_default,
            format_func=lambda pid: project_names.get(pid, pid),
        )
        submitted = st.form_submit_button("Zapisz")

    if submitted:
        if not first_name.strip() or not last_name.strip():
            st.error("Imię i nazwisko są wymagane.")
        else:
            payload = {
                "first_name": first_name.strip(),
                "last_name": last_name.strip(),
                "email": email.strip() or None,
                "hire_date": None if no_hire_date else hire_date.isoformat(),
                "position": position.strip() or None,
                "active": 1 if active else 0,
            }
            if editing:
                repo.update_champion(selected_id, payload)
                champion_id = selected_id
                st.success("Champion zaktualizowany.")
            else:
                champion_id = repo.create_champion(payload)
                st.success("Champion dodany.")
            repo.set_assigned_projects(champion_id, assigned_projects)
            st.rerun()

    st.subheader("Usuń championa")
    delete_id = st.selectbox(
        "Wybierz championa do usunięcia",
        ["(brak)"] + [champion["id"] for champion in champions],
        format_func=lambda cid: "(brak)"
        if cid == "(brak)"
        else f"{champions_by_id[cid]['first_name']} {champions_by_id[cid]['last_name']}",
        key="delete_select",
    )
    confirm_delete = st.checkbox(
        "Potwierdzam usunięcie championa",
        key="delete_confirm",
    )
    if st.button("Usuń", disabled=delete_id == "(brak)" or not confirm_delete):
        repo.delete_champion(delete_id)
        st.success("Champion usunięty.")
        st.rerun()

    st.subheader("Changelog")
    with st.expander("Changelog", expanded=False):
        changelog_entries = repo.list_changelog(limit=50)
        if not changelog_entries:
            st.caption("Brak wpisów w changelogu.")
        for entry in changelog_entries:
            changes = json.loads(entry["changes_json"])
            champion_name = " ".join(
                [
                    entry.get("first_name") or changes.get("first_name", ""),
                    entry.get("last_name") or changes.get("last_name", ""),
                ]
            ).strip()
            if not champion_name:
                champion_name = entry.get("champion_id", "Nieznany champion")
            st.markdown(
                f"**{entry['event_at']}** · {entry['event_type']} · {champion_name}"
            )
            st.caption(_format_changes(entry["event_type"], changes))
