from __future__ import annotations

import json
from datetime import date
import sqlite3
from typing import Any

import streamlit as st

from action_tracking.data.repositories import ChampionRepository, ProjectRepository


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


def _format_assigned_projects(assigned_names: list[str], max_names: int = 4) -> str:
    if not assigned_names:
        return "—"
    total = len(assigned_names)
    shown = assigned_names[:max_names]
    label = "; ".join(shown)
    if total <= max_names:
        return f"{total}: {label}"
    return f"{total}: {label} +{total - max_names}"


def render(con: sqlite3.Connection) -> None:
    st.header("Champions")

    repo = ChampionRepository(con)
    project_repo = ProjectRepository(con)

    projects = project_repo.list_projects()
    project_names = {project["id"]: project["name"] for project in projects}

    champions = repo.list_champions()
    champions_by_id = {champion["id"]: champion for champion in champions}

    table_rows = []
    for champion in champions:
        assigned_ids = repo.get_assigned_projects_with_fallback(champion["id"])
        assigned_names = [project_names.get(pid, pid) for pid in assigned_ids]
        table_rows.append(
            {
                "Imię": champion["first_name"],
                "Nazwisko": champion["last_name"],
                "Adres email": champion["email"],
                "Data zatrudnienia": champion["hire_date"],
                "Stanowisko": champion["position"],
                "Aktywny": "Tak" if int(champion["active"]) == 1 else "Nie",
                "Przypisane projekty": _format_assigned_projects(assigned_names),
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
