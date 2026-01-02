from __future__ import annotations

import json
import sqlite3
from datetime import date
from typing import Any

import streamlit as st

from action_tracking.data.repositories import (
    ACTION_CATEGORIES,
    ActionRepository,
    ChampionRepository,
    ProjectRepository,
)


FIELD_LABELS = {
    "project_id": "Projekt",
    "title": "Krótka nazwa",
    "description": "Opis",
    "owner_champion_id": "Właściciel",
    "priority": "Priorytet",
    "status": "Status",
    "due_date": "Termin zamknięcia",
    "created_at": "Data utworzenia",
    "closed_at": "Data zamknięcia",
    "impact_type": "Typ wpływu",
    "impact_value": "Wartość wpływu",
    "category": "Kategoria",
}


def _format_value(value: Any) -> str:
    if value in (None, ""):
        return "—"
    return str(value)


def _format_changes(event_type: str, changes: dict[str, Any], project_names: dict[str, str]) -> str:
    if event_type == "UPDATE":
        parts = []
        for field, payload in changes.items():
            label = FIELD_LABELS.get(field, field)
            before = payload.get("from")
            after = payload.get("to")
            if field == "project_id":
                before = project_names.get(before, before)
                after = project_names.get(after, after)
            parts.append(f"{label}: {_format_value(before)} → {_format_value(after)}")
        return "; ".join(parts) if parts else "Brak zmian."

    parts = []
    for field, value in changes.items():
        label = FIELD_LABELS.get(field, field)
        if field == "project_id":
            value = project_names.get(value, value)
        parts.append(f"{label}: {_format_value(value)}")
    return "; ".join(parts) if parts else "Brak danych."


def render(con: sqlite3.Connection) -> None:
    st.header("Akcje")

    repo = ActionRepository(con)
    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)

    projects = project_repo.list_projects(include_counts=False)
    project_names = {project["id"]: project["name"] for project in projects}
    champions = champion_repo.list_champions()
    champion_names = {champion["id"]: champion["display_name"] for champion in champions}

    status_options = ["(Wszystkie)", "open", "in_progress", "blocked", "done", "cancelled"]
    project_options = ["Wszystkie"] + [project["id"] for project in projects]
    champion_options = ["(Wszyscy)"] + [champion["id"] for champion in champions]

    col1, col2, col3, col4, col5 = st.columns([1.2, 1.4, 1.4, 1.1, 1.9])
    selected_status = col1.selectbox("Status", status_options, index=0)
    selected_project = col2.selectbox(
        "Projekt",
        project_options,
        index=0,
        format_func=lambda pid: pid if pid == "Wszystkie" else project_names.get(pid, pid),
    )
    selected_champion = col3.selectbox(
        "Champion",
        champion_options,
        index=0,
        format_func=lambda cid: cid if cid == "(Wszyscy)" else champion_names.get(cid, cid),
    )
    overdue_only = col4.checkbox("Tylko po terminie")
    search_text = col5.text_input("Szukaj (tytuł)")

    status_filter = None if selected_status == "(Wszystkie)" else selected_status
    project_filter = None if selected_project == "Wszystkie" else selected_project
    champion_filter = None if selected_champion == "(Wszyscy)" else selected_champion

    rows = repo.list_actions(
        status=status_filter,
        project_id=project_filter,
        champion_id=champion_filter,
        overdue_only=overdue_only,
        search_text=search_text or None,
    )

    st.subheader("Lista akcji")
    st.caption(f"Liczba akcji: {len(rows)}")
    table_rows = []
    for row in rows:
        owner = row.get("owner_name") or champion_names.get(row.get("owner_champion_id"), "")
        table_rows.append(
            {
                "Krótka nazwa": row.get("title"),
                "Kategoria": row.get("category"),
                "Projekt": project_names.get(row.get("project_id"), row.get("project_name")),
                "Owner": owner or "—",
                "Status": row.get("status"),
                "Priorytet": row.get("priority"),
                "Termin zamknięcia": row.get("due_date"),
                "Data utworzenia": row.get("created_at"),
                "Data zamknięcia": row.get("closed_at"),
            }
        )
    st.dataframe(table_rows, use_container_width=True)

    all_actions = repo.list_actions()
    actions_by_id = {action["id"]: action for action in all_actions}

    st.subheader("Dodaj / Edytuj akcję")
    action_options = ["(nowa)"] + [action["id"] for action in all_actions]
    selected_action = st.selectbox(
        "Wybierz akcję do edycji",
        action_options,
        format_func=lambda aid: "(nowa)"
        if aid == "(nowa)"
        else f"{actions_by_id[aid]['title']} ({project_names.get(actions_by_id[aid]['project_id'], '—')})",
    )
    editing = selected_action != "(nowa)"
    selected = actions_by_id.get(selected_action, {}) if editing else {}

    if not projects:
        st.warning("Dodawanie akcji wymaga wcześniej utworzonych projektów.")
        return

    due_date_value = None
    if selected.get("due_date"):
        due_date_value = date.fromisoformat(selected["due_date"])

    with st.form("action_form"):
        title = st.text_input(
            "Krótka nazwa",
            value=selected.get("title", ""),
            max_chars=20,
        )
        description = st.text_area(
            "Opis",
            value=selected.get("description", "") or "",
            max_chars=500,
        )
        category = st.selectbox(
            "Kategoria",
            ACTION_CATEGORIES,
            index=ACTION_CATEGORIES.index(selected.get("category"))
            if selected.get("category") in ACTION_CATEGORIES
            else 0,
        )
        project_id = st.selectbox(
            "Projekt",
            [project["id"] for project in projects],
            index=[project["id"] for project in projects].index(selected.get("project_id"))
            if selected.get("project_id") in project_names
            else 0,
            format_func=lambda pid: project_names.get(pid, pid),
        )
        owner_options = ["(brak)"] + [champion["id"] for champion in champions]
        owner_default = (
            owner_options.index(selected.get("owner_champion_id"))
            if selected.get("owner_champion_id") in owner_options
            else 0
        )
        owner_champion = st.selectbox(
            "Owner champion",
            owner_options,
            index=owner_default,
            format_func=lambda cid: cid
            if cid == "(brak)"
            else champion_names.get(cid, cid),
        )
        priority_options = ["low", "med", "high"]
        priority = st.selectbox(
            "Priorytet",
            priority_options,
            index=priority_options.index(selected.get("priority"))
            if selected.get("priority") in priority_options
            else 1,
        )
        status_options_form = ["open", "in_progress", "blocked", "done", "cancelled"]
        status = st.selectbox(
            "Status",
            status_options_form,
            index=status_options_form.index(selected.get("status"))
            if selected.get("status") in status_options_form
            else 0,
        )
        no_due_date = st.checkbox(
            "Brak terminu zamknięcia",
            value=due_date_value is None,
        )
        due_date = st.date_input(
            "Termin zamknięcia",
            value=due_date_value or date.today(),
            disabled=no_due_date,
        )
        st.caption("Zmiana statusu na inny niż 'done' czyści datę zamknięcia.")
        submitted = st.form_submit_button("Zapisz")

    if submitted:
        payload = {
            "title": title,
            "description": description,
            "category": category,
            "project_id": project_id,
            "owner_champion_id": None if owner_champion == "(brak)" else owner_champion,
            "priority": priority,
            "status": status,
            "due_date": None if no_due_date else due_date.isoformat(),
        }
        try:
            if editing:
                repo.update_action(selected_action, payload)
                st.success("Akcja zaktualizowana.")
            else:
                repo.create_action(payload)
                st.success("Akcja dodana.")
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))

    st.subheader("Usuń akcję")
    delete_options = ["(brak)"] + [action["id"] for action in all_actions]
    delete_id = st.selectbox(
        "Wybierz akcję do usunięcia",
        delete_options,
        format_func=lambda aid: "(brak)"
        if aid == "(brak)"
        else f"{actions_by_id[aid]['title']} ({project_names.get(actions_by_id[aid]['project_id'], '—')})",
        key="delete_action_select",
    )
    confirm_delete = st.checkbox(
        "Potwierdzam usunięcie akcji",
        key="delete_action_confirm",
    )
    if st.button("Usuń", disabled=delete_id == "(brak)" or not confirm_delete):
        repo.delete_action(delete_id)
        st.success("Akcja usunięta.")
        st.rerun()

    st.subheader("Changelog")
    with st.expander("Changelog", expanded=False):
        changelog_entries = repo.list_action_changelog(limit=50, project_id=project_filter)
        if not changelog_entries:
            st.caption("Brak wpisów w changelogu.")
        for entry in changelog_entries:
            changes = json.loads(entry["changes_json"])
            action_title = (
                entry.get("action_title")
                or changes.get("title")
                or entry.get("action_id")
                or "Nieznana akcja"
            )
            st.markdown(
                f"**{entry['event_at']}** · {entry['event_type']} · {action_title}"
            )
            st.caption(_format_changes(entry["event_type"], changes, project_names))
