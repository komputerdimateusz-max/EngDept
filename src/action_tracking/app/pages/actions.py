from __future__ import annotations

import json
import sqlite3
from datetime import date
from typing import Any

import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    ProjectRepository,
)
from action_tracking.domain.constants import ACTION_CATEGORIES


FIELD_LABELS: dict[str, str] = {
    "project_id": "Projekt",
    "title": "Krótka nazwa",
    "description": "Opis",
    "owner_champion_id": "Właściciel",
    "priority": "Priorytet",
    "status": "Status",
    "due_date": "Termin",
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


def _format_changes(
    event_type: str, changes: dict[str, Any], project_names: dict[str, str]
) -> str:
    if event_type == "UPDATE":
        parts: list[str] = []
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
    project_names = {
        p["id"]: (p.get("name") or p.get("project_name") or p["id"]) for p in projects
    }

    champions = champion_repo.list_champions()
    champion_names = {c["id"]: c["display_name"] for c in champions}

    status_options = ["(Wszystkie)", "open", "in_progress", "blocked", "done", "cancelled"]
    project_options = ["Wszystkie"] + [p["id"] for p in projects]
    champion_options = ["(Wszyscy)"] + [c["id"] for c in champions]
    category_options = ["(Wszystkie)"] + list(ACTION_CATEGORIES)

    col1, col2, col3, col4, col5, col6 = st.columns([1.2, 1.6, 1.6, 1.6, 1.1, 1.6])
    selected_status = col1.selectbox("Status", status_options, index=0)
    selected_project = col2.selectbox(
        "Projekt",
        project_options,
        index=0,
        format_func=lambda pid: pid
        if pid == "Wszystkie"
        else project_names.get(pid, pid),
    )
    selected_champion = col3.selectbox(
        "Champion",
        champion_options,
        index=0,
        format_func=lambda cid: cid
        if cid == "(Wszyscy)"
        else champion_names.get(cid, cid),
    )
    selected_category = col4.selectbox("Kategoria", category_options, index=0)
    overdue_only = col5.checkbox("Tylko po terminie")
    search_text = col6.text_input("Szukaj (tytuł)")

    status_filter = None if selected_status == "(Wszystkie)" else selected_status
    project_filter = None if selected_project == "Wszystkie" else selected_project
    champion_filter = None if selected_champion == "(Wszyscy)" else selected_champion
    category_filter = None if selected_category == "(Wszystkie)" else selected_category

    rows = repo.list_actions(
        status=status_filter,
        project_id=project_filter,
        champion_id=champion_filter,
        overdue_only=overdue_only,
        search_text=search_text or None,
    )

    # Category filter is applied on UI level for now (repository can be extended later).
    if category_filter:
        rows = [r for r in rows if r.get("category") == category_filter]

    st.subheader("Lista akcji")
    st.caption(f"Liczba akcji: {len(rows)}")

    table_rows: list[dict[str, Any]] = []
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
                "Termin": row.get("due_date"),
                "Data utworzenia": row.get("created_at"),
                "Data zamknięcia": row.get("closed_at"),
            }
        )
    st.dataframe(table_rows, use_container_width=True)

    all_actions = repo.list_actions()
    actions_by_id = {a["id"]: a for a in all_actions}

    st.subheader("Dodaj / Edytuj akcję")

    if not projects:
        st.warning("Dodawanie akcji wymaga wcześniej utworzonych projektów.")
        return

    action_options = ["(nowa)"] + [a["id"] for a in all_actions]
    selected_action = st.selectbox(
        "Wybierz akcję do edycji",
        action_options,
        format_func=lambda aid: "(nowa)"
        if aid == "(nowa)"
        else f"{actions_by_id[aid]['title']} ({project_names.get(actions_by_id[aid]['project_id'], '—')})",
    )

    editing = selected_action != "(nowa)"
    selected = actions_by_id.get(selected_action, {}) if editing else {}

    due_date_value = None
    if selected.get("due_date"):
        try:
            due_date_value = date.fromisoformat(selected["due_date"])
        except ValueError:
            due_date_value = None

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

        project_ids = [p["id"] for p in projects]
        project_id = st.selectbox(
            "Projekt",
            project_ids,
            index=project_ids.index(selected.get("project_id"))
            if selected.get("project_id") in project_ids
            else 0,
            format_func=lambda pid: project_names.get(pid, pid),
        )

        owner_options = ["(brak)"] + [c["id"] for c in champions]
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
            "Brak terminu",
            value=due_date_value is None,
        )
        due_date = st.date_input(
            "Termin",
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
    delete_options = ["(brak)"] + [a["id"] for a in all_actions]
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
        else:
            for entry in changelog_entries:
                changes = json.loads(entry["changes_json"])
                action_title = (
                    entry.get("action_title")
                    or changes.get("title")
                    or entry.get("action_id")
                    or "Nieznana akcja"
                )
                st.markdown(f"**{entry['event_at']}** · {entry['event_type']} · {action_title}")
                st.caption(_format_changes(entry["event_type"], changes, project_names))
