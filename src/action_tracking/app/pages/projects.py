from __future__ import annotations

import json
from datetime import date
import sqlite3
from typing import Any

import streamlit as st

from action_tracking.data.repositories import ChampionRepository, ProjectRepository
from action_tracking.domain.constants import PROJECT_CATEGORIES


FIELD_LABELS = {
    "name": "Nazwa projektu",
    "work_center": "Work center",
    "project_code": "Project Code",
    "project_sop": "Project SOP",
    "project_eop": "Project EOP",
    "related_work_center": "Powiązane Work Center",
    "type": "Typ",
    "owner_champion_id": "Champion",
    "status": "Status",
    "created_at": "Utworzono",
    "closed_at": "Zamknięto",
}


def _champion_display_name(champion: dict[str, Any]) -> str:
    full_name = f"{champion.get('first_name', '')} {champion.get('last_name', '')}".strip()
    if full_name:
        return full_name
    legacy_name = (champion.get("name") or "").strip()
    if legacy_name:
        return legacy_name
    return (champion.get("email") or "").strip()


def _format_value(field: str, value: Any, champion_names: dict[str, str]) -> str:
    if value in (None, ""):
        return "—"
    if field == "owner_champion_id":
        return champion_names.get(value, value)
    return str(value)


def _format_changes(
    event_type: str,
    changes: dict[str, Any],
    champion_names: dict[str, str],
) -> str:
    if event_type == "UPDATE":
        parts = []
        for field, payload in changes.items():
            label = FIELD_LABELS.get(field, field)
            before = _format_value(field, payload.get("from"), champion_names)
            after = _format_value(field, payload.get("to"), champion_names)
            parts.append(f"{label}: {before} → {after}")
        return "; ".join(parts) if parts else "Brak zmian."
    parts = []
    for field, value in changes.items():
        label = FIELD_LABELS.get(field, field)
        parts.append(f"{label}: {_format_value(field, value, champion_names)}")
    return "; ".join(parts) if parts else "Brak danych."


def render(con: sqlite3.Connection) -> None:
    st.header("Projekty")

    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)

    champions = champion_repo.list_champions()
    champion_names = {
        champion["id"]: _champion_display_name(champion) for champion in champions
    }
    champion_options = ["(brak)"] + [champion["id"] for champion in champions]

    projects = project_repo.list_projects(include_counts=True)
    projects_by_id = {project["id"]: project for project in projects}

    st.subheader("Lista projektów")
    st.caption(f"Liczba projektów: {len(projects)}")
    table_rows = []
    for project in projects:
        total = project.get("actions_total") or 0
        closed = project.get("actions_closed") or 0
        open_count = project.get("actions_open") or 0
        pct_closed = project.get("pct_closed")
        pct_label = f"{pct_closed:.1f}%" if pct_closed is not None else "—"
        table_rows.append(
            {
                "Nazwa projektu": project.get("name"),
                "Work center": project.get("work_center"),
                "Typ": project.get("type"),
                "Champion": project.get("owner_champion_name")
                or champion_names.get(project.get("owner_champion_id"), "—"),
                "Status": project.get("status"),
                "Akcje (łącznie)": total,
                "Akcje (otwarte)": open_count,
                "Akcje (zamknięte)": closed,
                "% zamkniętych": pct_label,
            }
        )
    st.dataframe(table_rows, use_container_width=True)

    st.subheader("Dodaj / Edytuj projekt")
    project_options = ["(nowy)"] + [project["id"] for project in projects]
    selected_id = st.selectbox(
        "Wybierz projekt do edycji",
        project_options,
        format_func=lambda pid: "(nowy)"
        if pid == "(nowy)"
        else projects_by_id[pid].get("name", pid),
    )
    editing = selected_id != "(nowy)"
    selected = projects_by_id.get(selected_id, {}) if editing else {}

    sop_value = (
        date.fromisoformat(selected["project_sop"])
        if selected.get("project_sop")
        else None
    )
    eop_value = (
        date.fromisoformat(selected["project_eop"])
        if selected.get("project_eop")
        else None
    )

    with st.form("project_form"):
        name = st.text_input(
            "Nazwa projektu",
            value=selected.get("name", "") or "",
        )
        work_center = st.text_input(
            "Work center",
            value=selected.get("work_center", "") or "",
        )
        project_code = st.text_input(
            "Project Code",
            value=selected.get("project_code", "") or "",
        )
        no_sop = st.checkbox("Brak daty SOP", value=sop_value is None)
        project_sop = st.date_input(
            "Project SOP",
            value=sop_value or date.today(),
            disabled=no_sop,
        )
        no_eop = st.checkbox("Brak daty EOP", value=eop_value is None)
        project_eop = st.date_input(
            "Project EOP",
            value=eop_value or date.today(),
            disabled=no_eop,
        )
        related_work_center = st.text_input(
            "Powiązane Work Center",
            value=selected.get("related_work_center", "") or "",
        )
        selected_category = selected.get("type")
        if editing and selected_category and selected_category not in PROJECT_CATEGORIES:
            st.caption(f"Legacy category detected: {selected_category}")
        category_index = PROJECT_CATEGORIES.index("Others")
        if selected_category in PROJECT_CATEGORIES:
            category_index = PROJECT_CATEGORIES.index(selected_category)
        project_type = st.selectbox(
            "Typ",
            PROJECT_CATEGORIES,
            index=category_index,
        )
        owner_champion_id = st.selectbox(
            "Champion",
            champion_options,
            index=champion_options.index(selected.get("owner_champion_id", "(brak)"))
            if editing and selected.get("owner_champion_id") in champion_options
            else 0,
            format_func=lambda cid: "(brak)"
            if cid == "(brak)"
            else champion_names.get(cid, cid),
        )
        status = st.selectbox(
            "Status",
            ["active", "closed", "on_hold"],
            index=["active", "closed", "on_hold"].index(
                selected.get("status") or "active"
            ),
        )
        submitted = st.form_submit_button("Zapisz")

    if submitted:
        if not name.strip() or not work_center.strip():
            st.error("Nazwa projektu i Work center są wymagane.")
        else:
            payload = {
                "name": name.strip(),
                "work_center": work_center.strip(),
                "project_code": project_code.strip() or None,
                "project_sop": None if no_sop else project_sop.isoformat(),
                "project_eop": None if no_eop else project_eop.isoformat(),
                "related_work_center": related_work_center.strip() or None,
                "type": project_type,
                "owner_champion_id": None
                if owner_champion_id == "(brak)"
                else owner_champion_id,
                "status": status,
            }
            if editing:
                project_repo.update_project(selected_id, payload)
                st.success("Projekt zaktualizowany.")
            else:
                project_repo.create_project(payload)
                st.success("Projekt dodany.")
            st.rerun()

    st.subheader("Usuń projekt")
    delete_id = st.selectbox(
        "Wybierz projekt do usunięcia",
        ["(brak)"] + [project["id"] for project in projects],
        format_func=lambda pid: "(brak)"
        if pid == "(brak)"
        else projects_by_id[pid].get("name", pid),
        key="delete_project_select",
    )
    confirm_delete = st.checkbox(
        "Potwierdzam usunięcie projektu",
        key="delete_project_confirm",
    )
    if st.button("Usuń", disabled=delete_id == "(brak)" or not confirm_delete):
        removed = project_repo.delete_project(delete_id)
        if removed:
            st.success("Projekt usunięty.")
            st.rerun()
        else:
            st.error("Nie można usunąć projektu powiązanego z akcjami.")

    st.subheader("Changelog")
    with st.expander("Changelog", expanded=False):
        changelog_entries = project_repo.list_changelog(limit=50)
        if not changelog_entries:
            st.caption("Brak wpisów w changelogu.")
        for entry in changelog_entries:
            changes = json.loads(entry["changes_json"])
            project_name = entry.get("name") or changes.get("name") or entry.get("project_id")
            st.markdown(
                f"**{entry['event_at']}** · {entry['event_type']} · {project_name}"
            )
            st.caption(_format_changes(entry["event_type"], changes, champion_names))
