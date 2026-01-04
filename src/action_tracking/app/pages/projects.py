from __future__ import annotations

import json
import sqlite3
from datetime import date
from typing import Any

import streamlit as st

from action_tracking.data.repositories import (
    ChampionRepository,
    ProductionDataRepository,
    ProjectRepository,
    WcInboxRepository,
)
from action_tracking.domain.constants import PROJECT_IMPORTANCE, PROJECT_TYPES
from action_tracking.services.effectiveness import normalize_wc, parse_work_centers, suggest_work_centers

FIELD_LABELS = {
    "name": "Nazwa projektu",
    "work_center": "Work center",
    "project_code": "Project Code",
    "project_sop": "Project SOP",
    "project_eop": "Project EOP",
    "related_work_center": "Powiązane Work Center",
    "type": "Typ",
    "importance": "Importance",
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


def _build_work_center_map(work_centers: list[str]) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for work_center in work_centers:
        normalized = normalize_wc(work_center)
        if not normalized:
            continue
        mapping.setdefault(normalized, [])
        if work_center not in mapping[normalized]:
            mapping[normalized].append(work_center)
    return mapping


def _resolve_work_center_default(
    current_value: str,
    work_center_map: dict[str, list[str]],
    options: list[str],
) -> str:
    normalized = normalize_wc(current_value)
    if normalized in work_center_map:
        candidate = work_center_map[normalized][0]
        if candidate in options:
            return candidate
    return options[0] if options else ""


def render(con: sqlite3.Connection) -> None:
    st.header("Projekty")

    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)
    production_repo = ProductionDataRepository(con)
    wc_inbox_repo = WcInboxRepository(con)

    champions = champion_repo.list_champions()
    champion_names = {c["id"]: _champion_display_name(c) for c in champions}
    champion_options = ["(brak)"] + [c["id"] for c in champions]

    projects = project_repo.list_projects(include_counts=True)
    projects_by_id = {p["id"]: p for p in projects}
    project_wc_norms = project_repo.list_project_work_centers_norms(include_related=True)

    wc_lists = production_repo.list_distinct_work_centers()
    all_prod_wcs = sorted(set(wc_lists.get("scrap_work_centers", []) + wc_lists.get("kpi_work_centers", [])))
    prod_work_center_map = _build_work_center_map(all_prod_wcs)
    prod_work_center_keys = set(prod_work_center_map)

    production_stats = production_repo.list_production_work_centers_with_stats()
    production_stats_by_norm = {row["wc_norm"]: row for row in production_stats if row.get("wc_norm")}

    with st.expander("📁 Zarządzanie projektami (lista + edycja)", expanded=False):
        st.subheader("Lista projektów")
        st.caption(f"Liczba projektów: {len(projects)}")
        if not projects:
            st.info("Brak projektów.")
        table_rows = []
        for project in projects:
            total = project.get("actions_total") or 0
            closed = project.get("actions_closed") or 0
            open_count = project.get("actions_open") or 0
            pct_closed = project.get("pct_closed")
            pct_label = f"{pct_closed:.1f}%" if pct_closed is not None else "—"
            importance = project.get("importance") or "Mid Runner"
            table_rows.append(
                {
                    "Nazwa projektu": project.get("name"),
                    "Work center": project.get("work_center"),
                    "Typ": project.get("type"),
                    "Importance": importance,
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
            format_func=lambda pid: "(nowy)" if pid == "(nowy)" else projects_by_id[pid].get("name", pid),
        )
        editing = selected_id != "(nowy)"
        selected = projects_by_id.get(selected_id, {}) if editing else {}

        sop_value = date.fromisoformat(selected["project_sop"]) if selected.get("project_sop") else None
        eop_value = date.fromisoformat(selected["project_eop"]) if selected.get("project_eop") else None

        with st.form("project_form"):
            name = st.text_input("Nazwa projektu", value=selected.get("name", "") or "")
            use_wc_picker = st.checkbox(
                "Wybierz Work Center z danych produkcyjnych",
                value=False,
                disabled=not all_prod_wcs,
                help="Opcjonalny wybór z listy WC dostępnych w danych produkcyjnych.",
            )
            if use_wc_picker and all_prod_wcs:
                default_work_center = _resolve_work_center_default(
                    selected.get("work_center", "") or "",
                    prod_work_center_map,
                    all_prod_wcs,
                )
                work_center_index = all_prod_wcs.index(default_work_center) if default_work_center else 0
                work_center = st.selectbox("Work center", all_prod_wcs, index=work_center_index)
            else:
                work_center = st.text_input("Work center", value=selected.get("work_center", "") or "")

            project_code = st.text_input("Project Code", value=selected.get("project_code", "") or "")

            no_sop = st.checkbox("Brak daty SOP", value=sop_value is None)
            project_sop = st.date_input("Project SOP", value=sop_value or date.today(), disabled=no_sop)

            no_eop = st.checkbox("Brak daty EOP", value=eop_value is None)
            project_eop = st.date_input("Project EOP", value=eop_value or date.today(), disabled=no_eop)

            if use_wc_picker and all_prod_wcs:
                related_defaults = []
                related_tokens = parse_work_centers(None, selected.get("related_work_center", "") or "")
                for token in related_tokens:
                    normalized = normalize_wc(token)
                    if normalized in prod_work_center_map:
                        related_defaults.append(prod_work_center_map[normalized][0])
                related_selection = st.multiselect("Powiązane Work Center", all_prod_wcs, default=related_defaults)
                related_work_center = "; ".join(related_selection)
            else:
                related_work_center = st.text_input(
                    "Powiązane Work Center",
                    value=selected.get("related_work_center", "") or "",
                )

            st.markdown("**Walidacja Work Center**")
            if not all_prod_wcs:
                st.info("Brak danych produkcyjnych w bazie – nie można zweryfikować WC.")
            else:
                primary_label = work_center.strip() or "—"
                related_list = parse_work_centers(None, related_work_center)
                related_label = ", ".join(related_list) if related_list else "—"
                st.write(f"Project WC (primary): {primary_label}")
                st.write(f"Related WC: {related_label}")

                def _render_wc_status(label: str, value: str) -> None:
                    normalized = normalize_wc(value)
                    if not normalized:
                        st.caption(f"{label}: brak wartości do sprawdzenia.")
                        return
                    if normalized in prod_work_center_keys:
                        st.success(f"{label}: ✅ Found in production data.")
                        return
                    suggestions = suggest_work_centers(value, all_prod_wcs)
                    message = f"{label}: ⚠ Nie znaleziono w danych produkcyjnych."
                    if suggestions:
                        message += f" Sugestie: {', '.join(suggestions)}."
                    st.warning(message)

                _render_wc_status("Project WC (primary)", work_center)
                for related in related_list:
                    _render_wc_status(f"Related WC ({related})", related)

            selected_category = selected.get("type")
            if editing and selected_category and selected_category not in PROJECT_TYPES:
                st.caption(f"Legacy type: {selected_category}")
            category_index = PROJECT_TYPES.index("Others")
            if selected_category in PROJECT_TYPES:
                category_index = PROJECT_TYPES.index(selected_category)

            project_type = st.selectbox("Typ", PROJECT_TYPES, index=category_index)

            importance_value = selected.get("importance") or "Mid Runner"
            if importance_value not in PROJECT_IMPORTANCE:
                importance_value = "Mid Runner"
            importance_index = PROJECT_IMPORTANCE.index(importance_value)
            importance = st.selectbox(
                "Importance",
                PROJECT_IMPORTANCE,
                index=importance_index,
            )
            owner_champion_id = st.selectbox(
                "Champion",
                champion_options,
                index=champion_options.index(selected.get("owner_champion_id", "(brak)"))
                if editing and selected.get("owner_champion_id") in champion_options
                else 0,
                format_func=lambda cid: "(brak)" if cid == "(brak)" else champion_names.get(cid, cid),
            )
            status = st.selectbox(
                "Status",
                ["active", "closed", "on_hold"],
                index=["active", "closed", "on_hold"].index(selected.get("status") or "active"),
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
                    "importance": importance,
                    "owner_champion_id": None if owner_champion_id == "(brak)" else owner_champion_id,
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
            format_func=lambda pid: "(brak)" if pid == "(brak)" else projects_by_id[pid].get("name", pid),
            key="delete_project_select",
        )
        confirm_delete = st.checkbox("Potwierdzam usunięcie projektu", key="delete_project_confirm")
        if st.button("Usuń", disabled=delete_id == "(brak)" or not confirm_delete):
            removed = project_repo.delete_project(delete_id)
            if removed:
                st.success("Projekt usunięty.")
                st.rerun()
            else:
                st.error("Nie można usunąć projektu powiązanego z akcjami.")

    st.subheader("Wykryte Work Center z produkcji (nowe / niepowiązane)")
    with st.expander("Pokaż wykryte Work Center", expanded=False):
        if st.button("Odśwież wykrywanie (scan DB)", key="wc_inbox_refresh"):
            refreshed_stats = production_repo.list_production_work_centers_with_stats()
            wc_inbox_repo.upsert_from_production(refreshed_stats, project_wc_norms)
            st.rerun()

        open_items = wc_inbox_repo.list_open()
        if not production_stats:
            st.caption("Brak danych produkcyjnych.")
        elif not open_items:
            st.success("Brak nowych WC — wszystko pokryte projektami.")
        else:
            table_rows = []
            for item in open_items:
                stats = production_stats_by_norm.get(item.get("wc_norm") or "", {})
                sources = item.get("sources") or []
                sources_label = ", ".join(sources) if sources else "—"
                table_rows.append(
                    {
                        "WC": item.get("wc_raw") or "—",
                        "Źródła": sources_label,
                        "First seen": item.get("first_seen_date") or "—",
                        "Last seen": item.get("last_seen_date") or "—",
                        "Days": stats.get("count_days_present") or "—",
                    }
                )
            st.dataframe(table_rows, use_container_width=True)

            project_link_options = ["(wybierz)"] + [project["id"] for project in projects]
            for item in open_items:
                wc_norm = item.get("wc_norm") or ""
                wc_raw = item.get("wc_raw") or ""
                wc_key = wc_norm or item.get("id") or wc_raw
                with st.expander(f"Akcje: {wc_raw}", expanded=False):
                    st.caption(f"Normalized WC: {wc_norm}")

                    with st.form(f"wc_inbox_create_{wc_key}"):
                        project_name = st.text_input("Nazwa projektu", value=wc_raw, key=f"wc_inbox_name_{wc_key}")
                        st.text_input("Work center", value=wc_raw, disabled=True, key=f"wc_inbox_wc_{wc_key}")
                        project_type = st.selectbox(
                            "Typ", PROJECT_TYPES, index=PROJECT_TYPES.index("Others"), key=f"wc_inbox_type_{wc_key}"
                        )
                        importance = st.selectbox(
                            "Importance",
                            PROJECT_IMPORTANCE,
                            index=PROJECT_IMPORTANCE.index("Mid Runner"),
                            key=f"wc_inbox_importance_{wc_key}",
                        )
                        owner_champion_id = st.selectbox(
                            "Champion (opcjonalnie)",
                            champion_options,
                            index=0,
                            format_func=lambda cid: "(brak)" if cid == "(brak)" else champion_names.get(cid, cid),
                            key=f"wc_inbox_owner_{wc_key}",
                        )
                        related_wc = st.text_input(
                            "Powiązane Work Center (opcjonalnie)", value="", key=f"wc_inbox_related_{wc_key}"
                        )
                        created = st.form_submit_button("Utwórz projekt")

                    if created:
                        if not project_name.strip() or not wc_raw.strip():
                            st.error("Nazwa projektu i Work center są wymagane.")
                        else:
                            payload = {
                                "name": project_name.strip(),
                                "work_center": wc_raw.strip(),
                                "type": project_type,
                                "importance": importance,
                                "owner_champion_id": None if owner_champion_id == "(brak)" else owner_champion_id,
                                "status": "active",
                                "related_work_center": related_wc.strip() or None,
                            }
                            new_project_id = project_repo.create_project(payload)
                            wc_inbox_repo.mark_created(wc_norm, new_project_id)
                            st.success("Projekt utworzony.")
                            st.rerun()

                    if projects:
                        with st.form(f"wc_inbox_link_{wc_key}"):
                            link_project_id = st.selectbox(
                                "Powiąż z istniejącym projektem",
                                project_link_options,
                                index=0,
                                format_func=lambda pid: "(wybierz)"
                                if pid == "(wybierz)"
                                else projects_by_id.get(pid, {}).get("name", pid),
                                key=f"wc_inbox_link_select_{wc_key}",
                            )
                            linked = st.form_submit_button("Powiąż")
                        if linked:
                            if link_project_id == "(wybierz)":
                                st.error("Wybierz projekt do powiązania.")
                            else:
                                wc_inbox_repo.link_to_project(wc_norm, link_project_id)
                                st.success("Work Center powiązany.")
                                st.rerun()
                    else:
                        st.info("Brak projektów do powiązania.")

                    with st.form(f"wc_inbox_ignore_{wc_key}"):
                        confirm_ignore = st.checkbox("Potwierdzam ignorowanie", key=f"wc_inbox_ignore_confirm_{wc_key}")
                        ignored = st.form_submit_button("Ignoruj", disabled=not confirm_ignore)
                    if ignored:
                        wc_inbox_repo.ignore(wc_norm)
                        st.success("Work Center oznaczony jako ignorowany.")
                        st.rerun()

    with st.expander("🧾 Changelog projektów", expanded=False):
        changelog_entries = project_repo.list_changelog(limit=50)
        if not changelog_entries:
            st.caption("Brak wpisów w changelogu.")
        for entry in changelog_entries:
            changes = json.loads(entry["changes_json"])
            project_name = entry.get("name") or changes.get("name") or entry.get("project_id")
            st.markdown(f"**{entry['event_at']}** · {entry['event_type']} · {project_name}")
            st.caption(_format_changes(entry["event_type"], changes, champion_names))
