from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from typing import Any

import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    EffectivenessRepository,
    GlobalSettingsRepository,
    ProductionDataRepository,
    ProjectRepository,
    SettingsRepository,
)
from action_tracking.services.effectiveness import (
    compute_kpi_effectiveness,
    compute_scrap_effectiveness,
    parse_date,
    parse_work_centers,
)
from action_tracking.services.normalize import normalize_key


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
    "manual_savings_amount": "Oszczędności ręczne (kwota)",
    "manual_savings_currency": "Oszczędności ręczne (waluta)",
    "manual_savings_note": "Oszczędności ręczne (opis)",
    "is_draft": "Szkic",
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


def _format_action_label(action: dict[str, Any], project_names: dict[str, str]) -> str:
    title = action.get("title") or "—"
    project_name = project_names.get(action.get("project_id"), "—")
    action_id = str(action.get("id") or "")
    if action_id.isdigit():
        suffix = action_id
    else:
        suffix = action_id[-6:] if action_id else "—"
    return f"{title} · {project_name} · #{suffix}"


def render(con: sqlite3.Connection) -> None:
    st.header("Akcje")

    repo = ActionRepository(con)
    project_repo = ProjectRepository(con)
    production_repo = ProductionDataRepository(con)
    effectiveness_repo = EffectivenessRepository(con)
    champion_repo = ChampionRepository(con)
    settings_repo = SettingsRepository(con)
    rules_repo = GlobalSettingsRepository(con)

    projects = project_repo.list_projects(include_counts=True)
    project_names = {
        p["id"]: (p.get("name") or p.get("project_name") or p["id"]) for p in projects
    }
    projects_by_id = {p["id"]: p for p in projects}

    champions = champion_repo.list_champions()
    champion_names = {c["id"]: c["display_name"] for c in champions}

    status_options = ["(Wszystkie)", "open", "in_progress", "blocked", "done", "cancelled"]
    project_options = ["Wszystkie"] + [p["id"] for p in projects]
    champion_options = ["(Wszyscy)"] + [c["id"] for c in champions]
    active_rule_rows = rules_repo.get_category_rules(only_active=True)
    active_categories = [row["category_label"] for row in active_rule_rows]
    if not active_categories:
        active_categories = [c["name"] for c in settings_repo.list_action_categories(active_only=True)]
    category_options = ["(Wszystkie)"] + active_categories

    def _fallback_rule(category: str) -> dict[str, Any]:
        return {
            "category_label": category,
            "effectiveness_model": "NONE",
            "savings_model": "MANUAL_REQUIRED",
            "requires_scope_link": False,
            "description": "Brak zdefiniowanej metodologii dla tej kategorii.",
            "is_active": True,
        }

    def _resolve_rule(category: str, warn: bool = False) -> dict[str, Any]:
        rule = rules_repo.resolve_category_rule(category)
        if rule is not None:
            return rule
        if warn:
            st.warning("Brak reguły dla kategorii. Użyto domyślnej: MANUAL_REQUIRED / NONE.")
            st.caption(
                f"Selected category raw: '{category}' | normalized: '{normalize_key(category)}'"
            )
            available_rules = rules_repo.get_category_rules(only_active=True)
            available = [
                f"{row['category_label']} -> {normalize_key(row['category_label'])}"
                for row in available_rules
            ]
            if available:
                st.caption("Available rules: " + ", ".join(available[:10]))
            else:
                st.caption("Available rules: (brak aktywnych reguł)")
        return _fallback_rule(category)

    col1, col2, col3, col4, col5, col6, col7 = st.columns(
        [1.2, 1.6, 1.6, 1.6, 1.2, 1.1, 1.6]
    )
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
    draft_options = ["Pokaż szkice", "Tylko szkice", "Ukryj szkice"]
    selected_draft_filter = col5.selectbox("Szkice", draft_options, index=0)
    overdue_only = col6.checkbox("Tylko po terminie")
    search_text = col7.text_input("Szukaj (tytuł)")

    status_filter = None if selected_status == "(Wszystkie)" else selected_status
    project_filter = None if selected_project == "Wszystkie" else selected_project
    champion_filter = None if selected_champion == "(Wszyscy)" else selected_champion
    category_filter = None if selected_category == "(Wszystkie)" else selected_category
    if selected_draft_filter == "Tylko szkice":
        draft_filter = True
    elif selected_draft_filter == "Ukryj szkice":
        draft_filter = False
    else:
        draft_filter = None

    rows = repo.list_actions(
        status=status_filter,
        project_id=project_filter,
        champion_id=champion_filter,
        is_draft=draft_filter,
        overdue_only=overdue_only,
        search_text=search_text or None,
    )

    # Category filter is applied on UI level for now (repository can be extended later).
    if category_filter:
        rows = [r for r in rows if r.get("category") == category_filter]

    st.subheader("Lista akcji")
    st.caption(f"Liczba akcji: {len(rows)}")

    eligible_for_recompute = [
        row
        for row in rows
        if row.get("status") == "done" and row.get("closed_at") and row.get("category")
    ]

    if st.button("Przelicz skuteczność (wg reguł)"):
        recomputed = 0
        skipped = 0
        for action in eligible_for_recompute:
            rule = _resolve_rule(action.get("category") or "")
            effect_model = rule.get("effectiveness_model")
            if effect_model == "NONE":
                skipped += 1
                continue
            closed_date = parse_date(action.get("closed_at"))
            if closed_date is None:
                skipped += 1
                continue

            project = projects_by_id.get(action.get("project_id") or "", {})
            work_centers = parse_work_centers(
                project.get("work_center"), project.get("related_work_center")
            )

            if rule.get("requires_scope_link") and not work_centers:
                skipped += 1
                continue

            if work_centers:
                date_from = closed_date - timedelta(days=14)
                date_to = closed_date + timedelta(days=14)
            else:
                date_from = None
                date_to = None

            payload = None
            if effect_model == "SCRAP":
                scrap_rows = (
                    production_repo.list_scrap_daily(
                        work_centers,
                        date_from,
                        date_to,
                        currency=None,
                    )
                    if work_centers
                    else []
                )
                payload = compute_scrap_effectiveness(action, work_centers, scrap_rows)
            elif effect_model in {"OEE", "PERFORMANCE"}:
                kpi_rows = (
                    production_repo.list_kpi_daily(
                        work_centers,
                        date_from,
                        date_to,
                    )
                    if work_centers
                    else []
                )
                metric_key = "oee_pct" if effect_model == "OEE" else "performance_pct"
                payload = compute_kpi_effectiveness(action, work_centers, kpi_rows, metric_key)

            if payload is None:
                skipped += 1
                continue
            effectiveness_repo.upsert_effectiveness(action["id"], payload)
            recomputed += 1

        st.success(
            f"Przeliczono skuteczność dla {recomputed} akcji. "
            f"Pominięto {skipped} akcji."
        )

    action_ids = [str(row.get("id")) for row in rows]
    effectiveness_map = effectiveness_repo.get_effectiveness_for_actions(action_ids)

    def _format_effectiveness(action: dict[str, Any]) -> tuple[str, str]:
        category = action.get("category") or ""
        rule = _resolve_rule(category)
        if rule.get("effectiveness_model") == "NONE" or action.get("status") != "done":
            return "—", "—"
        if not action.get("closed_at"):
            return "—", "—"
        row = effectiveness_map.get(action.get("id"))
        if not row:
            return "—", "—"
        classification = row.get("classification")
        label_map = {
            "effective": "✅ effective",
            "no_change": "➖ no_change",
            "worse": "❌ worse",
            "insufficient_data": "⚠️ insufficient_data",
            "no_scrap": "✅ no_scrap",
            "unknown": "❔ unknown",
        }
        label = label_map.get(classification, "—")
        pct_change = row.get("pct_change")
        pct_label = "—"
        if isinstance(pct_change, (int, float)):
            pct_label = f"{pct_change:.0%}"
        return label, pct_label

    table_rows: list[dict[str, Any]] = []
    for row in rows:
        owner = row.get("owner_name") or champion_names.get(row.get("owner_champion_id"), "")
        effect_label, pct_label = _format_effectiveness(row)
        table_rows.append(
            {
                "Krótka nazwa": row.get("title"),
                "Szkic": "tak" if row.get("is_draft") else "nie",
                "Kategoria": row.get("category"),
                "Projekt": project_names.get(row.get("project_id"), row.get("project_name")),
                "Owner": owner or "—",
                "Status": row.get("status"),
                "Priorytet": row.get("priority"),
                "Termin": row.get("due_date"),
                "Data utworzenia": row.get("created_at"),
                "Data zamknięcia": row.get("closed_at"),
                "Scrap effect": effect_label,
                "% change": pct_label,
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
        else _format_action_label(actions_by_id[aid], project_names),
    )

    editing = selected_action != "(nowa)"
    selected = actions_by_id.get(selected_action, {}) if editing else {}
    is_draft = bool(selected.get("is_draft"))

    due_date_value = None
    if selected.get("due_date"):
        try:
            due_date_value = date.fromisoformat(selected["due_date"])
        except ValueError:
            due_date_value = None

    if editing and is_draft:
        st.info("Wybrana akcja jest szkicem. Uzupełnij wymagane pola, aby ją zakończyć.")
        with st.form("complete_draft_form"):
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
            selected_category = selected.get("category")
            category_options_form = list(active_categories)
            legacy_category = None
            if selected_category and selected_category not in active_categories:
                legacy_category = selected_category
                category_options_form.append(selected_category)

            def _format_category_option(option: str) -> str:
                if legacy_category and option == legacy_category:
                    return f"(legacy: {option})"
                return option

            category = st.selectbox(
                "Kategoria",
                category_options_form,
                index=category_options_form.index(selected_category)
                if selected_category in category_options_form
                else 0,
                format_func=_format_category_option,
            )
            rule = _resolve_rule(category, warn=True)
            st.info(rule.get("description") or "Brak opisu metodologii dla tej kategorii.")
            st.caption(
                "Metoda obliczeń: "
                f"Skuteczność = {rule.get('effectiveness_model')}, "
                f"Oszczędności = {rule.get('savings_model')}, "
                f"Wymaga powiązania z WC = {'tak' if rule.get('requires_scope_link') else 'nie'}."
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

            manual_required = rule.get("savings_model") == "MANUAL_REQUIRED"
            manual_amount = None
            manual_currency = None
            manual_note = ""
            if manual_required:
                st.subheader("Oszczędności manualne")
                manual_amount = st.number_input(
                    "Kwota oszczędności",
                    min_value=0.0,
                    value=float(selected.get("manual_savings_amount") or 0.0),
                    step=100.0,
                )
                manual_currency = st.selectbox(
                    "Waluta",
                    ["PLN", "EUR"],
                    index=0
                    if (selected.get("manual_savings_currency") or "PLN") == "PLN"
                    else 1,
                )
                manual_note = st.text_area(
                    "Uzasadnienie oszczędności",
                    value=selected.get("manual_savings_note") or "",
                    max_chars=500,
                )

            st.caption("Zmiana statusu na inny niż 'done' czyści datę zamknięcia.")
            submitted = st.form_submit_button("Zakończ draft")

        if submitted:
            if category not in active_categories and category != selected.get("category"):
                st.error("Wybierz aktywną kategorię akcji.")
                return
            if rule.get("requires_scope_link"):
                project = projects_by_id.get(project_id)
                if not project or not str(project.get("work_center") or "").strip():
                    st.error(
                        "Ta kategoria wymaga powiązania z projektem posiadającym work center."
                    )
                    return
            if manual_required:
                if manual_amount is None:
                    st.error("Podaj kwotę oszczędności manualnych.")
                    return
                if not manual_currency:
                    st.error("Wybierz walutę oszczędności manualnych.")
                    return
                if not (manual_note or "").strip():
                    st.error("Uzupełnij uzasadnienie oszczędności manualnych.")
                    return
            payload = {
                "title": title,
                "description": description,
                "category": category,
                "project_id": project_id,
                "owner_champion_id": None if owner_champion == "(brak)" else owner_champion,
                "priority": priority,
                "status": status,
                "due_date": None if no_due_date else due_date.isoformat(),
                "manual_savings_amount": manual_amount if manual_required else None,
                "manual_savings_currency": manual_currency if manual_required else None,
                "manual_savings_note": manual_note if manual_required else None,
                "is_draft": 0,
            }
            try:
                repo.update_action(selected_action, payload)
                st.success("Draft uzupełniony.")
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))
    else:
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
            selected_category = selected.get("category")
            category_options_form = list(active_categories)
            legacy_category = None
            if selected_category and selected_category not in active_categories:
                legacy_category = selected_category
                category_options_form.append(selected_category)

            def _format_category_option(option: str) -> str:
                if legacy_category and option == legacy_category:
                    return f"(legacy: {option})"
                return option

            category = st.selectbox(
                "Kategoria",
                category_options_form,
                index=category_options_form.index(selected_category)
                if selected_category in category_options_form
                else 0,
                format_func=_format_category_option,
            )
            rule = _resolve_rule(category, warn=True)
            st.info(rule.get("description") or "Brak opisu metodologii dla tej kategorii.")
            st.caption(
                "Metoda obliczeń: "
                f"Skuteczność = {rule.get('effectiveness_model')}, "
                f"Oszczędności = {rule.get('savings_model')}, "
                f"Wymaga powiązania z WC = {'tak' if rule.get('requires_scope_link') else 'nie'}."
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

            manual_required = rule.get("savings_model") == "MANUAL_REQUIRED"
            manual_amount = None
            manual_currency = None
            manual_note = ""
            if manual_required:
                st.subheader("Oszczędności manualne")
                manual_amount = st.number_input(
                    "Kwota oszczędności",
                    min_value=0.0,
                    value=float(selected.get("manual_savings_amount") or 0.0),
                    step=100.0,
                )
                manual_currency = st.selectbox(
                    "Waluta",
                    ["PLN", "EUR"],
                    index=0
                    if (selected.get("manual_savings_currency") or "PLN") == "PLN"
                    else 1,
                )
                manual_note = st.text_area(
                    "Uzasadnienie oszczędności",
                    value=selected.get("manual_savings_note") or "",
                    max_chars=500,
                )

            st.caption("Zmiana statusu na inny niż 'done' czyści datę zamknięcia.")
            submitted = st.form_submit_button("Zapisz")

        if submitted:
            if category not in active_categories and category != selected.get("category"):
                st.error("Wybierz aktywną kategorię akcji.")
                return
            if rule.get("requires_scope_link"):
                project = projects_by_id.get(project_id)
                if not project or not str(project.get("work_center") or "").strip():
                    st.error(
                        "Ta kategoria wymaga powiązania z projektem posiadającym work center."
                    )
                    return
            if manual_required:
                if manual_amount is None:
                    st.error("Podaj kwotę oszczędności manualnych.")
                    return
                if not manual_currency:
                    st.error("Wybierz walutę oszczędności manualnych.")
                    return
                if not (manual_note or "").strip():
                    st.error("Uzupełnij uzasadnienie oszczędności manualnych.")
                    return
            payload = {
                "title": title,
                "description": description,
                "category": category,
                "project_id": project_id,
                "owner_champion_id": None if owner_champion == "(brak)" else owner_champion,
                "priority": priority,
                "status": status,
                "due_date": None if no_due_date else due_date.isoformat(),
                "manual_savings_amount": manual_amount if manual_required else None,
                "manual_savings_currency": manual_currency if manual_required else None,
                "manual_savings_note": manual_note if manual_required else None,
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
        else _format_action_label(actions_by_id[aid], project_names),
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
