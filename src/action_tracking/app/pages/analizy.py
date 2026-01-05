from __future__ import annotations

import json
import sqlite3
from datetime import date
from typing import Any

import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    AnalysisRepository,
    ChampionRepository,
    ProjectRepository,
)


TOOL_TYPES = ["5WHY", "Diagram Ishikawy", "Raport A3", "Raport 8D"]
AREA_OPTIONS = ["(brak)", "Montaż", "Wtrysk", "Metalizacja", "Podgrupa", "Inne"]


def _default_template(tool_type: str) -> dict[str, Any]:
    if tool_type == "5WHY":
        return {
            "problem_description": "",
            "why_1": "",
            "why_2": "",
            "why_3": "",
            "why_4": "",
            "why_5": "",
            "root_cause": "",
        }
    if tool_type == "Diagram Ishikawy":
        return {
            "problem": "",
            "categories": {
                "Man": "",
                "Machine": "",
                "Method": "",
                "Material": "",
                "Measurement": "",
                "Environment": "",
            },
        }
    if tool_type == "Raport A3":
        return {
            "background": "",
            "current_state": "",
            "target_state": "",
            "root_cause": "",
            "countermeasures": "",
            "follow_up": "",
        }
    if tool_type == "Raport 8D":
        return {
            "D1": "",
            "D2": "",
            "D3": "",
            "D4": "",
            "D5": "",
            "D6": "",
            "D7": "",
            "D8": "",
        }
    return {}


def _load_template(tool_type: str, template_json: str | None) -> dict[str, Any]:
    if template_json:
        try:
            parsed = json.loads(template_json)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
    return _default_template(tool_type)


def _format_project_label(row: dict[str, Any]) -> str:
    project_name = row.get("project_name") or row.get("project_id") or "—"
    work_center = row.get("work_center") or "—"
    return f"{project_name} / {work_center}"


def _render_template_fields(tool_type: str, template: dict[str, Any]) -> dict[str, Any]:
    if tool_type == "5WHY":
        template["problem_description"] = st.text_area(
            "Opis problemu", value=template.get("problem_description", "")
        )
        for idx in range(1, 6):
            key = f"why_{idx}"
            template[key] = st.text_area(f"Why {idx}", value=template.get(key, ""))
        template["root_cause"] = st.text_area(
            "Przyczyna źródłowa", value=template.get("root_cause", "")
        )
        return template

    if tool_type == "Diagram Ishikawy":
        template["problem"] = st.text_area(
            "Problem", value=template.get("problem", "")
        )
        categories = template.get("categories")
        if not isinstance(categories, dict):
            categories = _default_template(tool_type)["categories"]
        updated_categories: dict[str, str] = {}
        for category in [
            "Man",
            "Machine",
            "Method",
            "Material",
            "Measurement",
            "Environment",
        ]:
            updated_categories[category] = st.text_area(
                f"{category}", value=categories.get(category, "")
            )
        template["categories"] = updated_categories
        return template

    if tool_type == "Raport A3":
        template["background"] = st.text_area(
            "Tło", value=template.get("background", "")
        )
        template["current_state"] = st.text_area(
            "Stan obecny", value=template.get("current_state", "")
        )
        template["target_state"] = st.text_area(
            "Stan docelowy", value=template.get("target_state", "")
        )
        template["root_cause"] = st.text_area(
            "Przyczyna źródłowa", value=template.get("root_cause", "")
        )
        template["countermeasures"] = st.text_area(
            "Kontr-środki", value=template.get("countermeasures", "")
        )
        template["follow_up"] = st.text_area(
            "Follow-up", value=template.get("follow_up", "")
        )
        return template

    if tool_type == "Raport 8D":
        for key in ["D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8"]:
            template[key] = st.text_area(key, value=template.get(key, ""))
        return template

    return template


def _render_analysis_actions(
    analysis_repo: AnalysisRepository,
    action_repo: ActionRepository,
    analysis_id: str,
    analysis_project_id: str | None,
    analysis_champion_id: str | None,
    analysis_area: str | None,
    champions: list[dict[str, Any]],
) -> None:
    actions = analysis_repo.list_analysis_actions(analysis_id)
    champion_names = {
        c["id"]: c.get("display_name") or c.get("name") or c["id"] for c in champions
    }

    def _render_group(action_type: str, title: str) -> None:
        st.markdown(f"#### {title}")
        group_actions = [a for a in actions if a.get("action_type") == action_type]

        with st.form(f"analysis_action_form_{analysis_id}_{action_type}"):
            col1, col2 = st.columns([2, 1])
            action_title = col1.text_input("Tytuł", key=f"{analysis_id}_{action_type}_title")
            due_date = col2.date_input(
                "Termin", value=date.today(), key=f"{analysis_id}_{action_type}_due"
            )
            description = st.text_area(
                "Opis", key=f"{analysis_id}_{action_type}_desc"
            )
            owner_options = ["(brak)"] + [c["id"] for c in champions]
            owner_selected = st.selectbox(
                "Owner",
                owner_options,
                format_func=lambda cid: "(brak)"
                if cid == "(brak)"
                else champion_names.get(cid, cid),
                key=f"{analysis_id}_{action_type}_owner",
            )
            submitted = st.form_submit_button("Dodaj działanie")

        if submitted:
            if not action_title.strip():
                st.error("Tytuł działania jest wymagany.")
            else:
                analysis_repo.create_analysis_action(
                    analysis_id,
                    {
                        "action_type": action_type,
                        "title": action_title.strip(),
                        "description": description.strip() or None,
                        "due_date": due_date.isoformat() if due_date else None,
                        "owner_champion_id": None if owner_selected == "(brak)" else owner_selected,
                    },
                )
                st.success("Działanie dodane do analizy.")
                st.rerun()

        if not group_actions:
            st.caption("Brak działań w tej grupie.")
            return

        for analysis_action in group_actions:
            added_action_id = analysis_action.get("added_action_id")
            owner_name = champion_names.get(
                analysis_action.get("owner_champion_id"), "—"
            )
            col1, col2, col3, col4, col5 = st.columns([2.5, 3, 1.2, 1.5, 1.2])
            col1.write(analysis_action.get("title") or "—")
            col2.write(analysis_action.get("description") or "—")
            col3.write(analysis_action.get("due_date") or "—")
            col4.write(owner_name)
            if added_action_id:
                col5.button("Dodano", disabled=True, key=f"added_{analysis_action['id']}")
                continue

            if col5.button("Dodaj do Akcji", key=f"add_{analysis_action['id']}"):
                action_id = action_repo.create_action(
                    {
                        "project_id": analysis_project_id,
                        "analysis_id": analysis_id,
                        "title": analysis_action.get("title"),
                        "description": analysis_action.get("description"),
                        "owner_champion_id": analysis_action.get("owner_champion_id")
                        or analysis_champion_id,
                        "due_date": analysis_action.get("due_date"),
                        "source": "analysis",
                        "area": analysis_area,
                    }
                )
                analysis_repo.mark_analysis_action_added(analysis_action["id"], action_id)
                st.success("Dodano do modułu Akcje.")
                st.rerun()

    _render_group("corrective", "Działania korygujące")
    _render_group("preventive", "Działania zapobiegawcze")


def render(con: sqlite3.Connection) -> None:
    st.header("Analizy")

    analysis_repo = AnalysisRepository(con)
    action_repo = ActionRepository(con)
    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)

    projects = project_repo.list_projects()
    champions = champion_repo.list_champions()

    project_names = {
        p["id"]: (p.get("name") or p.get("project_name") or p["id"]) for p in projects
    }
    champion_names = {
        c["id"]: c.get("display_name") or c.get("name") or c["id"] for c in champions
    }

    analyses = analysis_repo.list_analyses()
    table_rows = []
    for analysis in analyses:
        table_rows.append(
            {
                "analysis_id": analysis.get("id"),
                "Projekt / WC": _format_project_label(analysis),
                "Champion": analysis.get("champion_name")
                or champion_names.get(analysis.get("champion_id"), "—"),
                "Typ narzędzia": analysis.get("tool_type") or "—",
                "Obszar": analysis.get("area") or "—",
                "Data utworzenia": analysis.get("created_at") or "—",
                "Data zamknięcia": analysis.get("closed_at") or "—",
                "Status": analysis.get("status") or "—",
            }
        )

    st.subheader("Lista analiz")
    st.dataframe(table_rows, use_container_width=True)

    st.subheader("Nowa analiza")
    if "show_new_analysis_form" not in st.session_state:
        st.session_state["show_new_analysis_form"] = False

    if st.button("Nowa analiza"):
        st.session_state["show_new_analysis_form"] = True

    if st.session_state.get("show_new_analysis_form"):
        with st.form("new_analysis_form"):
            project_options = ["(brak)"] + [p["id"] for p in projects]
            selected_project = st.selectbox(
                "Projekt",
                project_options,
                format_func=lambda pid: "(brak)"
                if pid == "(brak)"
                else project_names.get(pid, pid),
            )
            champion_options = ["(brak)"] + [c["id"] for c in champions]
            selected_champion = st.selectbox(
                "Champion",
                champion_options,
                format_func=lambda cid: "(brak)"
                if cid == "(brak)"
                else champion_names.get(cid, cid),
            )
            tool_type = st.selectbox("Typ narzędzia", TOOL_TYPES)
            area = st.selectbox("Obszar", AREA_OPTIONS)
            submitted = st.form_submit_button("Utwórz analizę")

        if submitted:
            analysis_repo.create_analysis(
                {
                    "project_id": None if selected_project == "(brak)" else selected_project,
                    "champion_id": None if selected_champion == "(brak)" else selected_champion,
                    "tool_type": tool_type,
                    "area": None if area == "(brak)" else area,
                    "template_json": _default_template(tool_type),
                }
            )
            st.session_state["show_new_analysis_form"] = False
            st.success("Analiza utworzona.")
            st.rerun()

    st.subheader("Szczegóły analizy")
    if not analyses:
        st.caption("Brak analiz do wyświetlenia.")
        return

    analysis_options = [analysis["id"] for analysis in analyses]
    selected_analysis_id = st.selectbox(
        "Wybierz analizę",
        analysis_options,
        format_func=lambda aid: next(
            (
                f"{_format_project_label(row)} · {row.get('tool_type')} · {aid[-6:]}"
                for row in analyses
                if row["id"] == aid
            ),
            aid,
        ),
    )

    selected_analysis = next(
        (analysis for analysis in analyses if analysis["id"] == selected_analysis_id), None
    )
    if not selected_analysis:
        return

    tool_type = selected_analysis.get("tool_type") or "5WHY"
    template = _load_template(tool_type, selected_analysis.get("template_json"))

    st.markdown(f"**Projekt / WC:** {_format_project_label(selected_analysis)}")
    st.markdown(
        f"**Champion:** {selected_analysis.get('champion_name') or champion_names.get(selected_analysis.get('champion_id'), '—')}"
    )
    st.markdown(f"**Obszar:** {selected_analysis.get('area') or '—'}")

    st.subheader("Szablon analizy")
    with st.form(f"analysis_template_form_{selected_analysis_id}"):
        updated_template = _render_template_fields(tool_type, dict(template))
        saved = st.form_submit_button("Zapisz analizę")

    if saved:
        analysis_repo.update_analysis(
            selected_analysis_id,
            {"template_json": updated_template},
        )
        st.success("Analiza zaktualizowana.")
        st.rerun()

    st.subheader("Działania wynikające z analizy")
    _render_analysis_actions(
        analysis_repo,
        action_repo,
        selected_analysis_id,
        selected_analysis.get("project_id"),
        selected_analysis.get("champion_id"),
        selected_analysis.get("area"),
        champions,
    )

    st.subheader("Status i narzędzia")
    current_status = selected_analysis.get("status") or "open"
    status = st.selectbox(
        "Status", ["open", "closed"], index=0 if current_status != "closed" else 1
    )
    current_area = selected_analysis.get("area") or "(brak)"
    area = st.selectbox(
        "Obszar",
        AREA_OPTIONS,
        index=AREA_OPTIONS.index(current_area) if current_area in AREA_OPTIONS else 0,
    )
    if st.button("Zapisz status"):
        analysis_repo.update_analysis(
            selected_analysis_id,
            {"status": status, "area": None if area == "(brak)" else area},
        )
        st.success("Status zaktualizowany.")
        st.rerun()

    st.subheader("Usuń analizę")
    confirm_delete = st.checkbox("Potwierdzam usunięcie analizy")
    if st.button("Usuń analizę", disabled=not confirm_delete):
        analysis_repo.delete_analysis(selected_analysis_id)
        st.success("Analiza usunięta.")
        st.rerun()

    st.subheader("Changelog")
    with st.expander("Changelog", expanded=False):
        changelog_entries = analysis_repo.list_changelog(limit=50, analysis_id=selected_analysis_id)
        if not changelog_entries:
            st.caption("Brak wpisów w changelogu.")
        for entry in changelog_entries:
            changes = json.loads(entry["changes_json"])
            st.markdown(f"**{entry['event_at']}** · {entry['event_type']}")
            st.caption(json.dumps(changes, ensure_ascii=False))
