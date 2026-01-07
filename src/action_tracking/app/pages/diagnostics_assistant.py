from __future__ import annotations

import os
import sqlite3
from datetime import date
from typing import Any
from uuid import uuid4

import streamlit as st

from action_tracking.data.repositories import ProductionDataRepository, ProjectRepository
from action_tracking.services import diagnostics_assistant


AREA_OPTIONS = ["Montaż", "Wtrysk", "Metalizacja", "Podgrupa", "Inne"]
INJECTION_DEFECTS = [
    "Short shot",
    "Sink mark",
    "Flash",
    "Weld line",
    "Burn mark",
    "Warpage",
    "Voids",
    "Black specks",
    "Silver streaks",
    "Inne",
]
ASSEMBLY_DEFECTS = [
    "Misfit",
    "Gap",
    "Rattle/Noise",
    "Scratch",
    "Wrong part",
    "Clip broken",
    "Loose fit",
    "Inne",
]


@st.cache_data(ttl=1800)
def _cached_tavily_search(
    context_hash: str,
    queries: tuple[str, ...],
    allowlist: tuple[str, ...],
    api_key: str,
) -> list[dict[str, Any]]:
    sources = diagnostics_assistant.tavily_search_from_queries(
        list(queries),
        list(allowlist),
        api_key,
        max_results=6,
    )
    return diagnostics_assistant.serialize_sources(sources)


@st.cache_data(ttl=900, hash_funcs={sqlite3.Connection: lambda _: "sqlite"})
def _cached_internal_retrieval(
    con: sqlite3.Connection,
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    hits = diagnostics_assistant.internal_retrieval(con, context, limit=6)
    return diagnostics_assistant.serialize_internal_hits(hits)


def _format_project_label(project: dict[str, Any]) -> str:
    name = project.get("name") or project.get("project_name") or project.get("id")
    project_type = project.get("type") or "custom"
    return f"{name} ({project_type})"


def _resolve_defect_options(area: str) -> list[str]:
    if area == "Wtrysk":
        return INJECTION_DEFECTS
    if area == "Montaż":
        return ASSEMBLY_DEFECTS
    return sorted(set(INJECTION_DEFECTS + ASSEMBLY_DEFECTS))


def _build_title(prefix: str, defect_type: str, area: str) -> str:
    parts = [prefix]
    if defect_type:
        parts.append(defect_type)
    if area:
        parts.append(f"({area})")
    return " ".join(parts).strip()


def _summary_from_answer(answer: dict[str, Any]) -> str:
    summary = answer.get("summary_text") or ""
    causes = answer.get("probable_causes") or []
    if causes:
        summary = f"{summary}\nNajbardziej prawdopodobne przyczyny: {', '.join(causes[:3])}."
    return summary.strip()


def render(con: sqlite3.Connection) -> None:
    st.header("Asystent Diagnostyki")
    st.caption("Diagnoza defektów wtrysku i montażu z wiedzy wewnętrznej + źródeł zaufanych.")

    project_repo = ProjectRepository(con)
    production_repo = ProductionDataRepository(con)

    projects = project_repo.list_projects()

    project_options = ["(brak)"] + [p["id"] for p in projects]
    project_labels = {p["id"]: _format_project_label(p) for p in projects}

    work_centers = production_repo.list_work_centers()

    with st.form("diagnostics_context"):
        st.subheader("Kontekst")
        col1, col2, col3 = st.columns([1.2, 1.8, 1.6])
        area = col1.selectbox("Obszar", AREA_OPTIONS, index=0)
        selected_project = col2.selectbox(
            "Projekt",
            project_options,
            format_func=lambda pid: "(brak)" if pid == "(brak)" else project_labels.get(pid, pid),
        )
        selected_work_centers = col3.multiselect(
            "Work Center (opcjonalnie)",
            options=work_centers,
            default=[],
        )

        defect_options = _resolve_defect_options(area)
        defect_type = st.selectbox("Typ defektu", defect_options, index=0)
        symptom = st.text_area("Opis objawów", max_chars=800)

        since_enabled = st.checkbox("Podaj datę wystąpienia")
        since_when = st.date_input("Od kiedy", value=date.today(), disabled=not since_enabled)

        st.markdown("**Sygnały procesu**")
        flag_cols = st.columns(3)
        flags = []
        flag_options = [
            "rosnący scrap",
            "spada OEE",
            "spada Performance",
            "zmiana materiału",
            "zmiana formy/narzędzia",
            "nowy operator",
            "zmiana parametrów",
        ]
        for idx, label in enumerate(flag_options):
            if flag_cols[idx % 3].checkbox(label, key=f"flag_{label}"):
                flags.append(label)

        submitted = st.form_submit_button("Szukaj i odpowiedz")

    if not submitted:
        return

    selected_project_data = (
        next((p for p in projects if p["id"] == selected_project), None)
        if selected_project != "(brak)"
        else None
    )
    project_name = selected_project_data.get("name") if selected_project_data else ""
    owner_id = selected_project_data.get("owner_champion_id") if selected_project_data else None

    context_inputs = {
        "area": area,
        "defect_type": defect_type,
        "symptom": symptom,
        "project_name": project_name,
        "work_centers": selected_work_centers,
        "flags": flags,
        "since_when": since_when if since_enabled else None,
    }
    context = diagnostics_assistant.build_query_context(context_inputs)

    allowlist = diagnostics_assistant.load_trusted_domains()
    # Dev note: set TAVILY_API_KEY in the environment to enable web search.
    tavily_key = os.getenv("TAVILY_API_KEY", "")

    sources: list[dict[str, Any]] = []
    tavily_error = None
    if tavily_key:
        try:
            queries = diagnostics_assistant.build_search_queries(context)
            sources = _cached_tavily_search(
                context["context_hash"],
                tuple(queries),
                tuple(allowlist),
                tavily_key,
            )
        except Exception as exc:
            tavily_error = exc
    else:
        tavily_error = "Brak klucza TAVILY_API_KEY (tryb internal-only)."

    if tavily_error:
        st.warning(f"Tavily: {tavily_error}")

    internal_hits = _cached_internal_retrieval(con, context)
    answer = diagnostics_assistant.synthesize_answer(
        context,
        [diagnostics_assistant.Source(**row) for row in sources],
        [diagnostics_assistant.InternalHit(**row) for row in internal_hits],
    )

    st.subheader("Wynik (podsumowanie)")
    st.markdown(f"**Podsumowanie:** {answer.get('summary_text')}")

    if answer.get("facts"):
        st.markdown("**Fakty z źródeł:**")
        for fact in answer["facts"]:
            st.markdown(f"- {fact['text']} ([źródło]({fact['url']}))")
    else:
        st.caption("Brak faktów z zewnętrznych źródeł.")

    st.markdown("**Wnioski / sugestie:**")
    st.markdown("- Najbardziej prawdopodobne przyczyny:")
    for item in answer.get("probable_causes", []):
        st.markdown(f"  - {item}")

    st.markdown("- Co sprawdzić najpierw:")
    for item in answer.get("checks", []):
        st.markdown(f"  - {item}")

    st.markdown("- Rekomendowane akcje korygujące:")
    for item in answer.get("corrective_actions", []):
        st.markdown(f"  - {item}")

    st.markdown("- Akcje prewencyjne:")
    for item in answer.get("preventive_actions", []):
        st.markdown(f"  - {item}")

    if area == "Wtrysk" and answer.get("parameter_hints"):
        st.markdown("- Wskazówki parametrów:")
        for item in answer.get("parameter_hints", []):
            st.markdown(f"  - {item}")

    st.info(answer.get("safety_note"))

    st.subheader("Źródła (Tavily)")
    if sources:
        for source in sources:
            st.markdown(
                f"- [{source['title']}]({source['url']}) — {source.get('domain') or '—'}"
            )
    else:
        st.caption("Brak źródeł Tavily (tryb internal-only).")

    st.subheader("Wiedza wewnętrzna (nasze dane)")
    if internal_hits:
        for hit in internal_hits:
            badge = "Akcja" if hit["record_type"] == "action" else "Analiza"
            header = f"**{badge}** · {hit['title']}"
            st.markdown(header)
            st.caption(
                f"Projekt: {hit.get('project_name') or '—'} | Obszar: {hit.get('area') or '—'}"
            )
            if hit.get("effectiveness_classification"):
                st.caption(f"Efektywność: {hit['effectiveness_classification']}")
            st.markdown(hit.get("snippet") or "—")

            if hit["record_type"] == "action":
                if st.button("Otwórz w Akcjach", key=f"open_action_{hit['record_id']}"):
                    st.session_state["nav_to_page"] = "Akcje"
                    st.session_state["action_edit_select"] = hit["record_id"]
                    st.rerun()
            else:
                if st.button("Otwórz w Analizach", key=f"open_analysis_{hit['record_id']}"):
                    st.session_state["nav_to_page"] = "Analizy"
                    st.session_state["analysis_select_id"] = hit["record_id"]
                    st.rerun()
            st.divider()
    else:
        st.caption("Brak podobnych akcji lub analiz.")

    st.subheader("Następne kroki")
    summary_text = _summary_from_answer(answer)
    nav_nonce = str(uuid4())

    col_a, col_b, col_c, col_d = st.columns(4)
    if col_a.button("Utwórz analizę 5WHY"):
        st.session_state["nav_to_page"] = "Analizy"
        st.session_state["nav_analysis_prefill"] = {
            "tool_type": "5WHY",
            "area": area,
            "project_id": None if selected_project == "(brak)" else selected_project,
            "champion_id": owner_id,
            "summary": summary_text,
            "nonce": nav_nonce,
        }
        st.rerun()

    if col_b.button("Utwórz analizę Ishikawa"):
        st.session_state["nav_to_page"] = "Analizy"
        st.session_state["nav_analysis_prefill"] = {
            "tool_type": "Diagram Ishikawy",
            "area": area,
            "project_id": None if selected_project == "(brak)" else selected_project,
            "champion_id": owner_id,
            "summary": summary_text,
            "nonce": nav_nonce,
        }
        st.rerun()

    action_payload = {
        "project_id": None if selected_project == "(brak)" else selected_project,
        "owner_champion_id": owner_id,
        "area": area,
        "description": summary_text,
    }

    if col_c.button("Dodaj akcję korygującą"):
        st.session_state["nav_to_page"] = "Akcje"
        st.session_state["nav_action_prefill"] = {
            **action_payload,
            "title": _build_title("Korekta", defect_type, area),
            "nonce": nav_nonce,
        }
        st.rerun()

    if col_d.button("Dodaj akcję prewencyjną"):
        st.session_state["nav_to_page"] = "Akcje"
        st.session_state["nav_action_prefill"] = {
            **action_payload,
            "title": _build_title("Prewencja", defect_type, area),
            "nonce": nav_nonce,
        }
        st.rerun()

    with st.expander("Copy prompt / kontekst"):
        st.json(context)
