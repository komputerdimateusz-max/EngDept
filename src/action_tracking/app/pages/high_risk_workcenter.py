from __future__ import annotations

from datetime import date, timedelta
import sqlite3
from typing import Any
from urllib.parse import quote
from uuid import uuid4

import streamlit as st

from action_tracking.data.repositories import (
    ChampionRepository,
    ProductionDataRepository,
    ProjectRepository,
)
from action_tracking.services.effectiveness import parse_work_centers
from action_tracking.services.kpi_windows import compute_project_kpi_windows
from action_tracking.services.normalize import normalize_key
from action_tracking.services.production_outcome import format_metric_value

IMPORTANCE_ORDER = {
    "High Runner": 0,
    "Mid Runner": 1,
    "Low Runner": 2,
    "Spare parts": 3,
}


def _build_work_center_map(work_centers: list[str]) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for work_center in work_centers:
        normalized = normalize_key(work_center)
        if not normalized:
            continue
        mapping.setdefault(normalized, [])
        if work_center not in mapping[normalized]:
            mapping[normalized].append(work_center)
    return mapping


def _resolve_project_work_centers(
    project: dict[str, Any],
    work_center_map: dict[str, list[str]],
    include_related: bool,
) -> list[str]:
    centers = parse_work_centers(
        project.get("work_center"),
        project.get("related_work_center") if include_related else None,
    )
    resolved: list[str] = []
    for center in centers:
        normalized = normalize_key(center)
        if not normalized:
            continue
        if normalized in work_center_map:
            for candidate in work_center_map[normalized]:
                if candidate not in resolved:
                    resolved.append(candidate)
    return resolved


def _format_scrap_cell(metric: dict[str, Any]) -> tuple[str, float | None]:
    current = metric.get("current")
    current_label = format_metric_value(current, "{:.2f}")
    delta_abs = metric.get("delta_abs")
    delta_pct = metric.get("delta_rel_pct")
    if delta_abs is None:
        return current_label, None
    pct_label = "n/a" if delta_pct is None else f"{delta_pct:+.1f}%"
    return f"{current_label} ({delta_abs:+.2f}, {pct_label})", delta_pct


def _format_kpi_cell(metric: dict[str, Any]) -> tuple[str, float | None]:
    current = metric.get("current")
    current_label = format_metric_value(current, "{:.1f}%")
    delta_pp = metric.get("delta_pp")
    if delta_pp is None:
        return current_label, None
    return f"{current_label} ({delta_pp:+.1f} pp)", delta_pp


def _format_window_label(window: dict[str, Any], status: str) -> str:
    if status != "ok":
        return "Okno KPI: — (insufficient data)"
    current_days = window.get("current_days") or 0
    baseline_days = window.get("baseline_days") or 0
    current_from = window.get("current_from")
    current_to = window.get("current_to")
    baseline_from = window.get("baseline_from")
    baseline_to = window.get("baseline_to")
    if not current_days or not baseline_days or not current_from or not baseline_from:
        return "Okno KPI: —"
    return (
        f"Okno KPI: Current {current_days} dni ({current_from}..{current_to}), "
        f"Baseline {baseline_days} dni ({baseline_from}..{baseline_to})"
    )


def render(con: sqlite3.Connection) -> None:
    st.header("High Risk project")

    production_repo = ProductionDataRepository(con)
    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)

    projects = project_repo.list_projects(include_counts=True)
    champions = champion_repo.list_champions()
    champion_names = {c["id"]: c["display_name"] for c in champions}

    wc_lists = production_repo.list_distinct_work_centers()
    all_work_centers = sorted(
        set(wc_lists.get("scrap_work_centers", []) + wc_lists.get("kpi_work_centers", []))
    )
    work_center_map = _build_work_center_map(all_work_centers)

    today = date.today()
    default_from = today - timedelta(days=90)
    searchback_calendar_days = 180

    filter_col1, filter_col2, filter_col3 = st.columns([1.2, 1.2, 1.6])
    selected_from = filter_col1.date_input("Data od", value=default_from)
    selected_to = filter_col2.date_input("Data do", value=today)
    include_related = filter_col3.checkbox(
        "Uwzględnij powiązane Work Center",
        value=True,
    )

    weekend_col1, weekend_col2 = st.columns(2)
    remove_saturdays = weekend_col1.checkbox("Usuń soboty", value=False)
    remove_sundays = weekend_col2.checkbox("Usuń niedziele", value=False)

    importance_options = ["High Runner", "Mid Runner", "Low Runner", "Spare parts"]
    default_importance = [option for option in importance_options if option != "Spare parts"]
    filter_col4, filter_col5 = st.columns([1.2, 1.6])
    selected_importance = filter_col4.multiselect(
        "Importance",
        importance_options,
        default=default_importance,
    )

    sort_modes = [
        "Bad trend",
        "Highest scrap PLN",
        "Lowest OEE",
        "Lowest Performance",
    ]
    sort_mode = filter_col5.selectbox("Tryb / sortowanie", sort_modes, index=0)

    threshold_col1, threshold_col2, threshold_col3 = st.columns(3)
    oee_threshold = threshold_col1.slider("OEE spadek (pp)", min_value=1, max_value=10, value=3)
    perf_threshold = threshold_col2.slider("Performance spadek (pp)", min_value=1, max_value=10, value=3)
    scrap_threshold = threshold_col3.slider("Scrap wzrost (%)", min_value=1, max_value=50, value=10)

    if selected_from > selected_to:
        st.error("Zakres dat jest nieprawidłowy (Data od > Data do).")
        return

    if not all_work_centers:
        st.info("Brak danych produkcyjnych do analizy.")
        return

    rows: list[dict[str, Any]] = []
    no_wc_count = 0
    searchback_from = selected_to - timedelta(days=searchback_calendar_days)

    for project in projects:
        importance = project.get("importance") or "Mid Runner"
        if selected_importance and importance not in selected_importance:
            continue

        work_centers = _resolve_project_work_centers(project, work_center_map, include_related)
        if not work_centers:
            no_wc_count += 1
            continue

        scrap_rows = production_repo.list_scrap_daily(
            work_centers,
            searchback_from,
            selected_to,
            currency="PLN",
        )
        kpi_rows = production_repo.list_kpi_daily(work_centers, searchback_from, selected_to)
        kpi_window = compute_project_kpi_windows(
            scrap_rows,
            kpi_rows,
            remove_saturdays,
            remove_sundays,
            searchback_calendar_days=searchback_calendar_days,
        )
        status = kpi_window.get("status", "insufficient_data")
        window = kpi_window.get("window", {})
        metrics = kpi_window.get("metrics", {})
        scrap_metrics = metrics.get("scrap_qty", {})
        scrap_pln_metrics = metrics.get("scrap_pln", {})
        oee_metrics = metrics.get("oee", {})
        perf_metrics = metrics.get("performance", {})

        scrap_label, scrap_delta_pct = _format_scrap_cell(scrap_metrics)
        scrap_pln_label, scrap_pln_delta_pct = _format_scrap_cell(scrap_pln_metrics)
        oee_label, oee_delta_pp = _format_kpi_cell(oee_metrics)
        perf_label, perf_delta_pp = _format_kpi_cell(perf_metrics)

        risk_flags: list[str] = []
        if oee_delta_pp is not None and oee_delta_pp < -float(oee_threshold):
            risk_flags.append("OEE↓")
        if perf_delta_pp is not None and perf_delta_pp < -float(perf_threshold):
            risk_flags.append("Perf↓")
        scrap_risk = False
        if scrap_delta_pct is not None:
            scrap_risk = scrap_delta_pct > float(scrap_threshold)
        elif scrap_pln_delta_pct is not None:
            scrap_risk = scrap_pln_delta_pct > float(scrap_threshold)
        if scrap_risk:
            risk_flags.append("Scrap↑")

        if sort_mode == "Bad trend" and not risk_flags:
            continue

        owner_id = project.get("owner_champion_id")
        owner_name = champion_names.get(owner_id) if owner_id else "—"

        rows.append(
            {
                "project_id": project.get("id"),
                "project_name": project.get("name") or project.get("id"),
                "importance": importance,
                "importance_order": IMPORTANCE_ORDER.get(importance, 99),
                "owner_champion_id": owner_id,
                "owner_name": owner_name or "—",
                "scrap_label": scrap_label,
                "scrap_pln_label": scrap_pln_label,
                "oee_label": oee_label,
                "perf_label": perf_label,
                "window_label": _format_window_label(window, status),
                "risk_flags": ", ".join(risk_flags),
                "risk_count": len(risk_flags),
                "scrap_delta_pct": scrap_delta_pct,
                "oee_delta_pp": oee_delta_pp,
                "perf_delta_pp": perf_delta_pp,
                "current_scrap_pln": scrap_pln_metrics.get("current"),
                "current_oee": oee_metrics.get("current"),
                "current_perf": perf_metrics.get("current"),
                "work_centers": work_centers,
            }
        )

    if no_wc_count:
        st.caption(f"Pominięto projekty bez Work Center: {no_wc_count}.")

    if not rows:
        if sort_mode == "Bad trend":
            st.info("Brak projektów spełniających kryteria ryzyka.")
        else:
            st.info("Brak projektów spełniających kryteria filtrowania.")
        return

    if sort_mode == "Highest scrap PLN":
        rows = sorted(
            rows,
            key=lambda r: (
                r["current_scrap_pln"] is None,
                -(r["current_scrap_pln"] or 0),
            ),
        )
    elif sort_mode == "Lowest OEE":
        rows = sorted(
            rows,
            key=lambda r: (
                r["current_oee"] is None,
                r["current_oee"] if r["current_oee"] is not None else float("inf"),
            ),
        )
    elif sort_mode == "Lowest Performance":
        rows = sorted(
            rows,
            key=lambda r: (
                r["current_perf"] is None,
                r["current_perf"] if r["current_perf"] is not None else float("inf"),
            ),
        )
    else:
        rows = sorted(
            rows,
            key=lambda r: (
                -r["risk_count"],
                r["importance_order"],
                r["project_name"] or "",
            ),
        )

    st.caption(
        "Definicja trendu: current = ostatnie dostępne dni produkcyjne (14/7/4), "
        "baseline = dni bezpośrednio wcześniej (maks. 90 dni). Okno oparte o dni z "
        "produkcją po filtrach weekendowych."
    )

    header = st.columns([2.6, 1.2, 1.6, 1.6, 1.6, 1.4, 1.6, 1.2, 1.0])
    header[0].markdown("**Projekt**")
    header[1].markdown("**Importance**")
    header[2].markdown("**Champion**")
    header[3].markdown("**Scrap avg**")
    header[4].markdown("**Scrap PLN avg**")
    header[5].markdown("**OEE avg**")
    header[6].markdown("**Performance avg**")
    header[7].markdown("**Ryzyko**")
    header[8].markdown("**Akcja**")

    for row in rows:
        project_id = row["project_id"]
        project_name = row["project_name"]
        project_link = (
            f"?page={quote('Production Explorer')}&project_id={quote(str(project_id))}"
        )
        cols = st.columns([2.6, 1.2, 1.6, 1.6, 1.6, 1.4, 1.6, 1.2, 1.0])
        cols[0].markdown(f"[{project_name}]({project_link})")
        cols[0].caption(row["window_label"])
        cols[1].markdown(row["importance"])
        cols[2].markdown(row["owner_name"])
        cols[3].markdown(row["scrap_label"])
        cols[4].markdown(row["scrap_pln_label"])
        cols[5].markdown(row["oee_label"])
        cols[6].markdown(row["perf_label"])
        cols[7].markdown(row["risk_flags"])

        if cols[8].button("Dodaj akcję", key=f"add_action_{project_id}"):
            nav_nonce = str(uuid4())
            st.session_state["nav_to_page"] = "Akcje"
            st.session_state["nav_action_prefill"] = {
                "project_id": project_id,
                "work_centers": row["work_centers"],
                "owner_champion_id": row["owner_champion_id"],
                "nonce": nav_nonce,
            }
            st.session_state["nav_nonce"] = nav_nonce
            st.rerun()
