from __future__ import annotations

# What changed:
# - Added project filter fallback to work centers for Production Explorer data loads.
# - Added empty-filter debug info and classification sanity checks.

from datetime import date, timedelta
import sqlite3
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    GlobalSettingsRepository,
    ProductionDataRepository,
    ProjectRepository,
)
from action_tracking.services.effectiveness import parse_date, parse_work_centers
from action_tracking.services.overlay_targets import (
    OVERLAY_TARGET_COLORS,
    OVERLAY_TARGET_LABELS,
    default_overlay_targets,
)
from action_tracking.services.kpi_windows import compute_project_kpi_windows
from action_tracking.services.production_outcome import (
    apply_weekend_filter,
    format_metric_value,
    load_daily_frames,
    metric_delta_label,
    scrap_delta_badge,
)
from action_tracking.services.workcenter_classifier import (
    KPI_COMPONENTS,
    SCRAP_COMPONENTS,
    classification_sanity_check,
    classify_workcenter,
    extract_injection_machines,
    filter_rows_by_areas,
)


def _weighted_or_mean(values: pd.Series, weights: pd.Series | None) -> float | None:
    if values.empty:
        return None
    if weights is None or weights.empty:
        mean_value = values.mean()
        return None if pd.isna(mean_value) else float(mean_value)
    weights = weights.fillna(0)
    valid_weights = weights.where(values.notna(), 0)
    if valid_weights.sum() > 0:
        weighted_values = values.fillna(0) * weights
        return float(weighted_values.sum() / valid_weights.sum())
    mean_value = values.mean()
    return None if pd.isna(mean_value) else float(mean_value)


def _weekly_bucket(df: pd.DataFrame, date_col: str = "metric_date") -> pd.Series:
    return df[date_col].dt.to_period("W-MON").apply(lambda period: period.start_time)


def _weekly_scrap_aggregation(scrap_df: pd.DataFrame) -> pd.DataFrame:
    if scrap_df.empty:
        return pd.DataFrame(columns=["metric_date", "scrap_qty_sum", "scrap_pln_sum"])
    temp = scrap_df.copy()
    temp["metric_date"] = _weekly_bucket(temp)
    return (
        temp.groupby("metric_date", as_index=False)
        .agg(
            scrap_qty_sum=("scrap_qty", "sum"),
            scrap_pln_sum=("scrap_cost_amount", "sum"),
        )
        .sort_values("metric_date")
    )


def _weekly_kpi_aggregation(kpi_df: pd.DataFrame) -> pd.DataFrame:
    if kpi_df.empty:
        return pd.DataFrame(columns=["metric_date", "oee_avg", "performance_avg"])
    temp = kpi_df.copy()
    temp["metric_date"] = _weekly_bucket(temp)
    return (
        temp.groupby("metric_date")
        .apply(
            lambda group: pd.Series(
                {
                    "oee_avg": _weighted_or_mean(
                        group["oee_pct"], group.get("worktime_min")
                    ),
                    "performance_avg": _weighted_or_mean(
                        group["performance_pct"], group.get("worktime_min")
                    ),
                }
            )
        )
        .reset_index()
        .sort_values("metric_date")
    )


def _line_chart_with_markers(
    data: pd.DataFrame,
    y_field: str,
    y_title: str,
    title: str,
    markers_df: pd.DataFrame,
    show_markers: bool,
    overlay_target: str,
) -> alt.Chart:
    base = (
        alt.Chart(data)
        .mark_line()
        .encode(
            x=alt.X("metric_date:T", title="Data"),
            y=alt.Y(f"{y_field}:Q", title=y_title),
        )
        .properties(title=title)
    )
    if not show_markers or markers_df.empty:
        return base
    filtered_markers = markers_df[markers_df["overlay_target"] == overlay_target]
    if filtered_markers.empty:
        return base
    marker_labels = list(OVERLAY_TARGET_COLORS.keys())
    marker_colors = [
        OVERLAY_TARGET_COLORS.get(label, "#9e9e9e") for label in marker_labels
    ]
    marker_layer = (
        alt.Chart(filtered_markers)
        .mark_rule(strokeDash=[6, 4])
        .encode(
            x=alt.X("closed_at:T"),
            color=alt.Color(
                "overlay_target:N",
                scale=alt.Scale(domain=marker_labels, range=marker_colors),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("action_title:N", title="Akcja"),
                alt.Tooltip("category:N", title="Kategoria"),
                alt.Tooltip("closed_at:T", title="Zamknięta"),
                alt.Tooltip("overlay_label:N", title="Wykres"),
            ],
        )
    )
    return base + marker_layer


def _resolve_overlay_targets(
    action_row: dict[str, Any],
    rules_repo: GlobalSettingsRepository,
) -> list[str]:
    rule = rules_repo.resolve_category_rule(action_row.get("category") or "")
    if rule and rule.get("overlay_targets_configured"):
        overlay_targets = list(rule.get("overlay_targets") or [])
    else:
        overlay_targets = []
    if overlay_targets:
        return overlay_targets
    effect_model = rule.get("effectiveness_model") if rule else None
    return default_overlay_targets(effect_model)


def _get_query_param(key: str) -> str | None:
    params = st.query_params if hasattr(st, "query_params") else st.experimental_get_query_params()
    value = params.get(key)
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _format_window_label(window: dict[str, Any]) -> str:
    current_days = window.get("current_days") or 0
    baseline_days = window.get("baseline_days") or 0
    current_from = window.get("current_from")
    current_to = window.get("current_to")
    baseline_from = window.get("baseline_from")
    baseline_to = window.get("baseline_to")
    if not current_days or not baseline_days or not current_from or not baseline_from:
        return "Okno KPI: —"
    return (
        "Okno KPI: "
        f"Current {current_days} prod dni ({current_from}..{current_to}), "
        f"Baseline {baseline_days} prod dni ({baseline_from}..{baseline_to})"
    )


def _resolve_project_filters(project: dict[str, Any] | None) -> tuple[list[str], list[str]]:
    if not project:
        return [], []
    full_project_candidates = [
        str(project.get("id") or "").strip(),
        str(project.get("name") or "").strip(),
        str(project.get("project_code") or "").strip(),
    ]
    full_project_values = [
        value for value in full_project_candidates if value and value.lower() != "none"
    ]
    work_centers = parse_work_centers(
        project.get("work_center"),
        project.get("related_work_center"),
    )
    return list(dict.fromkeys(full_project_values)), work_centers


def _load_kpi_rows_with_fallback(
    production_repo: ProductionDataRepository,
    date_from: date,
    date_to: date,
    full_project: str | list[str] | None,
    work_centers: list[str] | None,
    workcenter_areas: set[str] | None,
) -> tuple[list[dict[str, Any]], bool]:
    rows = production_repo.list_kpi_daily(
        None,
        date_from,
        date_to,
        full_project=full_project,
        workcenter_areas=workcenter_areas,
    )
    if rows or not work_centers:
        return rows, False
    fallback_rows = production_repo.list_kpi_daily(
        work_centers,
        date_from,
        date_to,
        full_project=None,
        workcenter_areas=workcenter_areas,
    )
    return fallback_rows, True


def render(con: sqlite3.Connection) -> None:
    st.header("Production Explorer")
    production_repo = ProductionDataRepository(con)
    project_repo = ProjectRepository(con)
    action_repo = ActionRepository(con)
    rules_repo = GlobalSettingsRepository(con)

    projects = project_repo.list_projects(include_counts=True)
    project_names = {p["id"]: p.get("name") or p.get("id") for p in projects}
    projects_by_id = {p["id"]: p for p in projects}

    classification_sanity_check()

    today = date.today()
    default_from = today - timedelta(days=90)
    searchback_calendar_days = 180

    preselected_project_id = _get_query_param("project_id")
    if preselected_project_id:
        st.session_state["production_explorer_selected_project_id"] = preselected_project_id
    selected_project_state = st.session_state.get("production_explorer_selected_project_id")

    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns(
        [1.4, 2.4, 2.0, 1.4, 1.4]
    )
    project_options = ["Wszystkie"] + [p["id"] for p in projects]
    project_index = 0
    if selected_project_state in project_options:
        project_index = project_options.index(selected_project_state)
    selected_project = filter_col1.selectbox(
        "Projekt",
        project_options,
        index=project_index,
        format_func=lambda pid: "Wszystkie" if pid == "Wszystkie" else project_names.get(pid, pid),
    )
    if selected_project == "Wszystkie":
        st.session_state.pop("production_explorer_selected_project_id", None)
        full_project_filter = None
        project_work_centers: list[str] | None = None
        selected_project_id = None
    else:
        st.session_state["production_explorer_selected_project_id"] = selected_project
        project = projects_by_id.get(selected_project)
        project_full_projects, project_work_centers = _resolve_project_filters(project)
        full_project_filter = project_full_projects or [selected_project]
        selected_project_id = selected_project

    scrap_component = filter_col2.selectbox(
        "Scrap – komponent",
        list(SCRAP_COMPONENTS.keys()),
        index=0,
    )
    kpi_component = filter_col3.selectbox(
        "OEE/Performance – obszar",
        list(KPI_COMPONENTS.keys()),
        index=0,
    )
    granularity = filter_col4.radio(
        "Granularność",
        ["Dziennie", "Tygodniowo"],
        index=0,
        horizontal=True,
    )
    currency = filter_col5.selectbox(
        "Waluta",
        ["PLN", "All currencies"],
        index=0,
    )

    date_col1, date_col2, machine_col = st.columns([1.4, 1.4, 1.4])
    selected_from = date_col1.date_input("Data od", value=default_from)
    selected_to = date_col2.date_input("Data do", value=today)
    selected_machine = None
    kpi_machine_filter: list[str] | None = None
    if kpi_component == "Wtrysk (Mxx)":
        injection_rows, used_wc_fallback = _load_kpi_rows_with_fallback(
            production_repo,
            selected_from,
            selected_to,
            full_project_filter,
            project_work_centers,
            KPI_COMPONENTS.get("Wtrysk (Mxx)"),
        )
        machine_options = ["Wszystkie"] + extract_injection_machines(injection_rows)
        selected_machine = machine_col.selectbox(
            "Wtrysk – maszyna",
            machine_options,
            index=0,
        )
        if selected_machine and selected_machine != "Wszystkie":
            kpi_machine_filter = [selected_machine]
    else:
        machine_col.caption(" ")

    weekend_col1, weekend_col2 = st.columns(2)
    remove_saturdays = weekend_col1.checkbox("Usuń soboty", value=False)
    remove_sundays = weekend_col2.checkbox("Usuń niedziele", value=False)

    if selected_from > selected_to:
        st.error("Zakres dat jest nieprawidłowy (Data od > Data do).")
        return

    scrap_area_filter = SCRAP_COMPONENTS.get(scrap_component)
    kpi_area_filter = KPI_COMPONENTS.get(kpi_component)
    currency_filter = "PLN" if currency == "PLN" else None
    scrap_daily, kpi_daily, _ = load_daily_frames(
        production_repo,
        None,
        kpi_machine_filter,
        selected_from,
        selected_to,
        currency=currency_filter,
        full_project=full_project_filter,
        scrap_areas=scrap_area_filter,
        kpi_areas=kpi_area_filter,
    )
    used_wc_fallback = False
    if (
        selected_project != "Wszystkie"
        and scrap_daily.empty
        and kpi_daily.empty
        and project_work_centers
    ):
        scrap_daily, kpi_daily, _ = load_daily_frames(
            production_repo,
            project_work_centers,
            kpi_machine_filter or project_work_centers,
            selected_from,
            selected_to,
            currency=currency_filter,
            full_project=None,
            scrap_areas=scrap_area_filter,
            kpi_areas=kpi_area_filter,
        )
        used_wc_fallback = True
    searchback_from = selected_to - timedelta(days=searchback_calendar_days)
    active_full_project = None if used_wc_fallback else full_project_filter
    active_work_centers = project_work_centers if used_wc_fallback else None
    kpi_scrap_rows = production_repo.list_scrap_daily(
        active_work_centers,
        searchback_from,
        selected_to,
        currency="PLN",
        full_project=active_full_project,
        workcenter_areas=scrap_area_filter,
    )
    kpi_window_rows = production_repo.list_kpi_daily(
        kpi_machine_filter or active_work_centers,
        searchback_from,
        selected_to,
        full_project=active_full_project,
        workcenter_areas=kpi_area_filter,
    )
    kpi_window = compute_project_kpi_windows(
        kpi_scrap_rows,
        kpi_window_rows,
        remove_saturdays,
        remove_sundays,
        searchback_calendar_days=searchback_calendar_days,
    )

    scrap_rows = scrap_daily.attrs.get("scrap_rows_filtered", [])
    kpi_rows = kpi_daily.attrs.get("kpi_rows", [])
    oee_scale = kpi_daily.attrs.get("oee_scale", "unknown")
    perf_scale = kpi_daily.attrs.get("perf_scale", "unknown")

    if scrap_daily.empty and kpi_daily.empty:
        pre_scrap_rows = production_repo.list_scrap_daily(
            active_work_centers,
            selected_from,
            selected_to,
            currency=currency_filter,
            full_project=active_full_project,
            workcenter_areas=None,
        )
        pre_kpi_rows = production_repo.list_kpi_daily(
            kpi_machine_filter or active_work_centers,
            selected_from,
            selected_to,
            full_project=active_full_project,
            workcenter_areas=None,
        )
        st.info(
            "Brak danych produkcyjnych po filtrach. "
            f"Scrap rows: {len(scrap_rows)} / {len(pre_scrap_rows)} | "
            f"KPI rows: {len(kpi_rows)} / {len(pre_kpi_rows)}."
        )
        if used_wc_fallback:
            st.caption("Użyto fallbacku na work center (pełny projekt bez dopasowania).")
        return

    if currency == "PLN":
        non_pln = scrap_daily.attrs.get("non_pln_currencies", [])
        if non_pln:
            st.info(
                "Dostępne są dane scrap w innych walutach (pominięto): "
                + ", ".join(sorted(non_pln))
            )

    scrap_df = pd.DataFrame(scrap_rows)
    if not scrap_df.empty:
        scrap_df["metric_date"] = pd.to_datetime(scrap_df["metric_date"], errors="coerce")
        scrap_df = scrap_df.dropna(subset=["metric_date"])

    kpi_df = pd.DataFrame(kpi_rows)
    if not kpi_df.empty:
        kpi_df["metric_date"] = pd.to_datetime(kpi_df["metric_date"], errors="coerce")
        kpi_df = kpi_df.dropna(subset=["metric_date"])

    if active_full_project or active_work_centers:
        totals_rows = production_repo.list_scrap_daily(
            active_work_centers,
            selected_from,
            selected_to,
            currency="PLN",
            full_project=active_full_project,
        )
        totals_rows = filter_rows_by_areas(totals_rows, SCRAP_COMPONENTS.get("TOTAL (all)"))
        totals_by_area: dict[str, dict[str, float]] = {}
        for row in totals_rows:
            area = classify_workcenter(row.get("work_center") or "").get("area") or "other"
            payload = totals_by_area.setdefault(area, {"scrap_qty": 0.0, "scrap_pln": 0.0})
            payload["scrap_qty"] += float(row.get("scrap_qty") or 0)
            payload["scrap_pln"] += float(row.get("scrap_cost_amount") or 0)

        if totals_by_area:
            totals_summary = [
                {
                    "Obszar": area,
                    "Scrap qty": metrics["scrap_qty"],
                    "Scrap PLN": metrics["scrap_pln"],
                }
                for area, metrics in sorted(totals_by_area.items())
            ]
            with st.expander("Debug: Scrap per area (PLN)", expanded=False):
                st.dataframe(totals_summary, use_container_width=True)

    daily_view = granularity == "Dziennie"
    if not daily_view:
        st.caption("Filtry weekendów dotyczą tylko widoku dziennego.")

    if daily_view:
        scrap_daily = apply_weekend_filter(scrap_daily, remove_saturdays, remove_sundays)
        kpi_daily = apply_weekend_filter(kpi_daily, remove_saturdays, remove_sundays)
    else:
        scrap_daily = _weekly_scrap_aggregation(scrap_df)
        kpi_daily = _weekly_kpi_aggregation(kpi_df)

    if daily_view and (scrap_daily.empty and kpi_daily.empty):
        st.info("Po odfiltrowaniu weekendów brak danych w zakresie.")
        return

    if kpi_window.get("status") == "insufficient_data":
        st.warning("Za mało dni produkcyjnych (<8), aby policzyć okna KPI.")

    window = kpi_window.get("window", {})
    metrics = kpi_window.get("metrics", {})
    scrap_metrics = metrics.get("scrap_qty", {})
    scrap_pln_metrics = metrics.get("scrap_pln", {})
    oee_metrics = metrics.get("oee", {})
    perf_metrics = metrics.get("performance", {})

    baseline_scrap_qty = scrap_metrics.get("baseline")
    current_scrap_qty = scrap_metrics.get("current")
    baseline_scrap_pln = scrap_pln_metrics.get("baseline")
    current_scrap_pln = scrap_pln_metrics.get("current")
    baseline_oee = oee_metrics.get("baseline")
    current_oee = oee_metrics.get("current")
    baseline_perf = perf_metrics.get("baseline")
    current_perf = perf_metrics.get("current")

    kpi_cols = st.columns(4)
    kpi_cols[0].metric("Śr. scrap qty/dzień", format_metric_value(current_scrap_qty, "{:.2f}"))
    kpi_cols[0].markdown(
        scrap_delta_badge(baseline_scrap_qty, current_scrap_qty, "{:.2f}"),
        unsafe_allow_html=True,
    )
    kpi_cols[0].caption(f"Baseline: {format_metric_value(baseline_scrap_qty, '{:.2f}')}")

    kpi_cols[1].metric("Śr. scrap PLN/dzień", format_metric_value(current_scrap_pln, "{:.2f}"))
    kpi_cols[1].markdown(
        scrap_delta_badge(baseline_scrap_pln, current_scrap_pln, "{:.2f}"),
        unsafe_allow_html=True,
    )
    kpi_cols[1].caption(f"Baseline: {format_metric_value(baseline_scrap_pln, '{:.2f}')}")

    kpi_cols[2].metric(
        "Śr. OEE%",
        format_metric_value(current_oee, "{:.1f}%"),
        delta=metric_delta_label(baseline_oee, current_oee, "{:+.1f} pp"),
    )
    kpi_cols[2].caption(f"Baseline: {format_metric_value(baseline_oee, '{:.1f}%')}")

    kpi_cols[3].metric(
        "Śr. Performance%",
        format_metric_value(current_perf, "{:.1f}%"),
        delta=metric_delta_label(baseline_perf, current_perf, "{:+.1f} pp"),
    )
    kpi_cols[3].caption(f"Baseline: {format_metric_value(baseline_perf, '{:.1f}%')}")

    st.caption("Dla scrap spadek = poprawa.")
    st.caption(_format_window_label(window))
    if oee_scale != "unknown" or perf_scale != "unknown":
        st.caption("KPI zapisane w bazie jako procenty (0-100).")

    markers_df = pd.DataFrame()
    if selected_project_id:
        markers: list[dict[str, Any]] = []
        actions = action_repo.list_actions(
            status="done",
            project_id=selected_project_id,
            is_draft=False,
        )
        for action in actions:
            closed_date = parse_date(action.get("closed_at"))
            if not closed_date:
                continue
            if not (selected_from <= closed_date <= selected_to):
                continue
            overlay_targets = _resolve_overlay_targets(action, rules_repo)
            for overlay_target in overlay_targets:
                markers.append(
                    {
                        "closed_at": pd.to_datetime(closed_date),
                        "action_title": action.get("title") or "—",
                        "category": action.get("category") or "—",
                        "overlay_target": overlay_target,
                        "overlay_label": OVERLAY_TARGET_LABELS.get(
                            overlay_target, overlay_target
                        ),
                    }
                )
        markers_df = pd.DataFrame(markers)
        if markers_df.empty:
            st.caption("Brak zamkniętych akcji w zakresie.")
    else:
        st.caption("Markery akcji są dostępne po wyborze projektu.")

    markers_available = not markers_df.empty
    chart_cols = st.columns(2)
    if not scrap_daily.empty:
        show_scrap_qty_markers = chart_cols[0].checkbox(
            "Pokaż markery akcji",
            value=markers_available,
            disabled=not markers_available,
            key="prod_explorer_markers_scrap_qty",
        )
        chart_cols[0].altair_chart(
            _line_chart_with_markers(
                scrap_daily,
                "scrap_qty_sum",
                "Scrap qty",
                "Scrap qty",
                markers_df,
                show_scrap_qty_markers,
                "SCRAP_QTY",
            ),
            use_container_width=True,
        )
        if currency == "All currencies":
            chart_cols[1].info("Koszt scrap: wybierz PLN, aby zobaczyć wykres.")
        else:
            show_scrap_cost_markers = chart_cols[1].checkbox(
                "Pokaż markery akcji",
                value=markers_available,
                disabled=not markers_available,
                key="prod_explorer_markers_scrap_cost",
            )
            chart_cols[1].altair_chart(
                _line_chart_with_markers(
                    scrap_daily,
                    "scrap_pln_sum",
                    "Scrap PLN",
                    "Scrap PLN",
                    markers_df,
                    show_scrap_cost_markers,
                    "SCRAP_COST",
                ),
                use_container_width=True,
            )
    else:
        st.info("Brak danych scrap w wybranym zakresie.")

    chart_cols = st.columns(2)
    if not kpi_daily.empty:
        show_oee_markers = chart_cols[0].checkbox(
            "Pokaż markery akcji",
            value=markers_available,
            disabled=not markers_available,
            key="prod_explorer_markers_oee",
        )
        chart_cols[0].altair_chart(
            _line_chart_with_markers(
                kpi_daily,
                "oee_avg",
                "OEE (%)",
                "OEE",
                markers_df,
                show_oee_markers,
                "OEE",
            ),
            use_container_width=True,
        )
        show_perf_markers = chart_cols[1].checkbox(
            "Pokaż markery akcji",
            value=markers_available,
            disabled=not markers_available,
            key="prod_explorer_markers_perf",
        )
        chart_cols[1].altair_chart(
            _line_chart_with_markers(
                kpi_daily,
                "performance_avg",
                "Performance (%)",
                "Performance",
                markers_df,
                show_perf_markers,
                "PERFORMANCE",
            ),
            use_container_width=True,
        )
    else:
        st.info("Brak danych KPI w wybranym zakresie.")

    if currency == "All currencies":
        st.subheader("Scrap cost (wszystkie waluty)")
        if scrap_df.empty:
            st.caption("Brak danych scrap dla wybranego zakresu.")
        else:
            scrap_currency_view = (
                scrap_df.groupby(["metric_date", "scrap_cost_currency"], as_index=False)
                .agg(scrap_cost_amount=("scrap_cost_amount", "sum"))
                .sort_values(["metric_date", "scrap_cost_currency"])
            )
            scrap_currency_view["metric_date"] = scrap_currency_view["metric_date"].dt.date
            st.dataframe(scrap_currency_view, use_container_width=True)

    st.caption(
        "Markery akcji są ustawiane na dacie zamknięcia i widoczne po wyborze projektu "
        "(mapowanie wykresów definiuje Global Settings)."
    )
