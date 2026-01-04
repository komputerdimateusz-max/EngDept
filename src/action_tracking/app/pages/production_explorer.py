from __future__ import annotations

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
from action_tracking.services.normalize import normalize_key
from action_tracking.services.overlay_targets import (
    OVERLAY_TARGET_COLORS,
    OVERLAY_TARGET_LABELS,
    default_overlay_targets,
)
from action_tracking.services.production_outcome import (
    apply_weekend_filter,
    compute_baseline_after_metrics,
    format_metric_value,
    load_daily_frames,
    metric_delta_label,
    scrap_delta_badge,
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


def _project_matches_work_center(project: dict[str, Any], target_key: str) -> bool:
    centers = parse_work_centers(
        project.get("work_center"),
        project.get("related_work_center"),
    )
    return any(normalize_key(center) == target_key for center in centers)


def render(con: sqlite3.Connection) -> None:
    st.header("Production Explorer")
    production_repo = ProductionDataRepository(con)
    project_repo = ProjectRepository(con)
    action_repo = ActionRepository(con)
    rules_repo = GlobalSettingsRepository(con)

    wc_lists = production_repo.list_distinct_work_centers()
    all_work_centers = sorted(
        set(wc_lists.get("scrap_work_centers", []) + wc_lists.get("kpi_work_centers", []))
    )

    today = date.today()
    default_from = today - timedelta(days=90)

    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns(
        [2.2, 1.4, 1.4, 1.2, 1.2]
    )
    selected_work_centers = filter_col1.multiselect(
        "Work Center",
        all_work_centers,
        default=all_work_centers,
    )
    selected_from = filter_col2.date_input("Data od", value=default_from)
    selected_to = filter_col3.date_input("Data do", value=today)
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

    weekend_col1, weekend_col2 = st.columns(2)
    remove_saturdays = weekend_col1.checkbox("Usuń soboty", value=False)
    remove_sundays = weekend_col2.checkbox("Usuń niedziele", value=False)

    if selected_from > selected_to:
        st.error("Zakres dat jest nieprawidłowy (Data od > Data do).")
        return

    if not selected_work_centers:
        st.info("Wybierz Work Center, aby zobaczyć dane.")
        return

    work_center_filter = selected_work_centers
    currency_filter = "PLN" if currency == "PLN" else None
    scrap_daily, kpi_daily, merged_daily = load_daily_frames(
        production_repo,
        work_center_filter,
        selected_from,
        selected_to,
        currency=currency_filter,
    )

    scrap_rows = scrap_daily.attrs.get("scrap_rows_filtered", [])
    kpi_rows = kpi_daily.attrs.get("kpi_rows", [])
    oee_scale = kpi_daily.attrs.get("oee_scale", "unknown")
    perf_scale = kpi_daily.attrs.get("perf_scale", "unknown")

    if scrap_daily.empty and kpi_daily.empty:
        st.info("Brak danych produkcyjnych dla wybranych filtrów.")
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

    daily_view = granularity == "Dziennie"
    if not daily_view:
        st.caption("Filtry weekendów dotyczą tylko widoku dziennego.")

    if daily_view:
        scrap_daily = apply_weekend_filter(scrap_daily, remove_saturdays, remove_sundays)
        kpi_daily = apply_weekend_filter(kpi_daily, remove_saturdays, remove_sundays)
        merged_daily = apply_weekend_filter(merged_daily, remove_saturdays, remove_sundays)
    else:
        scrap_daily = _weekly_scrap_aggregation(scrap_df)
        kpi_daily = _weekly_kpi_aggregation(kpi_df)

    if daily_view and (scrap_daily.empty and kpi_daily.empty):
        st.info("Po odfiltrowaniu weekendów brak danych w zakresie.")
        return

    baseline_daily = apply_weekend_filter(merged_daily, remove_saturdays, remove_sundays)
    if baseline_daily.empty:
        st.info("Po odfiltrowaniu weekendów brak danych w zakresie.")
        return

    metrics = compute_baseline_after_metrics(baseline_daily, selected_from, selected_to)
    if metrics.get("used_halves"):
        st.warning("Zakres < 28 dni: KPI obliczane jako połowy zakresu (baseline vs after).")

    baseline_scrap_qty = metrics.get("baseline_scrap_qty")
    after_scrap_qty = metrics.get("after_scrap_qty")
    baseline_scrap_pln = metrics.get("baseline_scrap_pln")
    after_scrap_pln = metrics.get("after_scrap_pln")
    baseline_oee = metrics.get("baseline_oee")
    after_oee = metrics.get("after_oee")
    baseline_perf = metrics.get("baseline_perf")
    after_perf = metrics.get("after_perf")

    kpi_cols = st.columns(4)
    kpi_cols[0].metric("Śr. scrap qty/dzień", format_metric_value(after_scrap_qty, "{:.2f}"))
    kpi_cols[0].markdown(
        scrap_delta_badge(baseline_scrap_qty, after_scrap_qty, "{:.2f}"),
        unsafe_allow_html=True,
    )
    kpi_cols[0].caption(f"Baseline: {format_metric_value(baseline_scrap_qty, '{:.2f}')}")

    kpi_cols[1].metric("Śr. scrap PLN/dzień", format_metric_value(after_scrap_pln, "{:.2f}"))
    kpi_cols[1].markdown(
        scrap_delta_badge(baseline_scrap_pln, after_scrap_pln, "{:.2f}"),
        unsafe_allow_html=True,
    )
    kpi_cols[1].caption(f"Baseline: {format_metric_value(baseline_scrap_pln, '{:.2f}')}")

    kpi_cols[2].metric(
        "Śr. OEE%",
        format_metric_value(after_oee, "{:.1f}%"),
        delta=metric_delta_label(baseline_oee, after_oee, "{:+.1f} pp"),
    )
    kpi_cols[2].caption(f"Baseline: {format_metric_value(baseline_oee, '{:.1f}%')}")

    kpi_cols[3].metric(
        "Śr. Performance%",
        format_metric_value(after_perf, "{:.1f}%"),
        delta=metric_delta_label(baseline_perf, after_perf, "{:+.1f} pp"),
    )
    kpi_cols[3].caption(f"Baseline: {format_metric_value(baseline_perf, '{:.1f}%')}")

    st.caption("Dla scrap spadek = poprawa.")
    if oee_scale != "unknown" or perf_scale != "unknown":
        st.caption(f"Wykryta skala KPI: OEE={oee_scale}, Performance={perf_scale}.")

    markers_df = pd.DataFrame()
    single_wc = len(selected_work_centers) == 1
    if single_wc:
        target_wc = selected_work_centers[0]
        target_key = normalize_key(target_wc)
        project_ids = [
            project["id"]
            for project in project_repo.list_projects(include_counts=True)
            if _project_matches_work_center(project, target_key)
        ]
        markers: list[dict[str, Any]] = []
        for project_id in project_ids:
            actions = action_repo.list_actions(
                status="done",
                project_id=project_id,
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

    if not single_wc:
        st.caption("Markery akcji są dostępne tylko dla pojedynczego Work Center.")

    if single_wc and markers_df.empty:
        st.caption("Brak zamkniętych akcji w zakresie.")

    chart_cols = st.columns(2)
    if not scrap_daily.empty:
        show_scrap_qty_markers = chart_cols[0].checkbox(
            "Pokaż markery akcji",
            value=single_wc and not markers_df.empty,
            disabled=not single_wc or markers_df.empty,
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
                value=single_wc and not markers_df.empty,
                disabled=not single_wc or markers_df.empty,
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
            value=single_wc and not markers_df.empty,
            disabled=not single_wc or markers_df.empty,
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
            value=single_wc and not markers_df.empty,
            disabled=not single_wc or markers_df.empty,
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
        "Markery akcji są ustawiane na dacie zamknięcia i widoczne wyłącznie "
        "dla pojedynczego Work Center (mapowanie wykresów definiuje Global Settings)."
    )
