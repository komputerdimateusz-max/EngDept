from __future__ import annotations

import json
from datetime import date, timedelta
import sqlite3
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    EffectivenessRepository,
    ProductionDataRepository,
    ProjectRepository,
)
from action_tracking.domain.constants import PROJECT_TYPES
from action_tracking.services.effectiveness import (
    normalize_wc,
    parse_date,
    parse_work_centers,
    suggest_work_centers,
)


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


def _window_bounds(
    date_from: date,
    date_to: date,
) -> tuple[date, date, date, date, bool]:
    total_days = (date_to - date_from).days + 1
    if total_days < 28:
        mid = date_from + timedelta(days=(total_days - 1) // 2)
        baseline_to = mid
        after_from = mid + timedelta(days=1)
        return date_from, baseline_to, after_from, date_to, True
    return date_from, date_from + timedelta(days=13), date_to - timedelta(days=13), date_to, False


def _format_metric_value(value: float | None, fmt: str) -> str:
    if value is None:
        return "—"
    return fmt.format(value)


def _metric_delta_label(
    baseline: float | None,
    after: float | None,
    fmt: str,
) -> str:
    if baseline is None or after is None:
        return "—"
    delta = after - baseline
    if baseline == 0:
        pct_label = "n/a"
    else:
        pct_label = f"{delta / baseline:+.1%}"
    return f"{fmt.format(delta)} ({pct_label})"


def _scrap_delta_badge(
    baseline: float | None,
    after: float | None,
    unit_fmt: str,
) -> str:
    if baseline is None or after is None:
        text = "—"
        color = "#616161"
        return _scrap_delta_badge_html(text, color)
    delta = after - baseline
    if delta == 0:
        text = "→ 0"
        color = "#616161"
        return _scrap_delta_badge_html(text, color)
    if delta < 0:
        arrow = "↓"
        sign = "-"
        color = "#2e7d32"
    else:
        arrow = "↑"
        sign = "+"
        color = "#c62828"
    if baseline == 0:
        pct_label = "n/a"
    else:
        pct_label = f"{delta / baseline:+.1%}"
    value_label = f"{sign}{unit_fmt.format(abs(delta))}"
    text = f"{arrow} {value_label} ({pct_label})"
    return _scrap_delta_badge_html(text, color)


def _scrap_delta_badge_html(text: str, color: str) -> str:
    return (
        "<span style=\""
        "padding: 0.15rem 0.5rem; "
        "border-radius: 999px; "
        "display: inline-block; "
        "font-size: 0.9rem; "
        f"color: {color}; "
        f"border: 1px solid {color}; "
        "\">"
        f"{text}"
        "</span>"
    )


def _mean_or_none(series: pd.Series | None) -> float | None:
    if series is None or series.empty:
        return None
    value = series.mean()
    if pd.isna(value):
        return None
    return float(value)


def _format_action_date(value: Any) -> str:
    parsed = parse_date(value)
    return parsed.isoformat() if parsed else "—"


def _short_action_id(action_id: str | None) -> str:
    if not action_id:
        return ""
    return action_id[:6]


def _effectiveness_style(
    row: dict[str, Any] | None,
) -> tuple[str, str]:
    if not row:
        return "Not computed", "#9e9e9e"
    classification = row.get("classification")
    label_map = {
        "effective": ("Effective", "#2ca02c"),
        "no_scrap": ("Effective", "#2ca02c"),
        "no_change": ("Neutral", "#9e9e9e"),
        "insufficient_data": ("Neutral", "#9e9e9e"),
        "unknown": ("Neutral", "#9e9e9e"),
        "worse": ("Ineffective", "#d62728"),
    }
    return label_map.get(classification, ("Neutral", "#9e9e9e"))


def _effectiveness_delta(
    row: dict[str, Any] | None,
    metric: str,
) -> float | None:
    if not row:
        return None
    if row.get("metric") != metric:
        return None
    delta = row.get("delta")
    if isinstance(delta, (int, float)):
        return float(delta)
    return None


def _format_delta(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:+.2f}"


def _actions_sort_key(action_row: dict[str, Any]) -> tuple[int, int, int]:
    closed_date = parse_date(action_row.get("closed_at"))
    created_date = parse_date(action_row.get("created_at"))
    closed_flag = 0 if closed_date else 1
    closed_ord = -closed_date.toordinal() if closed_date else 10**9
    created_ord = -created_date.toordinal() if created_date else 10**9
    return closed_flag, closed_ord, created_ord


def _line_chart_with_markers(
    data: pd.DataFrame,
    y_field: str,
    y_title: str,
    title: str,
    markers_df: pd.DataFrame,
    show_markers: bool,
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
    marker_colors = ["#2ca02c", "#9e9e9e", "#d62728"]
    marker_labels = ["Effective", "Neutral", "Ineffective"]
    marker_layer = (
        alt.Chart(markers_df)
        .mark_rule()
        .encode(
            x=alt.X("closed_at:T"),
            color=alt.Color(
                "effect_label:N",
                scale=alt.Scale(domain=marker_labels, range=marker_colors),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("action_label:N", title="Akcja"),
                alt.Tooltip("closed_at:T", title="Zamknięta"),
                alt.Tooltip("owner:N", title="Owner"),
                alt.Tooltip("effect_label:N", title="Skuteczność"),
                alt.Tooltip("delta_scrap_qty:N", title="Δ scrap qty"),
                alt.Tooltip("delta_scrap_pln:N", title="Δ scrap PLN"),
            ],
        )
    )
    return base + marker_layer


def _weighted_or_mean(values: pd.Series, weights: pd.Series | None) -> float | None:
    if values.empty:
        return None
    if weights is None or weights.empty:
        return _mean_or_none(values)
    weights = weights.fillna(0)
    valid_weights = weights.where(values.notna(), 0)
    if valid_weights.sum() > 0:
        weighted_values = values.fillna(0) * weights
        return float(weighted_values.sum() / valid_weights.sum())
    return _mean_or_none(values)


def _as_percent_series(series: pd.Series) -> tuple[pd.Series, str]:
    if series.empty:
        return series, "unknown"
    numeric = series.dropna()
    if numeric.empty:
        return series, "unknown"
    median_value = float(numeric.median())
    if median_value <= 1.5:
        scaled = series * 100
        if not scaled.dropna().empty and float(scaled.max()) > 1000:
            return series, "percent"
        return scaled, "fraction"
    return series, "percent"


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
    action_repo = ActionRepository(con)
    effectiveness_repo = EffectivenessRepository(con)
    production_repo = ProductionDataRepository(con)

    champions = champion_repo.list_champions()
    champion_names = {
        champion["id"]: _champion_display_name(champion) for champion in champions
    }
    champion_options = ["(brak)"] + [champion["id"] for champion in champions]

    projects = project_repo.list_projects(include_counts=True)
    projects_by_id = {project["id"]: project for project in projects}
    wc_lists = production_repo.list_distinct_work_centers()
    all_prod_wcs = sorted(
        set(wc_lists.get("scrap_work_centers", []) + wc_lists.get("kpi_work_centers", []))
    )
    prod_work_center_map = _build_work_center_map(all_prod_wcs)
    prod_work_center_keys = set(prod_work_center_map)

    st.subheader("Outcome projektu (produkcja)")
    outcome_available = True
    if not projects:
        st.info("Brak projektów do analizy.")
        outcome_available = False

    if outcome_available:
        today = date.today()
        default_from = today - timedelta(days=90)

        selector_col1, selector_col2, selector_col3, selector_col4 = st.columns(4)
        project_ids = [project["id"] for project in projects]
        selected_project_id = selector_col1.selectbox(
            "Projekt",
            project_ids,
            format_func=lambda pid: projects_by_id[pid].get("name", pid),
        )
        if selected_project_id not in projects_by_id and project_ids:
            selected_project_id = project_ids[0]
            st.info("Wybrany projekt nie istnieje — pokazuję pierwszy dostępny.")
        selected_from = selector_col2.date_input("Data od", value=default_from)
        selected_to = selector_col3.date_input("Data do", value=today)
        include_related = selector_col4.checkbox(
            "Uwzględnij powiązane Work Center",
            value=True,
        )
        st.caption("Waluta kosztów: PLN (v1).")

        if selected_from > selected_to:
            st.error("Zakres dat jest nieprawidłowy (Data od > Data do).")
            outcome_available = False

    if outcome_available:
        selected_project = projects_by_id.get(selected_project_id, {})
        primary_wc = selected_project.get("work_center")
        if not (primary_wc and primary_wc.strip()):
            st.error("Projekt nie ma przypisanego Work Center.")
            outcome_available = False

    if outcome_available:
        related_wc = (
            selected_project.get("related_work_center") if include_related else None
        )
        work_centers = parse_work_centers(primary_wc, related_wc)
        if not work_centers:
            st.error("Nie znaleziono Work Center dla projektu.")
            outcome_available = False

    if outcome_available:
        scrap_rows_all = production_repo.list_scrap_daily(
            work_centers,
            selected_from,
            selected_to,
            currency=None,
        )
        kpi_rows = production_repo.list_kpi_daily(
            work_centers,
            selected_from,
            selected_to,
        )

        if not scrap_rows_all and not kpi_rows:
            st.info("Brak danych produkcyjnych dla wybranego zakresu.")
            outcome_available = False

    if outcome_available:
        non_pln_currencies = {
            row.get("scrap_cost_currency")
            for row in scrap_rows_all
            if row.get("scrap_cost_currency")
            and row.get("scrap_cost_currency") != "PLN"
        }
        if non_pln_currencies:
            st.info(
                "Dostępne są dane scrap w innych walutach (pominięto): "
                + ", ".join(sorted(non_pln_currencies))
            )

        scrap_rows = [
            row
            for row in scrap_rows_all
            if row.get("scrap_cost_currency") == "PLN"
        ]

        scrap_df = pd.DataFrame(scrap_rows)
        if not scrap_df.empty:
            scrap_df["metric_date"] = pd.to_datetime(
                scrap_df["metric_date"], errors="coerce"
            )
            scrap_df = scrap_df.dropna(subset=["metric_date"])
            scrap_daily = (
                scrap_df.groupby("metric_date", as_index=False)
                .agg(
                    scrap_qty_sum=("scrap_qty", "sum"),
                    scrap_pln_sum=("scrap_cost_amount", "sum"),
                )
                .sort_values("metric_date")
            )
        else:
            scrap_daily = pd.DataFrame(
                columns=["metric_date", "scrap_qty_sum", "scrap_pln_sum"]
            )

        kpi_df = pd.DataFrame(kpi_rows)
        if not kpi_df.empty:
            kpi_df["metric_date"] = pd.to_datetime(
                kpi_df["metric_date"], errors="coerce"
            )
            kpi_df = kpi_df.dropna(subset=["metric_date"])
            kpi_daily = (
                kpi_df.groupby("metric_date")
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
        else:
            kpi_daily = pd.DataFrame(
                columns=["metric_date", "oee_avg", "performance_avg"]
            )

        oee_scale = "unknown"
        perf_scale = "unknown"
        if not kpi_daily.empty:
            kpi_daily["oee_avg"], oee_scale = _as_percent_series(kpi_daily["oee_avg"])
            kpi_daily["performance_avg"], perf_scale = _as_percent_series(
                kpi_daily["performance_avg"]
            )

        actions = action_repo.list_actions_for_project_outcome(
            selected_project_id,
            selected_from,
            selected_to,
        )
        action_ids = [str(row.get("id")) for row in actions]
        effectiveness_map = effectiveness_repo.get_effectiveness_for_actions(
            action_ids
        )

        closed_markers: list[dict[str, Any]] = []
        for action in actions:
            action_id = str(action.get("id") or "")
            owner = action.get("owner_name") or champion_names.get(
                action.get("owner_champion_id"), "—"
            )
            effect_row = effectiveness_map.get(action_id)
            effect_label, _ = _effectiveness_style(effect_row)
            title = action.get("title") or ""
            short_id = _short_action_id(action_id)
            action_label = f"{title} ({short_id})" if short_id else title
            closed_date = parse_date(action.get("closed_at"))
            if closed_date and selected_from <= closed_date <= selected_to:
                closed_markers.append(
                    {
                        "closed_at": pd.to_datetime(closed_date),
                        "action_label": action_label,
                        "owner": owner or "—",
                        "effect_label": "Neutral"
                        if effect_label == "Not computed"
                        else effect_label,
                        "delta_scrap_qty": _format_delta(
                            _effectiveness_delta(effect_row, "scrap_qty")
                        ),
                        "delta_scrap_pln": _format_delta(
                            _effectiveness_delta(effect_row, "scrap_cost")
                            or _effectiveness_delta(effect_row, "scrap_pln")
                        ),
                    }
                )

        markers_df = pd.DataFrame(closed_markers)

        merged_daily = pd.merge(
            scrap_daily, kpi_daily, on="metric_date", how="outer"
        ).sort_values("metric_date")

        if "metric_date" not in merged_daily.columns:
            st.info("Brak danych produkcyjnych dla wybranego zakresu.")
            outcome_available = False

    if outcome_available:
        merged_daily["metric_date"] = pd.to_datetime(
            merged_daily["metric_date"], errors="coerce"
        )
        merged_daily = merged_daily.dropna(subset=["metric_date"]).sort_values(
            "metric_date"
        )
        if merged_daily.empty:
            st.info("Brak poprawnych danych produkcyjnych dla wybranego zakresu.")
            outcome_available = False

    if outcome_available:
        baseline_from, baseline_to, after_from, after_to, used_halves = _window_bounds(
            selected_from,
            selected_to,
        )
        if used_halves:
            st.warning(
                "Zakres < 28 dni: KPI obliczane jako połowy zakresu (baseline vs after)."
            )

        baseline_mask = (merged_daily["metric_date"].dt.date >= baseline_from) & (
            merged_daily["metric_date"].dt.date <= baseline_to
        )
        after_mask = (merged_daily["metric_date"].dt.date >= after_from) & (
            merged_daily["metric_date"].dt.date <= after_to
        )

        baseline_slice = merged_daily.loc[baseline_mask]
        after_slice = merged_daily.loc[after_mask]

        baseline_scrap_qty = _mean_or_none(baseline_slice.get("scrap_qty_sum"))
        after_scrap_qty = _mean_or_none(after_slice.get("scrap_qty_sum"))
        baseline_scrap_pln = _mean_or_none(baseline_slice.get("scrap_pln_sum"))
        after_scrap_pln = _mean_or_none(after_slice.get("scrap_pln_sum"))
        baseline_oee = _mean_or_none(baseline_slice.get("oee_avg"))
        after_oee = _mean_or_none(after_slice.get("oee_avg"))
        baseline_perf = _mean_or_none(baseline_slice.get("performance_avg"))
        after_perf = _mean_or_none(after_slice.get("performance_avg"))

        kpi_cols = st.columns(4)
        kpi_cols[0].metric(
            "Śr. scrap qty/dzień",
            _format_metric_value(after_scrap_qty, "{:.2f}"),
        )
        kpi_cols[0].markdown(
            _scrap_delta_badge(baseline_scrap_qty, after_scrap_qty, "{:.2f}"),
            unsafe_allow_html=True,
        )
        kpi_cols[0].caption(
            f"Baseline: {_format_metric_value(baseline_scrap_qty, '{:.2f}')}"
        )
        kpi_cols[1].metric(
            "Śr. scrap PLN/dzień",
            _format_metric_value(after_scrap_pln, "{:.2f}"),
        )
        kpi_cols[1].markdown(
            _scrap_delta_badge(baseline_scrap_pln, after_scrap_pln, "{:.2f}"),
            unsafe_allow_html=True,
        )
        kpi_cols[1].caption(
            f"Baseline: {_format_metric_value(baseline_scrap_pln, '{:.2f}')}"
        )
        kpi_cols[2].metric(
            "Śr. OEE%",
            _format_metric_value(after_oee, "{:.1f}%"),
            delta=_metric_delta_label(baseline_oee, after_oee, "{:+.1f} pp"),
        )
        kpi_cols[2].caption(
            f"Baseline: {_format_metric_value(baseline_oee, '{:.1f}%')}"
        )
        kpi_cols[3].metric(
            "Śr. Performance%",
            _format_metric_value(after_perf, "{:.1f}%"),
            delta=_metric_delta_label(baseline_perf, after_perf, "{:+.1f} pp"),
        )
        kpi_cols[3].caption(
            f"Baseline: {_format_metric_value(baseline_perf, '{:.1f}%')}"
        )
        st.caption("Dla scrap spadek = poprawa.")
        if oee_scale != "unknown" or perf_scale != "unknown":
            st.caption(
                "Wykryta skala KPI: "
                f"OEE={oee_scale}, Performance={perf_scale}."
            )

        marker_count = len(markers_df)
        show_markers_default = marker_count > 0 and marker_count <= 20
        show_markers = st.checkbox(
            "Pokaż markery zamknięcia akcji",
            value=show_markers_default,
            disabled=marker_count == 0,
        )
        if marker_count == 0:
            st.caption("Brak zamkniętych akcji w wybranym zakresie dat.")
        if marker_count > 20:
            st.warning(
                "W zakresie jest dużo zamkniętych akcji — aby zachować czytelność, "
                "markery są domyślnie ukryte."
            )
            if show_markers:
                show_all_markers = st.checkbox("Pokaż wszystkie markery", value=False)
                if not show_all_markers:
                    markers_df = markers_df.sort_values("closed_at").tail(20)
                    st.caption("Pokazano 20 ostatnich zamkniętych akcji.")

        chart_cols = st.columns(2)
        if not scrap_daily.empty:
            chart_cols[0].altair_chart(
                _line_chart_with_markers(
                    scrap_daily,
                    "scrap_qty_sum",
                    "Scrap qty",
                    "Scrap qty (suma)",
                    markers_df,
                    show_markers,
                ),
                use_container_width=True,
            )
            chart_cols[1].altair_chart(
                _line_chart_with_markers(
                    scrap_daily,
                    "scrap_pln_sum",
                    "Scrap PLN",
                    "Scrap PLN (suma)",
                    markers_df,
                    show_markers,
                ),
                use_container_width=True,
            )
        else:
            st.info("Brak danych scrap (PLN) w wybranym zakresie.")

        chart_cols = st.columns(2)
        if not kpi_daily.empty:
            chart_cols[0].altair_chart(
                _line_chart_with_markers(
                    kpi_daily,
                    "oee_avg",
                    "OEE (%)",
                    "OEE (średnia, %)",
                    markers_df,
                    show_markers,
                ),
                use_container_width=True,
            )
            chart_cols[1].altair_chart(
                _line_chart_with_markers(
                    kpi_daily,
                    "performance_avg",
                    "Performance (%)",
                    "Performance (średnia, %)",
                    markers_df,
                    show_markers,
                ),
                use_container_width=True,
            )
        else:
            st.info("Brak danych KPI w wybranym zakresie.")

        st.caption(
            "Markery pokazują daty zamknięcia akcji. Kolor markera odzwierciedla "
            "skuteczność scrap (jeśli policzona): zielony = effective, szary = "
            "neutral/unknown, czerwony = ineffective."
        )

        st.subheader("Akcje w wybranym zakresie")
        if not actions:
            st.caption("Brak akcji w wybranym zakresie.")
        else:
            action_table: list[dict[str, Any]] = []
            for action in sorted(actions, key=_actions_sort_key):
                action_id = str(action.get("id") or "")
                owner = action.get("owner_name") or champion_names.get(
                    action.get("owner_champion_id"), "—"
                )
                effect_row = effectiveness_map.get(action_id)
                effect_label, _ = _effectiveness_style(effect_row)
                title = action.get("title") or ""
                short_id = _short_action_id(action_id)
                action_label = f"{title} ({short_id})" if short_id else title
                action_table.append(
                    {
                        "Zamknięta": _format_action_date(action.get("closed_at")),
                        "Akcja": action_label,
                        "Kategoria": action.get("category") or "—",
                        "Status": action.get("status") or "—",
                        "Termin": _format_action_date(action.get("due_date")),
                        "Owner": owner or "—",
                        "Skuteczność": effect_label,
                        "Δ scrap qty": _format_delta(
                            _effectiveness_delta(effect_row, "scrap_qty")
                        ),
                        "Δ scrap PLN": _format_delta(
                            _effectiveness_delta(effect_row, "scrap_cost")
                            or _effectiveness_delta(effect_row, "scrap_pln")
                        ),
                        "Utworzono": _format_action_date(action.get("created_at")),
                    }
                )
            st.dataframe(action_table, use_container_width=True)

        st.subheader("Dzienny podgląd (audit)")
        if merged_daily.empty:
            st.caption("Brak danych do wyświetlenia w tabeli.")
        else:
            merged_daily = merged_daily.sort_values("metric_date")
            if len(merged_daily) > 180:
                merged_daily = merged_daily.tail(180)
            audit_rows = merged_daily.rename(
                columns={
                    "metric_date": "metric_date",
                    "scrap_qty_sum": "scrap_qty_sum",
                    "scrap_pln_sum": "scrap_pln_sum",
                    "oee_avg": "oee_avg",
                    "performance_avg": "performance_avg",
                }
            )
            audit_rows["metric_date"] = audit_rows["metric_date"].dt.date
            st.dataframe(audit_rows, use_container_width=True)
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
                work_center_index = (
                    all_prod_wcs.index(default_work_center)
                    if default_work_center
                    else 0
                )
                work_center = st.selectbox(
                    "Work center",
                    all_prod_wcs,
                    index=work_center_index,
                )
            else:
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
            if use_wc_picker and all_prod_wcs:
                related_defaults = []
                related_tokens = parse_work_centers(
                    None,
                    selected.get("related_work_center", "") or "",
                )
                for token in related_tokens:
                    normalized = normalize_wc(token)
                    if normalized in prod_work_center_map:
                        related_defaults.append(prod_work_center_map[normalized][0])
                related_selection = st.multiselect(
                    "Powiązane Work Center",
                    all_prod_wcs,
                    default=related_defaults,
                )
                related_work_center = "; ".join(related_selection)
            else:
                related_work_center = st.text_input(
                    "Powiązane Work Center",
                    value=selected.get("related_work_center", "") or "",
                )

            st.markdown("**Walidacja Work Center**")
            if not all_prod_wcs:
                st.info(
                    "Brak danych produkcyjnych w bazie – nie można zweryfikować WC."
                )
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
            project_type = st.selectbox(
                "Typ",
                PROJECT_TYPES,
                index=category_index,
            )
            owner_champion_id = st.selectbox(
                "Champion",
                champion_options,
                index=champion_options.index(
                    selected.get("owner_champion_id", "(brak)")
                )
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

    with st.expander("🧾 Changelog projektów", expanded=False):
        changelog_entries = project_repo.list_changelog(limit=50)
        if not changelog_entries:
            st.caption("Brak wpisów w changelogu.")
        for entry in changelog_entries:
            changes = json.loads(entry["changes_json"])
            project_name = (
                entry.get("name") or changes.get("name") or entry.get("project_id")
            )
            st.markdown(
                f"**{entry['event_at']}** · {entry['event_type']} · {project_name}"
            )
            st.caption(_format_changes(entry["event_type"], changes, champion_names))
