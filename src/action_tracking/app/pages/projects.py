from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    EffectivenessRepository,
    GlobalSettingsRepository,
    ProductionDataRepository,
    ProjectRepository,
    WcInboxRepository,
)
from action_tracking.domain.constants import PROJECT_TYPES
from action_tracking.services.effectiveness import (
    normalize_wc,
    parse_date,
    parse_work_centers,
    suggest_work_centers,
)
from action_tracking.services.normalize import normalize_key
from action_tracking.services.overlay_targets import (
    OVERLAY_TARGET_COLORS,
    OVERLAY_TARGET_LABELS,
    default_overlay_targets,
)
from action_tracking.services.production_outcome import (
    apply_weekend_filter,
    format_metric_value,
    load_daily_frames,
    metric_delta_label,
    scrap_delta_badge,
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
    marker_colors = [OVERLAY_TARGET_COLORS.get(label, "#9e9e9e") for label in marker_labels]
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
                alt.Tooltip("action_label:N", title="Akcja"),
                alt.Tooltip("closed_at:T", title="Zamknięta"),
                alt.Tooltip("owner:N", title="Owner"),
                alt.Tooltip("category:N", title="Kategoria"),
                alt.Tooltip("overlay_label:N", title="Wykres"),
                alt.Tooltip("delta_scrap_qty:N", title="Δ scrap qty"),
                alt.Tooltip("delta_scrap_pln:N", title="Δ scrap PLN"),
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


def _project_matches_work_centers(
    project: dict[str, Any],
    work_center_keys: set[str],
) -> bool:
    centers = parse_work_centers(
        project.get("work_center"),
        project.get("related_work_center"),
    )
    return any(normalize_key(center) in work_center_keys for center in centers)



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


def _resolve_after_days(total_days: int) -> int:
    if total_days >= 28:
        after_days = 14
    elif total_days >= 14:
        after_days = 7
    elif total_days >= 8:
        after_days = 4
    else:
        after_days = max(2, total_days // 2)
    return min(total_days, after_days) if total_days > 0 else 0


def _resolve_windows(
    date_from: date,
    date_to: date,
    available_dates: list[date],
) -> dict[str, Any]:
    total_days = len(available_dates)
    after_days = _resolve_after_days(total_days)
    if total_days == 0 or after_days == 0:
        return {
            "baseline_from": date_from,
            "baseline_to": date_from,
            "after_from": date_to,
            "after_to": date_to,
            "after_days": 0,
            "total_days": total_days,
            "baseline_days": 0,
        }
    after_to = available_dates[-1]
    after_from = available_dates[-after_days]
    baseline_to = after_from - timedelta(days=1)
    baseline_from = max(date_from, after_from - timedelta(days=90))
    baseline_days = max(0, (after_from - baseline_from).days)
    return {
        "baseline_from": baseline_from,
        "baseline_to": baseline_to,
        "after_from": after_from,
        "after_to": after_to,
        "after_days": after_days,
        "total_days": total_days,
        "baseline_days": baseline_days,
    }


def _mean_or_none(series: pd.Series | None) -> float | None:
    if series is None or series.empty:
        return None
    value = series.mean()
    if pd.isna(value):
        return None
    return float(value)


def _compute_baseline_after_metrics(
    merged_daily: pd.DataFrame,
    date_from: date,
    date_to: date,
) -> dict[str, Any]:
    if merged_daily.empty or "metric_date" not in merged_daily.columns:
        return {"insufficient_data": True}

    data = merged_daily.copy()
    data["metric_date"] = pd.to_datetime(data["metric_date"], errors="coerce")
    data = data.dropna(subset=["metric_date"])
    if data.empty:
        return {"insufficient_data": True}

    available_dates = sorted({d.date() for d in data["metric_date"]})
    windows = _resolve_windows(date_from, date_to, available_dates)
    baseline_from = windows["baseline_from"]
    baseline_to = windows["baseline_to"]
    after_from = windows["after_from"]
    after_to = windows["after_to"]

    baseline_mask = (data["metric_date"].dt.date >= baseline_from) & (data["metric_date"].dt.date <= baseline_to)
    after_mask = (data["metric_date"].dt.date >= after_from) & (data["metric_date"].dt.date <= after_to)

    baseline_slice = data.loc[baseline_mask]
    after_slice = data.loc[after_mask]

    if baseline_slice.empty or after_slice.empty:
        return {**windows, "insufficient_data": True}

    return {
        **windows,
        "insufficient_data": False,
        "baseline_scrap_qty": _mean_or_none(baseline_slice.get("scrap_qty_sum")),
        "after_scrap_qty": _mean_or_none(after_slice.get("scrap_qty_sum")),
        "baseline_scrap_pln": _mean_or_none(baseline_slice.get("scrap_pln_sum")),
        "after_scrap_pln": _mean_or_none(after_slice.get("scrap_pln_sum")),
        "baseline_oee": _mean_or_none(baseline_slice.get("oee_avg")),
        "after_oee": _mean_or_none(after_slice.get("oee_avg")),
        "baseline_perf": _mean_or_none(baseline_slice.get("performance_avg")),
        "after_perf": _mean_or_none(after_slice.get("performance_avg")),
    }


def _project_work_centers(
    project: dict[str, Any],
    include_related: bool,
) -> list[str]:
    primary_wc = project.get("work_center")
    related_wc = project.get("related_work_center") if include_related else None
    return parse_work_centers(primary_wc, related_wc)


@st.cache_data(hash_funcs={sqlite3.Connection: id})
def _load_project_outcome_data(
    con: sqlite3.Connection,
    work_centers: tuple[str, ...],
    date_from: date,
    date_to: date,
    remove_saturdays: bool,
    remove_sundays: bool,
) -> dict[str, Any]:
    production_repo = ProductionDataRepository(con)
    scrap_daily, kpi_daily, merged_daily = load_daily_frames(
        production_repo,
        list(work_centers),
        date_from,
        date_to,
        currency="PLN",
    )

    scrap_daily_f = apply_weekend_filter(scrap_daily, remove_saturdays, remove_sundays)
    kpi_daily_f = apply_weekend_filter(kpi_daily, remove_saturdays, remove_sundays)
    merged_daily_f = apply_weekend_filter(merged_daily, remove_saturdays, remove_sundays)

    kpi_rows = kpi_daily.attrs.get("kpi_rows", [])
    kpi_rows_df = pd.DataFrame(kpi_rows)
    worktime_sum = 0.0
    kpi_days = 0
    if not kpi_rows_df.empty and "metric_date" in kpi_rows_df.columns:
        kpi_rows_df["metric_date"] = pd.to_datetime(kpi_rows_df["metric_date"], errors="coerce")
        kpi_rows_df = apply_weekend_filter(kpi_rows_df, remove_saturdays, remove_sundays)
        if "worktime_min" in kpi_rows_df.columns:
            worktime_sum = float(kpi_rows_df["worktime_min"].fillna(0).sum())
        kpi_days = kpi_rows_df["metric_date"].dt.date.nunique()

    scrap_days = 0
    if not scrap_daily_f.empty and "metric_date" in scrap_daily_f.columns:
        scrap_days = scrap_daily_f["metric_date"].dt.date.nunique()

    if worktime_sum > 0:
        volume_proxy = worktime_sum
    elif kpi_days > 0:
        volume_proxy = float(kpi_days)
    else:
        volume_proxy = float(scrap_days)

    return {
        "scrap_daily": scrap_daily_f,
        "kpi_daily": kpi_daily_f,
        "merged_daily": merged_daily_f,
        "oee_scale": kpi_daily.attrs.get("oee_scale", "unknown"),
        "perf_scale": kpi_daily.attrs.get("perf_scale", "unknown"),
        "non_pln_currencies": scrap_daily.attrs.get("non_pln_currencies", []),
        "volume_proxy": volume_proxy,
    }


def render(con: sqlite3.Connection) -> None:
    st.header("Projekty")

    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)
    action_repo = ActionRepository(con)
    effectiveness_repo = EffectivenessRepository(con)
    production_repo = ProductionDataRepository(con)

    # ✅ RESOLVE MERGE: both are required later in this page
    rules_repo = GlobalSettingsRepository(con)
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

    st.subheader("Outcome projektu (produkcja)")
    outcome_available = True
    if not projects:
        st.info("Brak projektów do analizy.")
        outcome_available = False

    if outcome_available:
        today = date.today()
        default_from = today - timedelta(days=90)

        mode = st.radio("Widok", ["Projekt", "Champion"], horizontal=True)
        selector_col1, selector_col2, selector_col3, selector_col4 = st.columns(4)
        project_ids = [p["id"] for p in projects]
        champion_ids = [c["id"] for c in champions]

        selected_project_id = ""
        selected_champion_id = ""
        if mode == "Projekt":
            selected_project_id = selector_col1.selectbox(
                "Projekt",
                project_ids,
                format_func=lambda pid: projects_by_id[pid].get("name", pid),
            )
            if selected_project_id not in projects_by_id and project_ids:
                selected_project_id = project_ids[0]
                st.info("Wybrany projekt nie istnieje — pokazuję pierwszy dostępny.")
        else:
            selected_champion_id = selector_col1.selectbox(
                "Champion",
                champion_ids,
                format_func=lambda cid: champion_names.get(cid, cid),
            )
        selected_from = selector_col2.date_input("Data od", value=default_from)
        selected_to = selector_col3.date_input("Data do", value=today)
        include_related = selector_col4.checkbox("Uwzględnij powiązane Work Center", value=True)
        st.caption("Waluta kosztów: PLN (v1).")

        if selected_from > selected_to:
            st.error("Zakres dat jest nieprawidłowy (Data od > Data do).")
            outcome_available = False

    selected_projects: list[dict[str, Any]] = []
    project_data_by_id: dict[str, dict[str, Any]] = {}
    missing_wc_projects: list[dict[str, Any]] = []
    include_closed_projects = False
    selected_project_ids: list[str] = []
    open_action_project_ids: list[str] = []
    remove_saturdays = False
    remove_sundays = False
    show_markers = True

    if outcome_available:
        filter_cols = st.columns([1.1, 1.1, 1.2])
        remove_saturdays = filter_cols[0].checkbox("Usuń soboty", value=False, key="remove_saturdays")
        remove_sundays = filter_cols[1].checkbox("Usuń niedziele", value=False, key="remove_sundays")
        show_markers = filter_cols[2].checkbox("Pokaż markery zamknięcia akcji", value=True)

    if outcome_available:
        if mode == "Projekt":
            selected_project = projects_by_id.get(selected_project_id, {})
            selected_projects = [selected_project] if selected_project else []
            if not selected_projects:
                st.info("Brak projektu do analizy.")
                outcome_available = False
        else:
            if not champion_ids:
                st.info("Brak championów do analizy.")
                outcome_available = False
            else:
                use_assignments = champion_repo.has_champion_projects_table()
                if use_assignments:
                    assigned_ids = champion_repo.get_assigned_projects(selected_champion_id)
                    selected_projects = [project for project in projects if project.get("id") in assigned_ids]
                else:
                    selected_projects = [
                        project for project in projects if project.get("owner_champion_id") == selected_champion_id
                    ]
                if not selected_projects:
                    st.info("Wybrany champion nie ma przypisanych projektów.")
                    outcome_available = False

    if outcome_available and mode == "Champion":
        include_closed_projects = st.checkbox(
            "Uwzględnij zamknięte / wstrzymane projekty",
            value=False,
        )
        if not include_closed_projects:
            selected_projects = [
                project for project in selected_projects if (project.get("status") or "active") == "active"
            ]
        if not selected_projects:
            st.info("Brak aktywnych projektów w portfelu.")
            outcome_available = False

    if outcome_available:
        if mode == "Champion":
            open_action_project_ids = [project["id"] for project in selected_projects if project.get("id")]
        else:
            open_action_project_ids = [selected_project_id] if selected_project_id else []

    if outcome_available:
        for project in selected_projects:
            work_centers = _project_work_centers(project, include_related)
            if not work_centers:
                missing_wc_projects.append(project)
                continue
            data = _load_project_outcome_data(
                con,
                tuple(work_centers),
                selected_from,
                selected_to,
                remove_saturdays,
                remove_sundays,
            )
            project_data_by_id[project["id"]] = {
                "project": project,
                "work_centers": work_centers,
                "data": data,
            }

        if missing_wc_projects:
            missing_names = ", ".join(
                sorted({project.get("name") or project.get("id") for project in missing_wc_projects})
            )
            if mode == "Projekt":
                st.error("Projekt nie ma przypisanego Work Center.")
                outcome_available = False
            else:
                st.warning(f"Pomijam projekty bez Work Center: {missing_names}.")

        if mode == "Champion":
            summary_rows = [
                {
                    "Projekt": project.get("name"),
                    "Work center": project.get("work_center") or "—",
                    "Powiązane WC": project.get("related_work_center") or "—",
                    "Status": project.get("status") or "—",
                }
                for project in selected_projects
            ]
            st.caption(f"Liczba projektów w portfelu: {len(selected_projects)}")
            st.dataframe(summary_rows, use_container_width=True)

    if outcome_available:
        volume_order = sorted(
            ((pid, info["data"]["volume_proxy"]) for pid, info in project_data_by_id.items()),
            key=lambda item: item[1],
            reverse=True,
        )
        ordered_project_ids = [pid for pid, _ in volume_order]

        if mode == "Champion":
            show_all_projects = st.checkbox("Pokaż wszystkie projekty", value=False, key="show_all_projects")
            if len(ordered_project_ids) <= 10:
                default_selection = ordered_project_ids
            else:
                default_selection = ordered_project_ids if show_all_projects else ordered_project_ids[:10]

            selected_project_ids = st.multiselect(
                "Filtruj projekty",
                ordered_project_ids,
                default=default_selection,
                format_func=lambda pid: projects_by_id[pid].get("name", pid),
            )
            if show_all_projects and len(ordered_project_ids) > 30:
                st.warning("Pokazujesz ponad 30 projektów — widok może być wolniejszy.")
            if not selected_project_ids:
                st.info("Brak wybranych projektów do wyświetlenia.")
        else:
            selected_project_ids = [selected_project_id]

    single_project_actions: list[dict[str, Any]] = []
    single_effectiveness_map: dict[str, dict[str, Any]] = {}
    single_merged_daily: pd.DataFrame | None = None

    if outcome_available and selected_project_ids:
        for project_id in ordered_project_ids:
            if project_id not in selected_project_ids:
                continue
            project_info = project_data_by_id.get(project_id)
            if not project_info:
                continue
            project = project_info["project"]
            project_name = project.get("name") or project.get("id")
            work_centers = project_info["work_centers"]
            data = project_info["data"]

            st.subheader(project_name)

            non_pln_currencies = data.get("non_pln_currencies", [])
            if non_pln_currencies and mode == "Projekt":
                st.info(
                    "Dostępne są dane scrap w innych walutach (pominięto): "
                    + ", ".join(sorted(non_pln_currencies))
                )

            action_project_ids = [project_id]
            if include_related:
                work_center_keys = {normalize_key(center) for center in work_centers if center}
                action_project_ids = (
                    [p["id"] for p in projects if _project_matches_work_centers(p, work_center_keys)]
                    or [project_id]
                )

            actions_map: dict[str, dict[str, Any]] = {}
            for action_project_id in action_project_ids:
                project_actions = action_repo.list_actions_for_project_outcome(
                    action_project_id,
                    selected_from,
                    selected_to,
                )
                for action in project_actions:
                    action_id = str(action.get("id") or "")
                    if action_id and action_id not in actions_map:
                        actions_map[action_id] = action
            actions = list(actions_map.values())
            action_ids = [str(row.get("id")) for row in actions]
            effectiveness_map = effectiveness_repo.get_effectiveness_for_actions(action_ids)

            closed_markers: list[dict[str, Any]] = []
            for action in actions:
                if action.get("status") != "done":
                    continue
                action_id = str(action.get("id") or "")
                owner = action.get("owner_name") or champion_names.get(action.get("owner_champion_id"), "—")
                effect_row = effectiveness_map.get(action_id)
                title = action.get("title") or ""
                short_id = _short_action_id(action_id)
                action_label = f"{title} ({short_id})" if short_id else title
                closed_date = parse_date(action.get("closed_at"))
                if closed_date and selected_from <= closed_date <= selected_to:
                    overlay_targets = _resolve_overlay_targets(action, rules_repo)
                    for overlay_target in overlay_targets:
                        closed_markers.append(
                            {
                                "closed_at": pd.to_datetime(closed_date),
                                "action_label": action_label,
                                "owner": owner or "—",
                                "category": action.get("category") or "—",
                                "overlay_target": overlay_target,
                                "overlay_label": OVERLAY_TARGET_LABELS.get(overlay_target, overlay_target),
                                "delta_scrap_qty": _format_delta(_effectiveness_delta(effect_row, "scrap_qty")),
                                "delta_scrap_pln": _format_delta(
                                    _effectiveness_delta(effect_row, "scrap_cost")
                                    or _effectiveness_delta(effect_row, "scrap_pln")
                                ),
                            }
                        )

            markers_df = pd.DataFrame(closed_markers)
            marker_count = len(markers_df)

            merged_daily_f = data["merged_daily"]
            metrics = _compute_baseline_after_metrics(merged_daily_f, selected_from, selected_to)
            insufficient_data = metrics.get("insufficient_data", True)

            if insufficient_data:
                st.warning("Brak danych do wyliczenia baseline/after dla wybranego zakresu.")

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

            caption_line = "Baseline = max 90 dni przed oknem AFTER (14/7/4); AFTER = ostatnie okno."
            after_days = metrics.get("after_days") or 0
            total_days = metrics.get("total_days") or 0
            if total_days and total_days < 28 and after_days:
                caption_line += f" Zakres < 28 dni → okno AFTER = {after_days} dni."
            st.caption(caption_line)
            st.caption("Dla scrap spadek = poprawa.")

            oee_scale = data.get("oee_scale", "unknown")
            perf_scale = data.get("perf_scale", "unknown")
            if oee_scale != "unknown" or perf_scale != "unknown":
                st.caption(f"Wykryta skala KPI: OEE={oee_scale}, Performance={perf_scale}.")

            if marker_count == 0:
                st.caption("Brak zamkniętych akcji w wybranym zakresie dat.")
            if marker_count > 20:
                st.warning(
                    "W zakresie jest dużo zamkniętych akcji — aby zachować czytelność, "
                    "markery są domyślnie ukryte."
                )
                if show_markers:
                    show_all_markers = st.checkbox(
                        "Pokaż wszystkie markery",
                        value=False,
                        key=f"show_all_markers_{project_id}",
                    )
                    if not show_all_markers:
                        markers_df = markers_df.sort_values("closed_at").tail(20)
                        st.caption("Pokazano 20 ostatnich zamkniętych akcji.")

            chart_cols = st.columns(2)
            scrap_daily_f = data["scrap_daily"]
            if not scrap_daily_f.empty:
                chart_cols[0].altair_chart(
                    _line_chart_with_markers(
                        scrap_daily_f,
                        "scrap_qty_sum",
                        "Scrap qty",
                        "Scrap qty (suma)",
                        markers_df,
                        show_markers,
                        "SCRAP_QTY",
                    ),
                    use_container_width=True,
                )
                chart_cols[1].altair_chart(
                    _line_chart_with_markers(
                        scrap_daily_f,
                        "scrap_pln_sum",
                        "Scrap PLN",
                        "Scrap PLN (suma)",
                        markers_df,
                        show_markers,
                        "SCRAP_COST",
                    ),
                    use_container_width=True,
                )
            else:
                st.info("Brak danych scrap (PLN) w wybranym zakresie.")

            chart_cols = st.columns(2)
            kpi_daily_f = data["kpi_daily"]
            if not kpi_daily_f.empty:
                chart_cols[0].altair_chart(
                    _line_chart_with_markers(
                        kpi_daily_f,
                        "oee_avg",
                        "OEE (%)",
                        "OEE (średnia, %)",
                        markers_df,
                        show_markers,
                        "OEE",
                    ),
                    use_container_width=True,
                )
                chart_cols[1].altair_chart(
                    _line_chart_with_markers(
                        kpi_daily_f,
                        "performance_avg",
                        "Performance (%)",
                        "Performance (średnia, %)",
                        markers_df,
                        show_markers,
                        "PERFORMANCE",
                    ),
                    use_container_width=True,
                )
            else:
                st.info("Brak danych KPI w wybranym zakresie.")

            st.caption(
                "Markery pokazują daty zamknięcia akcji oraz docelowe wykresy "
                "(ustawiane w Global Settings → reguły kategorii)."
            )

            if mode == "Projekt":
                single_project_actions = actions
                single_effectiveness_map = effectiveness_map
                single_merged_daily = merged_daily_f

    if outcome_available:
        with st.expander("Akcje (otwarte)", expanded=False):
            if not open_action_project_ids:
                st.caption("Brak projektów do filtrowania otwartych akcji.")
            else:
                open_actions = action_repo.list_open_actions(open_action_project_ids)
                if not open_actions:
                    st.caption("Brak otwartych akcji.")
                else:
                    open_table = []
                    today = date.today()
                    for action in open_actions:
                        due_date = parse_date(action.get("due_date"))
                        created_date = parse_date(action.get("created_at"))
                        overdue = bool(due_date and due_date < today)
                        open_table.append(
                            {
                                "Utworzono": created_date.isoformat() if created_date else "—",
                                "Termin": due_date.isoformat() if due_date else "—",
                                "Status": action.get("status") or "—",
                                "Priorytet": action.get("priority") or "—",
                                "Projekt": action.get("project_name")
                                or projects_by_id.get(action.get("project_id"), {}).get(
                                    "name",
                                    action.get("project_id") or "—",
                                ),
                                "Tytuł": action.get("title") or "—",
                                "Owner": action.get("owner_name")
                                or champion_names.get(action.get("owner_champion_id"), "—"),
                                "Po terminie": "⚠️" if overdue else "—",
                            }
                        )
                    st.dataframe(open_table, use_container_width=True)

    if outcome_available and mode == "Projekt":
        st.subheader("Akcje w wybranym zakresie")
        if not single_project_actions:
            st.caption("Brak akcji w wybranym zakresie.")
        else:
            action_table: list[dict[str, Any]] = []
            for action in sorted(single_project_actions, key=_actions_sort_key):
                action_id = str(action.get("id") or "")
                owner = action.get("owner_name") or champion_names.get(action.get("owner_champion_id"), "—")
                effect_row = single_effectiveness_map.get(action_id)
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
                        "Δ scrap qty": _format_delta(_effectiveness_delta(effect_row, "scrap_qty")),
                        "Δ scrap PLN": _format_delta(
                            _effectiveness_delta(effect_row, "scrap_cost")
                            or _effectiveness_delta(effect_row, "scrap_pln")
                        ),
                        "Utworzono": _format_action_date(action.get("created_at")),
                    }
                )
            st.dataframe(action_table, use_container_width=True)

        st.subheader("Dzienny podgląd (audit)")
        if single_merged_daily is None or single_merged_daily.empty:
            st.caption("Brak danych do wyświetlenia w tabeli.")
        else:
            merged_daily_f = single_merged_daily.sort_values("metric_date")
            if len(merged_daily_f) > 180:
                merged_daily_f = merged_daily_f.tail(180)
            audit_rows = merged_daily_f.rename(
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
