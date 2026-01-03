from __future__ import annotations

import json
from datetime import date, timedelta
import sqlite3
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from action_tracking.data.repositories import (
    ChampionRepository,
    ProductionDataRepository,
    ProjectRepository,
)
from action_tracking.domain.constants import PROJECT_TYPES
from action_tracking.services.effectiveness import parse_work_centers


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


def _mean_or_none(series: pd.Series | None) -> float | None:
    if series is None or series.empty:
        return None
    value = series.mean()
    if pd.isna(value):
        return None
    return float(value)


def render(con: sqlite3.Connection) -> None:
    st.header("Projekty")

    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)
    production_repo = ProductionDataRepository(con)

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

    st.subheader("Outcome projektu (produkcja)")
    if not projects:
        st.info("Brak projektów do analizy.")
        return

    today = date.today()
    default_from = today - timedelta(days=90)

    selector_col1, selector_col2, selector_col3, selector_col4 = st.columns(4)
    selected_project_id = selector_col1.selectbox(
        "Projekt",
        [project["id"] for project in projects],
        format_func=lambda pid: projects_by_id[pid].get("name", pid),
    )
    selected_from = selector_col2.date_input("Data od", value=default_from)
    selected_to = selector_col3.date_input("Data do", value=today)
    include_related = selector_col4.checkbox(
        "Uwzględnij powiązane Work Center",
        value=True,
    )
    st.caption("Waluta kosztów: PLN (v1).")

    if selected_from > selected_to:
        st.error("Zakres dat jest nieprawidłowy (Data od > Data do).")
        return

    selected_project = projects_by_id.get(selected_project_id, {})
    primary_wc = selected_project.get("work_center")
    if not (primary_wc and primary_wc.strip()):
        st.error("Projekt nie ma przypisanego Work Center.")
        return

    related_wc = selected_project.get("related_work_center") if include_related else None
    work_centers = parse_work_centers(primary_wc, related_wc)
    if not work_centers:
        st.error("Nie znaleziono Work Center dla projektu.")
        return

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
        return

    non_pln_currencies = {
        row.get("scrap_cost_currency")
        for row in scrap_rows_all
        if row.get("scrap_cost_currency") and row.get("scrap_cost_currency") != "PLN"
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
        scrap_df["metric_date"] = pd.to_datetime(scrap_df["metric_date"])
        scrap_daily = (
            scrap_df.groupby("metric_date", as_index=False)
            .agg(scrap_qty_sum=("scrap_qty", "sum"), scrap_pln_sum=("scrap_cost_amount", "sum"))
            .sort_values("metric_date")
        )
    else:
        scrap_daily = pd.DataFrame(columns=["metric_date", "scrap_qty_sum", "scrap_pln_sum"])

    kpi_df = pd.DataFrame(kpi_rows)
    if not kpi_df.empty:
        kpi_df["metric_date"] = pd.to_datetime(kpi_df["metric_date"])
        kpi_daily = (
            kpi_df.groupby("metric_date", as_index=False)
            .agg(oee_avg=("oee_pct", "mean"), performance_avg=("performance_pct", "mean"))
            .sort_values("metric_date")
        )
    else:
        kpi_daily = pd.DataFrame(columns=["metric_date", "oee_avg", "performance_avg"])

    merged_daily = pd.merge(scrap_daily, kpi_daily, on="metric_date", how="outer").sort_values(
        "metric_date"
    )

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
        delta=_metric_delta_label(baseline_scrap_qty, after_scrap_qty, "{:+.2f}"),
    )
    kpi_cols[0].caption(
        f"Baseline: {_format_metric_value(baseline_scrap_qty, '{:.2f}')}"
    )
    kpi_cols[1].metric(
        "Śr. scrap PLN/dzień",
        _format_metric_value(after_scrap_pln, "{:.2f}"),
        delta=_metric_delta_label(baseline_scrap_pln, after_scrap_pln, "{:+.2f}"),
    )
    kpi_cols[1].caption(
        f"Baseline: {_format_metric_value(baseline_scrap_pln, '{:.2f}')}"
    )
    kpi_cols[2].metric(
        "Śr. OEE%",
        _format_metric_value(after_oee, "{:.1f}%"),
        delta=_metric_delta_label(baseline_oee, after_oee, "{:+.1f} pp"),
    )
    kpi_cols[2].caption(f"Baseline: {_format_metric_value(baseline_oee, '{:.1f}%')}")
    kpi_cols[3].metric(
        "Śr. Performance%",
        _format_metric_value(after_perf, "{:.1f}%"),
        delta=_metric_delta_label(baseline_perf, after_perf, "{:+.1f} pp"),
    )
    kpi_cols[3].caption(
        f"Baseline: {_format_metric_value(baseline_perf, '{:.1f}%')}"
    )

    chart_cols = st.columns(2)
    if not scrap_daily.empty:
        chart_cols[0].altair_chart(
            alt.Chart(scrap_daily)
            .mark_line()
            .encode(
                x=alt.X("metric_date:T", title="Data"),
                y=alt.Y("scrap_qty_sum:Q", title="Scrap qty"),
            )
            .properties(title="Scrap qty (suma)"),
            use_container_width=True,
        )
        chart_cols[1].altair_chart(
            alt.Chart(scrap_daily)
            .mark_line()
            .encode(
                x=alt.X("metric_date:T", title="Data"),
                y=alt.Y("scrap_pln_sum:Q", title="Scrap PLN"),
            )
            .properties(title="Scrap PLN (suma)"),
            use_container_width=True,
        )
    else:
        st.info("Brak danych scrap (PLN) w wybranym zakresie.")

    chart_cols = st.columns(2)
    if not kpi_daily.empty:
        chart_cols[0].altair_chart(
            alt.Chart(kpi_daily)
            .mark_line()
            .encode(
                x=alt.X("metric_date:T", title="Data"),
                y=alt.Y("oee_avg:Q", title="OEE%"),
            )
            .properties(title="OEE% (średnia)"),
            use_container_width=True,
        )
        chart_cols[1].altair_chart(
            alt.Chart(kpi_daily)
            .mark_line()
            .encode(
                x=alt.X("metric_date:T", title="Data"),
                y=alt.Y("performance_avg:Q", title="Performance%"),
            )
            .properties(title="Performance% (średnia)"),
            use_container_width=True,
        )
    else:
        st.info("Brak danych KPI w wybranym zakresie.")

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
