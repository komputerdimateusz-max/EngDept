from __future__ import annotations

from datetime import date, timedelta
import sqlite3

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import ProductionDataRepository, ProjectRepository
from action_tracking.services.effectiveness import parse_work_centers


def _weighted_daily_average(
    df: pd.DataFrame,
    value_col: str,
    weight_col: str = "worktime_min",
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["metric_date", value_col])

    def _calc(group: pd.DataFrame) -> float | None:
        values = group[value_col]
        weights = group[weight_col].fillna(0)
        weighted_weights = weights.where(values.notna(), 0)
        if weighted_weights.sum() > 0:
            weighted_values = values.fillna(0) * weights
            return float(weighted_values.sum() / weighted_weights.sum())
        mean_value = values.mean()
        if pd.isna(mean_value):
            return None
        return float(mean_value)

    grouped = df.groupby("metric_date").apply(_calc)
    return grouped.reset_index(name=value_col)


def _daily_sum(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["metric_date", value_col])
    return df.groupby("metric_date", as_index=False)[value_col].sum()


def _apply_weekend_filter(
    df: pd.DataFrame,
    remove_sat: bool,
    remove_sun: bool,
) -> pd.DataFrame:
    if df.empty or "metric_date" not in df.columns:
        return df
    temp = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(temp["metric_date"]):
        temp["metric_date"] = pd.to_datetime(temp["metric_date"], errors="coerce")
    temp = temp.dropna(subset=["metric_date"])
    weekday = temp["metric_date"].dt.weekday
    mask = pd.Series(True, index=temp.index)
    if remove_sat:
        mask &= weekday != 5
    if remove_sun:
        mask &= weekday != 6
    return temp.loc[mask]


def render(con: sqlite3.Connection) -> None:
    st.header("Explorer (Produkcja)")
    repo = ProductionDataRepository(con)
    project_repo = ProjectRepository(con)

    projects = project_repo.list_projects(include_counts=False)
    projects_by_id = {project["id"]: project for project in projects}
    project_options = ["(Brak)"] + [project["id"] for project in projects]
    project_labels = {
        project["id"]: project.get("name") or project["id"] for project in projects
    }

    project_work_centers: dict[str, list[str]] = {}
    for project in projects:
        project_work_centers[project["id"]] = parse_work_centers(
            project.get("work_center"),
            project.get("related_work_center"),
        )

    stored_work_centers = set(repo.list_work_centers())
    project_centers = {center for centers in project_work_centers.values() for center in centers}
    all_work_centers = sorted(stored_work_centers | project_centers)

    default_start = date.today() - timedelta(days=90)
    default_end = date.today()

    col1, col2, col3, col4 = st.columns([1.8, 2.0, 1.2, 1.2])
    selected_project = col1.selectbox(
        "Projekt",
        project_options,
        index=0,
        format_func=lambda pid: "(Brak)" if pid == "(Brak)" else project_labels.get(pid, pid),
    )

    if selected_project != "(Brak)":
        project_centers = project_work_centers.get(selected_project, [])
        selected_work_centers = col2.multiselect(
            "Work Center (projekt)",
            project_centers,
            default=project_centers,
        )
    else:
        selected_work_centers = col2.multiselect(
            "Work Center",
            all_work_centers,
            default=all_work_centers,
        )

    selected_from = col3.date_input("Data od", value=default_start)
    selected_to = col4.date_input("Data do", value=default_end)

    metrics_options = ["Scrap qty", "Scrap PLN", "OEE %", "Performance %"]
    selected_metrics = st.multiselect(
        "Metryki",
        metrics_options,
        default=metrics_options,
    )

    if selected_from > selected_to:
        st.error("Zakres dat jest nieprawidłowy (Data od > Data do).")
        return

    if selected_project != "(Brak)" and not selected_work_centers:
        project_name = project_labels.get(selected_project, selected_project)
        st.info(f"Projekt {project_name} nie ma wybranych Work Center.")
        return

    work_center_filter: list[str] | None
    if selected_project != "(Brak)":
        work_center_filter = selected_work_centers
    else:
        if not selected_work_centers:
            st.info("Wybierz Work Center, aby zobaczyć dane.")
            return
        work_center_filter = (
            None if set(selected_work_centers) == set(all_work_centers) else selected_work_centers
        )

    scrap_rows = repo.list_scrap_daily(work_center_filter, selected_from, selected_to, currency=None)
    kpi_rows = repo.list_kpi_daily(work_center_filter, selected_from, selected_to)

    if not scrap_rows and not kpi_rows:
        st.info("Brak danych dla wybranych filtrów.")
        return

    scrap_df = pd.DataFrame(scrap_rows)
    if not scrap_df.empty:
        scrap_df["metric_date"] = pd.to_datetime(scrap_df["metric_date"])

    kpi_df = pd.DataFrame(kpi_rows)
    if not kpi_df.empty:
        kpi_df["metric_date"] = pd.to_datetime(kpi_df["metric_date"])

    filter_col1, filter_col2 = st.columns(2)
    remove_saturdays = filter_col1.checkbox(
        "Usuń soboty",
        value=False,
        key="explorer_remove_sat",
    )
    remove_sundays = filter_col2.checkbox(
        "Usuń niedziele",
        value=False,
        key="explorer_remove_sun",
    )

    if "Scrap qty" in selected_metrics:
        st.subheader("Scrap qty (dziennie)")
        if scrap_df.empty:
            st.info("Brak danych scrap qty.")
        else:
            daily_scrap_qty = _daily_sum(scrap_df, "scrap_qty")
            daily_scrap_qty = _apply_weekend_filter(
                daily_scrap_qty,
                remove_saturdays,
                remove_sundays,
            )
            if daily_scrap_qty.empty:
                st.info("Po odfiltrowaniu weekendów brak danych w zakresie.")
            else:
                st.line_chart(daily_scrap_qty.set_index("metric_date")["scrap_qty"])

    if "Scrap PLN" in selected_metrics:
        st.subheader("Scrap PLN (dziennie)")
        if scrap_df.empty:
            st.info("Brak danych scrap PLN.")
        else:
            pln_df = scrap_df[scrap_df["scrap_cost_currency"] == "PLN"]
            if pln_df.empty:
                st.info("Brak danych scrap PLN.")
            else:
                daily_scrap_pln = _daily_sum(pln_df, "scrap_cost_amount")
                daily_scrap_pln = _apply_weekend_filter(
                    daily_scrap_pln,
                    remove_saturdays,
                    remove_sundays,
                )
                if daily_scrap_pln.empty:
                    st.info("Po odfiltrowaniu weekendów brak danych w zakresie.")
                else:
                    st.line_chart(daily_scrap_pln.set_index("metric_date")["scrap_cost_amount"])

    if "OEE %" in selected_metrics:
        st.subheader("OEE % (dziennie)")
        if kpi_df.empty:
            st.info("Brak danych OEE.")
        else:
            daily_oee = _weighted_daily_average(kpi_df, "oee_pct")
            daily_oee = _apply_weekend_filter(
                daily_oee,
                remove_saturdays,
                remove_sundays,
            )
            if daily_oee.empty:
                st.info("Po odfiltrowaniu weekendów brak danych w zakresie.")
            else:
                st.line_chart(daily_oee.set_index("metric_date")["oee_pct"])

    if "Performance %" in selected_metrics:
        st.subheader("Performance % (dziennie)")
        if kpi_df.empty:
            st.info("Brak danych Performance.")
        else:
            daily_perf = _weighted_daily_average(kpi_df, "performance_pct")
            daily_perf = _apply_weekend_filter(
                daily_perf,
                remove_saturdays,
                remove_sundays,
            )
            if daily_perf.empty:
                st.info("Po odfiltrowaniu weekendów brak danych w zakresie.")
            else:
                st.line_chart(daily_perf.set_index("metric_date")["performance_pct"])

    st.subheader("Dane dzienne (audit)")
    kpi_audit = pd.DataFrame(
        columns=[
            "metric_date",
            "work_center",
            "worktime_min",
            "performance_pct",
            "oee_pct",
            "availability_pct",
            "quality_pct",
        ]
    )
    if not kpi_df.empty:
        kpi_audit = kpi_df[
            [
                "metric_date",
                "work_center",
                "worktime_min",
                "performance_pct",
                "oee_pct",
                "availability_pct",
                "quality_pct",
            ]
        ]

    scrap_audit = pd.DataFrame(columns=["metric_date", "work_center", "scrap_qty", "scrap_pln"])
    if not scrap_df.empty:
        scrap_qty = (
            scrap_df.groupby(["metric_date", "work_center"], as_index=False)["scrap_qty"].sum()
        )
        scrap_pln = (
            scrap_df[scrap_df["scrap_cost_currency"] == "PLN"]
            .groupby(["metric_date", "work_center"], as_index=False)["scrap_cost_amount"]
            .sum()
            .rename(columns={"scrap_cost_amount": "scrap_pln"})
        )
        scrap_audit = scrap_qty.merge(scrap_pln, on=["metric_date", "work_center"], how="left")

    audit_df = pd.merge(kpi_audit, scrap_audit, on=["metric_date", "work_center"], how="outer")
    audit_df_empty = audit_df.empty
    audit_df = _apply_weekend_filter(
        audit_df,
        remove_saturdays,
        remove_sundays,
    )
    if not audit_df.empty:
        audit_df = audit_df.sort_values(["metric_date", "work_center"]).reset_index(drop=True)
        audit_df["metric_date"] = audit_df["metric_date"].dt.date.astype(str)
        st.dataframe(audit_df, use_container_width=True)
        csv_data = audit_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Pobierz CSV",
            data=csv_data,
            file_name="production_explorer.csv",
            mime="text/csv",
        )
    else:
        if audit_df_empty:
            st.info("Brak danych do tabeli audit.")
        else:
            st.info("Po odfiltrowaniu weekendów brak danych w zakresie.")
