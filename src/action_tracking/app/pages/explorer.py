from __future__ import annotations

from datetime import date, timedelta
import sqlite3

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import ProductionDataRepository


def render(con: sqlite3.Connection) -> None:
    st.header("Explorer")
    repo = ProductionDataRepository(con)

    work_centers = repo.list_work_centers()
    work_center_options = ["(Wszystkie)"] + work_centers
    default_start = date.today() - timedelta(days=90)
    default_end = date.today()

    col1, col2, col3 = st.columns([1.6, 1.2, 1.2])
    selected_work_center = col1.selectbox(
        "Work Center",
        work_center_options,
        index=0,
    )
    selected_from = col2.date_input("Date from", value=default_start)
    selected_to = col3.date_input("Date to", value=default_end)

    work_center_filter = None if selected_work_center == "(Wszystkie)" else selected_work_center

    scrap_rows = repo.list_scrap_daily(work_center_filter, selected_from, selected_to)
    kpi_rows = repo.list_production_kpi_daily(work_center_filter, selected_from, selected_to)

    if not scrap_rows and not kpi_rows:
        st.info("Brak danych dla wybranych filtrów.")
        return

    if scrap_rows:
        scrap_df = pd.DataFrame(scrap_rows)
        scrap_df["metric_date"] = pd.to_datetime(scrap_df["metric_date"])
        st.subheader("Scrap quantity trend (daily)")
        if work_center_filter:
            st.line_chart(scrap_df.set_index("metric_date")["scrap_qty"])
        else:
            qty_pivot = scrap_df.pivot_table(
                index="metric_date",
                columns="work_center",
                values="scrap_qty",
                aggfunc="sum",
            )
            st.line_chart(qty_pivot)

        pln_df = scrap_df[scrap_df["scrap_cost_currency"] == "PLN"].copy()
        if not pln_df.empty:
            st.subheader("Scrap cost trend (PLN)")
            if work_center_filter:
                st.line_chart(pln_df.set_index("metric_date")["scrap_cost_amount"])
            else:
                cost_pivot = pln_df.pivot_table(
                    index="metric_date",
                    columns="work_center",
                    values="scrap_cost_amount",
                    aggfunc="sum",
                )
                st.line_chart(cost_pivot)
    else:
        st.info("Brak danych scrap dla wybranych filtrów.")

    if kpi_rows:
        kpi_df = pd.DataFrame(kpi_rows)
        kpi_df["metric_date"] = pd.to_datetime(kpi_df["metric_date"])
        st.subheader("OEE % trend")
        if work_center_filter:
            st.line_chart(kpi_df.set_index("metric_date")["oee_pct"])
        else:
            oee_pivot = kpi_df.pivot_table(
                index="metric_date",
                columns="work_center",
                values="oee_pct",
                aggfunc="mean",
            )
            st.line_chart(oee_pivot)

        st.subheader("Performance % trend")
        if work_center_filter:
            st.line_chart(kpi_df.set_index("metric_date")["performance_pct"])
        else:
            performance_pivot = kpi_df.pivot_table(
                index="metric_date",
                columns="work_center",
                values="performance_pct",
                aggfunc="mean",
            )
            st.line_chart(performance_pivot)
    else:
        st.info("Brak danych KPI dla wybranych filtrów.")
