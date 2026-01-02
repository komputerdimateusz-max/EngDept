from __future__ import annotations

from datetime import datetime
from io import BytesIO
import sqlite3
from typing import Any

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import ProductionDataRepository

SCRAP_REQUIRED_COLUMNS = [
    "DATE",
    "FULL PROJECT",
    "QTY [pcs]",
    "OK S. QTY [pcs]",
    "SCRAP VALUE [pln]",
]

KPI_REQUIRED_COLUMNS = [
    "DATE",
    "FULL PROJECT",
    "WORKTIME  [min]",
    "OEE [%]",
    "PERFORMANCE [%]",
]


DATE_FORMATS = ["%Y%m%d", "%Y-%m-%d", "%d.%m.%Y"]


def _parse_date_value(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        value = int(value)
    text = str(value).strip()
    if not text:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _read_production_csv(file_data: bytes) -> pd.DataFrame:
    return pd.read_csv(
        BytesIO(file_data),
        sep=";",
        decimal=",",
        encoding="utf-8-sig",
    )


def _validate_columns(df: pd.DataFrame, required: list[str]) -> list[str]:
    missing = [col for col in required if col not in df.columns]
    return missing


def _prepare_scrap_rows(df: pd.DataFrame) -> tuple[list[dict[str, Any]], int]:
    df = df.copy()
    df["metric_date"] = df["DATE"].apply(_parse_date_value)
    df["work_center"] = df["FULL PROJECT"].astype(str).str.strip()
    valid_mask = df["metric_date"].notna() & (df["work_center"] != "")
    skipped = int((~valid_mask).sum())
    df = df.loc[valid_mask].copy()

    df["qty"] = pd.to_numeric(df["QTY [pcs]"], errors="coerce").fillna(0)
    df["ok_qty"] = pd.to_numeric(df["OK S. QTY [pcs]"], errors="coerce").fillna(0)
    df["scrap_value"] = pd.to_numeric(df["SCRAP VALUE [pln]"], errors="coerce").fillna(0)

    grouped = (
        df.groupby(["metric_date", "work_center"], dropna=False)
        .agg(
            total_qty=("qty", "sum"),
            total_ok_qty=("ok_qty", "sum"),
            total_scrap_value=("scrap_value", "sum"),
        )
        .reset_index()
    )

    rows: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        scrap_qty = int(round(row["total_qty"] - row["total_ok_qty"]))
        rows.append(
            {
                "metric_date": row["metric_date"],
                "work_center": row["work_center"],
                "scrap_qty": scrap_qty,
                "scrap_cost_amount": float(row["total_scrap_value"]),
                "scrap_cost_currency": "PLN",
            }
        )
    return rows, skipped


def _prepare_kpi_rows(df: pd.DataFrame) -> tuple[list[dict[str, Any]], int]:
    df = df.copy()
    df["metric_date"] = df["DATE"].apply(_parse_date_value)
    df["work_center"] = df["FULL PROJECT"].astype(str).str.strip()
    valid_mask = df["metric_date"].notna() & (df["work_center"] != "")
    skipped = int((~valid_mask).sum())
    df = df.loc[valid_mask].copy()

    df["worktime"] = pd.to_numeric(df["WORKTIME  [min]"], errors="coerce").fillna(0)
    df["oee"] = pd.to_numeric(df["OEE [%]"], errors="coerce").fillna(0)
    df["performance"] = pd.to_numeric(df["PERFORMANCE [%]"], errors="coerce").fillna(0)

    df["oee_weighted"] = df["oee"] * df["worktime"]
    df["performance_weighted"] = df["performance"] * df["worktime"]

    grouped = (
        df.groupby(["metric_date", "work_center"], dropna=False)
        .agg(
            total_worktime=("worktime", "sum"),
            oee_weighted=("oee_weighted", "sum"),
            performance_weighted=("performance_weighted", "sum"),
        )
        .reset_index()
    )

    rows: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        total_worktime = row["total_worktime"]
        oee_pct = None
        performance_pct = None
        if total_worktime > 0:
            oee_pct = float(row["oee_weighted"] / total_worktime)
            performance_pct = float(row["performance_weighted"] / total_worktime)
        rows.append(
            {
                "metric_date": row["metric_date"],
                "work_center": row["work_center"],
                "oee_pct": oee_pct,
                "performance_pct": performance_pct,
            }
        )
    return rows, skipped


def _render_import_tab(
    *,
    label: str,
    required_columns: list[str],
    prepare_rows: callable,
    on_import: callable,
) -> None:
    uploaded_file = st.file_uploader(label, type=["csv"], key=label)
    if not uploaded_file:
        return

    df = _read_production_csv(uploaded_file.getvalue())
    st.caption("Podgląd pierwszych 50 wierszy")
    st.dataframe(df.head(50))

    missing_columns = _validate_columns(df, required_columns)
    if missing_columns:
        st.error(
            "Brakuje wymaganych kolumn: " + ", ".join(missing_columns)
        )
        return

    if st.button("Import / Update", key=f"import-{label}"):
        rows, skipped = prepare_rows(df)
        on_import(rows)
        st.success("Import zakończony")
        st.write(
            {
                "rows_read": int(len(df)),
                "rows_aggregated": int(len(rows)),
                "rows_skipped": int(skipped),
            }
        )


def render(con: sqlite3.Connection) -> None:
    st.header("Import danych produkcyjnych")
    st.write(
        "Wczytaj eksport CSV (separator ;, liczby z przecinkiem). Dane są agregowane dziennie "
        "per projekt (work center)."
    )

    repo = ProductionDataRepository(con)

    scrap_tab, kpi_tab = st.tabs(["Scrap", "OEE / Performance"])

    with scrap_tab:
        _render_import_tab(
            label="Scrap CSV",
            required_columns=SCRAP_REQUIRED_COLUMNS,
            prepare_rows=_prepare_scrap_rows,
            on_import=repo.upsert_scrap_daily,
        )

    with kpi_tab:
        _render_import_tab(
            label="OEE / Performance CSV",
            required_columns=KPI_REQUIRED_COLUMNS,
            prepare_rows=_prepare_kpi_rows,
            on_import=repo.upsert_production_kpi_daily,
        )
