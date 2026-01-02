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


def _normalize_column_name(value: str) -> str:
    normalized = value.replace("\ufeff", "").replace("\u00a0", " ")
    return " ".join(normalized.strip().split())


def _col(df: pd.DataFrame, expected_name: str) -> str | None:
    if expected_name in df.columns:
        return expected_name
    expected_normalized = _normalize_column_name(expected_name)
    for column in df.columns:
        if _normalize_column_name(column) == expected_normalized:
            return column
    return None


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
    missing = [col for col in required if _col(df, col) is None]
    return missing


def _prepare_scrap_rows(
    df: pd.DataFrame,
    column_map: dict[str, str],
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    df = df.copy()
    date_col = column_map["DATE"]
    project_col = column_map["FULL PROJECT"]
    ok_s_qty_col = column_map["OK S. QTY [pcs]"]
    scrap_value_col = column_map["SCRAP VALUE [pln]"]
    df["metric_date"] = df[date_col].apply(_parse_date_value)
    df["work_center"] = df[project_col].astype(str).str.strip()
    valid_mask = df["metric_date"].notna() & (df["work_center"] != "")
    skipped = int((~valid_mask).sum())
    df = df.loc[valid_mask].copy()

    df["ok_s_qty"] = pd.to_numeric(df[ok_s_qty_col], errors="coerce").fillna(0)
    df["scrap_value"] = pd.to_numeric(df[scrap_value_col], errors="coerce").fillna(0)

    grouped = (
        df.groupby(["metric_date", "work_center"], dropna=False)
        .agg(
            total_ok_s_qty=("ok_s_qty", "sum"),
            total_scrap_value=("scrap_value", "sum"),
        )
        .reset_index()
    )

    grouped["scrap_qty"] = grouped["total_ok_s_qty"].round().astype(int)
    aggregated_df = grouped.rename(
        columns={"total_scrap_value": "scrap_cost_amount"}
    )[
        ["metric_date", "work_center", "scrap_qty", "scrap_cost_amount"]
    ]

    rows: list[dict[str, Any]] = []
    for _, row in aggregated_df.iterrows():
        scrap_qty = int(row["scrap_qty"])
        rows.append(
            {
                "metric_date": row["metric_date"],
                "work_center": row["work_center"],
                "scrap_qty": scrap_qty,
                "scrap_cost_amount": float(row["scrap_cost_amount"]),
                "scrap_cost_currency": "PLN",
            }
        )
    raw_sum = (
        df.groupby(["metric_date", "work_center"], dropna=False)["ok_s_qty"]
        .sum()
        .reset_index()
        .rename(columns={"ok_s_qty": "raw_ok_s_qty"})
    )
    raw_sum["raw_ok_s_qty"] = raw_sum["raw_ok_s_qty"].round().astype(int)
    comparison = raw_sum.merge(
        aggregated_df,
        on=["metric_date", "work_center"],
        how="left",
    )
    mismatch = comparison[
        comparison["raw_ok_s_qty"] != comparison["scrap_qty"]
    ].copy()
    debug_payload = {
        "aggregated_df": aggregated_df,
        "mismatch_df": mismatch,
        "ok_s_qty_col": ok_s_qty_col,
    }
    return rows, skipped, debug_payload


def _prepare_kpi_rows(
    df: pd.DataFrame,
    column_map: dict[str, str],
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    df = df.copy()
    df["metric_date"] = df[column_map["DATE"]].apply(_parse_date_value)
    df["work_center"] = df[column_map["FULL PROJECT"]].astype(str).str.strip()
    valid_mask = df["metric_date"].notna() & (df["work_center"] != "")
    skipped = int((~valid_mask).sum())
    df = df.loc[valid_mask].copy()

    df["worktime"] = pd.to_numeric(
        df[column_map["WORKTIME  [min]"]], errors="coerce"
    ).fillna(0)
    df["oee"] = pd.to_numeric(
        df[column_map["OEE [%]"]], errors="coerce"
    ).fillna(0)
    df["performance"] = pd.to_numeric(
        df[column_map["PERFORMANCE [%]"]], errors="coerce"
    ).fillna(0)

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
    return rows, skipped, {}


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
    column_map = {col: _col(df, col) for col in required_columns}
    if label == "Scrap CSV":
        st.write("Detected columns:", list(df.columns))
        preview_columns = [
            column_map.get("DATE"),
            column_map.get("FULL PROJECT"),
            column_map.get("OK S. QTY [pcs]"),
            column_map.get("SCRAP VALUE [pln]"),
        ]
        missing_preview = [col for col in preview_columns if col is None]
        if missing_preview:
            st.warning(
                "Brakuje kolumn do podglądu: "
                + ", ".join(required_columns)
            )
        else:
            st.dataframe(df[preview_columns].head(20))
    st.caption("Podgląd pierwszych 50 wierszy")
    st.dataframe(df.head(50))

    missing_columns = _validate_columns(df, required_columns)
    if missing_columns:
        st.error(
            "Brakuje wymaganych kolumn: "
            + ", ".join(missing_columns)
            + ". Dostępne kolumny: "
            + ", ".join(df.columns)
        )
        return

    if st.button("Import / Update", key=f"import-{label}"):
        rows, skipped, debug_payload = prepare_rows(
            df,
            {key: value for key, value in column_map.items() if value is not None},
        )
        if label == "Scrap CSV":
            aggregated_df = debug_payload.get("aggregated_df")
            if aggregated_df is not None:
                st.dataframe(aggregated_df.head(20))
            mismatch_df = debug_payload.get("mismatch_df")
            if mismatch_df is not None and not mismatch_df.empty:
                st.error(
                    "Niezgodność scrap_qty z OK S. QTY [pcs] dla kluczy: "
                    + ", ".join(
                        [
                            f"{row['metric_date']} / {row['work_center']}"
                            for _, row in mismatch_df.head(5).iterrows()
                        ]
                    )
                )
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
        st.caption(
            "Ponowne zaimportowanie tego samego pliku nadpisze (upsert) wartości "
            "dla tych samych dni i projektów."
        )
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
