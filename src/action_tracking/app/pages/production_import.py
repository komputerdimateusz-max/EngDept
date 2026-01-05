from __future__ import annotations

from datetime import datetime
from io import BytesIO
import sqlite3
from typing import Any

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import ProductionDataRepository
from action_tracking.services.metrics_scale import detect_percent_scale, normalize_kpi_percent

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

KPI_OPTIONAL_COLUMNS = [
    "AVAILABILITY [%]",
    "QUALITY [%]",
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


def _normalize_work_center(value: Any) -> str:
    text = str(value or "").replace("\u00a0", " ")
    return " ".join(text.strip().split())


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
    df["work_center"] = df[project_col].apply(_normalize_work_center)
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
    source_file: str | None = None,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    df = df.copy()
    df["metric_date"] = df[column_map["DATE"]].apply(_parse_date_value)
    df["work_center"] = df[column_map["FULL PROJECT"]].apply(_normalize_work_center)
    valid_mask = df["metric_date"].notna() & (df["work_center"] != "")
    skipped = int((~valid_mask).sum())
    df = df.loc[valid_mask].copy()

    df["worktime_min"] = pd.to_numeric(
        df[column_map["WORKTIME  [min]"]], errors="coerce"
    ).fillna(0)
    df["oee_pct"] = pd.to_numeric(
        df[column_map["OEE [%]"]], errors="coerce"
    )
    df["performance_pct"] = pd.to_numeric(
        df[column_map["PERFORMANCE [%]"]], errors="coerce"
    )

    availability_col = column_map.get("AVAILABILITY [%]")
    quality_col = column_map.get("QUALITY [%]")
    if availability_col:
        df["availability_pct"] = pd.to_numeric(df[availability_col], errors="coerce")
    else:
        df["availability_pct"] = pd.Series([pd.NA] * len(df), dtype="float")
    if quality_col:
        df["quality_pct"] = pd.to_numeric(df[quality_col], errors="coerce")
    else:
        df["quality_pct"] = pd.Series([pd.NA] * len(df), dtype="float")

    scale_counts: dict[str, dict[str, int]] = {}
    for metric in ("oee_pct", "performance_pct", "availability_pct", "quality_pct"):
        counts = {"fraction": 0, "percent": 0, "invalid": 0}
        if metric in df.columns:
            for value in df[metric].dropna():
                scale = detect_percent_scale(value)
                if scale == "fraction":
                    counts["fraction"] += 1
                elif scale == "percent":
                    counts["percent"] += 1
                elif scale == "invalid":
                    counts["invalid"] += 1
        scale_counts[metric] = counts

    def _weighted_columns(metric: str) -> None:
        df[f"{metric}_weighted"] = df[metric].fillna(0) * df["worktime_min"]
        df[f"{metric}_weight"] = df["worktime_min"].where(df[metric].notna(), 0)

    for metric in ("oee_pct", "performance_pct", "availability_pct", "quality_pct"):
        _weighted_columns(metric)

    grouped = (
        df.groupby(["metric_date", "work_center"], dropna=False)
        .agg(
            total_worktime=("worktime_min", "sum"),
            oee_weighted=("oee_pct_weighted", "sum"),
            oee_weight=("oee_pct_weight", "sum"),
            oee_mean=("oee_pct", "mean"),
            performance_weighted=("performance_pct_weighted", "sum"),
            performance_weight=("performance_pct_weight", "sum"),
            performance_mean=("performance_pct", "mean"),
            availability_weighted=("availability_pct_weighted", "sum"),
            availability_weight=("availability_pct_weight", "sum"),
            availability_mean=("availability_pct", "mean"),
            quality_weighted=("quality_pct_weighted", "sum"),
            quality_weight=("quality_pct_weight", "sum"),
            quality_mean=("quality_pct", "mean"),
        )
        .reset_index()
    )

    rows: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        def _resolve_metric(weighted: float, weight: float, mean_value: float) -> float | None:
            if weight > 0:
                return float(weighted / weight)
            if pd.isna(mean_value):
                return None
            return float(mean_value)

        oee_pct = normalize_kpi_percent(
            _resolve_metric(row["oee_weighted"], row["oee_weight"], row["oee_mean"])
        )
        performance_pct = normalize_kpi_percent(
            _resolve_metric(
                row["performance_weighted"],
                row["performance_weight"],
                row["performance_mean"],
            )
        )
        availability_pct = normalize_kpi_percent(
            _resolve_metric(
                row["availability_weighted"],
                row["availability_weight"],
                row["availability_mean"],
            )
        )
        quality_pct = normalize_kpi_percent(
            _resolve_metric(
                row["quality_weighted"],
                row["quality_weight"],
                row["quality_mean"],
            )
        )
        rows.append(
            {
                "metric_date": row["metric_date"],
                "work_center": row["work_center"],
                "worktime_min": float(row["total_worktime"]),
                "oee_pct": oee_pct,
                "performance_pct": performance_pct,
                "availability_pct": availability_pct,
                "quality_pct": quality_pct,
                "source_file": source_file,
            }
        )
    return rows, skipped, {"scale_counts": scale_counts}


def _render_import_tab(
    *,
    label: str,
    required_columns: list[str],
    optional_columns: list[str] | None,
    prepare_rows: callable,
    on_import: callable,
) -> None:
    uploaded_file = st.file_uploader(label, type=["csv"], key=label)
    if not uploaded_file:
        return

    df = _read_production_csv(uploaded_file.getvalue())
    optional_columns = optional_columns or []
    column_map = {col: _col(df, col) for col in required_columns + optional_columns}
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
        column_selection = {key: value for key, value in column_map.items() if value is not None}
        if label == "OEE / Performance CSV":
            rows, skipped, debug_payload = prepare_rows(
                df,
                column_selection,
                uploaded_file.name,
            )
        else:
            rows, skipped, debug_payload = prepare_rows(
                df,
                column_selection,
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
        if label == "OEE / Performance CSV":
            scale_counts = debug_payload.get("scale_counts")
            if scale_counts:
                st.write("Wykryta skala KPI (liczba wartości):", scale_counts)


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
            optional_columns=None,
            prepare_rows=_prepare_scrap_rows,
            on_import=repo.upsert_scrap_daily,
        )

    with kpi_tab:
        _render_import_tab(
            label="OEE / Performance CSV",
            required_columns=KPI_REQUIRED_COLUMNS,
            optional_columns=KPI_OPTIONAL_COLUMNS,
            prepare_rows=_prepare_kpi_rows,
            on_import=repo.upsert_production_kpi_daily,
        )
