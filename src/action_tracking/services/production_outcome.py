from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd


def window_bounds(
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


def apply_weekend_filter(
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


def load_daily_frames(
    production_repo: Any,
    work_centers: list[str],
    date_from: date,
    date_to: date,
    currency: str | None = "PLN",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    scrap_rows_all = production_repo.list_scrap_daily(
        work_centers,
        date_from,
        date_to,
        currency=None,
    )
    non_pln_currencies = {
        row.get("scrap_cost_currency")
        for row in scrap_rows_all
        if row.get("scrap_cost_currency") and row.get("scrap_cost_currency") != "PLN"
    }

    if currency:
        scrap_rows = [row for row in scrap_rows_all if row.get("scrap_cost_currency") == currency]
    else:
        scrap_rows = scrap_rows_all

    scrap_df = pd.DataFrame(scrap_rows)
    if not scrap_df.empty:
        scrap_df["metric_date"] = pd.to_datetime(scrap_df["metric_date"], errors="coerce")
        scrap_df = scrap_df.dropna(subset=["metric_date"])
        scrap_daily = (
            scrap_df.groupby("metric_date", as_index=False)
            .agg(scrap_qty_sum=("scrap_qty", "sum"), scrap_pln_sum=("scrap_cost_amount", "sum"))
            .sort_values("metric_date")
        )
    else:
        scrap_daily = pd.DataFrame(columns=["metric_date", "scrap_qty_sum", "scrap_pln_sum"])

    kpi_rows = production_repo.list_kpi_daily(work_centers, date_from, date_to)
    kpi_df = pd.DataFrame(kpi_rows)
    if not kpi_df.empty:
        kpi_df["metric_date"] = pd.to_datetime(kpi_df["metric_date"], errors="coerce")
        kpi_df = kpi_df.dropna(subset=["metric_date"])
        kpi_daily = (
            kpi_df.groupby("metric_date")
            .apply(
                lambda group: pd.Series(
                    {
                        "oee_avg": _weighted_or_mean(group["oee_pct"], group.get("worktime_min")),
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
        kpi_daily = pd.DataFrame(columns=["metric_date", "oee_avg", "performance_avg"])

    oee_scale = "unknown"
    perf_scale = "unknown"
    if not kpi_daily.empty:
        kpi_daily["oee_avg"], oee_scale = _as_percent_series(kpi_daily["oee_avg"])
        kpi_daily["performance_avg"], perf_scale = _as_percent_series(kpi_daily["performance_avg"])

    scrap_daily.attrs["non_pln_currencies"] = sorted(non_pln_currencies)
    scrap_daily.attrs["scrap_rows_all"] = scrap_rows_all
    scrap_daily.attrs["scrap_rows_filtered"] = scrap_rows
    kpi_daily.attrs["kpi_rows"] = kpi_rows
    kpi_daily.attrs["oee_scale"] = oee_scale
    kpi_daily.attrs["perf_scale"] = perf_scale

    merged_daily = pd.merge(scrap_daily, kpi_daily, on="metric_date", how="outer").sort_values("metric_date")
    return scrap_daily, kpi_daily, merged_daily


def compute_baseline_after_metrics(
    merged_daily: pd.DataFrame,
    date_from: date,
    date_to: date,
) -> dict[str, Any]:
    if merged_daily.empty or "metric_date" not in merged_daily.columns:
        return {
            "baseline_from": date_from,
            "baseline_to": date_from,
            "after_from": date_to,
            "after_to": date_to,
            "used_halves": False,
            "baseline_scrap_qty": None,
            "after_scrap_qty": None,
            "baseline_scrap_pln": None,
            "after_scrap_pln": None,
            "baseline_oee": None,
            "after_oee": None,
            "baseline_perf": None,
            "after_perf": None,
        }

    data = merged_daily.copy()
    data["metric_date"] = pd.to_datetime(data["metric_date"], errors="coerce")
    data = data.dropna(subset=["metric_date"])

    baseline_from, baseline_to, after_from, after_to, used_halves = window_bounds(date_from, date_to)

    baseline_mask = (data["metric_date"].dt.date >= baseline_from) & (data["metric_date"].dt.date <= baseline_to)
    after_mask = (data["metric_date"].dt.date >= after_from) & (data["metric_date"].dt.date <= after_to)

    baseline_slice = data.loc[baseline_mask]
    after_slice = data.loc[after_mask]

    return {
        "baseline_from": baseline_from,
        "baseline_to": baseline_to,
        "after_from": after_from,
        "after_to": after_to,
        "used_halves": used_halves,
        "baseline_scrap_qty": _mean_or_none(baseline_slice.get("scrap_qty_sum")),
        "after_scrap_qty": _mean_or_none(after_slice.get("scrap_qty_sum")),
        "baseline_scrap_pln": _mean_or_none(baseline_slice.get("scrap_pln_sum")),
        "after_scrap_pln": _mean_or_none(after_slice.get("scrap_pln_sum")),
        "baseline_oee": _mean_or_none(baseline_slice.get("oee_avg")),
        "after_oee": _mean_or_none(after_slice.get("oee_avg")),
        "baseline_perf": _mean_or_none(baseline_slice.get("performance_avg")),
        "after_perf": _mean_or_none(after_slice.get("performance_avg")),
    }


def format_metric_value(value: float | None, fmt: str) -> str:
    if value is None:
        return "—"
    return fmt.format(value)


def metric_delta_label(
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


def scrap_delta_badge(
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
