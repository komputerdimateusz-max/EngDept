from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd


def compute_project_kpi_windows(
    scrap_rows: list[dict[str, Any]],
    kpi_rows: list[dict[str, Any]],
    remove_sat: bool,
    remove_sun: bool,
    current_days_target: int = 14,
    baseline_cap_days: int = 90,
    searchback_calendar_days: int = 180,
) -> dict[str, Any]:
    scrap_qty_by_day, scrap_pln_by_day = _aggregate_scrap_daily(scrap_rows)
    oee_by_day, performance_by_day = _aggregate_kpi_daily(kpi_rows)

    _apply_weekend_filter(scrap_qty_by_day, remove_sat, remove_sun)
    _apply_weekend_filter(scrap_pln_by_day, remove_sat, remove_sun)
    _apply_weekend_filter(oee_by_day, remove_sat, remove_sun)
    _apply_weekend_filter(performance_by_day, remove_sat, remove_sun)

    available_days = sorted(
        set(scrap_qty_by_day)
        | set(scrap_pln_by_day)
        | set(oee_by_day)
        | set(performance_by_day)
    )

    if not available_days:
        return _insufficient_data_payload()

    if searchback_calendar_days:
        max_day = available_days[-1]
        cutoff = max_day - timedelta(days=searchback_calendar_days - 1)
        scrap_qty_by_day = _filter_by_cutoff(scrap_qty_by_day, cutoff)
        scrap_pln_by_day = _filter_by_cutoff(scrap_pln_by_day, cutoff)
        oee_by_day = _filter_by_cutoff(oee_by_day, cutoff)
        performance_by_day = _filter_by_cutoff(performance_by_day, cutoff)
        available_days = sorted(
            set(scrap_qty_by_day)
            | set(scrap_pln_by_day)
            | set(oee_by_day)
            | set(performance_by_day)
        )

    baseline_days, current_days = _select_window_days(
        available_days,
        current_days_target,
        baseline_cap_days,
    )
    if not baseline_days or not current_days:
        return _insufficient_data_payload()

    baseline_scrap_qty = _mean_for_days(scrap_qty_by_day, baseline_days)
    current_scrap_qty = _mean_for_days(scrap_qty_by_day, current_days)
    baseline_scrap_pln = _mean_for_days(scrap_pln_by_day, baseline_days)
    current_scrap_pln = _mean_for_days(scrap_pln_by_day, current_days)
    baseline_oee = _mean_for_days(oee_by_day, baseline_days)
    current_oee = _mean_for_days(oee_by_day, current_days)
    baseline_perf = _mean_for_days(performance_by_day, baseline_days)
    current_perf = _mean_for_days(performance_by_day, current_days)

    return {
        "status": "ok",
        "window": {
            "current_days": len(current_days),
            "baseline_days": len(baseline_days),
            "current_from": current_days[0].isoformat(),
            "current_to": current_days[-1].isoformat(),
            "baseline_from": baseline_days[0].isoformat(),
            "baseline_to": baseline_days[-1].isoformat(),
        },
        "metrics": {
            "scrap_qty": _scrap_metric_payload(current_scrap_qty, baseline_scrap_qty),
            "scrap_pln": _scrap_metric_payload(current_scrap_pln, baseline_scrap_pln),
            "oee": _kpi_metric_payload(current_oee, baseline_oee),
            "performance": _kpi_metric_payload(current_perf, baseline_perf),
        },
    }


def _aggregate_scrap_daily(
    rows: list[dict[str, Any]],
) -> tuple[dict[date, float], dict[date, float]]:
    scrap_qty_by_day: dict[date, float] = defaultdict(float)
    scrap_pln_by_day: dict[date, float] = defaultdict(float)
    for row in rows:
        metric_date = _parse_date(row.get("metric_date"))
        if not metric_date:
            continue
        scrap_qty_by_day[metric_date] += _to_float(row.get("scrap_qty")) or 0.0
        scrap_pln_by_day[metric_date] += _to_float(row.get("scrap_cost_amount")) or 0.0
    return dict(scrap_qty_by_day), dict(scrap_pln_by_day)


def _aggregate_kpi_daily(
    rows: list[dict[str, Any]],
) -> tuple[dict[date, float], dict[date, float]]:
    by_day: dict[date, dict[str, list[float | None]]] = defaultdict(
        lambda: {
            "oee_values": [],
            "oee_weights": [],
            "performance_values": [],
            "performance_weights": [],
        }
    )
    for row in rows:
        metric_date = _parse_date(row.get("metric_date"))
        if not metric_date:
            continue
        weight = _to_float(row.get("worktime_min"))
        oee_value = _to_float(row.get("oee_pct"))
        if oee_value is not None:
            by_day[metric_date]["oee_values"].append(oee_value)
            by_day[metric_date]["oee_weights"].append(weight)
        perf_value = _to_float(row.get("performance_pct"))
        if perf_value is not None:
            by_day[metric_date]["performance_values"].append(perf_value)
            by_day[metric_date]["performance_weights"].append(weight)

    oee_by_day: dict[date, float] = {}
    performance_by_day: dict[date, float] = {}
    for metric_date, payload in by_day.items():
        oee_daily = _weighted_or_mean(payload["oee_values"], payload["oee_weights"])
        perf_daily = _weighted_or_mean(
            payload["performance_values"], payload["performance_weights"]
        )
        if oee_daily is not None:
            oee_by_day[metric_date] = oee_daily
        if perf_daily is not None:
            performance_by_day[metric_date] = perf_daily

    return oee_by_day, performance_by_day


def _weighted_or_mean(values: list[float], weights: list[float | None]) -> float | None:
    if not values:
        return None
    weighted_pairs = [
        (value, weight)
        for value, weight in zip(values, weights)
        if weight is not None and weight > 0
    ]
    if weighted_pairs:
        total_weight = sum(weight for _, weight in weighted_pairs)
        if total_weight > 0:
            return sum(value * weight for value, weight in weighted_pairs) / total_weight
    return sum(values) / len(values)


def _select_window_days(
    available_days: list[date],
    current_days_target: int,
    baseline_cap_days: int,
) -> tuple[list[date], list[date]]:
    if len(available_days) < 8:
        return [], []
    if len(available_days) >= current_days_target * 2:
        current_days = available_days[-current_days_target:]
        baseline_pool = available_days[:-current_days_target]
        baseline_days = baseline_pool[-min(baseline_cap_days, len(baseline_pool)) :]
        return baseline_days, current_days
    if len(available_days) >= 14:
        window_days = available_days[-14:]
        return window_days[:7], window_days[7:]
    window_days = available_days[-8:]
    return window_days[:4], window_days[4:]


def _mean_for_days(values_by_day: dict[date, float], days: list[date]) -> float | None:
    values = [values_by_day[day] for day in days if day in values_by_day]
    if not values:
        return None
    return sum(values) / len(values)


def _scrap_metric_payload(current: float | None, baseline: float | None) -> dict[str, Any]:
    delta_abs = None
    delta_rel_pct = None
    if current is not None and baseline is not None:
        delta_abs = current - baseline
        if baseline != 0:
            delta_rel_pct = (delta_abs / baseline) * 100
    return {
        "baseline": baseline,
        "current": current,
        "delta_abs": delta_abs,
        "delta_rel_pct": delta_rel_pct,
    }


def _kpi_metric_payload(current: float | None, baseline: float | None) -> dict[str, Any]:
    delta_pp = None
    if current is not None and baseline is not None:
        delta_pp = current - baseline
    return {
        "baseline": baseline,
        "current": current,
        "delta_pp": delta_pp,
    }


def _apply_weekend_filter(values_by_day: dict[date, float], remove_sat: bool, remove_sun: bool) -> None:
    if not (remove_sat or remove_sun):
        return
    to_drop = []
    for metric_date in values_by_day:
        weekday = metric_date.weekday()
        if remove_sat and weekday == 5:
            to_drop.append(metric_date)
        elif remove_sun and weekday == 6:
            to_drop.append(metric_date)
    for metric_date in to_drop:
        values_by_day.pop(metric_date, None)


def _filter_by_cutoff(values_by_day: dict[date, float], cutoff: date) -> dict[date, float]:
    return {metric_date: value for metric_date, value in values_by_day.items() if metric_date >= cutoff}


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if value in (None, ""):
        return None
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _insufficient_data_payload() -> dict[str, Any]:
    return {
        "status": "insufficient_data",
        "window": {
            "current_days": 0,
            "baseline_days": 0,
            "current_from": None,
            "current_to": None,
            "baseline_from": None,
            "baseline_to": None,
        },
        "metrics": {
            "scrap_qty": _scrap_metric_payload(None, None),
            "scrap_pln": _scrap_metric_payload(None, None),
            "oee": _kpi_metric_payload(None, None),
            "performance": _kpi_metric_payload(None, None),
        },
    }
