from __future__ import annotations

from typing import Any


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


def _direction(delta: float | None, improvement_is_negative: bool) -> str:
    if delta is None or delta == 0:
        return "neutral"
    if improvement_is_negative:
        return "improvement" if delta < 0 else "worsening"
    return "improvement" if delta > 0 else "worsening"


def compute_scrap_delta(current: Any, baseline: Any) -> dict[str, Any]:
    current_value = _to_float(current)
    baseline_value = _to_float(baseline)
    delta_abs = None
    if current_value is not None and baseline_value is not None:
        delta_abs = current_value - baseline_value
    delta_pct = None
    if delta_abs is not None and baseline_value not in (None, 0):
        delta_pct = (delta_abs / baseline_value) * 100
    return {
        "current": current_value,
        "baseline": baseline_value,
        "delta_abs": delta_abs,
        "delta_pct": delta_pct,
        "delta_pp": None,
        "direction": _direction(delta_abs, improvement_is_negative=True),
    }


def compute_kpi_pp_delta(current_pct: Any, baseline_pct: Any) -> dict[str, Any]:
    current_value = _to_float(current_pct)
    baseline_value = _to_float(baseline_pct)
    delta_pp = None
    if current_value is not None and baseline_value is not None:
        delta_pp = current_value - baseline_value
    return {
        "current": current_value,
        "baseline": baseline_value,
        "delta_abs": None,
        "delta_pct": None,
        "delta_pp": delta_pp,
        "direction": _direction(delta_pp, improvement_is_negative=False),
    }
