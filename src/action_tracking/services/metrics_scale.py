from __future__ import annotations

import math
from typing import Any


def _parse_numeric(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return None if math.isnan(number) else number
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in ("nan", "none", "null"):
        return None
    text = text.replace("%", "")
    text = text.replace("\u00a0", " ").replace(" ", "")
    if "," in text and "." not in text:
        text = text.replace(",", ".")
    try:
        number = float(text)
    except ValueError:
        return None
    return None if math.isnan(number) else number


def parse_float_pl(value: Any) -> float | None:
    return _parse_numeric(value)


def normalize_kpi_percent(value: Any) -> float | None:
    """
    Normalize KPI percent values to 0..100.

    >>> normalize_kpi_percent("0,9")
    90.0
    >>> normalize_kpi_percent("90%")
    90.0
    >>> normalize_kpi_percent(1.2)
    120.0
    """
    number = _parse_numeric(value)
    if number is None:
        return None
    if 0 <= number <= 1.5:
        return number * 100
    if 1.5 < number <= 200:
        return number
    return None


def normalize_percent(value: Any) -> float | None:
    return normalize_kpi_percent(value)


def detect_percent_scale(value: Any) -> str | None:
    number = _parse_numeric(value)
    if number is None:
        return None
    if 0 <= number <= 1.5:
        return "fraction"
    if 1.5 < number <= 200:
        return "percent"
    return "invalid"


if __name__ == "__main__":
    assert normalize_kpi_percent("0,92") == 92.0
    assert normalize_kpi_percent("92%") == 92.0
    assert normalize_kpi_percent("0.92") == 92.0
    assert normalize_kpi_percent("nan") is None
