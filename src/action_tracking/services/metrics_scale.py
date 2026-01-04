from __future__ import annotations

from typing import Any


def parse_float_pl(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(" ", "").replace("\u00a0", "")
    if "," in text and "." not in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def normalize_percent(value: Any) -> float | None:
    """
    Normalize KPI percent values to 0..100.

    >>> normalize_percent("0,9")
    90.0
    >>> normalize_percent("90")
    90.0
    >>> normalize_percent(1.2)
    120.0
    """
    number = parse_float_pl(value)
    if number is None:
        return None
    if 0 <= number <= 1.5:
        return number * 100
    if 1.5 < number <= 200:
        return number
    return None


def detect_percent_scale(value: Any) -> str | None:
    number = parse_float_pl(value)
    if number is None:
        return None
    if 0 <= number <= 1.5:
        return "fraction"
    if 1.5 < number <= 200:
        return "percent"
    return "invalid"
