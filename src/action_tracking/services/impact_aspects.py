from __future__ import annotations

import json
from typing import Any


IMPACT_ASPECTS: tuple[str, ...] = (
    "SCRAP",
    "OEE",
    "PERFORMANCE",
    "DLE",
    "DOWNTIMES",
)

IMPACT_ASPECT_LABELS: dict[str, str] = {
    "SCRAP": "Scrap",
    "OEE": "OEE",
    "PERFORMANCE": "Performance",
    "DLE": "DLE",
    "DOWNTIMES": "Downtimes",
}

IMPACT_ASPECT_COLORS: dict[str, str] = {
    "SCRAP": "#d62728",
    "OEE": "#1f77b4",
    "PERFORMANCE": "#2ca02c",
    "DLE": "#9467bd",
    "DOWNTIMES": "#8c564b",
}

_ASPECT_SYNONYMS: dict[str, str] = {
    "SCRAP": "SCRAP",
    "SCRAPS": "SCRAP",
    "SCRAPQTY": "SCRAP",
    "SCRAP_QTY": "SCRAP",
    "SCRAPCOST": "SCRAP",
    "SCRAP_COST": "SCRAP",
    "OEE": "OEE",
    "PERF": "PERFORMANCE",
    "PERFORMANCE": "PERFORMANCE",
    "DLE": "DLE",
    "DOWNTIME": "DOWNTIMES",
    "DOWNTIMES": "DOWNTIMES",
}


def parse_impact_aspects(value: Any) -> set[str]:
    if value in (None, ""):
        return set()

    raw: Any = value
    if isinstance(value, str):
        try:
            raw = json.loads(value)
        except json.JSONDecodeError:
            raw = [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]

    if isinstance(raw, str):
        raw = [raw]

    if not isinstance(raw, list):
        return set()

    normalized: set[str] = set()
    for item in raw:
        if item in (None, ""):
            continue
        key = str(item).strip().upper().replace(" ", "_")
        key = _ASPECT_SYNONYMS.get(key, key)
        if key in IMPACT_ASPECTS:
            normalized.add(key)
    return normalized


def normalize_impact_aspects(value: Any) -> list[str]:
    aspects = parse_impact_aspects(value)
    return [aspect for aspect in IMPACT_ASPECTS if aspect in aspects]
