from __future__ import annotations

import json
from typing import Any

from action_tracking.services.areas import normalize_area
from action_tracking.services.impact_aspects import IMPACT_ASPECT_COLORS


OVERLAY_TARGETS: tuple[str, ...] = (
    "SCRAP_QTY",
    "SCRAP_COST",
    "OEE",
    "PERFORMANCE",
)

OVERLAY_TARGET_LABELS: dict[str, str] = {
    "SCRAP_QTY": "Scrap qty",
    "SCRAP_COST": "Scrap PLN",
    "OEE": "OEE",
    "PERFORMANCE": "Performance",
}

OVERLAY_TARGET_COLORS: dict[str, str] = {
    "SCRAP_QTY": IMPACT_ASPECT_COLORS.get("SCRAP", "#d62728"),
    "SCRAP_COST": IMPACT_ASPECT_COLORS.get("SCRAP", "#d62728"),
    "OEE": IMPACT_ASPECT_COLORS.get("OEE", "#1f77b4"),
    "PERFORMANCE": IMPACT_ASPECT_COLORS.get("PERFORMANCE", "#2ca02c"),
}

AREA_SELECTION_HINTS: tuple[tuple[str, str], ...] = (
    ("montaż (pl", "Montaż"),
    ("montaz (pl", "Montaż"),
    ("wtrysk (m", "Wtrysk"),
    ("metalizacja (mzt", "Metalizacja"),
    ("metalizacja (mtz", "Metalizacja"),
    ("podgrupy (pl", "Podgrupa"),
    ("podgrupa", "Podgrupa"),
)


def parse_overlay_targets(value: Any) -> list[str]:
    if value in (None, ""):
        return []

    raw: Any = value
    if isinstance(value, str):
        try:
            raw = json.loads(value)
        except json.JSONDecodeError:
            raw = [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]

    if isinstance(raw, str):
        raw = [raw]

    if not isinstance(raw, list):
        return []

    normalized: set[str] = set()
    for item in raw:
        if item in (None, ""):
            continue
        key = str(item).strip().upper().replace(" ", "_")
        if key in OVERLAY_TARGETS:
            normalized.add(key)

    return [target for target in OVERLAY_TARGETS if target in normalized]


def serialize_overlay_targets(value: Any) -> str | None:
    normalized = parse_overlay_targets(value)
    if not normalized:
        return None
    return ",".join(normalized)


def default_overlay_targets(effect_model: str | None) -> list[str]:
    key = (effect_model or "NONE").strip().upper()
    if key == "SCRAP":
        return ["SCRAP_QTY", "SCRAP_COST"]
    if key == "OEE":
        return ["OEE"]
    if key == "PERFORMANCE":
        return ["PERFORMANCE"]
    return []


def normalize_action_area(value: Any) -> str | None:
    if value in (None, ""):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return normalize_area(raw)


# Normalize UI selections (e.g. "Montaż (PLxx/P)") to base action area labels.
def normalize_area_selection(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.casefold()
    if "total" in normalized or "all" in normalized:
        return None
    for hint, area in AREA_SELECTION_HINTS:
        if hint in normalized:
            return area
    return normalize_action_area(text)


def marker_areas_for_component(component_label: Any) -> set[str] | None:
    if component_label in (None, ""):
        return None
    text = str(component_label).strip()
    if not text:
        return None
    normalized = text.casefold()
    if "total" in normalized or "all" in normalized:
        return None

    areas: set[str] = set()
    normalized_selection = normalize_area_selection(text)
    if normalized_selection:
        areas.add(normalized_selection)

    if "montaż" in normalized or "montaz" in normalized:
        areas.add("Montaż")
        if "subgroup" in normalized or "podgrup" in normalized:
            areas.add("Podgrupa")
    if "podgrup" in normalized:
        areas.add("Podgrupa")
    if "wtrysk" in normalized:
        areas.add("Wtrysk")
    if "metaliz" in normalized or "mzt" in normalized or "mtz" in normalized:
        areas.add("Metalizacja")
    if "inne" in normalized:
        areas.add("Inne")

    return areas or None
