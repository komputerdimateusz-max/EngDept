from __future__ import annotations

from typing import Any
import unicodedata


_ALLOWED_AREAS = {"Montaż", "Wtrysk", "Metalizacja", "Podgrupa", "Inne"}

_AREA_ALIASES: dict[str, str] = {
    "montaz": "Montaż",
    "montaz_main": "Montaż",
    "montaz_line": "Montaż",
    "assembly": "Montaż",
    "assembly_main": "Montaż",
    "assembly_line": "Montaż",
    "wtrysk": "Wtrysk",
    "injection": "Wtrysk",
    "metalizacja": "Metalizacja",
    "metalization": "Metalizacja",
    "metalisation": "Metalizacja",
    "podgrupa": "Podgrupa",
    "podgrupy": "Podgrupa",
    "subgroup": "Podgrupa",
    "subgroups": "Podgrupa",
    "inne": "Inne",
    "other": "Inne",
    "unknown": "Inne",
}


def _normalize_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    stripped = "".join(char for char in normalized if not unicodedata.combining(char))
    return stripped.casefold().strip()


def normalize_area(value: Any) -> str:
    if value in (None, ""):
        return "Inne"
    raw = str(value).strip()
    if not raw:
        return "Inne"
    key = _normalize_key(raw)
    if key in _AREA_ALIASES:
        return _AREA_ALIASES[key]
    if raw in _ALLOWED_AREAS:
        return raw
    allowed_lookup = {_normalize_key(area): area for area in _ALLOWED_AREAS}
    if key in allowed_lookup:
        return allowed_lookup[key]
    return "Inne"


def scrap_component_to_allowed_areas(value: Any) -> set[str] | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = _normalize_key(text)
    if "total" in normalized or "all" in normalized:
        return None
    if "montaz" in normalized and ("podgrup" in normalized or "subgroup" in normalized):
        return {"Montaż", "Podgrupa"}
    if "montaz" in normalized:
        return {"Montaż"}
    if "podgrup" in normalized or "subgroup" in normalized:
        return {"Podgrupa"}
    if "wtrysk" in normalized:
        return {"Wtrysk"}
    if "metalizacja" in normalized or "mzt" in normalized or "mtz" in normalized:
        return {"Metalizacja"}
    if "inne" in normalized:
        return {"Inne"}
    return None


def kpi_area_to_allowed_areas(value: Any) -> set[str] | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = _normalize_key(text)
    if "total" in normalized or "all" in normalized:
        return None
    if "montaz" in normalized:
        return {"Montaż"}
    if "podgrup" in normalized or "subgroup" in normalized:
        return {"Podgrupa"}
    if "wtrysk" in normalized:
        return {"Wtrysk"}
    if "metalizacja" in normalized or "mzt" in normalized or "mtz" in normalized:
        return {"Metalizacja"}
    if "inne" in normalized:
        return {"Inne"}
    return None
