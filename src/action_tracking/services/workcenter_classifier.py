from __future__ import annotations

# What changed:
# - Added robust work center area classification with regex search helpers.
# - Introduced a shared classify_wc_area + sanity check to prevent regressions.

import logging
import re
from typing import Any

AREA_ASSEMBLY_MAIN = "assembly_main"
AREA_ASSEMBLY_LINE = "assembly_line"
AREA_SUBGROUP = "subgroup"
AREA_INJECTION = "injection"
AREA_METALIZATION = "metalization"
AREA_OTHER = "other"

AREA_LABELS = {
    AREA_ASSEMBLY_MAIN: "Montaż (PLxx/P)",
    AREA_ASSEMBLY_LINE: "Montaż (PLxx)",
    AREA_SUBGROUP: "Podgrupa (PLxxA)",
    AREA_INJECTION: "Wtrysk (Mxx)",
    AREA_METALIZATION: "Metalizacja (MTZ)",
    AREA_OTHER: "Inne",
}

SCRAP_COMPONENTS = {
    "TOTAL (all)": {
        AREA_ASSEMBLY_MAIN,
        AREA_ASSEMBLY_LINE,
        AREA_SUBGROUP,
        AREA_INJECTION,
        AREA_METALIZATION,
        AREA_OTHER,
    },
    "Montaż (PLxx/P + PLxx + subgroups)": {
        AREA_ASSEMBLY_MAIN,
        AREA_ASSEMBLY_LINE,
        AREA_SUBGROUP,
    },
    "Podgrupy (PLxx[A-Z])": {AREA_SUBGROUP},
    "Wtrysk (Mxx)": {AREA_INJECTION},
    "Metalizacja (MTZ)": {AREA_METALIZATION},
}

KPI_COMPONENTS = {
    "Montaż (PLxx/P)": {AREA_ASSEMBLY_MAIN},
    "Wtrysk (Mxx)": {AREA_INJECTION},
    "Metalizacja (MTZ)": {AREA_METALIZATION},
}

_INJECTION_RE = re.compile(r"M\d{2}", re.IGNORECASE)
_ASSEMBLY_MAIN_RE = re.compile(r"PL\d{2}/P", re.IGNORECASE)
_ASSEMBLY_LINE_RE = re.compile(r"PL\d{2}", re.IGNORECASE)
_SUBGROUP_RE = re.compile(r"PL\d{2}[A-Z]", re.IGNORECASE)
_METALIZATION_RE = re.compile(r"MTZ", re.IGNORECASE)

_LOGGER = logging.getLogger(__name__)


def _normalize_workcenter(value: Any) -> tuple[str, str]:
    raw = str(value or "").replace("\u00a0", " ")
    normalized = " ".join(raw.strip().split()).upper()
    token = re.sub(r"\s+", "", normalized)
    return normalized, token


def _match_code(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    return match.group(0).upper()


def classify_wc_area(wc: str | None) -> str:
    normalized, token = _normalize_workcenter(wc)
    if not token:
        return AREA_OTHER

    if _METALIZATION_RE.search(token):
        return AREA_METALIZATION
    if _INJECTION_RE.search(token):
        return AREA_INJECTION
    if _ASSEMBLY_MAIN_RE.search(token):
        return AREA_ASSEMBLY_MAIN
    if _SUBGROUP_RE.search(token):
        return AREA_SUBGROUP
    if _ASSEMBLY_LINE_RE.search(token):
        return AREA_ASSEMBLY_LINE
    return AREA_OTHER


def classify_workcenter(wc: str | None) -> dict[str, Any]:
    normalized, token = _normalize_workcenter(wc)
    if not token:
        return {"area": AREA_OTHER, "normalized_wc": normalized}

    if _METALIZATION_RE.search(token):
        return {
            "area": AREA_METALIZATION,
            "normalized_wc": normalized,
            "metalization_code": _match_code(_METALIZATION_RE, token) or token,
        }
    injection_code = _match_code(_INJECTION_RE, token)
    if injection_code:
        return {"area": AREA_INJECTION, "normalized_wc": normalized, "injection_code": injection_code}
    assembly_main_code = _match_code(_ASSEMBLY_MAIN_RE, token)
    if assembly_main_code:
        return {
            "area": AREA_ASSEMBLY_MAIN,
            "normalized_wc": normalized,
            "assembly_line_code": assembly_main_code,
        }
    subgroup_code = _match_code(_SUBGROUP_RE, token)
    if subgroup_code:
        return {"area": AREA_SUBGROUP, "normalized_wc": normalized, "subgroup_code": subgroup_code}
    assembly_line_code = _match_code(_ASSEMBLY_LINE_RE, token)
    if assembly_line_code:
        return {
            "area": AREA_ASSEMBLY_LINE,
            "normalized_wc": normalized,
            "assembly_line_code": assembly_line_code,
        }
    return {"area": AREA_OTHER, "normalized_wc": normalized}


def filter_rows_by_areas(
    rows: list[dict[str, Any]],
    areas: set[str] | None,
) -> list[dict[str, Any]]:
    if not areas:
        return rows
    filtered = []
    for row in rows:
        area = classify_wc_area(row.get("work_center") or "")
        if area in areas:
            filtered.append(row)
    return filtered


def extract_injection_machines(rows: list[dict[str, Any]]) -> list[str]:
    machines = set()
    for row in rows:
        payload = classify_workcenter(row.get("work_center") or "")
        if payload.get("area") == AREA_INJECTION:
            code = payload.get("normalized_wc")
            if code:
                machines.add(code)
    return sorted(machines)


def classification_sanity_check() -> list[str]:
    samples = {
        "PL01/P": AREA_ASSEMBLY_MAIN,
        "PL01": AREA_ASSEMBLY_LINE,
        "PL01A": AREA_SUBGROUP,
        "M12": AREA_INJECTION,
        "MTZ": AREA_METALIZATION,
        "": AREA_OTHER,
    }
    mismatches = []
    for sample, expected in samples.items():
        actual = classify_wc_area(sample)
        if actual != expected:
            mismatches.append(f"{sample} -> {actual} (expected {expected})")
    if mismatches:
        _LOGGER.warning("Workcenter classification sanity check failed: %s", mismatches)
    return mismatches
