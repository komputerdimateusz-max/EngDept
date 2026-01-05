from __future__ import annotations

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

_INJECTION_RE = re.compile(r"^M\d{2}$")
_ASSEMBLY_MAIN_RE = re.compile(r"^PL\d{2}/P$")
_ASSEMBLY_LINE_RE = re.compile(r"^PL\d{2}$")
_SUBGROUP_RE = re.compile(r"^PL\d{2}[A-Z]$")


def _normalize_workcenter(value: Any) -> tuple[str, str]:
    raw = str(value or "").replace("\u00a0", " ")
    normalized = " ".join(raw.strip().split()).upper()
    token = re.sub(r"\s+", "", normalized)
    return normalized, token


def classify_workcenter(wc: str | None) -> dict[str, Any]:
    normalized, token = _normalize_workcenter(wc)
    if not token:
        return {"area": AREA_OTHER, "normalized_wc": normalized}

    if "MTZ" in token:
        return {"area": AREA_METALIZATION, "normalized_wc": normalized, "metalization_code": token}
    if _INJECTION_RE.match(token):
        return {"area": AREA_INJECTION, "normalized_wc": normalized, "injection_code": token}
    if _ASSEMBLY_MAIN_RE.match(token):
        return {"area": AREA_ASSEMBLY_MAIN, "normalized_wc": normalized, "assembly_line_code": token}
    if _ASSEMBLY_LINE_RE.match(token):
        return {"area": AREA_ASSEMBLY_LINE, "normalized_wc": normalized, "assembly_line_code": token}
    if _SUBGROUP_RE.match(token):
        return {"area": AREA_SUBGROUP, "normalized_wc": normalized, "subgroup_code": token}
    return {"area": AREA_OTHER, "normalized_wc": normalized}


def filter_rows_by_areas(
    rows: list[dict[str, Any]],
    areas: set[str] | None,
) -> list[dict[str, Any]]:
    if not areas:
        return rows
    filtered = []
    for row in rows:
        area = classify_workcenter(row.get("work_center") or "").get("area")
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
