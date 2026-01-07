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

def parse_impact_aspects_from_db(value: Any) -> list[str]:
    if value in (None, ""):
        return []

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return [text]
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
            return [str(parsed).strip()] if str(parsed).strip() else []
        return [text]

    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]

    return []


def serialize_impact_aspects_to_db(value: Any) -> str | None:
    if value in (None, ""):
        return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return json.dumps([text], ensure_ascii=False)
            if isinstance(parsed, list):
                cleaned = [str(item).strip() for item in parsed if str(item).strip()]
                return json.dumps(cleaned, ensure_ascii=False) if cleaned else None
        return json.dumps([text], ensure_ascii=False)

    if isinstance(value, (list, tuple, set)):
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        return json.dumps(cleaned, ensure_ascii=False) if cleaned else None

    return None
