from __future__ import annotations

import re


def normalize_key(value: str | None) -> str:
    if not value:
        return ""
    cleaned = value.replace("\ufeff", "").replace("\u00a0", " ").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.casefold()
