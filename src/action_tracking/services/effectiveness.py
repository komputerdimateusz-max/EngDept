from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any


def normalize_wc(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        try:
            return datetime.fromisoformat(str(value)).date()
        except ValueError:
            return None


def parse_work_centers(primary: str | None, related: str | None) -> list[str]:
    centers: list[str] = []

    primary_value = normalize_wc(primary)
    if primary_value:
        centers.append(primary_value)

    if related:
        tokens = re.split(r"[,;|\n]+", related)
        for token in tokens:
            center = normalize_wc(token)
            if center and center not in centers:
                centers.append(center)

    return centers


def suggest_work_centers(
    target: str,
    candidates: list[str],
    limit: int = 8,
) -> list[str]:
    normalized_target = normalize_wc(target)
    if not normalized_target:
        return []

    scored: list[tuple[int, int, str]] = []
    for candidate in candidates:
        normalized_candidate = normalize_wc(candidate)
        if not normalized_candidate:
            continue
        if normalized_candidate == normalized_target:
            return [candidate]
        if normalized_candidate.startswith(normalized_target) or normalized_target.startswith(
            normalized_candidate
        ):
            score = 1
        elif normalized_target in normalized_candidate:
            score = 2
        else:
            score = 3 + abs(len(normalized_candidate) - len(normalized_target))
        length_diff = abs(len(normalized_candidate) - len(normalized_target))
        scored.append((score, length_diff, candidate))

    scored.sort(key=lambda item: (item[0], item[1], item[2]))
    return [candidate for _, _, candidate in scored[:limit]]


def compute_scrap_effectiveness(
    action: dict[str, Any],
    work_centers: list[str],
    scrap_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    closed_date = parse_date(action.get("closed_at"))
    if closed_date is None:
        return None

    baseline_from = closed_date - timedelta(days=14)
    baseline_to = closed_date - timedelta(days=1)
    after_from = closed_date + timedelta(days=1)
    after_to = closed_date + timedelta(days=14)

    computed_at = datetime.now(timezone.utc).isoformat()
    base_payload = {
        "metric": "scrap_qty",
        "baseline_from": baseline_from.isoformat(),
        "baseline_to": baseline_to.isoformat(),
        "after_from": after_from.isoformat(),
        "after_to": after_to.isoformat(),
        "baseline_days": 0,
        "after_days": 0,
        "baseline_avg": None,
        "after_avg": None,
        "delta": None,
        "pct_change": None,
        "classification": "unknown",
        "computed_at": computed_at,
    }

    if not work_centers:
        return base_payload

    daily_qty: dict[date, int] = defaultdict(int)
    for row in scrap_rows:
        metric_date = parse_date(row.get("metric_date"))
        if metric_date is None:
            continue
        daily_qty[metric_date] += int(row.get("scrap_qty") or 0)

    def _window_stats(start: date, end: date) -> tuple[int, float | None]:
        dates = [d for d in daily_qty if start <= d <= end]
        if not dates:
            return 0, None
        total = sum(daily_qty[d] for d in dates)
        return len(dates), total / len(dates)

    baseline_days, baseline_avg = _window_stats(baseline_from, baseline_to)
    after_days, after_avg = _window_stats(after_from, after_to)

    delta = None
    if baseline_avg is not None and after_avg is not None:
        delta = after_avg - baseline_avg

    pct_change = None
    classification = "insufficient_data"
    if baseline_days >= 5 and after_days >= 5:
        if baseline_avg == 0:
            if after_avg == 0:
                classification = "no_scrap"
            else:
                classification = "worse"
        else:
            pct_change = (after_avg - baseline_avg) / baseline_avg
            if pct_change <= -0.10:
                classification = "effective"
            elif pct_change < 0.10:
                classification = "no_change"
            else:
                classification = "worse"

    if pct_change is None and baseline_avg not in (None, 0) and after_avg is not None:
        pct_change = (after_avg - baseline_avg) / baseline_avg

    return {
        **base_payload,
        "baseline_days": baseline_days,
        "after_days": after_days,
        "baseline_avg": baseline_avg,
        "after_avg": after_avg,
        "delta": delta,
        "pct_change": pct_change,
        "classification": classification,
    }
