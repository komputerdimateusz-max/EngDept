from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import ActionRepository


def _parse_action_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None


def _build_week_buckets() -> list[dict[str, object]]:
    today = date.today()
    current_week_start = today - timedelta(days=today.isoweekday() - 1)
    week_starts = [current_week_start + timedelta(days=7 * i) for i in range(-4, 5)]
    buckets: list[dict[str, object]] = []
    for week_start in week_starts:
        week_end = week_start + timedelta(days=6)
        iso_year, iso_week, _ = week_start.isocalendar()
        label = f"{iso_year}-W{iso_week:02d}"
        buckets.append(
            {
                "label": label,
                "week_start": week_start,
                "week_end": week_end,
                "count": 0,
            }
        )
    return buckets


def render(con: sqlite3.Connection) -> None:
    st.header("KPI")

    repo = ActionRepository(con)
    actions = repo.list_actions()

    buckets = _build_week_buckets()
    for action in actions:
        if action.get("status") in {"done", "cancelled"}:
            continue
        created_at = _parse_action_date(action.get("created_at"))
        if not created_at:
            continue
        for bucket in buckets:
            if bucket["week_start"] <= created_at <= bucket["week_end"]:
                bucket["count"] = int(bucket["count"]) + 1
                break

    st.subheader("Weekly backlog")
    st.caption("Stałe okno: bieżący tydzień ± 4 tygodnie (ISO).")

    chart_data = pd.DataFrame(
        {
            "Week": [bucket["label"] for bucket in buckets],
            "Backlog": [bucket["count"] for bucket in buckets],
        }
    )
    st.bar_chart(chart_data.set_index("Week"))
