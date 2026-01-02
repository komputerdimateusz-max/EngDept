from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from statistics import median
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    EffectivenessRepository,
    ProjectRepository,
    SettingsRepository,
)


@dataclass(frozen=True)
class ParsedAction:
    action_id: str
    project_id: str | None
    champion_id: str | None
    status: str
    created: date
    closed: date | None
    due: date | None


def _parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value)
    try:
        return date.fromisoformat(s)
    except ValueError:
        try:
            return datetime.fromisoformat(s).date()
        except ValueError:
            return None


def _current_week_start(today: date) -> date:
    # ISO Monday start
    return today - timedelta(days=today.isoweekday() - 1)


def _build_week_buckets(today: date) -> list[dict[str, Any]]:
    """
    Fixed horizon: 4 weeks back + current + 4 weeks forward (always 9 buckets).
    Buckets are pre-created with zeros so X-axis is stable even with no data.
    """
    current_week_start = _current_week_start(today)
    week_starts = [current_week_start + timedelta(days=7 * i) for i in range(-4, 5)]
    buckets: list[dict[str, Any]] = []
    for week_start in week_starts:
        week_end = week_start + timedelta(days=6)
        iso_year, iso_week, _ = week_start.isocalendar()
        buckets.append(
            {
                "week_start": week_start,
                "week_end": week_end,
                "week_label": f"{iso_year}-W{iso_week:02d}",
                "actual_open": 0,
                "actual_overdue": 0,
                "planned_open": 0,
            }
        )
    return buckets


def _prepare_actions(rows: list[dict[str, Any]]) -> tuple[list[ParsedAction], int]:
    parsed: list[ParsedAction] = []
    issues = 0
    for row in rows:
        created = _parse_date(row.get("created_at"))
        if created is None:
            issues += 1
            continue
        closed = _parse_date(row.get("closed_at"))
        due = _parse_date(row.get("due_date"))
        parsed.append(
            ParsedAction(
                action_id=str(row.get("id", "")),
                project_id=row.get("project_id"),
                champion_id=row.get("owner_champion_id"),
                status=str(row.get("status") or ""),
                created=created,
                closed=closed,
                due=due,
            )
        )
    return parsed, issues


def _open_at_cutoff(action: ParsedAction, cutoff: date) -> bool:
    if action.status == "cancelled":
        return False
    return action.created <= cutoff and (action.closed is None or action.closed > cutoff)


def _weekly_backlog(actions: list[ParsedAction], today: date) -> pd.DataFrame:
    buckets = _build_week_buckets(today)

    for action in actions:
        if action.status == "cancelled":
            continue

        for bucket in buckets:
            week_end: date = bucket["week_end"]

            # ACTUAL OPEN at end of week
            if action.created <= week_end and (action.closed is None or action.closed > week_end):
                bucket["actual_open"] += 1
                if action.due is not None and action.due < week_end:
                    bucket["actual_overdue"] += 1

            # PLANNED OPEN at end of week (assume closure exactly at due_date; no early closures)
            if action.created <= week_end and (action.due is None or action.due > week_end):
                bucket["planned_open"] += 1

    for bucket in buckets:
        bucket["on_time_open"] = max(int(bucket["actual_open"]) - int(bucket["actual_overdue"]), 0)

    return pd.DataFrame(buckets)


def render(con: sqlite3.Connection) -> None:
    st.title("KPI")
    st.caption("4 tygodnie wstecz + bieżący + 4 tygodnie w przód")

    repo = ActionRepository(con)
    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)
    effectiveness_repo = EffectivenessRepository(con)
    settings_repo = SettingsRepository(con)

    projects = project_repo.list_projects(include_counts=False)
    champions = champion_repo.list_champions()

    project_names = {p["id"]: (p.get("name") or p.get("project_name") or p["id"]) for p in projects}
    champion_names = {c["id"]: (c.get("display_name") or c.get("name") or c["id"]) for c in champions}

    # Filters (no lookback here; fixed 9-week axis)
    project_options = ["Wszystkie"] + [p["id"] for p in projects]
    champion_options = ["(Wszyscy)"] + [c["id"] for c in champions]
    active_categories = [c["name"] for c in settings_repo.list_action_categories(active_only=True)]
    category_options = ["(Wszystkie)"] + active_categories

    st.subheader("Filtry")
    f1, f2, f3 = st.columns([1.6, 1.6, 1.2])
    selected_project = f1.selectbox(
        "Projekt",
        project_options,
        index=0,
        format_func=lambda pid: pid if pid == "Wszystkie" else project_names.get(pid, pid),
    )
    selected_champion = f2.selectbox(
        "Champion",
        champion_options,
        index=0,
        format_func=lambda cid: cid if cid == "(Wszyscy)" else champion_names.get(cid, cid),
    )
    selected_category = f3.selectbox("Kategoria", category_options, index=0)

    project_filter = None if selected_project == "Wszystkie" else selected_project
    champion_filter = None if selected_champion == "(Wszyscy)" else selected_champion
    category_filter = None if selected_category == "(Wszystkie)" else selected_category

    rows = repo.list_actions_for_kpi(
        project_id=project_filter,
        champion_id=champion_filter,
        category=category_filter,
    )
    actions, data_issues = _prepare_actions(rows)
    effectiveness_map = effectiveness_repo.get_effectiveness_for_actions(
        [str(row.get("id")) for row in rows]
    )

    today = date.today()
    current_week_start = _current_week_start(today)
    current_week_end = current_week_start + timedelta(days=6)

    open_now = [a for a in actions if _open_at_cutoff(a, today)]
    overdue_now = [a for a in open_now if a.due is not None and a.due < today]

    created_this_week = [
        a for a in actions if a.status != "cancelled" and current_week_start <= a.created <= current_week_end
    ]
    closed_this_week = [
        a
        for a in actions
        if a.status != "cancelled"
        and a.closed is not None
        and current_week_start <= a.closed <= current_week_end
    ]

    # On-time close rate: only actions with due_date are eligible
    eligible_closed = [a for a in closed_this_week if a.due is not None and a.closed is not None]
    on_time_closed = [a for a in eligible_closed if a.closed <= a.due]  # type: ignore[operator]

    overdue_rate = (len(overdue_now) / len(open_now)) if open_now else None
    on_time_rate = (len(on_time_closed) / len(eligible_closed)) if eligible_closed else None

    close_durations = [(a.closed - a.created).days for a in actions if a.closed is not None]
    median_close_days = float(median(close_durations)) if close_durations else None

    # KPI tiles
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Otwarte (teraz)", f"{len(open_now)}")
    k2.metric("Po terminie (teraz)", f"{len(overdue_now)}")
    k3.metric("Overdue rate", "—" if overdue_rate is None else f"{overdue_rate:.1%}")
    k4.metric("Utworzone w tym tyg.", f"{len(created_this_week)}")
    k5.metric("Zamknięte w tym tyg.", f"{len(closed_this_week)}")
    k6.metric("On-time close rate", "—" if on_time_rate is None else f"{on_time_rate:.1%}")

    st.caption(f"Median time-to-close: {'—' if median_close_days is None else f'{median_close_days:.1f} dni'}")
    if data_issues:
        st.caption(f"Pominięto {data_issues} rekordów z błędną datą.")

    scrap_actions = [
        row
        for row in rows
        if row.get("category") == "Scrap reduction"
        and row.get("status") == "done"
        and row.get("closed_at")
    ]
    scrap_effectiveness = [
        effectiveness_map.get(row.get("id") or "", {}).get("classification")
        for row in scrap_actions
    ]
    effective_count = sum(1 for c in scrap_effectiveness if c == "effective")
    no_change_count = sum(1 for c in scrap_effectiveness if c == "no_change")
    worse_count = sum(1 for c in scrap_effectiveness if c == "worse")
    insufficient_count = sum(1 for c in scrap_effectiveness if c == "insufficient_data")
    eligible_effective = effective_count + no_change_count + worse_count
    effective_rate = (effective_count / eligible_effective) if eligible_effective else None

    st.subheader("Scrap effectiveness (done)")
    e1, e2, e3, e4 = st.columns(4)
    e1.metric("Effective actions (scrap)", f"{effective_count}")
    e2.metric("Ineffective actions (scrap)", f"{worse_count}")
    e3.metric("Insufficient data", f"{insufficient_count}")
    e4.metric("Effective rate", "—" if effective_rate is None else f"{effective_rate:.1%}")

    worse_rows: list[dict[str, Any]] = []
    for row in scrap_actions:
        effect = effectiveness_map.get(row.get("id") or "")
        if not effect or effect.get("classification") != "worse":
            continue
        pct_change = effect.get("pct_change")
        worse_rows.append(
            {
                "Action": row.get("title") or "—",
                "Project": project_names.get(row.get("project_id"), row.get("project_id") or "—"),
                "Pct change": "—" if not isinstance(pct_change, (int, float)) else f"{pct_change:.0%}",
                "Baseline avg": effect.get("baseline_avg"),
                "After avg": effect.get("after_avg"),
                "_pct_change_value": pct_change if isinstance(pct_change, (int, float)) else None,
            }
        )

    if worse_rows:
        worse_rows = sorted(
            worse_rows,
            key=lambda item: item["_pct_change_value"] or 0,
            reverse=True,
        )
        st.subheader("Top worse actions")
        st.dataframe(
            [{k: v for k, v in row.items() if not k.startswith("_")} for row in worse_rows[:5]],
            use_container_width=True,
        )

    # Weekly chart (fixed 9 weeks)
    st.subheader("Weekly backlog (otwarte akcje na koniec tygodnia)")
    weekly_df = _weekly_backlog(actions, today)
    week_order = weekly_df["week_label"].tolist()

    stacked = (
        alt.Chart(weekly_df)
        .transform_fold(["on_time_open", "actual_overdue"], as_=["metric", "count"])
        .transform_calculate(
            metric_label="datum.metric == 'on_time_open' ? 'On-time open' : 'Overdue open'"
        )
        .mark_bar()
        .encode(
            x=alt.X("week_label:N", sort=week_order, title="ISO week"),
            y=alt.Y("count:Q", title="Liczba otwartych akcji", stack="zero"),
            color=alt.Color(
                "metric_label:N",
                scale=alt.Scale(domain=["On-time open", "Overdue open"], range=["#4C78A8", "#E45756"]),
                legend=alt.Legend(title=None),
            ),
            tooltip=[
                alt.Tooltip("week_label:N", title="Tydzień"),
                alt.Tooltip("on_time_open:Q", title="On-time open"),
                alt.Tooltip("actual_overdue:Q", title="Overdue open"),
                alt.Tooltip("actual_open:Q", title="Actual open"),
                alt.Tooltip("planned_open:Q", title="Planned open"),
            ],
        )
    )

    planned_outline = (
        alt.Chart(weekly_df)
        .mark_bar(fillOpacity=0.0, stroke="#333333", strokeDash=[4, 2], strokeWidth=2)
        .encode(
            x=alt.X("week_label:N", sort=week_order),
            y=alt.Y("planned_open:Q"),
            tooltip=[
                alt.Tooltip("week_label:N", title="Tydzień"),
                alt.Tooltip("planned_open:Q", title="Planned open"),
            ],
        )
    )

    st.altair_chart(alt.layer(stacked, planned_outline).properties(height=320), use_container_width=True)

    # Champion chart (current week / now)
    st.subheader("Champion backlog (obecny tydzień)")
    chart_col, table_col = st.columns([0.6, 0.4])

    open_by_champion: dict[str, dict[str, int]] = {}
    for a in open_now:
        key = a.champion_id or "unassigned"
        open_by_champion.setdefault(key, {"open": 0, "overdue": 0})
        open_by_champion[key]["open"] += 1
        if a.due is not None and a.due < today:
            open_by_champion[key]["overdue"] += 1

    champion_rows: list[dict[str, Any]] = []
    for champion_id, counts in open_by_champion.items():
        label = "Nieprzypisany" if champion_id == "unassigned" else champion_names.get(champion_id, champion_id)
        total_open = counts["open"]
        overdue_open = counts["overdue"]
        champion_rows.append(
            {"Champion": label, "type": "On-time open", "count": max(total_open - overdue_open, 0), "total": total_open}
        )
        champion_rows.append(
            {"Champion": label, "type": "Overdue open", "count": overdue_open, "total": total_open}
        )

    with chart_col:
        if not champion_rows:
            st.info("Brak otwartych akcji dla wybranych filtrów.")
        else:
            champ_df = pd.DataFrame(champion_rows)
            order = (
                champ_df.drop_duplicates("Champion")
                .sort_values("total", ascending=False)["Champion"]
                .tolist()
            )
            champ_chart = (
                alt.Chart(champ_df)
                .mark_bar()
                .encode(
                    x=alt.X("count:Q", title="Otwarte"),
                    y=alt.Y("Champion:N", sort=order, title=None),
                    color=alt.Color(
                        "type:N",
                        scale=alt.Scale(domain=["On-time open", "Overdue open"], range=["#4C78A8", "#E45756"]),
                        legend=alt.Legend(title=None),
                    ),
                    tooltip=[alt.Tooltip("Champion:N"), alt.Tooltip("type:N"), alt.Tooltip("count:Q")],
                )
                .properties(height=320)
            )
            st.altair_chart(champ_chart, use_container_width=True)

    with table_col:
        st.markdown("#### KPI detail table (Top 10)")
        view = st.selectbox("Widok tabeli", ["Champion", "Projekt"], index=0)

        if view == "Projekt":
            group_labels = project_names
            get_key = lambda a: a.project_id or "unassigned"
        else:
            group_labels = champion_names
            get_key = lambda a: a.champion_id or "unassigned"

        open_by_group: dict[str, dict[str, int]] = {}
        for a in open_now:
            k = get_key(a)
            open_by_group.setdefault(k, {"open": 0, "overdue": 0})
            open_by_group[k]["open"] += 1
            if a.due is not None and a.due < today:
                open_by_group[k]["overdue"] += 1

        closed_by_group: dict[str, int] = {}
        for a in closed_this_week:
            k = get_key(a)
            closed_by_group[k] = closed_by_group.get(k, 0) + 1

        rows_out: list[dict[str, Any]] = []
        for k, counts in open_by_group.items():
            label = "Nieprzypisany" if k == "unassigned" else group_labels.get(k, k)
            total_open = counts["open"]
            overdue = counts["overdue"]
            overdue_pct = (overdue / total_open) if total_open else None
            rows_out.append(
                {
                    view: label,
                    "Open": total_open,
                    "Overdue": overdue,
                    "Overdue %": "—" if overdue_pct is None else f"{overdue_pct:.1%}",
                    "Closed this week": closed_by_group.get(k, 0),
                }
            )

        if not rows_out:
            st.info("Brak danych KPI dla wybranych filtrów.")
        else:
            df_out = pd.DataFrame(rows_out).sort_values("Open", ascending=False)
            st.dataframe(df_out.head(10), use_container_width=True, height=320)

    with st.expander("Definicje i założenia", expanded=False):
        st.markdown(
            """
- **Planned open**: zakładamy zamknięcie w `due_date` (bez wcześniejszych zamknięć).
- **Overdue**: `due_date < cutoff_date`.
- **Open**: `created <= cutoff_date` oraz (`closed is null` lub `closed > cutoff_date`).
"""
        )
