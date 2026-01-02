from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from statistics import median

import altair as alt
import pandas as pd
import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    ProjectRepository,
)
from action_tracking.domain.constants import ACTION_CATEGORIES


@dataclass(frozen=True)
class ParsedAction:
    project_id: str | None
    champion_id: str | None
    status: str
    created: date
    closed: date | None
    due: date | None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            return None


def _current_week_start(today: date) -> date:
    iso_weekday = today.isoweekday()
    return today - timedelta(days=iso_weekday - 1)


def _build_week_buckets(today: date) -> list[dict[str, object]]:
    current_week_start = _current_week_start(today)
    week_starts = [current_week_start + timedelta(days=7 * i) for i in range(-4, 5)]
    buckets: list[dict[str, object]] = []
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


def _prepare_actions(rows: list[dict[str, object]]) -> tuple[list[ParsedAction], int]:
    parsed: list[ParsedAction] = []
    data_issues = 0
    for row in rows:
        created = _parse_date(row.get("created_at"))
        if not created:
            data_issues += 1
            continue
        closed = _parse_date(row.get("closed_at"))
        due = _parse_date(row.get("due_date"))
        parsed.append(
            ParsedAction(
                project_id=row.get("project_id"),
                champion_id=row.get("owner_champion_id"),
                status=row.get("status") or "",
                created=created,
                closed=closed,
                due=due,
            )
        )
    return parsed, data_issues


def _weekly_backlog(actions: list[ParsedAction], today: date) -> pd.DataFrame:
    buckets = _build_week_buckets(today)
    for action in actions:
        if action.status == "cancelled":
            continue
        for bucket in buckets:
            week_end = bucket["week_end"]
            if action.created <= week_end and (action.closed is None or action.closed > week_end):
                bucket["actual_open"] = int(bucket["actual_open"]) + 1
                if action.due is not None and action.due < week_end:
                    bucket["actual_overdue"] = int(bucket["actual_overdue"]) + 1
            if action.created <= week_end and (action.due is None or action.due > week_end):
                bucket["planned_open"] = int(bucket["planned_open"]) + 1

    for bucket in buckets:
        bucket["on_time_open"] = int(bucket["actual_open"]) - int(bucket["actual_overdue"])
    return pd.DataFrame(buckets)


def _open_now(actions: list[ParsedAction], today: date) -> list[ParsedAction]:
    open_actions: list[ParsedAction] = []
    for action in actions:
        if action.status == "cancelled":
            continue
        if action.created <= today and (action.closed is None or action.closed > today):
            open_actions.append(action)
    return open_actions


def render(con: sqlite3.Connection) -> None:
    st.title("KPI")
    st.caption("4 tygodnie wstecz + bieżący + 4 tygodnie w przód")

    repo = ActionRepository(con)
    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)

    projects = project_repo.list_projects(include_counts=False)
    champions = champion_repo.list_champions()

    project_names = {p["id"]: p.get("name") or p["id"] for p in projects}
    champion_names = {c["id"]: c.get("display_name") or c["id"] for c in champions}

    project_options = ["Wszystkie"] + [p["id"] for p in projects]
    champion_options = ["(Wszyscy)"] + [c["id"] for c in champions]
    category_options = ["(Wszystkie)"] + list(ACTION_CATEGORIES)

    filter_col1, filter_col2, filter_col3 = st.columns([1.4, 1.4, 1.2])
    selected_project = filter_col1.selectbox(
        "Projekt",
        project_options,
        index=0,
        format_func=lambda pid: pid
        if pid == "Wszystkie"
        else project_names.get(pid, pid),
    )
    selected_champion = filter_col2.selectbox(
        "Champion",
        champion_options,
        index=0,
        format_func=lambda cid: cid
        if cid == "(Wszyscy)"
        else champion_names.get(cid, cid),
    )
    selected_category = filter_col3.selectbox("Kategoria", category_options, index=0)

    project_filter = None if selected_project == "Wszystkie" else selected_project
    champion_filter = None if selected_champion == "(Wszyscy)" else selected_champion
    category_filter = None if selected_category == "(Wszystkie)" else selected_category

    rows = repo.list_actions_for_kpi(
        project_id=project_filter,
        champion_id=champion_filter,
        category=category_filter,
    )
    actions, data_issues = _prepare_actions(rows)

    today = date.today()
    current_week_start = _current_week_start(today)
    current_week_end = current_week_start + timedelta(days=6)

    open_actions = _open_now(actions, today)
    overdue_actions = [a for a in open_actions if a.due is not None and a.due < today]

    created_this_week = [
        a
        for a in actions
        if a.status != "cancelled" and current_week_start <= a.created <= current_week_end
    ]
    closed_this_week = [
        a
        for a in actions
        if a.status != "cancelled"
        and a.closed is not None
        and current_week_start <= a.closed <= current_week_end
    ]
    on_time_closed = [
        a
        for a in closed_this_week
        if a.due is None or (a.closed is not None and a.closed <= a.due)
    ]

    overdue_rate = (
        round((len(overdue_actions) / len(open_actions)) * 100, 1) if open_actions else 0.0
    )
    on_time_close_rate = (
        round((len(on_time_closed) / len(closed_this_week)) * 100, 1)
        if closed_this_week
        else 0.0
    )

    close_durations = [
        (a.closed - a.created).days for a in actions if a.closed is not None
    ]
    median_close_days = median(close_durations) if close_durations else None

    metric_cols = st.columns(6)
    metric_cols[0].metric("Open (now)", len(open_actions))
    metric_cols[1].metric("Overdue (now)", len(overdue_actions))
    metric_cols[2].metric("Overdue rate %", f"{overdue_rate:.1f}%")
    metric_cols[3].metric("Created this week", len(created_this_week))
    metric_cols[4].metric("Closed this week", len(closed_this_week))
    metric_cols[5].metric("On-time close rate %", f"{on_time_close_rate:.1f}%")

    median_label = f"{median_close_days:.0f} dni" if median_close_days is not None else "—"
    st.caption(f"Median time-to-close: {median_label}")

    if data_issues:
        st.caption(f"Pominięto {data_issues} rekordów z błędną datą.")

    st.subheader("Weekly Backlog")
    weekly_df = _weekly_backlog(actions, today)
    week_order = list(weekly_df["week_label"])

    bars = (
        alt.Chart(weekly_df)
        .transform_fold(
            ["on_time_open", "actual_overdue"],
            as_=["metric", "count"],
        )
        .transform_calculate(
            metric_label=(
                "datum.metric == 'on_time_open' ? 'On-time open' : 'Overdue open'"
            )
        )
        .mark_bar()
        .encode(
            x=alt.X("week_label:N", sort=week_order, title="Week"),
            y=alt.Y("count:Q", title="Count", stack="zero"),
            color=alt.Color(
                "metric_label:N",
                scale=alt.Scale(
                    domain=["On-time open", "Overdue open"],
                    range=["#4c78a8", "#e45756"],
                ),
                title=None,
            ),
            tooltip=[
                alt.Tooltip("week_label:N", title="Week"),
                alt.Tooltip("on_time_open:Q", title="On-time open"),
                alt.Tooltip("actual_overdue:Q", title="Overdue open"),
                alt.Tooltip("actual_open:Q", title="Actual open"),
                alt.Tooltip("planned_open:Q", title="Planned open"),
            ],
        )
    )

    planned_line = (
        alt.Chart(weekly_df)
        .mark_line(color="#222222", strokeDash=[6, 4], strokeWidth=2)
        .encode(
            x=alt.X("week_label:N", sort=week_order),
            y=alt.Y("planned_open:Q", title="Count"),
            tooltip=[
                alt.Tooltip("week_label:N", title="Week"),
                alt.Tooltip("planned_open:Q", title="Planned open"),
            ],
        )
    )

    st.altair_chart(
        (bars + planned_line).properties(height=320),
        use_container_width=True,
    )

    st.subheader("Champion Backlog (current week)")
    chart_col, table_col = st.columns([0.6, 0.4])
    open_by_champion: dict[str, dict[str, int]] = {}
    for action in open_actions:
        key = action.champion_id or "unassigned"
        bucket = open_by_champion.setdefault(key, {"open": 0, "overdue": 0})
        bucket["open"] += 1
        if action.due is not None and action.due < today:
            bucket["overdue"] += 1

    champion_rows = []
    for champion_id, counts in open_by_champion.items():
        champion_label = (
            champion_names.get(champion_id, champion_id)
            if champion_id != "unassigned"
            else "Nieprzypisany"
        )
        champion_rows.append(
            {
                "champion_id": champion_id,
                "Champion": champion_label,
                "On-time open": counts["open"] - counts["overdue"],
                "Overdue open": counts["overdue"],
                "Total open": counts["open"],
            }
        )

    if champion_rows:
        champion_df = pd.DataFrame(champion_rows).sort_values(
            "Total open", ascending=False
        )
    else:
        champion_df = pd.DataFrame(
            columns=["champion_id", "Champion", "On-time open", "Overdue open", "Total open"]
        )
    if not champion_df.empty:
        stacked_champions = champion_df.melt(
            id_vars=["Champion", "Total open"],
            value_vars=["On-time open", "Overdue open"],
            var_name="metric",
            value_name="count",
        )
        champ_chart = (
            alt.Chart(stacked_champions)
            .mark_bar()
            .encode(
                x=alt.X("count:Q", title="Open"),
                y=alt.Y(
                    "Champion:N",
                    sort=champion_df["Champion"].tolist(),
                    title=None,
                ),
                color=alt.Color(
                    "metric:N",
                    scale=alt.Scale(
                        domain=["On-time open", "Overdue open"],
                        range=["#4c78a8", "#e45756"],
                    ),
                    title=None,
                ),
                tooltip=[
                    alt.Tooltip("Champion:N"),
                    alt.Tooltip("metric:N", title="Metric"),
                    alt.Tooltip("count:Q", title="Count"),
                ],
            )
            .properties(height=320)
        )
        with chart_col:
            st.altair_chart(champ_chart, use_container_width=True)
    else:
        with chart_col:
            st.info("Brak otwartych akcji dla wybranych filtrów.")

    with table_col:
        st.markdown("#### KPI detail table")
        view_options = ["Champion", "Projekt"]
        selected_view = st.selectbox("Widok tabeli", view_options, index=0)

        if selected_view == "Projekt":
            group_labels = project_names
            group_key = "project_id"
        else:
            group_labels = champion_names
            group_key = "champion_id"

        open_by_group: dict[str, dict[str, int]] = {}
        for action in open_actions:
            key = getattr(action, group_key) or "unassigned"
            bucket = open_by_group.setdefault(key, {"open": 0, "overdue": 0})
            bucket["open"] += 1
            if action.due is not None and action.due < today:
                bucket["overdue"] += 1

        closed_by_group: dict[str, int] = {}
        for action in closed_this_week:
            key = getattr(action, group_key) or "unassigned"
            closed_by_group[key] = closed_by_group.get(key, 0) + 1

        table_rows: list[dict[str, object]] = []
        for key, counts in open_by_group.items():
            label = (
                group_labels.get(key, key) if key != "unassigned" else "Nieprzypisany"
            )
            total_open = counts["open"]
            overdue = counts["overdue"]
            overdue_pct = round((overdue / total_open) * 100, 1) if total_open else 0.0
            table_rows.append(
                {
                    selected_view: label,
                    "Open": total_open,
                    "Overdue": overdue,
                    "Overdue %": f"{overdue_pct:.1f}%",
                    "Closed this week": closed_by_group.get(key, 0),
                }
            )

        if not table_rows:
            st.info("Brak danych KPI dla wybranych filtrów.")
        else:
            table_df = pd.DataFrame(table_rows).sort_values("Open", ascending=False)
            st.dataframe(table_df.head(10), use_container_width=True, height=320)

    with st.expander("Definicje i założenia", expanded=False):
        st.markdown(
            """
            - **Planned open**: zakładamy zamknięcie w due_date (bez wcześniejszych zamknięć).
            - **Overdue**: due_date < cutoff_date.
            - **Open**: created <= cutoff_date oraz (closed is null lub closed > cutoff_date).
            """
        )
