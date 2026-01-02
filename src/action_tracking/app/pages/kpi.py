from __future__ import annotations

import importlib.util
import sqlite3
from datetime import date, datetime, timedelta
from statistics import median
from typing import Any

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import ActionRepository, ChampionRepository, ProjectRepository
from action_tracking.domain.constants import ACTION_CATEGORIES


def _parse_date(value: Any, issues: list[str], field: str, action_id: str) -> date | None:
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
            issues.append(f"{action_id}:{field}")
            return None


def _week_end(target: date) -> date:
    """Return the Sunday for the ISO week containing target."""
    return target + timedelta(days=(7 - target.isoweekday()))


def _week_start(target: date) -> date:
    return target - timedelta(days=(target.isoweekday() - 1))


def _format_rate(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.1%}"


def _format_days(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.1f} dni"


def _format_count(value: int | None) -> str:
    if value is None:
        return "—"
    return f"{value}"


def render(con: sqlite3.Connection) -> None:
    st.header("KPI")

    repo = ActionRepository(con)
    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)

    projects = project_repo.list_projects(include_counts=False)
    champions = champion_repo.list_champions()

    project_names = {p["id"]: (p.get("name") or p["id"]) for p in projects}
    champion_names = {c["id"]: c["display_name"] for c in champions}

    st.subheader("Filtry")
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1, 1.6, 1.6, 1.6])
    lookback_weeks = filter_col1.selectbox("Lookback (tyg.)", [4, 8, 12, 24, 52], index=2)
    selected_project = filter_col2.selectbox(
        "Projekt",
        ["Wszystkie"] + [p["id"] for p in projects],
        index=0,
        format_func=lambda pid: pid if pid == "Wszystkie" else project_names.get(pid, pid),
    )
    selected_champion = filter_col3.selectbox(
        "Champion",
        ["(Wszyscy)"] + [c["id"] for c in champions],
        index=0,
        format_func=lambda cid: cid if cid == "(Wszyscy)" else champion_names.get(cid, cid),
    )
    selected_category = filter_col4.selectbox(
        "Kategoria",
        ["(Wszystkie)"] + list(ACTION_CATEGORIES),
        index=0,
    )

    project_filter = None if selected_project == "Wszystkie" else selected_project
    champion_filter = None if selected_champion == "(Wszyscy)" else selected_champion
    category_filter = None if selected_category == "(Wszystkie)" else selected_category

    rows = repo.list_actions_for_kpi(
        project_id=project_filter,
        champion_id=champion_filter,
        category=category_filter,
    )

    today = date.today()
    week_end = _week_end(today)
    week_start = _week_start(today)
    lookback_end = week_end
    lookback_start = lookback_end - timedelta(days=7 * (lookback_weeks - 1) + 6)

    parsed_actions: list[dict[str, Any]] = []
    issues: list[str] = []
    for row in rows:
        created_at = _parse_date(row.get("created_at"), issues, "created_at", row.get("id", "?"))
        if created_at is None:
            continue
        parsed_actions.append(
            {
                "id": row.get("id"),
                "status": row.get("status"),
                "created_at": created_at,
                "closed_at": _parse_date(row.get("closed_at"), issues, "closed_at", row.get("id", "?")),
                "due_date": _parse_date(row.get("due_date"), issues, "due_date", row.get("id", "?")),
                "owner_champion_id": row.get("owner_champion_id"),
            }
        )

    open_now = 0
    overdue_now = 0
    closed_this_week = 0
    created_this_week = 0

    closed_in_period: list[dict[str, Any]] = []

    for action in parsed_actions:
        created_at = action["created_at"]
        closed_at = action["closed_at"]
        due_date = action["due_date"]
        status = action["status"]

        is_open_now = (
            created_at <= today
            and (closed_at is None or closed_at > today)
            and status not in ("cancelled",)
        )
        if is_open_now:
            open_now += 1
            if due_date and due_date < today:
                overdue_now += 1

        if closed_at and week_start <= closed_at <= week_end:
            closed_this_week += 1

        if week_start <= created_at <= week_end:
            created_this_week += 1

        if closed_at and lookback_start <= closed_at <= lookback_end:
            closed_in_period.append(action)

    overdue_rate = (overdue_now / open_now) if open_now else None

    on_time_eligible = [a for a in closed_in_period if a["due_date"]]
    on_time_closed = [a for a in on_time_eligible if a["closed_at"] and a["closed_at"] <= a["due_date"]]
    on_time_rate = (len(on_time_closed) / len(on_time_eligible)) if on_time_eligible else None

    close_durations = [
        (a["closed_at"] - a["created_at"]).days
        for a in closed_in_period
        if a["closed_at"]
    ]
    median_close_days = float(median(close_durations)) if close_durations else None

    st.subheader("KPI (teraz)")
    kpi_cols = st.columns(4)
    kpi_cols[0].metric("Otwarte", _format_count(open_now))
    kpi_cols[1].metric("Po terminie", _format_count(overdue_now))
    kpi_cols[2].metric("Overdue rate", _format_rate(overdue_rate))
    kpi_cols[3].metric("Zamknięte w tym tygodniu", _format_count(closed_this_week))

    kpi_cols_2 = st.columns(3)
    kpi_cols_2[0].metric("Utworzone w tym tygodniu", _format_count(created_this_week))
    kpi_cols_2[1].metric("On-time close rate", _format_rate(on_time_rate))
    kpi_cols_2[2].metric("Median time-to-close", _format_days(median_close_days))

    if issues:
        st.caption(
            "Pominięto rekordy z nieprawidłowym formatem daty: "
            f"{len(issues)} pól."
        )

    weekly_weeks = [lookback_start + timedelta(days=7 * i + 6) for i in range(lookback_weeks)]
    weekly_rows: list[dict[str, Any]] = []
    planned_rows: list[dict[str, Any]] = []

    for week_end_date in weekly_weeks:
        actual_open = 0
        actual_overdue = 0
        planned_open = 0

        for action in parsed_actions:
            created_at = action["created_at"]
            closed_at = action["closed_at"]
            due_date = action["due_date"]
            status = action["status"]

            is_actual_open = (
                created_at <= week_end_date
                and (closed_at is None or closed_at > week_end_date)
                and status not in ("cancelled",)
            )
            if is_actual_open:
                actual_open += 1
                if due_date and due_date < week_end_date:
                    actual_overdue += 1

            is_planned_open = (
                created_at <= week_end_date
                and (due_date is None or due_date > week_end_date)
                and status not in ("cancelled",)
            )
            if is_planned_open:
                planned_open += 1

        week_label = f"{week_end_date.isocalendar().year}-W{week_end_date.isocalendar().week:02d}"
        actual_on_time = max(actual_open - actual_overdue, 0)
        weekly_rows.append(
            {
                "week_label": week_label,
                "type": "On-time open",
                "count": actual_on_time,
            }
        )
        weekly_rows.append(
            {
                "week_label": week_label,
                "type": "Overdue open",
                "count": actual_overdue,
            }
        )
        planned_rows.append(
            {
                "week_label": week_label,
                "planned_open": planned_open,
            }
        )

    st.subheader("Weekly backlog (open actions na koniec tygodnia)")

    has_altair = importlib.util.find_spec("altair") is not None
    if has_altair:
        import altair as alt

        weekly_df = pd.DataFrame(weekly_rows)
        planned_df = pd.DataFrame(planned_rows)
        order = [row["week_label"] for row in planned_rows]

        base = alt.Chart(weekly_df).encode(
            x=alt.X("week_label:N", sort=order, title="ISO week (koniec tygodnia)"),
            y=alt.Y("count:Q", title="Liczba otwartych akcji"),
            color=alt.Color(
                "type:N",
                scale=alt.Scale(
                    domain=["On-time open", "Overdue open"],
                    range=["#4C78A8", "#E45756"],
                ),
                legend=alt.Legend(title="Status"),
            ),
        )

        actual_bars = base.mark_bar()
        planned_overlay = (
            alt.Chart(planned_df)
            .mark_bar(
                fillOpacity=0.0,
                stroke="#333333",
                strokeDash=[4, 2],
                strokeWidth=2,
            )
            .encode(
                x=alt.X("week_label:N", sort=order),
                y=alt.Y("planned_open:Q"),
            )
        )

        chart = alt.layer(actual_bars, planned_overlay).resolve_scale(y="shared")
        st.altair_chart(chart, use_container_width=True)
    else:
        weekly_table = pd.DataFrame(planned_rows).set_index("week_label")
        st.bar_chart(weekly_table, height=280)
        st.caption("Altair niedostępny: pokazano tylko planned open.")

    st.subheader("Champion backlog (obecny tydzień)")

    champion_counts: dict[str, dict[str, int]] = {}
    for action in parsed_actions:
        created_at = action["created_at"]
        closed_at = action["closed_at"]
        due_date = action["due_date"]
        status = action["status"]
        if not (
            created_at <= today
            and (closed_at is None or closed_at > today)
            and status not in ("cancelled",)
        ):
            continue

        champion_id = action.get("owner_champion_id") or "(brak)"
        champion_counts.setdefault(champion_id, {"open": 0, "overdue": 0})
        champion_counts[champion_id]["open"] += 1
        if due_date and due_date < today:
            champion_counts[champion_id]["overdue"] += 1

    champion_rows: list[dict[str, Any]] = []
    for champion_id, counts in champion_counts.items():
        total_open = counts["open"]
        overdue_open = counts["overdue"]
        on_time_open = max(total_open - overdue_open, 0)
        champion_label = champion_names.get(champion_id, champion_id)
        champion_rows.append(
            {
                "champion": champion_label,
                "type": "On-time open",
                "count": on_time_open,
                "total_open": total_open,
            }
        )
        champion_rows.append(
            {
                "champion": champion_label,
                "type": "Overdue open",
                "count": overdue_open,
                "total_open": total_open,
            }
        )

    if not champion_rows:
        st.info("Brak otwartych akcji dla wybranych filtrów.")
        return

    champion_df = pd.DataFrame(champion_rows)
    order = (
        champion_df.drop_duplicates("champion")
        .sort_values("total_open", ascending=False)["champion"]
        .tolist()
    )

    if has_altair:
        import altair as alt

        champ_chart = (
            alt.Chart(champion_df)
            .mark_bar()
            .encode(
                x=alt.X("champion:N", sort=order, title="Champion"),
                y=alt.Y("count:Q", title="Liczba otwartych akcji"),
                color=alt.Color(
                    "type:N",
                    scale=alt.Scale(
                        domain=["On-time open", "Overdue open"],
                        range=["#4C78A8", "#E45756"],
                    ),
                    legend=alt.Legend(title="Status"),
                ),
            )
        )
        st.altair_chart(champ_chart, use_container_width=True)
    else:
        fallback = champion_df.pivot(index="champion", columns="type", values="count").fillna(0)
        st.bar_chart(fallback, height=280)
        st.caption("Altair niedostępny: wykres uproszczony.")
