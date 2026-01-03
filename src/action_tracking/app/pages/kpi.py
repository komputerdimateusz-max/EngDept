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

IMPACT_TARGET_PLN = 5000
CLOSE_TARGET = 5
OVERDUE_TOLERANCE = 3


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


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _impact_delta_pln(effect_row: dict[str, Any] | None) -> float | None:
    if not effect_row:
        return None
    metric = effect_row.get("metric")
    if metric not in {"scrap_cost", "scrap_pln", "scrap_cost_pln", "scrap_cost_amount"}:
        return None
    delta = effect_row.get("delta")
    if isinstance(delta, (int, float)):
        return float(delta)
    return None


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

    st.subheader("Champion ranking")
    st.caption("Ranking oparty o wpływ biznesowy, skuteczność i terminowość.")

    ranking_date_to = date.today()
    ranking_date_from = ranking_date_to - timedelta(days=90)

    r1, r2, r3, r4, r5 = st.columns([1.5, 1.2, 1.2, 1.2, 1.1])
    ranking_range = r1.date_input(
        "Zakres dat (zamknięcia)",
        value=(ranking_date_from, ranking_date_to),
    )
    ranking_project = r2.selectbox(
        "Projekt (ranking)",
        project_options,
        index=project_options.index(selected_project),
        format_func=lambda pid: pid if pid == "Wszystkie" else project_names.get(pid, pid),
    )
    ranking_category = r3.selectbox(
        "Kategoria (ranking)",
        category_options,
        index=category_options.index(selected_category),
    )
    include_unassigned = r4.checkbox("Uwzględnij nieprzypisane", value=False)
    show_inactive = r5.checkbox("Pokaż nieaktywnych", value=False)

    if isinstance(ranking_range, tuple) and len(ranking_range) == 2:
        date_from, date_to = ranking_range
    else:
        date_from, date_to = ranking_date_from, ranking_date_to

    ranking_project_filter = None if ranking_project == "Wszystkie" else ranking_project
    ranking_category_filter = None if ranking_category == "(Wszystkie)" else ranking_category

    ranking_rows = repo.list_actions_for_ranking(
        project_id=ranking_project_filter,
        category=ranking_category_filter,
        date_from=date_from,
        date_to=date_to,
    )
    ranking_actions, ranking_issues = _prepare_actions(ranking_rows)
    ranking_effectiveness = effectiveness_repo.list_effectiveness_for_actions(
        [action.action_id for action in ranking_actions]
    )

    cutoff_date = date_to

    effective_labels = {"effective", "no_scrap"}
    champion_stats: dict[str, dict[str, Any]] = {}

    def _ensure_stats(champion_key: str) -> dict[str, Any]:
        if champion_key not in champion_stats:
            champion_stats[champion_key] = {
                "open_now": 0,
                "overdue_now": 0,
                "closed_in_range": 0,
                "closed_with_due": 0,
                "closed_on_time": 0,
                "effectiveness_total": 0,
                "effectiveness_effective": 0,
                "impact_pln": 0.0,
                "durations": [],
            }
        return champion_stats[champion_key]

    for action in ranking_actions:
        if action.status == "cancelled":
            continue
        if not include_unassigned and action.champion_id is None:
            continue
        champion_key = action.champion_id or "unassigned"
        stats = _ensure_stats(champion_key)

        if _open_at_cutoff(action, cutoff_date):
            stats["open_now"] += 1
            if action.due is not None and action.due < cutoff_date:
                stats["overdue_now"] += 1

        if action.closed is not None and date_from <= action.closed <= date_to:
            stats["closed_in_range"] += 1
            if action.due is not None:
                stats["closed_with_due"] += 1
                if action.closed <= action.due:
                    stats["closed_on_time"] += 1
            stats["durations"].append((action.closed - action.created).days)

            effect_row = ranking_effectiveness.get(action.action_id)
            if effect_row:
                stats["effectiveness_total"] += 1
                if effect_row.get("classification") in effective_labels:
                    stats["effectiveness_effective"] += 1
                impact_delta = _impact_delta_pln(effect_row)
                if impact_delta is not None:
                    stats["impact_pln"] += max(0.0, -impact_delta)

    if show_inactive:
        for champ in champions:
            champ_id = champ.get("id")
            if champ_id:
                _ensure_stats(champ_id)

    ranking_rows_out: list[dict[str, Any]] = []
    breakdown_rows: list[dict[str, Any]] = []

    for champion_id, stats in champion_stats.items():
        if not show_inactive and not (
            stats["open_now"] or stats["overdue_now"] or stats["closed_in_range"]
        ):
            continue

        on_time_rate = (
            stats["closed_on_time"] / stats["closed_with_due"]
            if stats["closed_with_due"]
            else None
        )
        effectiveness_rate = (
            stats["effectiveness_effective"] / stats["effectiveness_total"]
            if stats["effectiveness_total"]
            else None
        )
        median_ttc = (
            float(median(stats["durations"])) if stats["durations"] else None
        )
        impact_pln = float(stats["impact_pln"])

        impact_points = 40 * _clamp(impact_pln / IMPACT_TARGET_PLN)
        effective_points = 25 * (effectiveness_rate or 0.0)
        timeliness_points = 20 * (on_time_rate or 0.0)
        volume_points = 15 * _clamp(stats["closed_in_range"] / CLOSE_TARGET)
        overdue_penalty = 15 * _clamp(stats["overdue_now"] / OVERDUE_TOLERANCE)

        score = _clamp(
            impact_points + effective_points + timeliness_points + volume_points - overdue_penalty,
            lower=0.0,
            upper=100.0,
        )

        label = (
            "Nieprzypisany"
            if champion_id == "unassigned"
            else champion_names.get(champion_id, champion_id)
        )
        ranking_rows_out.append(
            {
                "Champion": label,
                "Score": round(score, 1),
                "Open actions now": stats["open_now"],
                "Overdue now": stats["overdue_now"],
                "Closed in range": stats["closed_in_range"],
                "On-time close rate": "—"
                if on_time_rate is None
                else f"{on_time_rate:.1%}",
                "Effective close rate (scrap)": "—"
                if effectiveness_rate is None
                else f"{effectiveness_rate:.1%}",
                "Impact PLN": round(impact_pln, 2),
                "Median time-to-close (days)": "—"
                if median_ttc is None
                else f"{median_ttc:.1f}",
            }
        )
        breakdown_rows.append(
            {
                "Champion": label,
                "Impact": impact_points,
                "Effectiveness": effective_points,
                "Timeliness": timeliness_points,
                "Delivery": volume_points,
                "Overdue penalty": overdue_penalty,
                "Score": score,
            }
        )

    if ranking_rows_out:
        ranking_df = pd.DataFrame(ranking_rows_out).sort_values("Score", ascending=False)
        ranking_df.insert(0, "Rank", range(1, len(ranking_df) + 1))
        st.dataframe(ranking_df, use_container_width=True, height=360)

        top10 = ranking_df.head(10)
        chart = (
            alt.Chart(top10)
            .mark_bar()
            .encode(
                x=alt.X("Score:Q", title="Score (0-100)"),
                y=alt.Y("Champion:N", sort="-x", title=None),
                tooltip=[alt.Tooltip("Champion:N"), alt.Tooltip("Score:Q")],
            )
            .properties(height=280)
        )
        st.altair_chart(chart, use_container_width=True)

        with st.expander("Score breakdown", expanded=False):
            breakdown_df = pd.DataFrame(breakdown_rows).sort_values("Score", ascending=False)
            breakdown_df["Impact"] = breakdown_df["Impact"].map(lambda v: f"{v:.1f}/40")
            breakdown_df["Effectiveness"] = breakdown_df["Effectiveness"].map(lambda v: f"{v:.1f}/25")
            breakdown_df["Timeliness"] = breakdown_df["Timeliness"].map(lambda v: f"{v:.1f}/20")
            breakdown_df["Delivery"] = breakdown_df["Delivery"].map(lambda v: f"{v:.1f}/15")
            breakdown_df["Overdue penalty"] = breakdown_df["Overdue penalty"].map(lambda v: f"-{v:.1f}")
            breakdown_df["Score"] = breakdown_df["Score"].map(lambda v: f"{v:.1f}")
            st.dataframe(
                breakdown_df[
                    [
                        "Champion",
                        "Impact",
                        "Effectiveness",
                        "Timeliness",
                        "Delivery",
                        "Overdue penalty",
                        "Score",
                    ]
                ],
                use_container_width=True,
            )
    else:
        st.info("Brak danych rankingowych dla wybranych filtrów.")

    if ranking_issues:
        st.caption(f"Pominięto {ranking_issues} rekordów z błędną datą w rankingu.")

    with st.expander("Jak liczony jest score", expanded=False):
        st.markdown(
            f"""
- Okno zamknięć: **{date_from.isoformat()} → {date_to.isoformat()}**.
- Cutoff dla otwartych/opóźnionych: **{cutoff_date.isoformat()}**.
- Impact PLN to suma **max(0, -Δ scrap)** z `action_effectiveness` (ujemna delta oznacza oszczędność).
- Składowe score:
  - Impact: `40 × clamp(impact / {IMPACT_TARGET_PLN})`
  - Effectiveness: `25 × skuteczność scrap`
  - Timeliness: `20 × on-time close rate`
  - Delivery: `15 × clamp(closed / {CLOSE_TARGET})`
  - Overdue penalty: `15 × clamp(overdue / {OVERDUE_TOLERANCE})`
- Finalny score: suma składowych minus kara, ograniczona do 0–100.
"""
        )
