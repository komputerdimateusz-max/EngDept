from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from statistics import median
from typing import Any

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import (
    ActionRepository,
    ChampionRepository,
    GlobalSettingsRepository,
    ProjectRepository,
    SettingsRepository,
)

DELIVERY_WEIGHT = 0.55
IMPACT_WEIGHT = 0.45

OVERDUE_PENALTY_PER = 5
OVERDUE_PENALTY_CAP = 40
OPEN_TOLERANCE = 5
OPEN_PENALTY_PER = 2
OPEN_PENALTY_CAP = 20
ON_TIME_BASELINE = 0.7
ON_TIME_BONUS_CAP = 20
TTC_BASELINE_DAYS = 30
TTC_PENALTY_PER_DAY = 0.5
TTC_PENALTY_CAP = 20

MISSING_MANUAL_PENALTY_PER = 2
MISSING_MANUAL_PENALTY_CAP = 20


@dataclass(frozen=True)
class RankingAction:
    action_id: str
    title: str
    project_id: str | None
    project_name: str | None
    champion_id: str | None
    category: str | None
    status: str
    created: date
    closed: date | None
    due: date | None
    manual_savings_amount: float | None
    manual_savings_currency: str | None
    effectiveness_metric: str | None
    effectiveness_delta: float | None


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


def _parse_actions(rows: list[dict[str, Any]]) -> tuple[list[RankingAction], int]:
    parsed: list[RankingAction] = []
    issues = 0
    for row in rows:
        created = _parse_date(row.get("created_at"))
        if created is None:
            issues += 1
            continue
        parsed.append(
            RankingAction(
                action_id=str(row.get("id", "")),
                title=str(row.get("title") or ""),
                project_id=row.get("project_id"),
                project_name=row.get("project_name"),
                champion_id=row.get("owner_champion_id"),
                category=row.get("category"),
                status=str(row.get("status") or ""),
                created=created,
                closed=_parse_date(row.get("closed_at")),
                due=_parse_date(row.get("due_date")),
                manual_savings_amount=row.get("manual_savings_amount"),
                manual_savings_currency=row.get("manual_savings_currency"),
                effectiveness_metric=row.get("effectiveness_metric"),
                effectiveness_delta=row.get("effectiveness_delta"),
            )
        )
    return parsed, issues


def _impact_delta_pln(action: RankingAction) -> float | None:
    metric = action.effectiveness_metric
    if metric not in {"scrap_cost", "scrap_pln", "scrap_cost_pln", "scrap_cost_amount"}:
        return None
    delta = action.effectiveness_delta
    if isinstance(delta, (int, float)):
        return float(delta)
    return None


def _clamp(value: float, lower: float = 0.0, upper: float = 100.0) -> float:
    return max(lower, min(upper, value))


def _closed_in_window(action: RankingAction, date_from: date | None, date_to: date | None) -> bool:
    if action.closed is None:
        return False
    if date_from and action.closed < date_from:
        return False
    if date_to and action.closed > date_to:
        return False
    return True


def _open_in_window(action: RankingAction, date_from: date | None, date_to: date | None) -> bool:
    if action.status in {"done", "cancelled"} or action.closed is not None:
        return False
    if date_from and action.created < date_from:
        return False
    if date_to and action.created > date_to:
        return False
    return True


def _delivery_score(
    open_now: int,
    overdue_now: int,
    on_time_rate: float,
    median_ttc_days: float | None,
) -> float:
    overdue_penalty = min(OVERDUE_PENALTY_CAP, overdue_now * OVERDUE_PENALTY_PER)
    open_penalty = min(OPEN_PENALTY_CAP, max(0, open_now - OPEN_TOLERANCE) * OPEN_PENALTY_PER)
    on_time_bonus = _clamp((on_time_rate - ON_TIME_BASELINE) * 100, -ON_TIME_BONUS_CAP, ON_TIME_BONUS_CAP)
    ttc_value = median_ttc_days or 0.0
    ttc_penalty = _clamp((ttc_value - TTC_BASELINE_DAYS) * TTC_PENALTY_PER_DAY, 0, TTC_PENALTY_CAP)
    return _clamp(100 - overdue_penalty - open_penalty - ttc_penalty + on_time_bonus)


def _impact_score(impact_pln: float, max_pln: float, missing_manual: int) -> float:
    if max_pln <= 0:
        base = 0.0
    else:
        base = 100 * (impact_pln / max_pln)
    missing_penalty = min(MISSING_MANUAL_PENALTY_CAP, missing_manual * MISSING_MANUAL_PENALTY_PER)
    return _clamp(base - missing_penalty)


def render(con: sqlite3.Connection) -> None:
    st.title("Champions ranking")
    st.caption("Transparentny ranking championów oparty o reguły kategorii i okna czasowe.")

    action_repo = ActionRepository(con)
    project_repo = ProjectRepository(con)
    champion_repo = ChampionRepository(con)
    settings_repo = SettingsRepository(con)
    rules_repo = GlobalSettingsRepository(con)

    today = date.today()

    timeframe_options = {
        "Last 90 days": 90,
        "Last 180 days": 180,
        "One year": 365,
        "Total": None,
    }

    projects = project_repo.list_projects(include_counts=False)
    champions = champion_repo.list_champions()

    project_names = {p["id"]: (p.get("name") or p.get("project_name") or p["id"]) for p in projects}
    champion_names = {c["id"]: (c.get("display_name") or c.get("name") or c["id"]) for c in champions}

    active_rule_rows = rules_repo.get_category_rules(only_active=True)
    active_categories = [row["category_label"] for row in active_rule_rows]
    if not active_categories:
        active_categories = [c["name"] for c in settings_repo.list_action_categories(active_only=True)]

    st.subheader("Filters")
    c1, c2, c3, c4 = st.columns([1.2, 1.4, 1.3, 1.1])
    selected_timeframe = c1.selectbox("Timeframe", list(timeframe_options.keys()), index=0)
    project_options = ["(All)"] + [p["id"] for p in projects]
    category_options = ["(All)"] + active_categories
    selected_project = c2.selectbox(
        "Project",
        project_options,
        index=0,
        format_func=lambda pid: pid if pid == "(All)" else project_names.get(pid, pid),
    )
    selected_category = c3.selectbox("Category", category_options, index=0)
    include_unassigned = c4.checkbox("Include unassigned", value=False)

    date_from: date | None
    date_to: date | None
    window_days = timeframe_options[selected_timeframe]
    if window_days is None:
        date_from = None
        date_to = None
    else:
        date_to = today
        date_from = today - timedelta(days=window_days)

    project_filter = None if selected_project == "(All)" else selected_project
    category_filter = None if selected_category == "(All)" else selected_category

    ranking_rows = action_repo.list_actions_for_ranking(
        project_id=project_filter,
        category=category_filter,
        date_from=date_from,
        date_to=date_to,
    )
    ranking_actions, ranking_issues = _parse_actions(ranking_rows)

    champion_stats: dict[str, dict[str, Any]] = {}
    action_details: list[dict[str, Any]] = []

    def _ensure_stats(champion_key: str) -> dict[str, Any]:
        if champion_key not in champion_stats:
            champion_stats[champion_key] = {
                "open_now": 0,
                "overdue_now": 0,
                "closed_in_window": 0,
                "closed_on_time": 0,
                "durations": [],
                "impact_pln": 0.0,
                "impact_eur": 0.0,
                "missing_manual": 0,
                "missing_scope": 0,
            }
        return champion_stats[champion_key]

    for action in ranking_actions:
        if action.status == "cancelled":
            continue
        if not include_unassigned and action.champion_id is None:
            continue
        champion_key = action.champion_id or "unassigned"
        stats = _ensure_stats(champion_key)

        rule = rules_repo.resolve_category_rule(action.category or "")
        savings_model = (rule or {}).get("savings_model", "NONE")
        requires_scope = bool((rule or {}).get("requires_scope_link"))

        if requires_scope and not action.project_id:
            stats["missing_scope"] += 1

        is_open = _open_in_window(action, date_from, date_to)
        is_closed = _closed_in_window(action, date_from, date_to)

        impact_pln = 0.0
        impact_eur = 0.0
        missing_manual = False

        if is_open:
            stats["open_now"] += 1
            if action.due and action.due < today:
                stats["overdue_now"] += 1

        if is_closed:
            stats["closed_in_window"] += 1
            if action.due is None or action.closed <= action.due:
                stats["closed_on_time"] += 1
            if action.closed and action.created:
                stats["durations"].append((action.closed - action.created).days)

            if savings_model == "AUTO_SCRAP_COST":
                impact_delta = _impact_delta_pln(action)
                if impact_delta is not None:
                    impact_pln = max(0.0, -impact_delta)
            elif savings_model == "MANUAL_REQUIRED":
                manual_amount = action.manual_savings_amount
                currency = (action.manual_savings_currency or "").upper()
                if isinstance(manual_amount, (int, float)):
                    if currency == "PLN":
                        impact_pln = max(0.0, float(manual_amount))
                    elif currency == "EUR":
                        impact_eur = max(0.0, float(manual_amount))
                else:
                    missing_manual = True

            stats["impact_pln"] += impact_pln
            stats["impact_eur"] += impact_eur
            if missing_manual:
                stats["missing_manual"] += 1

        action_details.append(
            {
                "champion_key": champion_key,
                "id": action.action_id,
                "title": action.title,
                "project_name": action.project_name,
                "category": action.category,
                "created": action.created,
                "closed": action.closed,
                "due": action.due,
                "status": action.status,
                "is_open": is_open,
                "is_closed": is_closed,
                "impact_pln": impact_pln,
                "impact_eur": impact_eur,
                "missing_manual": missing_manual,
            }
        )

    if include_unassigned and "unassigned" not in champion_stats:
        _ensure_stats("unassigned")

    max_pln = max((stats["impact_pln"] for stats in champion_stats.values()), default=0.0)

    leaderboard_rows: list[dict[str, Any]] = []
    for champion_id, stats in champion_stats.items():
        if not (stats["open_now"] or stats["overdue_now"] or stats["closed_in_window"]):
            continue

        closed_total = stats["closed_in_window"]
        on_time_rate = stats["closed_on_time"] / closed_total if closed_total else 0.0
        median_ttc = float(median(stats["durations"])) if stats["durations"] else None

        delivery_score = _delivery_score(
            open_now=stats["open_now"],
            overdue_now=stats["overdue_now"],
            on_time_rate=on_time_rate,
            median_ttc_days=median_ttc,
        )
        impact_score = _impact_score(stats["impact_pln"], max_pln, stats["missing_manual"])
        total_score = round(DELIVERY_WEIGHT * delivery_score + IMPACT_WEIGHT * impact_score, 1)

        label = (
            "Unassigned"
            if champion_id == "unassigned"
            else champion_names.get(champion_id, champion_id)
        )
        scope_note = ""
        if stats["missing_scope"] >= 3:
            scope_note = f"⚠️ {stats['missing_scope']} missing scope"
        elif stats["missing_scope"]:
            scope_note = f"{stats['missing_scope']} missing scope"

        leaderboard_rows.append(
            {
                "Champion": label,
                "Open now": stats["open_now"],
                "Overdue now": stats["overdue_now"],
                "Closed (window)": stats["closed_in_window"],
                "On-time % (window)": round(on_time_rate * 100, 1) if closed_total else None,
                "Median TTC (days)": round(median_ttc, 1) if median_ttc is not None else None,
                "Impact PLN (window)": round(stats["impact_pln"], 2),
                "Impact EUR (window)": round(stats["impact_eur"], 2),
                "Delivery Score": round(delivery_score, 1),
                "Impact Score": round(impact_score, 1),
                "Total Score": total_score,
                "Scope issues": scope_note,
                "Champion key": champion_id,
                "Missing manual": stats["missing_manual"],
                "On-time rate": on_time_rate,
                "Median TTC raw": median_ttc,
            }
        )

    st.subheader("KPI (overall)")
    total_open = sum(stats["open_now"] for stats in champion_stats.values())
    total_overdue = sum(stats["overdue_now"] for stats in champion_stats.values())
    total_closed = sum(stats["closed_in_window"] for stats in champion_stats.values())
    total_on_time = sum(stats["closed_on_time"] for stats in champion_stats.values())
    total_durations: list[int] = []
    for stats in champion_stats.values():
        total_durations.extend(stats["durations"])
    total_impact_pln = sum(stats["impact_pln"] for stats in champion_stats.values())
    total_impact_eur = sum(stats["impact_eur"] for stats in champion_stats.values())

    on_time_rate_total = total_on_time / total_closed if total_closed else 0.0
    median_ttc_total = float(median(total_durations)) if total_durations else None

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Open actions now", total_open)
    k2.metric("Overdue actions now", total_overdue)
    k3.metric("Closed in window", total_closed)
    k4.metric("On-time close rate", f"{on_time_rate_total:.1%}" if total_closed else "—")
    k5.metric("Median time-to-close", f"{median_ttc_total:.1f} days" if median_ttc_total else "—")
    k6.metric("Total savings PLN", f"{total_impact_pln:,.0f}")

    if total_impact_eur:
        st.metric("Total savings EUR", f"{total_impact_eur:,.0f}")

    if ranking_issues:
        st.caption(f"Skipped {ranking_issues} actions with invalid created_at.")

    with st.expander("Show methodology", expanded=False):
        st.markdown(
            f"""
**Time window**: {selected_timeframe} (closed actions by `closed_at`, open actions by `created_at`).

**Delivery metrics**
- Open now: open actions created in window (or all open for Total).
- Overdue now: open actions with `due_date < today`.
- Closed in window: `closed_at` within window (status != cancelled).
- On-time close rate: closed actions with `due_date` missing or `closed_at <= due_date`.
- Median TTC: median of `closed_at - created_at` in days.

**Impact metrics**
- AUTO_SCRAP_COST: `action_effectiveness.delta` (scrap cost) when closed in window.
- MANUAL_REQUIRED: manual savings amount when closed in window (PLN or EUR).
- Open actions show impact as pending (not counted).

**Scoring**
- Delivery Score:
  - overdue_penalty = min(40, overdue_now * 5)
  - open_penalty = min(20, max(0, open_now - 5) * 2)
  - on_time_bonus = clamp((on_time_rate - 0.7) * 100, -20, +20)
  - ttc_penalty = clamp((median_ttc_days - 30) * 0.5, 0, 20)
  - delivery = clamp(100 - overdue_penalty - open_penalty - ttc_penalty + on_time_bonus, 0, 100)
- Impact Score:
  - impact = 100 * (champion_pln / max_pln)
  - missing_penalty = min(20, missing_manual_count * 2)
  - impact = clamp(impact - missing_penalty, 0, 100)
- Total Score = {DELIVERY_WEIGHT:.2f} × Delivery + {IMPACT_WEIGHT:.2f} × Impact
"""
        )

    st.subheader("Leaderboard")
    if leaderboard_rows:
        leaderboard_df = pd.DataFrame(leaderboard_rows)
        leaderboard_df = leaderboard_df.sort_values("Total Score", ascending=False)
        leaderboard_df.insert(0, "Rank", range(1, len(leaderboard_df) + 1))
        st.dataframe(
            leaderboard_df[
                [
                    "Rank",
                    "Champion",
                    "Open now",
                    "Overdue now",
                    "Closed (window)",
                    "On-time % (window)",
                    "Median TTC (days)",
                    "Impact PLN (window)",
                    "Impact EUR (window)",
                    "Delivery Score",
                    "Impact Score",
                    "Total Score",
                    "Scope issues",
                ]
            ],
            use_container_width=True,
            height=360,
        )
    else:
        st.info("No ranking data for selected filters.")

    st.subheader("Drilldown")
    if not leaderboard_rows:
        st.info("Select a champion once data is available.")
        return

    champion_keys = [row["Champion key"] for row in leaderboard_rows]
    default_key = champion_keys[0] if champion_keys else "unassigned"
    selected_champion = st.selectbox(
        "Champion to inspect",
        champion_keys,
        index=0,
        format_func=lambda cid: "Unassigned" if cid == "unassigned" else champion_names.get(cid, cid),
    )

    selected_stats = champion_stats.get(selected_champion, {})
    selected_label = (
        "Unassigned"
        if selected_champion == "unassigned"
        else champion_names.get(selected_champion, selected_champion)
    )

    if selected_stats:
        st.markdown(
            f"""
**{selected_label} – scoring notes**
- Open now: {selected_stats.get("open_now", 0)}
- Overdue now: {selected_stats.get("overdue_now", 0)}
- Closed in window: {selected_stats.get("closed_in_window", 0)}
- Missing manual savings: {selected_stats.get("missing_manual", 0)}
- Missing scope links: {selected_stats.get("missing_scope", 0)}
"""
        )

    champ_actions = [a for a in action_details if a["champion_key"] == selected_champion]
    impact_actions = [
        a
        for a in champ_actions
        if a["is_closed"] and a["impact_pln"] > 0
    ]
    impact_actions.sort(key=lambda a: abs(a["impact_pln"]), reverse=True)

    overdue_actions = [
        a
        for a in champ_actions
        if a["is_open"] and a["due"] is not None and a["due"] < today
    ]
    for action in overdue_actions:
        action["days_overdue"] = (today - action["due"]).days
    overdue_actions.sort(key=lambda a: a.get("days_overdue", 0), reverse=True)

    closed_actions = [a for a in champ_actions if a["is_closed"]]
    closed_actions.sort(key=lambda a: a["closed"] or date.min, reverse=True)

    st.markdown("**Top impact actions**")
    if impact_actions:
        impact_df = pd.DataFrame(
            [
                {
                    "Action ID": a["id"],
                    "Title": a["title"],
                    "Category": a["category"],
                    "Project": a["project_name"],
                    "Closed at": a["closed"],
                    "Savings PLN": round(a["impact_pln"], 2),
                }
                for a in impact_actions
            ]
        )
        st.dataframe(impact_df, use_container_width=True, height=260)
    else:
        st.caption("No closed actions with PLN impact in the selected window.")

    st.markdown("**Worst / risk actions (overdue open)**")
    if overdue_actions:
        overdue_df = pd.DataFrame(
            [
                {
                    "Action ID": a["id"],
                    "Title": a["title"],
                    "Category": a["category"],
                    "Project": a["project_name"],
                    "Due date": a["due"],
                    "Days overdue": a.get("days_overdue", 0),
                }
                for a in overdue_actions
            ]
        )
        st.dataframe(overdue_df, use_container_width=True, height=260)
    else:
        st.caption("No overdue open actions in the selected window.")

    st.markdown("**Recently closed actions**")
    if closed_actions:
        closed_df = pd.DataFrame(
            [
                {
                    "Action ID": a["id"],
                    "Title": a["title"],
                    "Category": a["category"],
                    "Project": a["project_name"],
                    "Closed at": a["closed"],
                    "On time": "Yes" if a["due"] is None or (a["closed"] and a["closed"] <= a["due"]) else "No",
                    "TTC (days)": (a["closed"] - a["created"]).days if a["closed"] else None,
                }
                for a in closed_actions
            ]
        )
        st.dataframe(closed_df, use_container_width=True, height=260)
    else:
        st.caption("No closed actions in the selected window.")
