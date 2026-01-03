from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

# Optional imports (don't crash if modules moved)
try:
    from action_tracking.domain.constants import ACTION_CATEGORIES as DEFAULT_ACTION_CATEGORIES
except Exception:  # pragma: no cover
    DEFAULT_ACTION_CATEGORIES = [
        "Scrap reduction",
        "OEE improvement",
        "Cost savings",
        "Vave",
        "PDP",
        "Development",
    ]

try:
    from action_tracking.services.normalize import normalize_key
except Exception:  # pragma: no cover
    def normalize_key(value: str) -> str:
        return (value or "").strip().lower()


# =====================================================
# HELPERS
# =====================================================

def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    cur = con.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table,),
    )
    return cur.fetchone() is not None


def _normalize_impact_aspects_payload(value: Any) -> str | None:
    """
    impact_aspects stored as JSON string in DB.
    If normalize_impact_aspects exists, use it; otherwise accept list/str.
    """
    try:
        from action_tracking.services.impact_aspects import normalize_impact_aspects
        normalized = normalize_impact_aspects(value)
        if not normalized:
            return None
        return json.dumps(normalized, ensure_ascii=False)
    except Exception:
        if value in (None, "", []):
            return None
        if isinstance(value, str):
            v = value.strip()
            if v.startswith("[") and v.endswith("]"):
                return v
            return json.dumps([v], ensure_ascii=False)
        if isinstance(value, (list, tuple, set)):
            arr = [str(x).strip() for x in value if str(x).strip()]
            return json.dumps(sorted(set(arr)), ensure_ascii=False) if arr else None
        return None


def _safe_get(row: dict[str, Any], key: str, default: Any = None) -> Any:
    return row[key] if key in row else default


# =====================================================
# SETTINGS / GLOBAL RULES
# =====================================================

class SettingsRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_action_categories(self, active_only: bool = True) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "action_categories"):
            return [
                {"id": name, "name": name, "is_active": True, "sort_order": (i + 1) * 10, "created_at": None}
                for i, name in enumerate(DEFAULT_ACTION_CATEGORIES)
            ]

        query = """
            SELECT id, name, is_active, sort_order, created_at
            FROM action_categories
        """
        params: list[Any] = []
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY sort_order ASC, name ASC"
        cur = self.con.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["is_active"] = bool(r.get("is_active"))
        return rows


class GlobalSettingsRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def get_category_rules(self, only_active: bool = True) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "category_rules"):
            return []

        query = """
            SELECT category AS category_label,
                   effect_model AS effectiveness_model,
                   savings_model,
                   requires_scope_link,
                   description,
                   is_active
            FROM category_rules
        """
        params: list[Any] = []
        if only_active:
            query += " WHERE is_active = 1"
        query += " ORDER BY category ASC"
        cur = self.con.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["requires_scope_link"] = bool(r.get("requires_scope_link"))
            r["is_active"] = bool(r.get("is_active"))
        return rows

    def resolve_category_rule(self, category_label: str) -> dict[str, Any] | None:
        if not category_label:
            return None
        rules = self.get_category_rules(only_active=True)
        rules_map = {normalize_key(r.get("category_label") or ""): r for r in rules}
        return rules_map.get(normalize_key(category_label))


# =====================================================
# NOTIFICATIONS (email log)
# =====================================================

class NotificationRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def was_sent(self, unique_key: str) -> bool:
        if not unique_key:
            return False
        if not _table_exists(self.con, "email_notifications_log"):
            return False
        try:
            cur = self.con.execute(
                """
                SELECT 1
                FROM email_notifications_log
                WHERE unique_key = ?
                LIMIT 1
                """,
                (unique_key,),
            )
            return cur.fetchone() is not None
        except sqlite3.Error:
            return False


# =====================================================
# ACTIONS
# =====================================================

class ActionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_actions(
        self,
        status: str | None = None,
        project_id: str | None = None,
        champion_id: str | None = None,
        is_draft: bool | None = None,
        overdue_only: bool = False,
        search_text: str | None = None,
    ) -> list[dict[str, Any]]:
        base_query = """
            SELECT a.*,
                   p.name AS project_name,
                   TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, '')) AS owner_name
            FROM actions a
            LEFT JOIN projects p ON p.id = a.project_id
            LEFT JOIN champions ch ON ch.id = a.owner_champion_id
        """
        filters: list[str] = []
        params: list[Any] = []
        today = date.today().isoformat()

        if status:
            filters.append("a.status = ?")
            params.append(status)
        if project_id:
            filters.append("a.project_id = ?")
            params.append(project_id)
        if champion_id:
            filters.append("a.owner_champion_id = ?")
            params.append(champion_id)
        if is_draft is not None:
            filters.append("a.is_draft = ?")
            params.append(1 if is_draft else 0)
        if overdue_only:
            filters.append(
                "a.due_date IS NOT NULL AND a.due_date < ? AND a.status NOT IN ('done','cancelled')"
            )
            params.append(today)
        if search_text:
            filters.append("a.title LIKE ?")
            params.append(f"%{search_text.strip()}%")

        if filters:
            base_query += " WHERE " + " AND ".join(filters)

        base_query += """
            ORDER BY
                CASE
                    WHEN a.due_date IS NOT NULL
                         AND a.due_date < ?
                         AND a.status NOT IN ('done','cancelled')
                    THEN 0 ELSE 1 END,
                a.due_date IS NULL,
                a.due_date,
                a.created_at DESC
        """
        params.append(today)

        cur = self.con.execute(base_query, params)
        return [dict(r) for r in cur.fetchall()]

    # ---- REQUIRED BY KPI PAGE ----
    def list_actions_for_kpi(
        self,
        project_id: str | None = None,
        champion_id: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Minimal schema used by KPI page.
        """
        query = """
            SELECT id,
                   title,
                   created_at,
                   closed_at,
                   due_date,
                   status,
                   owner_champion_id,
                   category,
                   project_id,
                   impact_aspects
            FROM actions
            WHERE is_draft = 0
        """
        filters: list[str] = []
        params: list[Any] = []
        if project_id:
            filters.append("project_id = ?")
            params.append(project_id)
        if champion_id:
            filters.append("owner_champion_id = ?")
            params.append(champion_id)
        if category:
            filters.append("category = ?")
            params.append(category)
        if filters:
            query += " AND " + " AND ".join(filters)
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    # ---- REQUIRED BY Champions ranking v2 ----
    def list_actions_for_ranking(
        self,
        project_id: str | None = None,
        category: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT a.id,
                   a.title,
                   a.owner_champion_id,
                   a.project_id,
                   p.name AS project_name,
                   a.category,
                   a.status,
                   a.created_at,
                   a.due_date,
                   a.closed_at,
                   a.manual_savings_amount,
                   a.manual_savings_currency,
                   a.manual_savings_note,
                   ae.metric AS effectiveness_metric,
                   ae.delta AS effectiveness_delta
            FROM actions a
            LEFT JOIN projects p ON p.id = a.project_id
            LEFT JOIN action_effectiveness ae ON ae.action_id = a.id
            WHERE a.created_at IS NOT NULL AND a.is_draft = 0
        """
        filters: list[str] = []
        params: list[Any] = []
        if project_id:
            filters.append("a.project_id = ?")
            params.append(project_id)
        if category:
            filters.append("a.category = ?")
            params.append(category)
        if date_to:
            filters.append("date(a.created_at) <= date(?)")
            params.append(date_to.isoformat())
        if date_from:
            filters.append(
                "(date(a.created_at) >= date(?) OR (a.closed_at IS NOT NULL AND date(a.closed_at) >= date(?)))"
            )
            params.extend([date_from.isoformat(), date_from.isoformat()])
        if filters:
            query += " AND " + " AND ".join(filters)
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


# =====================================================
# EFFECTIVENESS
# =====================================================

class EffectivenessRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con


# =====================================================
# PROJECTS
# =====================================================

class ProjectRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_projects(self, include_counts: bool = True) -> list[dict[str, Any]]:
        if include_counts:
            cur = self.con.execute(
                """
                SELECT p.id,
                       p.name,
                       p.type,
                       p.owner_champion_id,
                       p.status,
                       p.created_at,
                       p.closed_at,
                       p.work_center,
                       p.project_code,
                       p.project_sop,
                       p.project_eop,
                       p.related_work_center,
                       COUNT(a.id) AS actions_total,
                       COALESCE(SUM(CASE WHEN a.status IN ('done','cancelled') THEN 1 ELSE 0 END), 0) AS actions_closed,
                       COALESCE(SUM(CASE WHEN a.status NOT IN ('done','cancelled') THEN 1 ELSE 0 END), 0) AS actions_open
                FROM projects p
                LEFT JOIN actions a ON a.project_id = p.id AND a.is_draft = 0
                GROUP BY p.id
                ORDER BY p.name
                """
            )
            return [dict(r) for r in cur.fetchall()]

        cur = self.con.execute("SELECT * FROM projects ORDER BY name")
        return [dict(r) for r in cur.fetchall()]

    # ---- REQUIRED BY projects.py (wc inbox) ----
    def list_project_work_centers_norms(self, include_related: bool = True) -> set[str]:
        """
        Returns set of normalized work centers used by existing projects.
        """
        try:
            from action_tracking.services.effectiveness import normalize_wc, parse_work_centers
        except Exception:
            def normalize_wc(v: Any) -> str:
                return normalize_key(str(v or ""))

            def parse_work_centers(_: Any, value: Any) -> list[str]:
                if not value:
                    return []
                return [t.strip() for t in str(value).split(",") if t.strip()]

        if not _table_exists(self.con, "projects"):
            return set()

        cur = self.con.execute("SELECT work_center, related_work_center FROM projects")
        norms: set[str] = set()
        for row in cur.fetchall():
            rowd = dict(row)
            primary = normalize_wc(rowd.get("work_center"))
            if primary:
                norms.add(primary)
            if include_related:
                for token in parse_work_centers(None, rowd.get("related_work_center")):
                    n = normalize_wc(token)
                    if n:
                        norms.add(n)
        return norms


# =====================================================
# CHAMPIONS
# =====================================================

class ChampionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_champions(self) -> list[dict[str, Any]]:
        """
        Must include hire_date to avoid KeyError in champions page.
        If DB doesn't have the column yet, return None.
        """
        # check if hire_date exists
        hire_date_exists = False
        try:
            cur = self.con.execute("PRAGMA table_info(champions)")
            cols = [r[1] for r in cur.fetchall()]  # (cid, name, type, notnull, dflt_value, pk)
            hire_date_exists = "hire_date" in cols
        except sqlite3.Error:
            hire_date_exists = False

        fields = "id, first_name, last_name, email, team, active"
        if hire_date_exists:
            fields += ", hire_date"

        cur = self.con.execute(
            f"""
            SELECT {fields}
            FROM champions
            ORDER BY last_name, first_name
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["display_name"] = f"{r.get('first_name','')} {r.get('last_name','')}".strip() or r.get("id")
            r["active"] = bool(r.get("active"))
            if "hire_date" not in r:
                r["hire_date"] = None
        return rows

    def get_assigned_projects(self, champion_id: str) -> list[str]:
        if not _table_exists(self.con, "champion_projects"):
            return []
        cur = self.con.execute(
            """
            SELECT project_id
            FROM champion_projects
            WHERE champion_id = ?
            ORDER BY project_id
            """,
            (champion_id,),
        )
        return [row["project_id"] for row in cur.fetchall()]


# =====================================================
# PRODUCTION DATA (placeholder; you already have it working)
# =====================================================

class ProductionDataRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

# =====================================================
# WC INBOX (required by projects.py)
# Fix: ImportError: cannot import name 'WcInboxRepository'
# Wklej CAŁY ten blok do: src/action_tracking/data/repositories.py
# (najlepiej na sam koniec pliku)
# =====================================================

class WcInboxRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_from_production(
        self,
        work_centers_stats: list[dict[str, Any]],
        existing_project_wc_norms: set[str],
    ) -> None:
        """
        work_centers_stats: list of dicts produced by ProductionDataRepository.list_production_work_centers_with_stats()
        Expected keys (best effort):
          - wc_raw, wc_norm
          - has_scrap (bool), has_kpi (bool)
          - first_seen_date, last_seen_date
        """
        if not _table_exists(self.con, "wc_inbox"):
            return

        # normalize helpers (avoid crash if module path changed)
        try:
            from action_tracking.services.effectiveness import normalize_wc
        except Exception:
            def normalize_wc(v: Any) -> str:
                return normalize_key(str(v or ""))

        # read existing inbox rows
        existing_rows: dict[str, dict[str, Any]] = {}
        cur = self.con.execute(
            """
            SELECT wc_norm, wc_raw, sources, status, first_seen_date, last_seen_date
            FROM wc_inbox
            """
        )
        for r in cur.fetchall():
            existing_rows[r["wc_norm"]] = dict(r)

        now = datetime.now(timezone.utc).isoformat()

        for row in work_centers_stats or []:
            wc_norm = normalize_wc(row.get("wc_norm") or row.get("wc_raw"))
            if not wc_norm:
                continue

            existing = existing_rows.get(wc_norm)

            # if already linked to project WC -> mark linked (optional)
            if wc_norm in (existing_project_wc_norms or set()):
                if existing and existing.get("status") == "open":
                    self._set_status(wc_norm, "linked", None)
                continue

            sources: list[str] = []
            if row.get("has_scrap"):
                sources.append("scrap")
            if row.get("has_kpi"):
                sources.append("kpi")

            if existing:
                try:
                    prev_sources = json.loads(existing.get("sources") or "[]")
                except json.JSONDecodeError:
                    prev_sources = []
                sources = sorted(set(prev_sources) | set(sources))

            # choose wc_raw (prefer earlier first_seen)
            wc_raw_value = (row.get("wc_raw") or "").strip()
            if existing and existing.get("wc_raw"):
                if existing.get("first_seen_date") and row.get("first_seen_date"):
                    if str(existing["first_seen_date"]) <= str(row["first_seen_date"]):
                        wc_raw_value = existing.get("wc_raw") or wc_raw_value
                else:
                    wc_raw_value = existing.get("wc_raw") or wc_raw_value

            payload = {
                "id": str(uuid4()),
                "wc_raw": wc_raw_value,
                "wc_norm": wc_norm,
                "sources": json.dumps(sources, ensure_ascii=False),
                "first_seen_date": row.get("first_seen_date"),
                "last_seen_date": row.get("last_seen_date"),
                "status": "open",
                "linked_project_id": None,
                "created_at": now,
                "updated_at": now,
            }

            self.con.execute(
                """
                INSERT INTO wc_inbox (
                    id, wc_raw, wc_norm, sources,
                    first_seen_date, last_seen_date,
                    status, linked_project_id,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wc_norm) DO UPDATE SET
                    wc_raw = excluded.wc_raw,
                    sources = excluded.sources,
                    first_seen_date = COALESCE(
                        MIN(wc_inbox.first_seen_date, excluded.first_seen_date),
                        excluded.first_seen_date,
                        wc_inbox.first_seen_date
                    ),
                    last_seen_date = COALESCE(
                        MAX(wc_inbox.last_seen_date, excluded.last_seen_date),
                        excluded.last_seen_date,
                        wc_inbox.last_seen_date
                    ),
                    updated_at = excluded.updated_at
                """,
                (
                    payload["id"],
                    payload["wc_raw"],
                    payload["wc_norm"],
                    payload["sources"],
                    payload["first_seen_date"],
                    payload["last_seen_date"],
                    payload["status"],
                    payload["linked_project_id"],
                    payload["created_at"],
                    payload["updated_at"],
                ),
            )

        self.con.commit()

    def list_open(self, limit: int = 200) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "wc_inbox"):
            return []
        cur = self.con.execute(
            """
            SELECT *
            FROM wc_inbox
            WHERE status = 'open'
            ORDER BY last_seen_date DESC, wc_raw ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            try:
                r["sources"] = json.loads(r.get("sources") or "[]")
            except json.JSONDecodeError:
                r["sources"] = []
        return rows

    def ignore(self, wc_norm: str) -> None:
        if not _table_exists(self.con, "wc_inbox"):
            return
        self._set_status(wc_norm, "ignored", None)

    def link_to_project(self, wc_norm: str, project_id: str) -> None:
        if not _table_exists(self.con, "wc_inbox"):
            return
        self._set_status(wc_norm, "linked", project_id)

    def mark_created(self, wc_norm: str, project_id: str) -> None:
        if not _table_exists(self.con, "wc_inbox"):
            return
        self._set_status(wc_norm, "created", project_id)

    def _set_status(self, wc_norm: str, status: str, project_id: str | None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.con.execute(
            """
            UPDATE wc_inbox
            SET status = ?,
                linked_project_id = ?,
                updated_at = ?
            WHERE wc_norm = ?
            """,
            (status, project_id, now, wc_norm),
        )
        self.con.commit()

