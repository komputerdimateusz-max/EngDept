from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

# Optional imports (don’t crash app if some modules moved during merges)
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


def _as_bool(v: Any) -> bool:
    return bool(int(v)) if isinstance(v, (int, float, str)) and str(v).isdigit() else bool(v)


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
            # accept already-json or single token
            v = value.strip()
            if v.startswith("[") and v.endswith("]"):
                return v
            return json.dumps([v], ensure_ascii=False)
        if isinstance(value, (list, tuple, set)):
            arr = [str(x).strip() for x in value if str(x).strip()]
            return json.dumps(sorted(set(arr)), ensure_ascii=False) if arr else None
        return None


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

    def create_action_category(self, name: str, sort_order: int | None) -> str:
        clean = (name or "").strip()
        if not clean:
            raise ValueError("Nazwa kategorii jest wymagana.")
        if not _table_exists(self.con, "action_categories"):
            raise sqlite3.OperationalError("action_categories table missing; migration required")

        exists = self.con.execute("SELECT 1 FROM action_categories WHERE name = ?", (clean,)).fetchone()
        if exists:
            raise ValueError("Kategoria o tej nazwie już istnieje.")

        cid = str(uuid4())
        self.con.execute(
            """
            INSERT INTO action_categories (id, name, is_active, sort_order, created_at)
            VALUES (?, ?, 1, ?, ?)
            """,
            (cid, clean, int(sort_order) if sort_order is not None else 100, datetime.now(timezone.utc).isoformat()),
        )
        self.con.commit()
        return cid

    def update_action_category(self, category_id: str, name: str, is_active: bool, sort_order: int) -> None:
        if not _table_exists(self.con, "action_categories"):
            raise sqlite3.OperationalError("action_categories table missing; migration required")
        clean = (name or "").strip()
        if not clean:
            raise ValueError("Nazwa kategorii jest wymagana.")
        exists = self.con.execute(
            "SELECT 1 FROM action_categories WHERE name = ? AND id != ?",
            (clean, category_id),
        ).fetchone()
        if exists:
            raise ValueError("Kategoria o tej nazwie już istnieje.")
        self.con.execute(
            """
            UPDATE action_categories
            SET name = ?, is_active = ?, sort_order = ?
            WHERE id = ?
            """,
            (clean, 1 if is_active else 0, int(sort_order), category_id),
        )
        self.con.commit()

    def deactivate_action_category(self, category_id: str) -> None:
        if not _table_exists(self.con, "action_categories"):
            return
        self.con.execute("UPDATE action_categories SET is_active = 0 WHERE id = ?", (category_id,))
        self.con.commit()


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

    def upsert_category_rule(self, category: str, payload: dict[str, Any]) -> None:
        if not _table_exists(self.con, "category_rules"):
            raise sqlite3.OperationalError("category_rules table missing; migration required")

        cat = (category or "").strip()
        if not cat:
            raise ValueError("Nazwa kategorii jest wymagana.")

        description = (payload.get("description") or "").strip() or None
        if description and len(description) > 500:
            raise ValueError("Opis metodologii nie może przekraczać 500 znaków.")

        rule = {
            "category": cat,
            "effect_model": (payload.get("effect_model") or payload.get("effectiveness_model") or "NONE").strip(),
            "savings_model": (payload.get("savings_model") or "NONE").strip(),
            "requires_scope_link": bool(payload.get("requires_scope_link")),
            "is_active": bool(payload.get("is_active", True)),
            "description": description,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        self.con.execute(
            """
            INSERT INTO category_rules (
                category, effect_model, savings_model,
                requires_scope_link, is_active, description, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(category) DO UPDATE SET
                effect_model = excluded.effect_model,
                savings_model = excluded.savings_model,
                requires_scope_link = excluded.requires_scope_link,
                is_active = excluded.is_active,
                description = excluded.description,
                updated_at = excluded.updated_at
            """,
            (
                rule["category"],
                rule["effect_model"],
                rule["savings_model"],
                1 if rule["requires_scope_link"] else 0,
                1 if rule["is_active"] else 0,
                rule["description"],
                rule["updated_at"],
            ),
        )
        self.con.commit()


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

    def log_sent(
        self,
        notification_type: str,
        recipient_email: str,
        action_id: str | None,
        payload: dict[str, Any] | None,
        unique_key: str,
    ) -> None:
        if not unique_key:
            return
        if not _table_exists(self.con, "email_notifications_log"):
            return

        payload_json = json.dumps(payload, ensure_ascii=False) if payload else None
        now = datetime.now(timezone.utc).isoformat()

        try:
            self.con.execute(
                """
                INSERT INTO email_notifications_log (
                    id, created_at, notification_type, recipient_email,
                    action_id, payload_json, unique_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    now,
                    (notification_type or "").strip(),
                    (recipient_email or "").strip(),
                    action_id,
                    payload_json,
                    unique_key,
                ),
            )
            self.con.commit()
        except sqlite3.IntegrityError:
            self.con.rollback()
        except sqlite3.Error:
            self.con.rollback()

    def list_recent(self, limit: int = 50) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "email_notifications_log"):
            return []
        try:
            cur = self.con.execute(
                """
                SELECT id, created_at, notification_type, recipient_email,
                       action_id, payload_json, unique_key
                FROM email_notifications_log
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            return [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []


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

    def list_action_changelog(self, limit: int = 50, project_id: str | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT acl.*,
                   a.title AS action_title,
                   a.project_id AS project_id
            FROM action_changelog acl
            LEFT JOIN actions a ON a.id = acl.action_id
        """
        filters: list[str] = []
        params: list[Any] = []
        if project_id:
            filters.append("a.project_id = ?")
            params.append(project_id)
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY acl.event_at DESC LIMIT ?"
        params.append(int(limit))
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def create_action(self, payload: dict[str, Any]) -> str:
        action_id = payload.get("id") or str(uuid4())

        cols = [
            "id",
            "project_id",
            "title",
            "description",
            "owner_champion_id",
            "priority",
            "status",
            "is_draft",
            "due_date",
            "created_at",
            "closed_at",
            "impact_type",
            "impact_value",
            "impact_aspects",
            "category",
            "manual_savings_amount",
            "manual_savings_currency",
            "manual_savings_note",
            "source",
            "source_message_id",
            "submitted_by_email",
            "submitted_at",
        ]

        now = datetime.now(timezone.utc).isoformat()
        normalized = dict(payload)
        normalized["id"] = action_id
        normalized.setdefault("created_at", date.today().isoformat())
        normalized.setdefault("priority", "med")
        normalized.setdefault("status", "open")
        normalized.setdefault("is_draft", 0)
        normalized["impact_aspects"] = _normalize_impact_aspects_payload(payload.get("impact_aspects"))
        normalized.setdefault("submitted_at", None)
        normalized.setdefault("source", None)
        normalized.setdefault("source_message_id", None)
        normalized.setdefault("submitted_by_email", None)
        normalized.setdefault("due_date", None)
        normalized.setdefault("closed_at", None)
        normalized.setdefault("manual_savings_amount", None)
        normalized.setdefault("manual_savings_currency", None)
        normalized.setdefault("manual_savings_note", None)

        values = [normalized.get(c) for c in cols]
        placeholders = ", ".join(["?"] * len(cols))

        self.con.execute(
            f"INSERT INTO actions ({', '.join(cols)}) VALUES ({placeholders})",
            values,
        )
        if _table_exists(self.con, "action_changelog"):
            self._log_changelog(action_id, "CREATE", json.dumps(normalized, ensure_ascii=False))
        self.con.commit()
        return action_id

    def update_action(self, action_id: str, payload: dict[str, Any]) -> None:
        before = self._get_action(action_id)
        if not before:
            raise ValueError("Action not found")

        merged = dict(before)
        merged.update(payload)
        merged["impact_aspects"] = _normalize_impact_aspects_payload(merged.get("impact_aspects"))

        self.con.execute(
            """
            UPDATE actions
            SET project_id = ?,
                title = ?,
                description = ?,
                owner_champion_id = ?,
                priority = ?,
                status = ?,
                is_draft = ?,
                due_date = ?,
                created_at = ?,
                closed_at = ?,
                impact_type = ?,
                impact_value = ?,
                impact_aspects = ?,
                category = ?,
                manual_savings_amount = ?,
                manual_savings_currency = ?,
                manual_savings_note = ?,
                source = ?,
                source_message_id = ?,
                submitted_by_email = ?,
                submitted_at = ?
            WHERE id = ?
            """,
            (
                merged.get("project_id"),
                merged.get("title"),
                merged.get("description"),
                merged.get("owner_champion_id"),
                merged.get("priority"),
                merged.get("status"),
                int(bool(merged.get("is_draft"))),
                merged.get("due_date"),
                merged.get("created_at"),
                merged.get("closed_at"),
                merged.get("impact_type"),
                merged.get("impact_value"),
                merged.get("impact_aspects"),
                merged.get("category"),
                merged.get("manual_savings_amount"),
                merged.get("manual_savings_currency"),
                merged.get("manual_savings_note"),
                merged.get("source"),
                merged.get("source_message_id"),
                merged.get("submitted_by_email"),
                merged.get("submitted_at"),
                action_id,
            ),
        )

        if _table_exists(self.con, "action_changelog"):
            # lightweight changelog: store full merged snapshot
            self._log_changelog(action_id, "UPDATE", json.dumps(merged, ensure_ascii=False))

        self.con.commit()

    def delete_action(self, action_id: str) -> None:
        if _table_exists(self.con, "action_changelog"):
            before = self._get_action(action_id)
            if before:
                self._log_changelog(action_id, "DELETE", json.dumps(before, ensure_ascii=False))
        self.con.execute("DELETE FROM actions WHERE id = ?", (action_id,))
        self.con.commit()

    def _get_action(self, action_id: str) -> dict[str, Any] | None:
        cur = self.con.execute("SELECT * FROM actions WHERE id = ?", (action_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def _log_changelog(self, action_id: str, event_type: str, changes_json: str) -> None:
        event_at = datetime.now(timezone.utc).isoformat()
        self.con.execute(
            """
            INSERT INTO action_changelog (id, action_id, event_type, event_at, changes_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(uuid4()), action_id, event_type, event_at, changes_json),
        )


# =====================================================
# EFFECTIVENESS
# =====================================================

class EffectivenessRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_effectiveness(self, action_id: str, payload: dict[str, Any]) -> None:
        if not _table_exists(self.con, "action_effectiveness"):
            raise sqlite3.OperationalError("action_effectiveness table missing; migration required")

        record_id = payload.get("id") or str(uuid4())
        self.con.execute(
            """
            INSERT INTO action_effectiveness (
                id, action_id, metric,
                baseline_from, baseline_to,
                after_from, after_to,
                baseline_days, after_days,
                baseline_avg, after_avg,
                delta, pct_change,
                classification, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(action_id) DO UPDATE SET
                metric = excluded.metric,
                baseline_from = excluded.baseline_from,
                baseline_to = excluded.baseline_to,
                after_from = excluded.after_from,
                after_to = excluded.after_to,
                baseline_days = excluded.baseline_days,
                after_days = excluded.after_days,
                baseline_avg = excluded.baseline_avg,
                after_avg = excluded.after_avg,
                delta = excluded.delta,
                pct_change = excluded.pct_change,
                classification = excluded.classification,
                computed_at = excluded.computed_at
            """,
            (
                record_id,
                action_id,
                payload["metric"],
                payload["baseline_from"],
                payload["baseline_to"],
                payload["after_from"],
                payload["after_to"],
                int(payload["baseline_days"]),
                int(payload["after_days"]),
                payload.get("baseline_avg"),
                payload.get("after_avg"),
                payload.get("delta"),
                payload.get("pct_change"),
                payload["classification"],
                payload["computed_at"],
            ),
        )
        self.con.commit()

    def get_effectiveness_for_actions(self, action_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not action_ids or not _table_exists(self.con, "action_effectiveness"):
            return {}
        placeholders = ", ".join(["?"] * len(action_ids))
        cur = self.con.execute(
            f"SELECT * FROM action_effectiveness WHERE action_id IN ({placeholders})",
            action_ids,
        )
        rows = [dict(r) for r in cur.fetchall()]
        return {r["action_id"]: r for r in rows}


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
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                total = r.get("actions_total") or 0
                closed = r.get("actions_closed") or 0
                r["pct_closed"] = round((closed / total) * 100, 1) if total else None
            return rows

        cur = self.con.execute(
            """
            SELECT p.*
            FROM projects p
            ORDER BY p.name
            """
        )
        return [dict(r) for r in cur.fetchall()]

    def create_project(self, data: dict[str, Any]) -> str:
        project_id = data.get("id") or str(uuid4())
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "id": project_id,
            "name": (data.get("name") or "").strip(),
            "type": data.get("type") or "custom",
            "owner_champion_id": data.get("owner_champion_id"),
            "status": data.get("status") or "active",
            "created_at": data.get("created_at") or now,
            "closed_at": data.get("closed_at"),
            "work_center": (data.get("work_center") or "").strip(),
            "project_code": data.get("project_code"),
            "project_sop": data.get("project_sop"),
            "project_eop": data.get("project_eop"),
            "related_work_center": data.get("related_work_center"),
        }
        self.con.execute(
            """
            INSERT INTO projects (
                id, name, type, owner_champion_id,
                status, created_at, closed_at,
                work_center, project_code,
                project_sop, project_eop,
                related_work_center
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["id"],
                payload["name"],
                payload["type"],
                payload["owner_champion_id"],
                payload["status"],
                payload["created_at"],
                payload["closed_at"],
                payload["work_center"],
                payload["project_code"],
                payload["project_sop"],
                payload["project_eop"],
                payload["related_work_center"],
            ),
        )
        self.con.commit()
        return project_id

    def delete_project(self, project_id: str) -> bool:
        try:
            self.con.execute("DELETE FROM projects WHERE id = ?", (project_id,))
            self.con.commit()
            return True
        except sqlite3.IntegrityError:
            self.con.rollback()
            return False


# =====================================================
# CHAMPIONS
# =====================================================

class ChampionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_champions(self) -> list[dict[str, Any]]:
        cur = self.con.execute(
            """
            SELECT id, first_name, last_name, email, team, active
            FROM champions
            ORDER BY last_name, first_name
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["display_name"] = f"{r.get('first_name','')} {r.get('last_name','')}".strip() or r.get("id")
            r["active"] = bool(r.get("active"))
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

    def set_assigned_projects(self, champion_id: str, project_ids: list[str]) -> None:
        if not _table_exists(self.con, "champion_projects"):
            raise sqlite3.OperationalError("champion_projects table missing; migration required")
        self.con.execute("DELETE FROM champion_projects WHERE champion_id = ?", (champion_id,))
        if project_ids:
            rows = [(champion_id, pid) for pid in project_ids]
            self.con.executemany(
                """
                INSERT OR IGNORE INTO champion_projects (champion_id, project_id)
                VALUES (?, ?)
                """,
                rows,
            )
        self.con.commit()


# =====================================================
# PRODUCTION DATA (SCRAP / KPI)
# =====================================================

class ProductionDataRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    # ---------- UPSERTS ----------

    def upsert_scrap_daily(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if not _table_exists(self.con, "scrap_daily"):
            raise sqlite3.OperationalError("scrap_daily table missing; migration required")
        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                r.get("id") or str(uuid4()),
                r["metric_date"],
                r["work_center"],
                int(r["scrap_qty"]),
                r.get("scrap_cost_amount"),
                (r.get("scrap_cost_currency") or "PLN"),
                r.get("created_at") or now,
            )
            for r in rows
        ]
        self.con.executemany(
            """
            INSERT INTO scrap_daily (
                id, metric_date, work_center,
                scrap_qty, scrap_cost_amount,
                scrap_cost_currency, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_date, work_center, scrap_cost_currency)
            DO UPDATE SET
                scrap_qty = excluded.scrap_qty,
                scrap_cost_amount = excluded.scrap_cost_amount,
                scrap_cost_currency = excluded.scrap_cost_currency
            """,
            payload,
        )
        self.con.commit()

    def upsert_production_kpi_daily(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if not _table_exists(self.con, "production_kpi_daily"):
            raise sqlite3.OperationalError("production_kpi_daily table missing; migration required")
        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                r.get("id") or str(uuid4()),
                r["metric_date"],
                r["work_center"],
                r.get("worktime_min"),
                r.get("performance_pct"),
                r.get("oee_pct"),
                r.get("availability_pct"),
                r.get("quality_pct"),
                r.get("source_file"),
                r.get("imported_at") or now,
                r.get("created_at") or r.get("imported_at") or now,
            )
            for r in rows
        ]
        self.con.executemany(
            """
            INSERT INTO production_kpi_daily (
                id, metric_date, work_center,
                worktime_min,
                performance_pct, oee_pct,
                availability_pct, quality_pct,
                source_file, imported_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_date, work_center)
            DO UPDATE SET
                worktime_min = excluded.worktime_min,
                performance_pct = excluded.performance_pct,
                oee_pct = excluded.oee_pct,
                availability_pct = excluded.availability_pct,
                quality_pct = excluded.quality_pct,
                source_file = excluded.source_file,
                imported_at = excluded.imported_at,
                created_at = excluded.created_at
            """,
            payload,
        )
        self.con.commit()

    # ---------- QUERIES ----------

    def list_scrap_daily(
        self,
        work_centers: str | list[str] | None,
        date_from: date | str | None,
        date_to: date | str | None,
        currency: str | None = "PLN",
    ) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "scrap_daily"):
            return []
        query = """
            SELECT metric_date, work_center, scrap_qty, scrap_cost_amount, scrap_cost_currency
            FROM scrap_daily
        """
        filters: list[str] = []
        params: list[Any] = []

        if work_centers is not None:
            if isinstance(work_centers, str):
                wc = work_centers.strip()
                if wc:
                    filters.append("work_center = ?")
                    params.append(wc)
            else:
                if not work_centers:
                    return []
                placeholders = ", ".join(["?"] * len(work_centers))
                filters.append(f"work_center IN ({placeholders})")
                params.extend(work_centers)

        if date_from:
            filters.append("metric_date >= ?")
            params.append(self._normalize_date_filter(date_from))
        if date_to:
            filters.append("metric_date <= ?")
            params.append(self._normalize_date_filter(date_to))

        if currency:
            filters.append("scrap_cost_currency = ?")
            params.append(currency)

        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY metric_date ASC, work_center ASC"
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def list_kpi_daily(
        self,
        work_centers: str | list[str] | None,
        date_from: date | str | None,
        date_to: date | str | None,
    ) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "production_kpi_daily"):
            return []
        query = """
            SELECT metric_date, work_center, worktime_min,
                   performance_pct, oee_pct, availability_pct, quality_pct
            FROM production_kpi_daily
        """
        filters: list[str] = []
        params: list[Any] = []

        if work_centers is not None:
            if isinstance(work_centers, str):
                wc = work_centers.strip()
                if wc:
                    filters.append("work_center = ?")
                    params.append(wc)
            else:
                if not work_centers:
                    return []
                placeholders = ", ".join(["?"] * len(work_centers))
                filters.append(f"work_center IN ({placeholders})")
                params.extend(work_centers)

        if date_from:
            filters.append("metric_date >= ?")
            params.append(self._normalize_date_filter(date_from))
        if date_to:
            filters.append("metric_date <= ?")
            params.append(self._normalize_date_filter(date_to))

        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY metric_date ASC, work_center ASC"
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def list_distinct_work_centers(self) -> dict[str, list[str]]:
        """
        Required by: production_explorer.py
        """
        scrap_work_centers: list[str] = []
        kpi_work_centers: list[str] = []

        if _table_exists(self.con, "scrap_daily"):
            cur = self.con.execute("SELECT DISTINCT work_center FROM scrap_daily ORDER BY work_center")
            scrap_work_centers = [row["work_center"] for row in cur.fetchall()]

        if _table_exists(self.con, "production_kpi_daily"):
            cur = self.con.execute("SELECT DISTINCT work_center FROM production_kpi_daily ORDER BY work_center")
            kpi_work_centers = [row["work_center"] for row in cur.fetchall()]

        return {"scrap_work_centers": scrap_work_centers, "kpi_work_centers": kpi_work_centers}

    def list_work_centers(self) -> list[str]:
        if not _table_exists(self.con, "scrap_daily") and not _table_exists(self.con, "production_kpi_daily"):
            return []
        cur = self.con.execute(
            """
            SELECT work_center FROM scrap_daily
            UNION
            SELECT work_center FROM production_kpi_daily
            ORDER BY work_center
            """
        )
        return [row["work_center"] for row in cur.fetchall()]

    def list_production_work_centers_with_stats(self) -> list[dict[str, Any]]:
        """
        Used by WC inbox feature.
        """
        try:
            from action_tracking.services.effectiveness import normalize_wc
        except Exception:
            def normalize_wc(v: Any) -> str:
                return normalize_key(str(v or ""))

        stats: dict[str, dict[str, Any]] = {}

        if _table_exists(self.con, "scrap_daily"):
            cur = self.con.execute(
                """
                SELECT work_center,
                       MIN(metric_date) AS first_seen_date,
                       MAX(metric_date) AS last_seen_date,
                       COUNT(DISTINCT metric_date) AS count_days_present
                FROM scrap_daily
                GROUP BY work_center
                """
            )
            for row in cur.fetchall():
                wc_raw = row["work_center"]
                wc_norm = normalize_wc(wc_raw)
                if not wc_norm:
                    continue
                entry = stats.setdefault(
                    wc_norm,
                    {
                        "wc_raw": wc_raw,
                        "wc_norm": wc_norm,
                        "has_scrap": False,
                        "has_kpi": False,
                        "first_seen_date": row["first_seen_date"],
                        "last_seen_date": row["last_seen_date"],
                        "count_days_present": 0,
                    },
                )
                entry["has_scrap"] = True
                entry["count_days_present"] += int(row["count_days_present"] or 0)

        if _table_exists(self.con, "production_kpi_daily"):
            cur = self.con.execute(
                """
                SELECT work_center,
                       MIN(metric_date) AS first_seen_date,
                       MAX(metric_date) AS last_seen_date,
                       COUNT(DISTINCT metric_date) AS count_days_present
                FROM production_kpi_daily
                GROUP BY work_center
                """
            )
            for row in cur.fetchall():
                wc_raw = row["work_center"]
                wc_norm = normalize_wc(wc_raw)
                if not wc_norm:
                    continue
                entry = stats.setdefault(
                    wc_norm,
                    {
                        "wc_raw": wc_raw,
                        "wc_norm": wc_norm,
                        "has_scrap": False,
                        "has_kpi": False,
                        "first_seen_date": row["first_seen_date"],
                        "last_seen_date": row["last_seen_date"],
                        "count_days_present": 0,
                    },
                )
                entry["has_kpi"] = True
                entry["count_days_present"] += int(row["count_days_present"] or 0)

        return list(stats.values())

    @staticmethod
    def _normalize_date_filter(value: date | str) -> str:
        return value.isoformat() if isinstance(value, date) else str(value)


# =====================================================
# WC INBOX
# =====================================================

class WcInboxRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_from_production(
        self,
        work_centers_stats: list[dict[str, Any]],
        existing_project_wc_norms: set[str],
    ) -> None:
        if not _table_exists(self.con, "wc_inbox"):
            return

        try:
            from action_tracking.services.effectiveness import normalize_wc
        except Exception:
            def normalize_wc(v: Any) -> str:
                return normalize_key(str(v or ""))

        existing_rows: dict[str, dict[str, Any]] = {}
        cur = self.con.execute(
            """
            SELECT wc_norm, wc_raw, sources, status, first_seen_date, last_seen_date
            FROM wc_inbox
            """
        )
        for row in cur.fetchall():
            existing_rows[row["wc_norm"]] = dict(row)

        now = datetime.now(timezone.utc).isoformat()

        for row in work_centers_stats:
            wc_norm = normalize_wc(row.get("wc_norm") or row.get("wc_raw"))
            if not wc_norm:
                continue

            existing = existing_rows.get(wc_norm)

            if wc_norm in existing_project_wc_norms:
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
                    existing_sources = json.loads(existing.get("sources") or "[]")
                except json.JSONDecodeError:
                    existing_sources = []
                sources = sorted(set(existing_sources) | set(sources))

            payload = {
                "id": str(uuid4()),
                "wc_raw": (row.get("wc_raw") or (existing or {}).get("wc_raw") or "").strip(),
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
            (int(limit),),
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
            SET status = ?, linked_project_id = ?, updated_at = ?
            WHERE wc_norm = ?
            """,
            (status, project_id, now, wc_norm),
        )
        self.con.commit()
