from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

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


# =====================================================
# SETTINGS / GLOBAL RULES
# =====================================================

class SettingsRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_action_categories(self, active_only: bool = True) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "action_categories"):
            return []
        query = """
            SELECT id, name, is_active, sort_order, created_at
            FROM action_categories
        """
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY sort_order, name"
        cur = self.con.execute(query)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["is_active"] = bool(r["is_active"])
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
        if only_active:
            query += " WHERE is_active = 1"
        query += " ORDER BY category"
        cur = self.con.execute(query)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["requires_scope_link"] = bool(r["requires_scope_link"])
            r["is_active"] = bool(r["is_active"])
        return rows


# =====================================================
# ACTIONS
# =====================================================

class ActionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_actions(self) -> list[dict[str, Any]]:
        cur = self.con.execute(
            """
            SELECT a.*,
                   p.name AS project_name
            FROM actions a
            LEFT JOIN projects p ON p.id = a.project_id
            ORDER BY a.created_at DESC
            """
        )
        return [dict(r) for r in cur.fetchall()]

    def create_action(self, payload: dict[str, Any]) -> str:
        action_id = payload.get("id") or str(uuid4())
        cols = [
            "id","project_id","title","description","owner_champion_id",
            "priority","status","is_draft","due_date","created_at","closed_at",
            "impact_type","impact_value","impact_aspects","category",
            "manual_savings_amount","manual_savings_currency","manual_savings_note",
            "source","source_message_id","submitted_by_email","submitted_at"
        ]
        values = [payload.get(c) for c in cols]
        placeholders = ", ".join(["?"] * len(cols))
        self.con.execute(
            f"INSERT INTO actions ({', '.join(cols)}) VALUES ({placeholders})",
            values,
        )
        self.con.commit()
        return action_id


# =====================================================
# EFFECTIVENESS
# =====================================================

class EffectivenessRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_effectiveness(self, action_id: str, payload: dict[str, Any]) -> None:
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
                delta = excluded.delta,
                pct_change = excluded.pct_change,
                classification = excluded.classification,
                computed_at = excluded.computed_at
            """,
            (
                payload.get("id") or str(uuid4()),
                action_id,
                payload["metric"],
                payload["baseline_from"],
                payload["baseline_to"],
                payload["after_from"],
                payload["after_to"],
                payload["baseline_days"],
                payload["after_days"],
                payload["baseline_avg"],
                payload["after_avg"],
                payload["delta"],
                payload["pct_change"],
                payload["classification"],
                payload["computed_at"],
            ),
        )
        self.con.commit()


# =====================================================
# PROJECTS
# =====================================================

class ProjectRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_projects(self) -> list[dict[str, Any]]:
        cur = self.con.execute(
            """
            SELECT p.*,
                   COUNT(a.id) AS actions_total
            FROM projects p
            LEFT JOIN actions a ON a.project_id = p.id AND a.is_draft = 0
            GROUP BY p.id
            ORDER BY p.name
            """
        )
        return [dict(r) for r in cur.fetchall()]

    def create_project(self, data: dict[str, Any]) -> str:
        project_id = data.get("id") or str(uuid4())
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
                project_id,
                data["name"],
                data.get("type", "custom"),
                data.get("owner_champion_id"),
                data.get("status", "active"),
                data.get("created_at"),
                data.get("closed_at"),
                data.get("work_center", ""),
                data.get("project_code"),
                data.get("project_sop"),
                data.get("project_eop"),
                data.get("related_work_center"),
            ),
        )
        self.con.commit()
        return project_id


# =====================================================
# CHAMPIONS
# =====================================================

class ChampionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_champions(self) -> list[dict[str, Any]]:
        cur = self.con.execute(
            """
            SELECT id, first_name, last_name, email, active
            FROM champions
            ORDER BY last_name, first_name
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["display_name"] = f"{r.get('first_name','')} {r.get('last_name','')}".strip()
        return rows


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
        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                r.get("id") or str(uuid4()),
                r["metric_date"],
                r["work_center"],
                int(r["scrap_qty"]),
                r.get("scrap_cost_amount"),
                r.get("scrap_cost_currency", "PLN"),
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
                scrap_cost_amount = excluded.scrap_cost_amount
            """,
            payload,
        )
        self.con.commit()

    def upsert_production_kpi_daily(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                r.get("id") or str(uuid4()),
                r["metric_date"],
                r["work_center"],
                r.get("worktime_min"),
                r.get("oee_pct"),
                r.get("performance_pct"),
                r.get("availability_pct"),
                r.get("quality_pct"),
                r.get("source_file"),
                r.get("imported_at") or now,
                r.get("created_at") or now,
            )
            for r in rows
        ]
        self.con.executemany(
            """
            INSERT INTO production_kpi_daily (
                id, metric_date, work_center,
                worktime_min, oee_pct, performance_pct,
                availability_pct, quality_pct,
                source_file, imported_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_date, work_center)
            DO UPDATE SET
                oee_pct = excluded.oee_pct,
                performance_pct = excluded.performance_pct,
                availability_pct = excluded.availability_pct,
                quality_pct = excluded.quality_pct
            """,
            payload,
        )
        self.con.commit()

    # ---------- QUERIES ----------

    def list_scrap_daily(
        self,
        work_center: str | None,
        date_from: date | str | None,
        date_to: date | str | None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM scrap_daily WHERE 1=1"
        params: list[Any] = []
        if work_center:
            query += " AND work_center = ?"
            params.append(work_center)
        if date_from:
            query += " AND metric_date >= ?"
            params.append(str(date_from))
        if date_to:
            query += " AND metric_date <= ?"
            params.append(str(date_to))
        query += " ORDER BY metric_date"
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def list_kpi_daily(
        self,
        work_center: str | None,
        date_from: date | str | None,
        date_to: date | str | None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM production_kpi_daily WHERE 1=1"
        params: list[Any] = []
        if work_center:
            query += " AND work_center = ?"
            params.append(work_center)
        if date_from:
            query += " AND metric_date >= ?"
            params.append(str(date_from))
        if date_to:
            query += " AND metric_date <= ?"
            params.append(str(date_to))
        query += " ORDER BY metric_date"
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


# =====================================================
# WC INBOX
# =====================================================

class WcInboxRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_open(self) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "wc_inbox"):
            return []
        cur = self.con.execute(
            """
            SELECT *
            FROM wc_inbox
            WHERE status = 'open'
            ORDER BY last_seen_date DESC
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["sources"] = json.loads(r.get("sources") or "[]")
        return rows
# =====================================================
# NOTIFICATIONS (email log)
# =====================================================

class NotificationRepository:
    """
    Used by: app/pages/settings.py
    Provides a simple log + dedup (unique_key) for sent notifications.
    Table: email_notifications_log
    """

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
            # migration not applied yet -> ignore silently (so app doesn't crash)
            return

        payload_json = json.dumps(payload, ensure_ascii=False) if payload else None
        now = datetime.now(timezone.utc).isoformat()

        try:
            self.con.execute(
                """
                INSERT INTO email_notifications_log (
                    id,
                    created_at,
                    notification_type,
                    recipient_email,
                    action_id,
                    payload_json,
                    unique_key
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
            # unique_key already exists -> treat as "already logged"
            self.con.rollback()
        except sqlite3.Error:
            self.con.rollback()

    def list_recent(self, limit: int = 50) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "email_notifications_log"):
            return []
        try:
            cur = self.con.execute(
                """
                SELECT id,
                       created_at,
                       notification_type,
                       recipient_email,
                       action_id,
                       payload_json,
                       unique_key
                FROM email_notifications_log
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            return [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []
