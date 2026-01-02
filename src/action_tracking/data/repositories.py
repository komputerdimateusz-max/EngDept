from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from action_tracking.domain.constants import ACTION_CATEGORIES as DEFAULT_ACTION_CATEGORIES


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


class SettingsRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_action_categories(self, active_only: bool = True) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "settings_action_categories"):
            return [
                {
                    "id": name,
                    "name": name,
                    "is_active": True,
                    "sort_order": index + 1,
                }
                for index, name in enumerate(DEFAULT_ACTION_CATEGORIES)
            ]
        query = """
            SELECT id, name, is_active, sort_order, created_at
            FROM settings_action_categories
        """
        params: list[Any] = []
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY sort_order ASC, name ASC"
        cur = self.con.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        for row in rows:
            row["is_active"] = bool(row.get("is_active"))
        return rows

    def create_action_category(self, name: str, sort_order: int) -> str:
        category_id = str(uuid4())
        self.con.execute(
            """
            INSERT INTO settings_action_categories (id, name, is_active, sort_order, created_at)
            VALUES (?, ?, 1, ?, ?)
            """,
            (category_id, name.strip(), int(sort_order), datetime.now(timezone.utc).isoformat()),
        )
        self.con.commit()
        return category_id

    def update_action_category(
        self,
        category_id: str,
        name: str,
        is_active: bool,
        sort_order: int,
    ) -> None:
        self.con.execute(
            """
            UPDATE settings_action_categories
            SET name = ?, is_active = ?, sort_order = ?
            WHERE id = ?
            """,
            (name.strip(), 1 if is_active else 0, int(sort_order), category_id),
        )
        self.con.commit()

    def delete_action_category(self, category_id: str) -> None:
        self.con.execute(
            """
            UPDATE settings_action_categories
            SET is_active = 0
            WHERE id = ?
            """,
            (category_id,),
        )
        self.con.commit()


class ActionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_actions(
        self,
        status: str | None = None,
        project_id: str | None = None,
        champion_id: str | None = None,
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
        if overdue_only:
            filters.append(
                "a.due_date IS NOT NULL AND a.due_date < ? AND a.status NOT IN ('done', 'cancelled')"
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
                         AND a.status NOT IN ('done', 'cancelled')
                    THEN 0
                    ELSE 1
                END,
                a.due_date IS NULL,
                a.due_date,
                a.created_at
        """
        params.append(today)

        cur = self.con.execute(base_query, params)
        return [dict(r) for r in cur.fetchall()]

    def list_actions_for_kpi(
        self,
        project_id: str | None = None,
        champion_id: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT id,
                   title,
                   created_at,
                   closed_at,
                   due_date,
                   status,
                   owner_champion_id,
                   category,
                   project_id
            FROM actions
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
            query += " WHERE " + " AND ".join(filters)
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def list_done_actions_for_effectiveness(
        self,
        project_id: str | None = None,
        champion_id: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT id,
                   project_id,
                   closed_at,
                   status,
                   category
            FROM actions
            WHERE status = 'done' AND closed_at IS NOT NULL
        """
        params: list[Any] = []
        if project_id:
            query += " AND project_id = ?"
            params.append(project_id)
        if champion_id:
            query += " AND owner_champion_id = ?"
            params.append(champion_id)
        if category:
            query += " AND category = ?"
            params.append(category)
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def list_action_changelog(
        self,
        limit: int = 50,
        project_id: str | None = None,
        action_id: str | None = None,
    ) -> list[dict[str, Any]]:
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
        if action_id:
            filters.append("acl.action_id = ?")
            params.append(action_id)
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY acl.event_at DESC LIMIT ?"
        params.append(limit)
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def create_action(self, data: dict[str, Any]) -> str:
        action_id = data.get("id") or str(uuid4())
        payload = self._normalize_action_payload(action_id, data, existing=None)
        self.con.execute(
            """
            INSERT INTO actions (
                id,
                project_id,
                title,
                description,
                owner_champion_id,
                priority,
                status,
                due_date,
                created_at,
                closed_at,
                impact_type,
                impact_value,
                category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["id"],
                payload["project_id"],
                payload["title"],
                payload["description"],
                payload["owner_champion_id"],
                payload["priority"],
                payload["status"],
                payload["due_date"],
                payload["created_at"],
                payload["closed_at"],
                payload["impact_type"],
                payload["impact_value"],
                payload["category"],
            ),
        )
        self._log_changelog(
            action_id,
            "CREATE",
            self._serialize_changes(payload),
        )
        self.con.commit()
        return action_id

    def update_action(self, action_id: str, data: dict[str, Any]) -> None:
        before = self._get_action(action_id)
        if not before:
            raise ValueError("Action not found")
        merged = self._merge_action_payload(before, data)
        payload = self._normalize_action_payload(action_id, merged, existing=before)
        changes = self._diff_changes(before, payload)
        self.con.execute(
            """
            UPDATE actions
            SET project_id = ?,
                title = ?,
                description = ?,
                owner_champion_id = ?,
                priority = ?,
                status = ?,
                due_date = ?,
                created_at = ?,
                closed_at = ?,
                impact_type = ?,
                impact_value = ?,
                category = ?
            WHERE id = ?
            """,
            (
                payload["project_id"],
                payload["title"],
                payload["description"],
                payload["owner_champion_id"],
                payload["priority"],
                payload["status"],
                payload["due_date"],
                payload["created_at"],
                payload["closed_at"],
                payload["impact_type"],
                payload["impact_value"],
                payload["category"],
                action_id,
            ),
        )
        if changes:
            self._log_changelog(
                action_id,
                "UPDATE",
                self._serialize_changes(changes),
            )
        self.con.commit()

    def delete_action(self, action_id: str) -> None:
        before = self._get_action(action_id)
        if not before:
            return
        self._log_changelog(
            action_id,
            "DELETE",
            self._serialize_changes(before),
        )
        self.con.execute("DELETE FROM actions WHERE id = ?", (action_id,))
        self.con.commit()

    def _merge_action_payload(self, before: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
        fields = [
            "project_id",
            "title",
            "description",
            "owner_champion_id",
            "priority",
            "status",
            "due_date",
            "created_at",
            "closed_at",
            "impact_type",
            "impact_value",
            "category",
        ]
        merged: dict[str, Any] = {}
        for field in fields:
            if field in data:
                merged[field] = data[field]
            else:
                merged[field] = before.get(field)
        return merged

    def _normalize_action_payload(
        self,
        action_id: str,
        data: dict[str, Any],
        existing: dict[str, Any] | None,
    ) -> dict[str, Any]:
        title = (data.get("title") or "").strip()
        if not title:
            raise ValueError("Krótka nazwa akcji jest wymagana.")
        if len(title) > 20:
            raise ValueError("Krótka nazwa akcji nie może przekraczać 20 znaków.")

        description = (data.get("description") or "").strip()
        if description and len(description) > 500:
            raise ValueError("Opis akcji nie może przekraczać 500 znaków.")
        description = description or None

        category = data.get("category")
        if not category:
            raise ValueError("Kategoria akcji jest wymagana.")
        if category not in self._list_active_action_categories():
            raise ValueError("Wybrana kategoria akcji jest nieprawidłowa.")

        project_id = (data.get("project_id") or "").strip()
        if not project_id:
            raise ValueError("Akcja musi być przypisana do projektu.")

        owner_champion_id = (data.get("owner_champion_id") or "").strip() or None
        priority = data.get("priority") or "med"
        status = data.get("status") or "open"
        due_date = data.get("due_date") or None
        if due_date:
            due_date = self._parse_date(due_date, "due_date").isoformat()

        created_at = data.get("created_at") or (existing or {}).get("created_at")
        created_at = created_at or date.today().isoformat()
        created_date = self._parse_date(created_at, "created_at")
        closed_at = data.get("closed_at") or None

        if status == "done":
            if not closed_at:
                closed_at = date.today().isoformat()
            closed_date = self._parse_date(closed_at, "closed_at")
            if closed_date < created_date:
                raise ValueError("Data zamknięcia nie może być wcześniejsza niż data utworzenia.")
        else:
            closed_at = None

        return {
            "id": action_id,
            "project_id": project_id,
            "title": title,
            "description": description,
            "owner_champion_id": owner_champion_id,
            "priority": priority,
            "status": status,
            "due_date": due_date,
            "created_at": created_date.isoformat(),
            "closed_at": closed_at,
            "impact_type": data.get("impact_type"),
            "impact_value": data.get("impact_value"),
            "category": category,
        }

    def _list_active_action_categories(self) -> list[str]:
        settings_repo = SettingsRepository(self.con)
        categories = settings_repo.list_action_categories(active_only=True)
        return [row["name"] for row in categories]

    def _parse_date(self, value: Any, field_name: str) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return date.fromisoformat(str(value))
        except ValueError as exc:
            try:
                return datetime.fromisoformat(str(value)).date()
            except ValueError as exc_two:
                raise ValueError(f"Nieprawidłowy format daty dla pola {field_name}.") from exc_two

    def _get_action(self, action_id: str) -> dict[str, Any] | None:
        cur = self.con.execute(
            """
            SELECT *
            FROM actions
            WHERE id = ?
            """,
            (action_id,),
        )
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

    def _serialize_changes(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False)

    def _diff_changes(self, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
        diffs: dict[str, Any] = {}
        for key, value in after.items():
            if before.get(key) != value:
                diffs[key] = {"from": before.get(key), "to": value}
        return diffs


class EffectivenessRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_effectiveness(self, action_id: str, payload: dict[str, Any]) -> None:
        record_id = payload.get("id") or str(uuid4())
        self.con.execute(
            """
            INSERT INTO action_effectiveness (
                id,
                action_id,
                metric,
                baseline_from,
                baseline_to,
                after_from,
                after_to,
                baseline_days,
                after_days,
                baseline_avg,
                after_avg,
                delta,
                pct_change,
                classification,
                computed_at
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
                payload["baseline_avg"],
                payload["after_avg"],
                payload["delta"],
                payload["pct_change"],
                payload["classification"],
                payload["computed_at"],
            ),
        )
        self.con.commit()

    def get_effectiveness_for_actions(self, action_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not action_ids:
            return {}
        placeholders = ", ".join(["?"] * len(action_ids))
        cur = self.con.execute(
            f"""
            SELECT *
            FROM action_effectiveness
            WHERE action_id IN ({placeholders})
            """,
            action_ids,
        )
        rows = [dict(r) for r in cur.fetchall()]
        return {row["action_id"]: row for row in rows}


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
                       CASE
                           WHEN TRIM(COALESCE(ch.first_name, '')) != ''
                             OR TRIM(COALESCE(ch.last_name, '')) != ''
                               THEN TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, ''))
                           WHEN TRIM(COALESCE(ch.name, '')) != '' THEN ch.name
                           WHEN TRIM(COALESCE(ch.email, '')) != '' THEN ch.email
                           ELSE NULL
                       END AS owner_champion_name,
                       p.status,
                       p.created_at,
                       p.closed_at,
                       p.work_center,
                       p.project_code,
                       p.project_sop,
                       p.project_eop,
                       p.related_work_center,
                       COUNT(a.id) AS actions_total,
                       COALESCE(SUM(CASE WHEN a.status IN ('done', 'cancelled') THEN 1 ELSE 0 END), 0)
                           AS actions_closed,
                       COALESCE(SUM(CASE WHEN a.status NOT IN ('done', 'cancelled') THEN 1 ELSE 0 END), 0)
                           AS actions_open
                FROM projects p
                LEFT JOIN champions ch ON ch.id = p.owner_champion_id
                LEFT JOIN actions a ON a.project_id = p.id
                GROUP BY p.id
                ORDER BY p.name
                """
            )
        else:
            cur = self.con.execute(
                """
                SELECT p.id,
                       p.name,
                       p.owner_champion_id,
                       CASE
                           WHEN TRIM(COALESCE(ch.first_name, '')) != ''
                             OR TRIM(COALESCE(ch.last_name, '')) != ''
                               THEN TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, ''))
                           WHEN TRIM(COALESCE(ch.name, '')) != '' THEN ch.name
                           WHEN TRIM(COALESCE(ch.email, '')) != '' THEN ch.email
                           ELSE NULL
                       END AS owner_champion_name
                FROM projects p
                LEFT JOIN champions ch ON ch.id = p.owner_champion_id
                ORDER BY p.name
                """
            )
        rows = [dict(r) for r in cur.fetchall()]
        if include_counts:
            for row in rows:
                total = row.get("actions_total") or 0
                closed = row.get("actions_closed") or 0
                row["pct_closed"] = round((closed / total) * 100, 1) if total else None
        return rows

    def list_changelog(self, limit: int = 50) -> list[dict[str, Any]]:
        cur = self.con.execute(
            """
            SELECT pcl.*,
                   p.name
            FROM project_changelog pcl
            LEFT JOIN projects p ON p.id = pcl.project_id
            ORDER BY pcl.event_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]

    def create_project(self, data: dict[str, Any]) -> str:
        project_id = data.get("id") or str(uuid4())
        payload = self._normalize_project_payload(project_id, data)
        self.con.execute(
            """
            INSERT INTO projects (
                id,
                name,
                type,
                owner_champion_id,
                status,
                created_at,
                closed_at,
                work_center,
                project_code,
                project_sop,
                project_eop,
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
        self._log_changelog(
            project_id,
            "CREATE",
            self._serialize_changes(payload),
        )
        self.con.commit()
        return project_id

    def update_project(self, project_id: str, data: dict[str, Any]) -> None:
        before = self._get_project(project_id)
        if not before:
            raise ValueError("Project not found")

        next_payload = {
            "name": data.get("name", before["name"]),
            "type": data.get("type", before["type"]),
            "owner_champion_id": data.get("owner_champion_id", before["owner_champion_id"]),
            "status": data.get("status", before["status"]),
            "created_at": data.get("created_at", before["created_at"]),
            "closed_at": data.get("closed_at", before["closed_at"]),
            "work_center": data.get("work_center", before.get("work_center")),
            "project_code": data.get("project_code", before.get("project_code")),
            "project_sop": data.get("project_sop", before.get("project_sop")),
            "project_eop": data.get("project_eop", before.get("project_eop")),
            "related_work_center": data.get("related_work_center", before.get("related_work_center")),
        }
        changes = self._diff_changes(before, next_payload)
        self.con.execute(
            """
            UPDATE projects
            SET name = ?,
                type = ?,
                owner_champion_id = ?,
                status = ?,
                created_at = ?,
                closed_at = ?,
                work_center = ?,
                project_code = ?,
                project_sop = ?,
                project_eop = ?,
                related_work_center = ?
            WHERE id = ?
            """,
            (
                next_payload["name"],
                next_payload["type"],
                next_payload["owner_champion_id"],
                next_payload["status"],
                next_payload["created_at"],
                next_payload["closed_at"],
                next_payload["work_center"],
                next_payload["project_code"],
                next_payload["project_sop"],
                next_payload["project_eop"],
                next_payload["related_work_center"],
                project_id,
            ),
        )
        if changes:
            self._log_changelog(
                project_id,
                "UPDATE",
                self._serialize_changes(changes),
            )
        self.con.commit()

    def delete_project(self, project_id: str) -> bool:
        before = self._get_project(project_id)
        if not before:
            return True
        try:
            self.con.execute("BEGIN")
            self._log_changelog(
                project_id,
                "DELETE",
                self._serialize_changes(before),
            )
            self.con.execute("DELETE FROM projects WHERE id = ?", (project_id,))
            self.con.commit()
            return True
        except sqlite3.IntegrityError:
            self.con.rollback()
            return False

    def _get_project(self, project_id: str) -> dict[str, Any] | None:
        cur = self.con.execute(
            """
            SELECT *
            FROM projects
            WHERE id = ?
            """,
            (project_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def _log_changelog(self, project_id: str, event_type: str, changes_json: str) -> None:
        event_at = datetime.now(timezone.utc).isoformat()
        self.con.execute(
            """
            INSERT INTO project_changelog (id, project_id, event_type, event_at, changes_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(uuid4()), project_id, event_type, event_at, changes_json),
        )

    def _normalize_project_payload(self, project_id: str, data: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        return {
            "id": project_id,
            "name": data.get("name", "").strip(),
            "type": data.get("type") or "Others",
            "owner_champion_id": data.get("owner_champion_id"),
            "status": data.get("status") or "active",
            "created_at": data.get("created_at") or now,
            "closed_at": data.get("closed_at"),
            "work_center": data.get("work_center", "").strip(),
            "project_code": data.get("project_code"),
            "project_sop": data.get("project_sop"),
            "project_eop": data.get("project_eop"),
            "related_work_center": data.get("related_work_center"),
        }

    def _serialize_changes(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False)

    def _diff_changes(self, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
        diffs: dict[str, Any] = {}
        for key, value in after.items():
            if before.get(key) != value:
                diffs[key] = {"from": before.get(key), "to": value}
        return diffs


class ChampionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_champions(self) -> list[dict[str, Any]]:
        cur = self.con.execute(
            """
            SELECT id,
                   first_name,
                   last_name,
                   email,
                   hire_date,
                   position,
                   active
            FROM champions
            ORDER BY last_name, first_name
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        for row in rows:
            display = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
            row["display_name"] = display or row.get("id")
        return rows

    def list_changelog(self, limit: int = 50) -> list[dict[str, Any]]:
        cur = self.con.execute(
            """
            SELECT ccl.*,
                   ch.first_name,
                   ch.last_name
            FROM champion_changelog ccl
            LEFT JOIN champions ch ON ch.id = ccl.champion_id
            ORDER BY ccl.event_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]

    def create_champion(self, data: dict[str, Any]) -> str:
        champion_id = data.get("id") or str(uuid4())
        first_name = data.get("first_name", "").strip()
        last_name = data.get("last_name", "").strip()
        full_name = f"{first_name} {last_name}".strip()
        payload = {
            "id": champion_id,
            "name": full_name or data.get("name", ""),
            "first_name": first_name,
            "last_name": last_name,
            "email": data.get("email"),
            "hire_date": data.get("hire_date"),
            "position": data.get("position"),
            "active": int(data.get("active", 1)),
        }
        self.con.execute(
            """
            INSERT INTO champions (
                id,
                name,
                email,
                active,
                first_name,
                last_name,
                hire_date,
                position
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["id"],
                payload["name"],
                payload["email"],
                payload["active"],
                payload["first_name"],
                payload["last_name"],
                payload["hire_date"],
                payload["position"],
            ),
        )
        self._log_changelog(
            champion_id,
            "CREATE",
            self._serialize_changes(payload),
        )
        self.con.commit()
        return champion_id

    def update_champion(self, champion_id: str, data: dict[str, Any]) -> None:
        before = self._get_champion(champion_id)
        if not before:
            raise ValueError("Champion not found")

        next_payload = {
            "first_name": data.get("first_name", before["first_name"]),
            "last_name": data.get("last_name", before["last_name"]),
            "email": data.get("email", before["email"]),
            "hire_date": data.get("hire_date", before["hire_date"]),
            "position": data.get("position", before["position"]),
            "active": int(data.get("active", before["active"])),
        }
        full_name = f"{next_payload['first_name']} {next_payload['last_name']}".strip()

        changes = self._diff_changes(before, next_payload)

        self.con.execute(
            """
            UPDATE champions
            SET name = ?,
                first_name = ?,
                last_name = ?,
                email = ?,
                hire_date = ?,
                position = ?,
                active = ?
            WHERE id = ?
            """,
            (
                full_name or before["name"],
                next_payload["first_name"],
                next_payload["last_name"],
                next_payload["email"],
                next_payload["hire_date"],
                next_payload["position"],
                next_payload["active"],
                champion_id,
            ),
        )

        if changes:
            self._log_changelog(
                champion_id,
                "UPDATE",
                self._serialize_changes(changes),
            )
        self.con.commit()

    def delete_champion(self, champion_id: str) -> None:
        before = self._get_champion(champion_id)
        if not before:
            return
        self._log_changelog(
            champion_id,
            "DELETE",
            self._serialize_changes(before),
        )
        self.con.execute("DELETE FROM champions WHERE id = ?", (champion_id,))
        self.con.commit()

    def set_assigned_projects(self, champion_id: str, project_ids: list[str]) -> None:
        self.con.execute(
            "DELETE FROM champion_projects WHERE champion_id = ?",
            (champion_id,),
        )
        if project_ids:
            rows = [(champion_id, project_id) for project_id in project_ids]
            self.con.executemany(
                """
                INSERT OR IGNORE INTO champion_projects (champion_id, project_id)
                VALUES (?, ?)
                """,
                rows,
            )
        self.con.commit()

    def get_assigned_projects(self, champion_id: str) -> list[str]:
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


class ProductionDataRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_scrap_daily(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                row.get("id") or str(uuid4()),
                row["metric_date"],
                row["work_center"],
                int(row["scrap_qty"]),
                row.get("scrap_cost_amount"),
                row.get("scrap_cost_currency", "PLN"),
                row.get("created_at") or now,
            )
            for row in rows
        ]
        self.con.executemany(
            """
            INSERT INTO scrap_daily (
                id,
                metric_date,
                work_center,
                scrap_qty,
                scrap_cost_amount,
                scrap_cost_currency,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_date, work_center, scrap_cost_currency) DO UPDATE SET
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
        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                row.get("id") or str(uuid4()),
                row["metric_date"],
                row["work_center"],
                row.get("oee_pct"),
                row.get("performance_pct"),
                row.get("created_at") or now,
            )
            for row in rows
        ]
        self.con.executemany(
            """
            INSERT INTO production_kpi_daily (
                id,
                metric_date,
                work_center,
                oee_pct,
                performance_pct,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_date, work_center) DO UPDATE SET
                oee_pct = excluded.oee_pct,
                performance_pct = excluded.performance_pct
            """,
            payload,
        )
        self.con.commit()

    def list_scrap_daily(
        self,
        work_centers: str | list[str] | None,
        date_from: date | str | None,
        date_to: date | str | None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT metric_date,
                   work_center,
                   scrap_qty,
                   scrap_cost_amount,
                   scrap_cost_currency
            FROM scrap_daily
        """
        filters: list[str] = []
        params: list[Any] = []
        if work_centers is not None:
            if isinstance(work_centers, str):
                if work_centers:
                    filters.append("work_center = ?")
                    params.append(work_centers)
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

    def list_production_kpi_daily(
        self,
        work_center: str | None,
        date_from: date | str | None,
        date_to: date | str | None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT metric_date,
                   work_center,
                   oee_pct,
                   performance_pct
            FROM production_kpi_daily
        """
        filters: list[str] = []
        params: list[Any] = []
        if work_center:
            filters.append("work_center = ?")
            params.append(work_center)
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

    def list_work_centers(self) -> list[str]:
        cur = self.con.execute(
            """
            SELECT work_center FROM scrap_daily
            UNION
            SELECT work_center FROM production_kpi_daily
            ORDER BY work_center ASC
            """
        )
        return [row["work_center"] for row in cur.fetchall()]

    @staticmethod
    def _normalize_date_filter(value: date | str) -> str:
        if isinstance(value, date):
            return value.isoformat()
        return value

    def _get_champion(self, champion_id: str) -> dict[str, Any] | None:
        cur = self.con.execute(
            """
            SELECT *
            FROM champions
            WHERE id = ?
            """,
            (champion_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def _log_changelog(self, champion_id: str, event_type: str, changes_json: str) -> None:
        event_at = datetime.now(timezone.utc).isoformat()
        self.con.execute(
            """
            INSERT INTO champion_changelog (id, champion_id, event_type, event_at, changes_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(uuid4()), champion_id, event_type, event_at, changes_json),
        )

    def _serialize_changes(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False)

    def _diff_changes(self, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
        diffs: dict[str, Any] = {}
        for key, value in after.items():
            if before.get(key) != value:
                diffs[key] = {"from": before.get(key), "to": value}
        return diffs
