from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


class ActionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_actions(self, status: str | None = None) -> list[dict[str, Any]]:
        if status:
            cur = self.con.execute(
                """
                SELECT a.*
                FROM actions a
                WHERE a.status = ?
                ORDER BY a.due_date IS NULL, a.due_date, a.created_at
                """,
                (status,),
            )
        else:
            cur = self.con.execute(
                """
                SELECT a.*
                FROM actions a
                ORDER BY a.due_date IS NULL, a.due_date, a.created_at
                """
            )
        return [dict(r) for r in cur.fetchall()]


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
                ORDER BY name
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
            "type": data.get("type") or "custom",
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
        return [dict(r) for r in cur.fetchall()]

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
