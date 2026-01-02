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

    def list_projects(self) -> list[dict[str, Any]]:
        cur = self.con.execute(
            """
            SELECT id, name
            FROM projects
            ORDER BY name
            """
        )
        return [dict(r) for r in cur.fetchall()]


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
