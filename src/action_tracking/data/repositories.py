# === HOTFIX: ProjectRepository missing after merge ===
# Wklej TEN BLOK do: src/action_tracking/data/repositories.py
# (na sam koniec pliku, albo w miejscu gdzie trzymasz inne *Repository)

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


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


class ProjectRepository:
    """
    Minimalna implementacja wymagana przez UI (Explorer/Projects/Outcome/WC inbox).
    Jeżeli masz już ProjectRepository w innym pliku, to znaczy że po merge
    repositories.py został nadpisany/ucięty i UI nie może go zaimportować.
    """

    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_projects(self, include_counts: bool = True) -> list[dict[str, Any]]:
        if include_counts and _table_exists(self.con, "actions"):
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
                LEFT JOIN actions a ON a.project_id = p.id AND COALESCE(a.is_draft, 0) = 0
                GROUP BY p.id
                ORDER BY p.name
                """
            )
        else:
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
                       p.related_work_center
                FROM projects p
                LEFT JOIN champions ch ON ch.id = p.owner_champion_id
                ORDER BY p.name
                """
            )

        rows = [dict(r) for r in cur.fetchall()]
        if include_counts:
            for row in rows:
                total = int(row.get("actions_total") or 0)
                closed = int(row.get("actions_closed") or 0)
                row["pct_closed"] = round((closed / total) * 100, 1) if total else None
        return rows

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
        self._log_changelog(project_id, "CREATE", self._serialize_changes(payload))
        self.con.commit()
        return project_id

    def update_project(self, project_id: str, data: dict[str, Any]) -> None:
        before = self._get_project(project_id)
        if not before:
            raise ValueError("Project not found")

        next_payload = {
            "name": data.get("name", before.get("name")),
            "type": data.get("type", before.get("type")),
            "owner_champion_id": data.get("owner_champion_id", before.get("owner_champion_id")),
            "status": data.get("status", before.get("status") or "active"),
            "created_at": data.get("created_at", before.get("created_at")),
            "closed_at": data.get("closed_at", before.get("closed_at")),
            "work_center": data.get("work_center", before.get("work_center") or ""),
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
                (next_payload["name"] or "").strip(),
                next_payload["type"] or "Others",
                next_payload["owner_champion_id"],
                next_payload["status"] or "active",
                next_payload["created_at"],
                next_payload["closed_at"],
                (next_payload["work_center"] or "").strip(),
                next_payload["project_code"],
                next_payload["project_sop"],
                next_payload["project_eop"],
                next_payload["related_work_center"],
                project_id,
            ),
        )
        if changes:
            self._log_changelog(project_id, "UPDATE", self._serialize_changes(changes))
        self.con.commit()

    def delete_project(self, project_id: str) -> bool:
        before = self._get_project(project_id)
        if not before:
            return True
        try:
            self.con.execute("BEGIN")
            self._log_changelog(project_id, "DELETE", self._serialize_changes(before))
            self.con.execute("DELETE FROM projects WHERE id = ?", (project_id,))
            self.con.commit()
            return True
        except sqlite3.IntegrityError:
            self.con.rollback()
            return False

    def list_changelog(self, limit: int = 50) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "project_changelog"):
            return []
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

    def list_project_work_centers_norms(self, include_related: bool = True) -> set[str]:
        # importy lokalne, żeby nie robić cykli
        from action_tracking.services.effectiveness import normalize_wc, parse_work_centers

        cur = self.con.execute(
            """
            SELECT work_center,
                   related_work_center
            FROM projects
            """
        )
        norms: set[str] = set()
        for row in cur.fetchall():
            rowd = dict(row)
            primary_norm = normalize_wc(rowd.get("work_center"))
            if primary_norm:
                norms.add(primary_norm)
            if include_related:
                related = parse_work_centers(None, rowd.get("related_work_center"))
                for token in related:
                    token_norm = normalize_wc(token)
                    if token_norm:
                        norms.add(token_norm)
        return norms

    # --- helpers ---

    def _get_project(self, project_id: str) -> dict[str, Any] | None:
        cur = self.con.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def _log_changelog(self, project_id: str, event_type: str, changes_json: str) -> None:
        if not _table_exists(self.con, "project_changelog"):
            # jeżeli ktoś odpalił UI bez migracji – nie wywalaj całej aplikacji
            return
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
        name = (data.get("name") or "").strip()
        if not name:
            raise ValueError("Nazwa projektu jest wymagana.")
        return {
            "id": project_id,
            "name": name,
            "type": data.get("type") or "Others",
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

    def _serialize_changes(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False)

    def _diff_changes(self, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
        diffs: dict[str, Any] = {}
        for key, value in after.items():
            if before.get(key) != value:
                diffs[key] = {"from": before.get(key), "to": value}
        return diffs
