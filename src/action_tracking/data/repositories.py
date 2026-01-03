# ============================
# HOTFIX: ProjectRepository + __all__ exports (quick rescue)
# ============================
# Problem: ImportError: cannot import name 'ProjectRepository' ...
# To oznacza, że w Twoim src/action_tracking/data/repositories.py
# NIE MA już klasy ProjectRepository (albo została uszkodzona/wycięta podczas merge).
#
# NAJSZYBSZE rozwiązanie: dodaj minimalną, kompatybilną wersję ProjectRepository
# do TEGO SAMEGO pliku repositories.py (najlepiej na samym końcu, POD ProductionDataRepository).
#
# UWAGA: Ta wersja wystarczy żeby Explorer/Projects startowały (list_projects, create/update/delete).
# Jeśli masz w projekcie „pełniejszą” wersję ProjectRepository, możesz potem ją przywrócić,
# ale teraz celem jest uruchomienie aplikacji.

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
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_projects(self, include_counts: bool = True) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "projects"):
            return []

        if include_counts and _table_exists(self.con, "actions"):
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
                       COALESCE(SUM(CASE WHEN a.status IN ('done','cancelled') THEN 1 ELSE 0 END),0) AS actions_closed,
                       COALESCE(SUM(CASE WHEN a.status NOT IN ('done','cancelled') THEN 1 ELSE 0 END),0) AS actions_open
                FROM projects p
                LEFT JOIN actions a ON a.project_id = p.id AND COALESCE(a.is_draft,0) = 0
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
                       p.status,
                       p.created_at,
                       p.closed_at,
                       p.work_center,
                       p.project_code,
                       p.project_sop,
                       p.project_eop,
                       p.related_work_center
                FROM projects p
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

    def create_project(self, data: dict[str, Any]) -> str:
        if not _table_exists(self.con, "projects"):
            raise sqlite3.OperationalError("projects table missing; migration required")

        project_id = (data.get("id") or "").strip() or str(uuid4())
        now = datetime.now(timezone.utc).isoformat()

        payload = {
            "id": project_id,
            "name": (data.get("name") or "").strip(),
            "type": (data.get("type") or "custom").strip(),
            "owner_champion_id": data.get("owner_champion_id"),
            "status": (data.get("status") or "active").strip(),
            "created_at": data.get("created_at") or now,
            "closed_at": data.get("closed_at"),
            "work_center": (data.get("work_center") or "").strip(),
            "project_code": data.get("project_code"),
            "project_sop": data.get("project_sop"),
            "project_eop": data.get("project_eop"),
            "related_work_center": data.get("related_work_center"),
        }

        if not payload["name"]:
            raise ValueError("Nazwa projektu jest wymagana.")

        self.con.execute(
            """
            INSERT INTO projects (
                id, name, type, owner_champion_id, status,
                created_at, closed_at, work_center,
                project_code, project_sop, project_eop, related_work_center
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
        self._log_changelog(project_id, "CREATE", payload)
        self.con.commit()
        return project_id

    def update_project(self, project_id: str, data: dict[str, Any]) -> None:
        before = self._get_project(project_id)
        if not before:
            raise ValueError("Project not found")

        payload = dict(before)
        for k in [
            "name",
            "type",
            "owner_champion_id",
            "status",
            "created_at",
            "closed_at",
            "work_center",
            "project_code",
            "project_sop",
            "project_eop",
            "related_work_center",
        ]:
            if k in data:
                payload[k] = data[k]

        payload["name"] = (payload.get("name") or "").strip()
        payload["type"] = (payload.get("type") or "custom").strip()
        payload["status"] = (payload.get("status") or "active").strip()
        payload["work_center"] = (payload.get("work_center") or "").strip()

        if not payload["name"]:
            raise ValueError("Nazwa projektu jest wymagana.")

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
                payload.get("name"),
                payload.get("type"),
                payload.get("owner_champion_id"),
                payload.get("status"),
                payload.get("created_at"),
                payload.get("closed_at"),
                payload.get("work_center"),
                payload.get("project_code"),
                payload.get("project_sop"),
                payload.get("project_eop"),
                payload.get("related_work_center"),
                project_id,
            ),
        )
        self._log_changelog(project_id, "UPDATE", {"from": before, "to": payload})
        self.con.commit()

    def delete_project(self, project_id: str) -> bool:
        before = self._get_project(project_id)
        if not before:
            return True

        try:
            self.con.execute("BEGIN")
            self._log_changelog(project_id, "DELETE", before)
            self.con.execute("DELETE FROM projects WHERE id = ?", (project_id,))
            self.con.commit()
            return True
        except sqlite3.IntegrityError:
            self.con.rollback()
            return False

    # ---------
    # internals
    # ---------

    def _get_project(self, project_id: str) -> dict[str, Any] | None:
        cur = self.con.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def _log_changelog(self, project_id: str, event_type: str, payload: dict[str, Any]) -> None:
        if not _table_exists(self.con, "project_changelog"):
            # jak nie ma tabeli (stara baza) — nie blokuj działania
            return
        event_at = datetime.now(timezone.utc).isoformat()
        self.con.execute(
            """
            INSERT INTO project_changelog (id, project_id, event_type, event_at, changes_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(uuid4()), project_id, event_type, json.dumps(payload, ensure_ascii=False)),
        )


# (opcjonalnie) wymuś eksport nazw, jeśli gdzieś używasz: from ...repositories import *
__all__ = [
    "ProductionDataRepository",
    "ProjectRepository",
]
