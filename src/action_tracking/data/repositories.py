from __future__ import annotations

import sqlite3
from typing import Any


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
