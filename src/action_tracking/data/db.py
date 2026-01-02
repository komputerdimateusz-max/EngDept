from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from uuid import uuid4

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS champions (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT,
  team TEXT,
  active INTEGER NOT NULL DEFAULT 1,
  first_name TEXT NOT NULL DEFAULT '',
  last_name TEXT NOT NULL DEFAULT '',
  hire_date TEXT,
  position TEXT
);

CREATE TABLE IF NOT EXISTS projects (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL DEFAULT 'custom',
  owner_champion_id TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT,
  closed_at TEXT,
  work_center TEXT NOT NULL DEFAULT '',
  project_code TEXT,
  project_sop TEXT,
  project_eop TEXT,
  related_work_center TEXT,
  FOREIGN KEY(owner_champion_id) REFERENCES champions(id)
);

CREATE TABLE IF NOT EXISTS actions (
  id TEXT PRIMARY KEY,
  project_id TEXT,
  title TEXT NOT NULL,
  description TEXT,
  owner_champion_id TEXT,
  priority TEXT NOT NULL DEFAULT 'med',
  status TEXT NOT NULL DEFAULT 'open',
  due_date TEXT,
  created_at TEXT,
  closed_at TEXT,
  impact_type TEXT,
  impact_value REAL,
  category TEXT,
  FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(owner_champion_id) REFERENCES champions(id)
);

CREATE TABLE IF NOT EXISTS champion_projects (
  champion_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  PRIMARY KEY (champion_id, project_id),
  FOREIGN KEY(champion_id) REFERENCES champions(id) ON DELETE CASCADE,
  FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS champion_changelog (
  id TEXT PRIMARY KEY,
  champion_id TEXT,
  event_type TEXT NOT NULL,
  event_at TEXT NOT NULL,
  changes_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_changelog (
  id TEXT PRIMARY KEY,
  project_id TEXT,
  event_type TEXT NOT NULL,
  event_at TEXT NOT NULL,
  changes_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS action_changelog (
  id TEXT PRIMARY KEY,
  action_id TEXT,
  event_type TEXT NOT NULL,
  event_at TEXT NOT NULL,
  changes_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings_action_categories (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  is_active INTEGER NOT NULL DEFAULT 1,
  sort_order INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
"""

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path.as_posix())
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def _get_user_version(con: sqlite3.Connection) -> int:
    row = con.execute("PRAGMA user_version;").fetchone()
    return int(row[0]) if row else 0


def _set_user_version(con: sqlite3.Connection, version: int) -> None:
    con.execute(f"PRAGMA user_version = {version};")


def _column_exists(con: sqlite3.Connection, table: str, column: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table});")
    return any(row["name"] == column for row in cur.fetchall())


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


def _backfill_champion_names(con: sqlite3.Connection) -> None:
    rows = con.execute(
        """
        SELECT id, name, first_name, last_name
        FROM champions
        """
    ).fetchall()
    updates: list[tuple[str, str, str]] = []
    for row in rows:
        if (row["first_name"] or row["last_name"]) or not row["name"]:
            continue
        parts = row["name"].strip().split()
        if not parts:
            continue
        first_name = parts[0]
        last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
        updates.append((first_name, last_name, row["id"]))
    if updates:
        con.executemany(
            """
            UPDATE champions
            SET first_name = ?, last_name = ?
            WHERE id = ?
            """,
            updates,
        )


def _migrate_to_v2(con: sqlite3.Connection) -> None:
    if not _column_exists(con, "champions", "first_name"):
        con.execute("ALTER TABLE champions ADD COLUMN first_name TEXT NOT NULL DEFAULT '';")
    if not _column_exists(con, "champions", "last_name"):
        con.execute("ALTER TABLE champions ADD COLUMN last_name TEXT NOT NULL DEFAULT '';")
    if not _column_exists(con, "champions", "hire_date"):
        con.execute("ALTER TABLE champions ADD COLUMN hire_date TEXT;")
    if not _column_exists(con, "champions", "position"):
        con.execute("ALTER TABLE champions ADD COLUMN position TEXT;")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS champion_projects (
          champion_id TEXT NOT NULL,
          project_id TEXT NOT NULL,
          PRIMARY KEY (champion_id, project_id),
          FOREIGN KEY(champion_id) REFERENCES champions(id) ON DELETE CASCADE,
          FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS champion_changelog (
          id TEXT PRIMARY KEY,
          champion_id TEXT,
          event_type TEXT NOT NULL,
          event_at TEXT NOT NULL,
          changes_json TEXT NOT NULL
        );
        """
    )
    _backfill_champion_names(con)
    _set_user_version(con, 2)


def _migrate_to_v3(con: sqlite3.Connection) -> None:
    project_columns = {
        "work_center": "TEXT NOT NULL DEFAULT ''",
        "project_code": "TEXT",
        "project_sop": "TEXT",
        "project_eop": "TEXT",
        "related_work_center": "TEXT",
    }
    for column, column_type in project_columns.items():
        if not _column_exists(con, "projects", column):
            con.execute(f"ALTER TABLE projects ADD COLUMN {column} {column_type};")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS project_changelog (
          id TEXT PRIMARY KEY,
          project_id TEXT,
          event_type TEXT NOT NULL,
          event_at TEXT NOT NULL,
          changes_json TEXT NOT NULL
        );
        """
    )
    _set_user_version(con, 3)


def _migrate_to_v4(con: sqlite3.Connection) -> None:
    if not _column_exists(con, "actions", "category"):
        con.execute("ALTER TABLE actions ADD COLUMN category TEXT;")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS action_changelog (
          id TEXT PRIMARY KEY,
          action_id TEXT,
          event_type TEXT NOT NULL,
          event_at TEXT NOT NULL,
          changes_json TEXT NOT NULL
        );
        """
    )
    _set_user_version(con, 4)


def _migrate_to_v5(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS settings_action_categories (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          is_active INTEGER NOT NULL DEFAULT 1,
          sort_order INTEGER NOT NULL,
          created_at TEXT NOT NULL
        );
        """
    )
    _set_user_version(con, 5)


def _seed_action_categories(con: sqlite3.Connection) -> None:
    if not _table_exists(con, "settings_action_categories"):
        return
    row = con.execute("SELECT COUNT(1) AS n FROM settings_action_categories").fetchone()
    if row and int(row["n"]) > 0:
        return
    defaults = [
        "Scrap reduction",
        "OEE improvement",
        "Cost savings",
        "Vave",
        "PDP",
        "Development",
    ]
    created_at = datetime.now(timezone.utc).isoformat()
    payload = [
        (str(uuid4()), name, 1, index + 1, created_at)
        for index, name in enumerate(defaults)
    ]
    con.executemany(
        """
        INSERT INTO settings_action_categories (id, name, is_active, sort_order, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        payload,
    )


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA_SQL)
    current_version = _get_user_version(con)
    if current_version < 2:
        _migrate_to_v2(con)
    if current_version < 3:
        _migrate_to_v3(con)
    if current_version < 4:
        _migrate_to_v4(con)
    if current_version < 5:
        _migrate_to_v5(con)
    _seed_action_categories(con)
    con.commit()

def table_count(con: sqlite3.Connection, table: str) -> int:
    cur = con.execute(f"SELECT COUNT(1) AS n FROM {table}")
    return int(cur.fetchone()["n"])
