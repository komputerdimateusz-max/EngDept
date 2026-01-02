from __future__ import annotations

from pathlib import Path
import sqlite3

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS champions (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT,
  team TEXT,
  active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS projects (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  owner_champion_id TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT,
  closed_at TEXT,
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
  FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(owner_champion_id) REFERENCES champions(id)
);
"""

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path.as_posix())
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def init_db(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA_SQL)
    con.commit()

def table_count(con: sqlite3.Connection, table: str) -> int:
    cur = con.execute(f"SELECT COUNT(1) AS n FROM {table}")
    return int(cur.fetchone()["n"])
