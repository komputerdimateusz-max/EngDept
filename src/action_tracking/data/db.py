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
  is_draft INTEGER NOT NULL DEFAULT 0,
  due_date TEXT,
  created_at TEXT,
  closed_at TEXT,
  impact_type TEXT,
  impact_value REAL,
  category TEXT,
  manual_savings_amount REAL,
  manual_savings_currency TEXT,
  manual_savings_note TEXT,
  source TEXT,
  source_message_id TEXT UNIQUE,
  submitted_by_email TEXT,
  submitted_at TEXT,
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

CREATE TABLE IF NOT EXISTS action_categories (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  is_active INTEGER NOT NULL DEFAULT 1,
  sort_order INTEGER NOT NULL DEFAULT 100,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_action_categories_active_order
  ON action_categories (is_active, sort_order);

CREATE TABLE IF NOT EXISTS scrap_daily (
  id TEXT PRIMARY KEY,
  metric_date TEXT NOT NULL,
  work_center TEXT NOT NULL,
  scrap_qty INTEGER NOT NULL,
  scrap_cost_amount REAL,
  scrap_cost_currency TEXT NOT NULL DEFAULT 'PLN',
  created_at TEXT NOT NULL,
  UNIQUE(metric_date, work_center, scrap_cost_currency)
);

CREATE TABLE IF NOT EXISTS production_kpi_daily (
  id TEXT PRIMARY KEY,
  metric_date TEXT NOT NULL,
  work_center TEXT NOT NULL,
  worktime_min REAL,
  performance_pct REAL,
  oee_pct REAL,
  availability_pct REAL,
  quality_pct REAL,
  source_file TEXT,
  imported_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(metric_date, work_center)
);

CREATE TABLE IF NOT EXISTS action_effectiveness (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL UNIQUE,
  metric TEXT NOT NULL,
  baseline_from TEXT NOT NULL,
  baseline_to TEXT NOT NULL,
  after_from TEXT NOT NULL,
  after_to TEXT NOT NULL,
  baseline_days INTEGER NOT NULL,
  after_days INTEGER NOT NULL,
  baseline_avg REAL,
  after_avg REAL,
  delta REAL,
  pct_change REAL,
  classification TEXT NOT NULL,
  computed_at TEXT NOT NULL,
  FOREIGN KEY(action_id) REFERENCES actions(id)
);

CREATE TABLE IF NOT EXISTS category_rules (
  category TEXT PRIMARY KEY,
  effect_model TEXT NOT NULL,
  savings_model TEXT NOT NULL,
  requires_scope_link INTEGER NOT NULL,
  is_active INTEGER NOT NULL,
  description TEXT,
  updated_at TEXT NOT NULL
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
        CREATE TABLE IF NOT EXISTS action_categories (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          is_active INTEGER NOT NULL DEFAULT 1,
          sort_order INTEGER NOT NULL DEFAULT 100,
          created_at TEXT NOT NULL
        );
        """
    )
    _set_user_version(con, 5)


def _migrate_to_v6(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS scrap_daily (
          id TEXT PRIMARY KEY,
          metric_date TEXT NOT NULL,
          work_center TEXT NOT NULL,
          scrap_qty INTEGER NOT NULL,
          scrap_cost_amount REAL,
          scrap_cost_currency TEXT NOT NULL DEFAULT 'PLN',
          created_at TEXT NOT NULL,
          UNIQUE(metric_date, work_center, scrap_cost_currency)
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS production_kpi_daily (
          id TEXT PRIMARY KEY,
          metric_date TEXT NOT NULL,
          work_center TEXT NOT NULL,
          oee_pct REAL,
          performance_pct REAL,
          created_at TEXT NOT NULL,
          UNIQUE(metric_date, work_center)
        );
        """
    )
    _set_user_version(con, 6)


def _migrate_to_v7(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS action_effectiveness (
          id TEXT PRIMARY KEY,
          action_id TEXT NOT NULL UNIQUE,
          metric TEXT NOT NULL,
          baseline_from TEXT NOT NULL,
          baseline_to TEXT NOT NULL,
          after_from TEXT NOT NULL,
          after_to TEXT NOT NULL,
          baseline_days INTEGER NOT NULL,
          after_days INTEGER NOT NULL,
          baseline_avg REAL,
          after_avg REAL,
          delta REAL,
          pct_change REAL,
          classification TEXT NOT NULL,
          computed_at TEXT NOT NULL,
          FOREIGN KEY(action_id) REFERENCES actions(id)
        );
        """
    )
    _set_user_version(con, 7)


def _migrate_to_v8(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS action_categories (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          is_active INTEGER NOT NULL DEFAULT 1,
          sort_order INTEGER NOT NULL DEFAULT 100,
          created_at TEXT NOT NULL
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_action_categories_active_order
        ON action_categories (is_active, sort_order);
        """
    )
    if _table_exists(con, "settings_action_categories"):
        row = con.execute("SELECT COUNT(1) AS n FROM action_categories").fetchone()
        if not row or int(row["n"]) == 0:
            con.execute(
                """
                INSERT INTO action_categories (id, name, is_active, sort_order, created_at)
                SELECT id, name, is_active, sort_order, created_at
                FROM settings_action_categories
                """
            )
    _set_user_version(con, 8)


def _migrate_to_v9(con: sqlite3.Connection) -> None:
    if not _column_exists(con, "production_kpi_daily", "worktime_min"):
        con.execute("ALTER TABLE production_kpi_daily ADD COLUMN worktime_min REAL;")
    if not _column_exists(con, "production_kpi_daily", "availability_pct"):
        con.execute("ALTER TABLE production_kpi_daily ADD COLUMN availability_pct REAL;")
    if not _column_exists(con, "production_kpi_daily", "quality_pct"):
        con.execute("ALTER TABLE production_kpi_daily ADD COLUMN quality_pct REAL;")
    if not _column_exists(con, "production_kpi_daily", "source_file"):
        con.execute("ALTER TABLE production_kpi_daily ADD COLUMN source_file TEXT;")
    if not _column_exists(con, "production_kpi_daily", "imported_at"):
        con.execute(
            "ALTER TABLE production_kpi_daily ADD COLUMN imported_at TEXT NOT NULL DEFAULT '';"
        )
    if _column_exists(con, "production_kpi_daily", "imported_at"):
        now = datetime.now(timezone.utc).isoformat()
        if _column_exists(con, "production_kpi_daily", "created_at"):
            con.execute(
                """
                UPDATE production_kpi_daily
                SET imported_at = COALESCE(NULLIF(imported_at, ''), created_at, ?)
                """,
                (now,),
            )
        else:
            con.execute(
                """
                UPDATE production_kpi_daily
                SET imported_at = COALESCE(NULLIF(imported_at, ''), ?)
                """,
                (now,),
            )
    _set_user_version(con, 9)


def _migrate_to_v10(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS category_rules (
          category TEXT PRIMARY KEY,
          effect_model TEXT NOT NULL,
          savings_model TEXT NOT NULL,
          requires_scope_link INTEGER NOT NULL,
          is_active INTEGER NOT NULL,
          description TEXT,
          updated_at TEXT NOT NULL
        );
        """
    )
    if not _column_exists(con, "actions", "manual_savings_amount"):
        con.execute("ALTER TABLE actions ADD COLUMN manual_savings_amount REAL;")
    if not _column_exists(con, "actions", "manual_savings_currency"):
        con.execute("ALTER TABLE actions ADD COLUMN manual_savings_currency TEXT;")
    if not _column_exists(con, "actions", "manual_savings_note"):
        con.execute("ALTER TABLE actions ADD COLUMN manual_savings_note TEXT;")
    _set_user_version(con, 10)


def _migrate_to_v11(con: sqlite3.Connection) -> None:
    if not _column_exists(con, "actions", "is_draft"):
        con.execute("ALTER TABLE actions ADD COLUMN is_draft INTEGER NOT NULL DEFAULT 0;")
    if not _column_exists(con, "actions", "source"):
        con.execute("ALTER TABLE actions ADD COLUMN source TEXT;")
    if not _column_exists(con, "actions", "source_message_id"):
        con.execute("ALTER TABLE actions ADD COLUMN source_message_id TEXT;")
    if not _column_exists(con, "actions", "submitted_by_email"):
        con.execute("ALTER TABLE actions ADD COLUMN submitted_by_email TEXT;")
    if not _column_exists(con, "actions", "submitted_at"):
        con.execute("ALTER TABLE actions ADD COLUMN submitted_at TEXT;")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_actions_source_message_id
        ON actions (source_message_id);
        """
    )
    _set_user_version(con, 11)


def _seed_action_categories(con: sqlite3.Connection) -> None:
    if not _table_exists(con, "action_categories"):
        return
    row = con.execute("SELECT COUNT(1) AS n FROM action_categories").fetchone()
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
        (str(uuid4()), name, 1, (index + 1) * 10, created_at)
        for index, name in enumerate(defaults)
    ]
    con.executemany(
        """
        INSERT INTO action_categories (id, name, is_active, sort_order, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        payload,
    )


def _seed_category_rules(con: sqlite3.Connection) -> None:
    if not _table_exists(con, "category_rules"):
        return
    row = con.execute("SELECT COUNT(1) AS n FROM category_rules").fetchone()
    if row and int(row["n"]) > 0:
        return
    defaults = [
        (
            "Scrap reduction",
            "SCRAP",
            "AUTO_SCRAP_COST",
            1,
            1,
            "Automatyczna ocena redukcji złomu i oszczędności kosztu scrapu.",
        ),
        (
            "OEE improvement",
            "OEE",
            "NONE",
            1,
            1,
            "Ocena zmian OEE na podstawie danych produkcyjnych (bez wyceny PLN).",
        ),
        (
            "Cost savings",
            "NONE",
            "MANUAL_REQUIRED",
            0,
            1,
            "Oszczędności wprowadzane ręcznie przez właściciela akcji.",
        ),
        (
            "Vave",
            "NONE",
            "MANUAL_REQUIRED",
            0,
            1,
            "Oszczędności VAVE wprowadzane ręcznie przez właściciela akcji.",
        ),
        (
            "PDP",
            "NONE",
            "NONE",
            0,
            1,
            "Brak automatycznych obliczeń; opisujemy rezultat w treści akcji.",
        ),
        (
            "Development",
            "NONE",
            "NONE",
            0,
            1,
            "Akcja rozwojowa bez automatycznych KPI i wyceny oszczędności.",
        ),
    ]
    updated_at = datetime.now(timezone.utc).isoformat()
    payload = [(*row, updated_at) for row in defaults]
    con.executemany(
        """
        INSERT INTO category_rules (
          category,
          effect_model,
          savings_model,
          requires_scope_link,
          is_active,
          description,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
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
    if current_version < 6:
        _migrate_to_v6(con)
    if current_version < 7:
        _migrate_to_v7(con)
    if current_version < 8:
        _migrate_to_v8(con)
    if current_version < 9:
        _migrate_to_v9(con)
    if current_version < 10:
        _migrate_to_v10(con)
    if current_version < 11:
        _migrate_to_v11(con)
    _seed_action_categories(con)
    _seed_category_rules(con)
    con.commit()

def table_count(con: sqlite3.Connection, table: str) -> int:
    cur = con.execute(f"SELECT COUNT(1) AS n FROM {table}")
    return int(cur.fetchone()["n"])
