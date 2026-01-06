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
  importance TEXT,
  FOREIGN KEY(owner_champion_id) REFERENCES champions(id)
);

CREATE TABLE IF NOT EXISTS actions (
  id TEXT PRIMARY KEY,
  project_id TEXT,
  analysis_id TEXT,
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
  impact_aspects TEXT,
  category TEXT,
  area TEXT,
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
  full_project TEXT,
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
  full_project TEXT,
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

CREATE TABLE IF NOT EXISTS wc_inbox (
  id TEXT PRIMARY KEY,
  wc_raw TEXT NOT NULL,
  wc_norm TEXT NOT NULL UNIQUE,
  full_project TEXT,
  sources TEXT NOT NULL,
  first_seen_date TEXT,
  last_seen_date TEXT,
  status TEXT NOT NULL DEFAULT 'open',
  linked_project_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wc_inbox_status
  ON wc_inbox (status);

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
  overlay_targets TEXT,
  requires_scope_link INTEGER NOT NULL,
  is_active INTEGER NOT NULL,
  description TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS email_notifications_log (
  id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  notification_type TEXT NOT NULL,
  recipient_email TEXT NOT NULL,
  action_id TEXT,
  payload_json TEXT,
  unique_key TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS analyses (
  id TEXT PRIMARY KEY,
  project_id TEXT,
  champion_id TEXT,
  tool_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  created_at TEXT NOT NULL,
  closed_at TEXT,
  area TEXT,
  template_json TEXT NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(champion_id) REFERENCES champions(id)
);

CREATE TABLE IF NOT EXISTS analysis_actions (
  id TEXT PRIMARY KEY,
  analysis_id TEXT NOT NULL,
  action_type TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  due_date TEXT,
  owner_champion_id TEXT,
  added_action_id TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(analysis_id) REFERENCES analyses(id),
  FOREIGN KEY(owner_champion_id) REFERENCES champions(id),
  FOREIGN KEY(added_action_id) REFERENCES actions(id)
);

CREATE INDEX IF NOT EXISTS idx_analysis_actions_analysis
  ON analysis_actions (analysis_id);

CREATE TABLE IF NOT EXISTS analysis_changelog (
  id TEXT PRIMARY KEY,
  analysis_id TEXT,
  event_type TEXT NOT NULL,
  event_at TEXT NOT NULL,
  changes_json TEXT NOT NULL
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
          overlay_targets TEXT,
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


def _migrate_to_v12(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS email_notifications_log (
          id TEXT PRIMARY KEY,
          created_at TEXT NOT NULL,
          notification_type TEXT NOT NULL,
          recipient_email TEXT NOT NULL,
          action_id TEXT,
          payload_json TEXT,
          unique_key TEXT NOT NULL UNIQUE
        );
        """
    )
    _set_user_version(con, 12)


def _migrate_to_v13(con: sqlite3.Connection) -> None:
    # 1) New Work Center detection inbox (post-import review)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS wc_inbox (
          id TEXT PRIMARY KEY,
          wc_raw TEXT NOT NULL,
          wc_norm TEXT NOT NULL UNIQUE,
          full_project TEXT,
          sources TEXT NOT NULL,
          first_seen_date TEXT,
          last_seen_date TEXT,
          status TEXT NOT NULL DEFAULT 'open',
          linked_project_id TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_wc_inbox_status
        ON wc_inbox (status);
        """
    )

    # 2) Action impact aspects (what KPI dimensions this action affects)
    if not _column_exists(con, "actions", "impact_aspects"):
        con.execute("ALTER TABLE actions ADD COLUMN impact_aspects TEXT;")

    # Optional backfill based on category rules (if table exists)
    if _table_exists(con, "category_rules"):
        backfills = [
            ("SCRAP", '["SCRAP"]'),
            ("OEE", '["OEE"]'),
            ("PERFORMANCE", '["PERFORMANCE"]'),
        ]
        for effect_model, payload in backfills:
            con.execute(
                """
                UPDATE actions
                SET impact_aspects = ?
                WHERE impact_aspects IS NULL
                  AND category IN (
                    SELECT category
                    FROM category_rules
                    WHERE effect_model = ?
                  )
                """,
                (payload, effect_model),
            )

    _set_user_version(con, 13)


def _migrate_to_v14(con: sqlite3.Connection) -> None:
    if _table_exists(con, "category_rules") and not _column_exists(
        con, "category_rules", "overlay_targets"
    ):
        con.execute("ALTER TABLE category_rules ADD COLUMN overlay_targets TEXT;")
    _set_user_version(con, 14)


def _migrate_to_v15(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS analyses (
          id TEXT PRIMARY KEY,
          project_id TEXT,
          champion_id TEXT,
          tool_type TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'open',
          created_at TEXT NOT NULL,
          closed_at TEXT,
          template_json TEXT NOT NULL,
          FOREIGN KEY(project_id) REFERENCES projects(id),
          FOREIGN KEY(champion_id) REFERENCES champions(id)
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS analysis_actions (
          id TEXT PRIMARY KEY,
          analysis_id TEXT NOT NULL,
          action_type TEXT NOT NULL,
          title TEXT NOT NULL,
          description TEXT,
          due_date TEXT,
          owner_champion_id TEXT,
          added_action_id TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(analysis_id) REFERENCES analyses(id),
          FOREIGN KEY(owner_champion_id) REFERENCES champions(id),
          FOREIGN KEY(added_action_id) REFERENCES actions(id)
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_analysis_actions_analysis
          ON analysis_actions (analysis_id);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS analysis_changelog (
          id TEXT PRIMARY KEY,
          analysis_id TEXT,
          event_type TEXT NOT NULL,
          event_at TEXT NOT NULL,
          changes_json TEXT NOT NULL
        );
        """
    )
    if _table_exists(con, "actions") and not _column_exists(con, "actions", "analysis_id"):
        con.execute("ALTER TABLE actions ADD COLUMN analysis_id TEXT;")
    _set_user_version(con, 15)


def _migrate_to_v16(con: sqlite3.Connection) -> None:
    if _table_exists(con, "projects") and not _column_exists(con, "projects", "importance"):
        con.execute("ALTER TABLE projects ADD COLUMN importance TEXT;")
    _set_user_version(con, 16)


def _migrate_to_v17(con: sqlite3.Connection) -> None:
    if _table_exists(con, "scrap_daily") and not _column_exists(con, "scrap_daily", "full_project"):
        con.execute("ALTER TABLE scrap_daily ADD COLUMN full_project TEXT;")
    if _table_exists(con, "production_kpi_daily") and not _column_exists(con, "production_kpi_daily", "full_project"):
        con.execute("ALTER TABLE production_kpi_daily ADD COLUMN full_project TEXT;")
    if _table_exists(con, "actions") and not _column_exists(con, "actions", "area"):
        con.execute("ALTER TABLE actions ADD COLUMN area TEXT;")
    if _table_exists(con, "analyses") and not _column_exists(con, "analyses", "area"):
        con.execute("ALTER TABLE analyses ADD COLUMN area TEXT;")
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scrap_daily_full_project_date
          ON scrap_daily (full_project, metric_date);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_kpi_daily_full_project_date
          ON production_kpi_daily (full_project, metric_date);
        """
    )
    _set_user_version(con, 17)


def _migrate_to_v18(con: sqlite3.Connection) -> None:
    if _table_exists(con, "wc_inbox") and not _column_exists(con, "wc_inbox", "full_project"):
        con.execute("ALTER TABLE wc_inbox ADD COLUMN full_project TEXT;")
    _set_user_version(con, 18)


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
    if current_version < 12:
        _migrate_to_v12(con)
    if current_version < 13:
        _migrate_to_v13(con)
    if current_version < 14:
        _migrate_to_v14(con)
    if current_version < 15:
        _migrate_to_v15(con)
    if current_version < 16:
        _migrate_to_v16(con)
    if current_version < 17:
        _migrate_to_v17(con)
    if current_version < 18:
        _migrate_to_v18(con)
    _seed_action_categories(con)
    _seed_category_rules(con)
    con.commit()


def table_count(con: sqlite3.Connection, table: str) -> int:
    cur = con.execute(f"SELECT COUNT(1) AS n FROM {table}")
    return int(cur.fetchone()["n"])
