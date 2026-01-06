from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from action_tracking.services.metrics_scale import normalize_kpi_percent
from action_tracking.services.workcenter_classifier import filter_rows_by_areas

# =====================================================
# OPTIONAL IMPORTS (never crash if modules moved / missing)
# =====================================================

try:
    from action_tracking.domain.constants import ACTION_CATEGORIES as DEFAULT_ACTION_CATEGORIES
except Exception:  # pragma: no cover
    DEFAULT_ACTION_CATEGORIES = [
        "Scrap reduction",
        "OEE improvement",
        "Cost savings",
        "Vave",
        "PDP",
        "Development",
    ]

try:
    from action_tracking.services.normalize import normalize_key
except Exception:  # pragma: no cover
    def normalize_key(value: str) -> str:
        return (value or "").strip().lower()

try:
    # preferred: shared service
    from action_tracking.services.impact_aspects import normalize_impact_aspects  # type: ignore
except Exception:  # pragma: no cover
    def normalize_impact_aspects(value: Any) -> list[str]:
        if value in (None, "", []):
            return []
        if isinstance(value, str):
            v = value.strip()
            if not v:
                return []
            # If it's already a JSON list string, best-effort parse
            if v.startswith("[") and v.endswith("]"):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    pass
            return [v]
        if isinstance(value, (list, tuple, set)):
            return [str(x).strip() for x in value if str(x).strip()]
        return []

try:
    from action_tracking.services.overlay_targets import (  # type: ignore
        parse_overlay_targets,
        serialize_overlay_targets,
    )
except Exception:  # pragma: no cover
    def parse_overlay_targets(value: Any) -> list[str]:
        """
        Accept: None / "" / JSON string list / list/tuple/set / comma separated string.
        Return: list[str]
        """
        if value in (None, ""):
            return []
        if isinstance(value, str):
            v = value.strip()
            if not v:
                return []
            # JSON list?
            if v.startswith("[") and v.endswith("]"):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    pass
            # comma separated
            return [t.strip() for t in v.split(",") if t.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(x).strip() for x in value if str(x).strip()]
        return []

    def serialize_overlay_targets(value: Any) -> str | None:
        arr = parse_overlay_targets(value)
        return json.dumps(arr, ensure_ascii=False) if arr else None


# =====================================================
# HELPERS
# =====================================================

_CONFIGURED_CONNECTIONS: set[int] = set()


def _configure_sqlite_connection(con: sqlite3.Connection) -> None:
    """
    Apply SQLite settings once per connection to reduce lock contention.
    WAL + busy_timeout are safe defaults for Streamlit reruns.
    """
    con_id = id(con)
    if con_id in _CONFIGURED_CONNECTIONS:
        return
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA busy_timeout = 5000;")
    except sqlite3.Error:
        # Defensive: do not block app startup if pragmas are unsupported.
        return
    _CONFIGURED_CONNECTIONS.add(con_id)


def _rollback_safely(con: sqlite3.Connection) -> None:
    try:
        con.execute("ROLLBACK")
    except sqlite3.Error:
        pass


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


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    try:
        cur = con.execute(f"PRAGMA table_info({table})")
        return {r[1] for r in cur.fetchall()}
    except sqlite3.Error:
        return set()


def _ensure_column(
    con: sqlite3.Connection,
    table: str,
    column: str,
    column_type: str,
) -> None:
    if not _table_exists(con, table):
        return
    columns = _table_columns(con, table)
    if column in columns:
        return
    try:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type};")
    except sqlite3.Error:
        return


def _ensure_index(con: sqlite3.Connection, ddl: str) -> None:
    try:
        con.execute(ddl)
    except sqlite3.Error:
        return


def _normalize_impact_aspects_payload(value: Any) -> str | None:
    """
    impact_aspects stored as JSON string in DB.
    Accept list/str/None -> returns JSON list string or None.
    """
    normalized = normalize_impact_aspects(value)
    if not normalized:
        return None
    # de-dup + stable order
    cleaned = sorted({x for x in (str(v).strip() for v in normalized) if x})
    return json.dumps(cleaned, ensure_ascii=False) if cleaned else None


def _normalize_percent(value: Any) -> float | None:
    return normalize_kpi_percent(value)


def _normalize_int(value: Any, default: int | None = None) -> int | None:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalize_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# =====================================================
# DEFAULT CATEGORY RULES (fallback if category_rules missing)
# =====================================================

DEFAULT_CATEGORY_RULES: dict[str, dict[str, Any]] = {
    "Scrap reduction": {
        "effect_model": "SCRAP",
        "savings_model": "AUTO_SCRAP_COST",
        "requires_scope_link": True,
        "is_active": True,
        "description": "Automatyczna ocena redukcji złomu i oszczędności kosztu scrapu.",
    },
    "OEE improvement": {
        "effect_model": "OEE",
        "savings_model": "NONE",
        "requires_scope_link": True,
        "is_active": True,
        "description": "Ocena zmian OEE na podstawie danych produkcyjnych (bez wyceny PLN).",
    },
    "Cost savings": {
        "effect_model": "NONE",
        "savings_model": "MANUAL_REQUIRED",
        "requires_scope_link": False,
        "is_active": True,
        "description": "Oszczędności wprowadzane ręcznie przez właściciela akcji.",
    },
    "Vave": {
        "effect_model": "NONE",
        "savings_model": "MANUAL_REQUIRED",
        "requires_scope_link": False,
        "is_active": True,
        "description": "Oszczędności VAVE wprowadzane ręcznie przez właściciela akcji.",
    },
    "PDP": {
        "effect_model": "NONE",
        "savings_model": "NONE",
        "requires_scope_link": False,
        "is_active": True,
        "description": "Brak automatycznych obliczeń; rezultat opisujemy w treści akcji.",
    },
    "Development": {
        "effect_model": "NONE",
        "savings_model": "NONE",
        "requires_scope_link": False,
        "is_active": True,
        "description": "Akcja rozwojowa bez automatycznych KPI i wyceny oszczędności.",
    },
}


def _default_category_rule(category: str) -> dict[str, Any]:
    base = DEFAULT_CATEGORY_RULES.get(category)
    if base:
        return {
            "category": category,
            "effect_model": base["effect_model"],
            "savings_model": base["savings_model"],
            "requires_scope_link": bool(base["requires_scope_link"]),
            "is_active": bool(base["is_active"]),
            "description": base.get("description"),
            "overlay_targets": None,
            "updated_at": None,
        }
    return {
        "category": category,
        "effect_model": "NONE",
        "savings_model": "NONE",
        "requires_scope_link": False,
        "is_active": True,
        "description": "Brak zdefiniowanej metodologii dla tej kategorii.",
        "overlay_targets": None,
        "updated_at": None,
    }


def _default_category_rules_list(include_inactive: bool = True) -> list[dict[str, Any]]:
    # ensure we include defaults + any categories defined in constants
    categories = list(dict.fromkeys(list(DEFAULT_ACTION_CATEGORIES) + list(DEFAULT_CATEGORY_RULES.keys())))
    rows = [_default_category_rule(c) for c in categories]
    if not include_inactive:
        rows = [r for r in rows if bool(r.get("is_active", True))]
    return rows


# =====================================================
# CHANGELOG READER (UI CONTRACT)
# =====================================================

def _list_changelog_generic(
    con: sqlite3.Connection,
    table_candidates: list[str],
    limit: int = 50,
    entity_id: str | None = None,
) -> list[dict[str, Any]]:
    """
    Uniwersalny reader changelogów.

    Kontrakt UI (pages/champions.py, pages/projects.py, pages/actions.py):
      - entry["event_at"]
      - entry["event_type"]
      - entry["changes_json"]  (JSON string)

    Dlatego ZAWSZE zwracamy te klucze, nawet jeśli DB ma inne nazwy kolumn.
    Jeśli tabela/kolumny nie istnieją -> [] (bez crasha UI).
    """
    if not table_candidates:
        return []

    table: str | None = None
    for t in table_candidates:
        if _table_exists(con, t):
            table = t
            break
    if not table:
        return []

    try:
        cur = con.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
    except sqlite3.Error:
        cols = []
    if not cols:
        return []

    time_col: str | None = None
    for c in ("event_at", "changed_at", "created_at", "timestamp", "ts"):
        if c in cols:
            time_col = c
            break

    event_type_col: str | None = None
    for c in ("event_type", "change_type", "action", "event", "type", "entity_type"):
        if c in cols:
            event_type_col = c
            break

    entity_col: str | None = None
    for c in ("entity_id", "object_id", "record_id", "champion_id", "project_id", "action_id"):
        if c in cols:
            entity_col = c
            break

    preferred = [
        "id",
        "entity_type",
        "entity_id",
        "champion_id",
        "project_id",
        "action_id",
        "event_type",
        "change_type",
        "field",
        "old_value",
        "new_value",
        "summary",
        "message",
        "changes_json",
        "payload_json",
        "user_email",
        "changed_by",
        "created_by",
        "source",
        "event_at",
        "changed_at",
        "created_at",
        "timestamp",
        "ts",
    ]
    selected_cols = [c for c in preferred if c in cols]
    select_sql = ", ".join(selected_cols) if selected_cols else "*"

    query = f"SELECT {select_sql} FROM {table}"
    params: list[Any] = []

    if entity_id and entity_col:
        query += f" WHERE {entity_col} = ?"
        params.append(entity_id)

    if time_col:
        query += f" ORDER BY {time_col} DESC"
    else:
        query += " ORDER BY rowid DESC"

    query += " LIMIT ?"
    params.append(int(limit))

    try:
        cur = con.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
    except sqlite3.Error:
        return []

    def _ensure_str_json(value: Any) -> str:
        if value is None:
            return "{}"
        if isinstance(value, str):
            s = value.strip()
            if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                return s
            try:
                return json.dumps({"message": s}, ensure_ascii=False)
            except Exception:
                return "{}"
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return "{}"

    for r in rows:
        # event_at MUST exist
        if "event_at" not in r or r.get("event_at") in (None, ""):
            if r.get("changed_at") not in (None, ""):
                r["event_at"] = r.get("changed_at")
            elif r.get("created_at") not in (None, ""):
                r["event_at"] = r.get("created_at")
            elif r.get("timestamp") not in (None, ""):
                r["event_at"] = r.get("timestamp")
            elif r.get("ts") not in (None, ""):
                r["event_at"] = r.get("ts")
            else:
                r["event_at"] = None

        # event_type MUST exist
        if "event_type" not in r or r.get("event_type") in (None, ""):
            if r.get("change_type") not in (None, ""):
                r["event_type"] = r.get("change_type")
            elif r.get("entity_type") not in (None, ""):
                r["event_type"] = r.get("entity_type")
            elif r.get("field") not in (None, ""):
                r["event_type"] = f"field_change:{r.get('field')}"
            elif r.get("summary") not in (None, ""):
                r["event_type"] = "summary"
            elif r.get("message") not in (None, ""):
                r["event_type"] = "message"
            else:
                r["event_type"] = "change"

        # changes_json MUST exist
        if "changes_json" not in r or r.get("changes_json") in (None, ""):
            if r.get("payload_json") not in (None, ""):
                r["changes_json"] = _ensure_str_json(r.get("payload_json"))
            else:
                minimal: dict[str, Any] = {}
                if r.get("field") is not None:
                    minimal["field"] = r.get("field")
                if "old_value" in r:
                    minimal["old_value"] = r.get("old_value")
                if "new_value" in r:
                    minimal["new_value"] = r.get("new_value")
                if r.get("summary"):
                    minimal["summary"] = r.get("summary")
                if r.get("message"):
                    minimal["message"] = r.get("message")
                r["changes_json"] = _ensure_str_json(minimal)

        # aliases for legacy code
        if "changed_at" not in r or r.get("changed_at") in (None, ""):
            r["changed_at"] = r.get("event_at")
        if "change_type" not in r or r.get("change_type") in (None, ""):
            r["change_type"] = r.get("event_type")

        # pre-parse
        try:
            r["changes"] = json.loads(r["changes_json"]) if r.get("changes_json") else {}
        except Exception:
            r["changes"] = {}

        # legacy "payload"
        if "payload" not in r and r.get("payload_json"):
            try:
                r["payload"] = json.loads(r["payload_json"])
            except Exception:
                r["payload"] = None

    return rows


# =====================================================
# SETTINGS / GLOBAL RULES
# =====================================================

class SettingsRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        _configure_sqlite_connection(self.con)
        self._ensure_production_schema()

    def _ensure_production_schema(self) -> None:
        _ensure_column(self.con, "scrap_daily", "full_project", "TEXT")
        _ensure_column(self.con, "production_kpi_daily", "full_project", "TEXT")
        _ensure_index(
            self.con,
            "CREATE INDEX IF NOT EXISTS idx_scrap_daily_full_project_date "
            "ON scrap_daily (full_project, metric_date);",
        )
        _ensure_index(
            self.con,
            "CREATE INDEX IF NOT EXISTS idx_kpi_daily_full_project_date "
            "ON production_kpi_daily (full_project, metric_date);",
        )

    def list_action_categories(self, active_only: bool = True) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "action_categories"):
            return [
                {
                    "id": name,
                    "name": name,
                    "is_active": True,
                    "sort_order": (i + 1) * 10,
                    "created_at": None,
                }
                for i, name in enumerate(DEFAULT_ACTION_CATEGORIES)
            ]

        cols = _table_columns(self.con, "action_categories")
        if not cols:
            return []

        select_cols = [c for c in ("id", "name", "is_active", "sort_order", "created_at") if c in cols]
        if not select_cols:
            return []

        query = f"""
            SELECT {", ".join(select_cols)}
            FROM action_categories
        """
        params: list[Any] = []
        if active_only:
            if "is_active" in cols:
                query += " WHERE is_active = 1"
        if "sort_order" in cols and "name" in cols:
            query += " ORDER BY sort_order ASC, name ASC"
        elif "name" in cols:
            query += " ORDER BY name ASC"
        cur = self.con.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r.setdefault("id", r.get("name"))
            r.setdefault("name", r.get("id"))
            r["is_active"] = bool(r.get("is_active", True))
            r.setdefault("sort_order", None)
            r.setdefault("created_at", None)
        return rows

    def create_action_category(self, name: str, sort_order: int | None = None) -> str | None:
        if not name:
            return None
        if not _table_exists(self.con, "action_categories"):
            return None

        cols = _table_columns(self.con, "action_categories")
        if not cols:
            return None

        category_id = str(uuid4()) if "id" in cols else name
        now = datetime.now(timezone.utc).isoformat()
        payload: dict[str, Any] = {}
        if "id" in cols:
            payload["id"] = category_id
        if "name" in cols:
            payload["name"] = name
        if "is_active" in cols:
            payload["is_active"] = 1
        if "sort_order" in cols and sort_order is not None:
            payload["sort_order"] = int(sort_order)
        if "created_at" in cols:
            payload["created_at"] = now

        if not payload:
            return None

        insert_cols = list(payload.keys())
        placeholders = ", ".join(["?"] * len(insert_cols))
        self.con.execute(
            f"INSERT INTO action_categories ({', '.join(insert_cols)}) VALUES ({placeholders})",
            [payload[c] for c in insert_cols],
        )
        self.con.commit()
        return category_id

    def update_action_category(
        self,
        category_id: str,
        name: str | None = None,
        sort_order: int | None = None,
        is_active: bool | None = None,
    ) -> None:
        if not category_id:
            return
        if not _table_exists(self.con, "action_categories"):
            return

        cols = _table_columns(self.con, "action_categories")
        if not cols:
            return

        payload: dict[str, Any] = {}
        if name is not None and "name" in cols:
            payload["name"] = name
        if sort_order is not None and "sort_order" in cols:
            payload["sort_order"] = int(sort_order)
        if is_active is not None and "is_active" in cols:
            payload["is_active"] = 1 if is_active else 0

        if not payload:
            return

        key_col = "id" if "id" in cols else ("name" if "name" in cols else None)
        if not key_col:
            return

        sets = [f"{col} = ?" for col in payload.keys()]
        params = list(payload.values())
        params.append(category_id)
        self.con.execute(
            f"UPDATE action_categories SET {', '.join(sets)} WHERE {key_col} = ?",
            params,
        )
        self.con.commit()

    def deactivate_action_category(self, category_id: str) -> None:
        if not category_id:
            return
        if not _table_exists(self.con, "action_categories"):
            return
        cols = _table_columns(self.con, "action_categories")
        if not cols or "is_active" not in cols:
            return
        key_col = "id" if "id" in cols else ("name" if "name" in cols else None)
        if not key_col:
            return
        self.con.execute(
            f"UPDATE action_categories SET is_active = 0 WHERE {key_col} = ?",
            (category_id,),
        )
        self.con.commit()


class GlobalSettingsRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    # --- UI-facing (Projects / Settings pages expect these keys) ---
    def get_category_rules(self, only_active: bool = True) -> list[dict[str, Any]]:
        """
        Returns rows shaped as:
          {
            category_label,
            effectiveness_model,
            savings_model,
            overlay_targets (list[str]),
            overlay_targets_configured (bool),
            requires_scope_link (bool),
            description,
            is_active (bool)
          }
        """
        include_inactive = not only_active

        if not _table_exists(self.con, "category_rules"):
            return [self._normalize_category_rule_row(r) for r in _default_category_rules_list(include_inactive)]

        # Try query WITH overlay_targets (new schema), fallback to old schema if missing column
        try:
            query = """
                SELECT category AS category_label,
                       effect_model AS effectiveness_model,
                       savings_model,
                       overlay_targets,
                       requires_scope_link,
                       description,
                       is_active
                FROM category_rules
            """
            params: list[Any] = []
            if only_active:
                query += " WHERE is_active = 1"
            query += " ORDER BY category ASC"
            cur = self.con.execute(query, params)
            rows = [self._normalize_category_rule_row(dict(r)) for r in cur.fetchall()]
            if not rows:
                return [self._normalize_category_rule_row(r) for r in _default_category_rules_list(include_inactive)]
            return rows
        except sqlite3.Error:
            # Old schema (no overlay_targets)
            try:
                query = """
                    SELECT category AS category_label,
                           effect_model AS effectiveness_model,
                           savings_model,
                           requires_scope_link,
                           description,
                           is_active
                    FROM category_rules
                """
                params = []
                if only_active:
                    query += " WHERE is_active = 1"
                query += " ORDER BY category ASC"
                cur = self.con.execute(query, params)
                rows = [self._normalize_category_rule_row(dict(r)) for r in cur.fetchall()]
                if not rows:
                    return [self._normalize_category_rule_row(r) for r in _default_category_rules_list(include_inactive)]
                return rows
            except sqlite3.Error:
                return [self._normalize_category_rule_row(r) for r in _default_category_rules_list(include_inactive)]

    def resolve_category_rule(self, category_label: str) -> dict[str, Any] | None:
        if not category_label:
            return None
        rules = self.get_category_rules(only_active=True)
        rules_map = {normalize_key(r.get("category_label") or ""): r for r in rules}
        return rules_map.get(normalize_key(category_label))

    # --- Admin / internal CRUD (used by configurable overlays/settings) ---
    def list_category_rules(self, include_inactive: bool = False) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "category_rules"):
            return _default_category_rules_list(include_inactive=True if include_inactive else False)

        # Prefer new schema
        try:
            query = """
                SELECT category,
                       effect_model,
                       savings_model,
                       overlay_targets,
                       requires_scope_link,
                       is_active,
                       description,
                       updated_at
                FROM category_rules
            """
            params: list[Any] = []
            if not include_inactive:
                query += " WHERE is_active = 1"
            query += " ORDER BY category ASC"
            cur = self.con.execute(query, params)
            rows = [self._normalize_rule_row(dict(r)) for r in cur.fetchall()]
            if not rows:
                return _default_category_rules_list(include_inactive=True if include_inactive else False)
            return rows
        except sqlite3.Error:
            # Old schema
            try:
                query = """
                    SELECT category,
                           effect_model,
                           savings_model,
                           requires_scope_link,
                           is_active,
                           description,
                           updated_at
                    FROM category_rules
                """
                params = []
                if not include_inactive:
                    query += " WHERE is_active = 1"
                query += " ORDER BY category ASC"
                cur = self.con.execute(query, params)
                rows = [self._normalize_rule_row(dict(r)) for r in cur.fetchall()]
                if not rows:
                    return _default_category_rules_list(include_inactive=True if include_inactive else False)
                return rows
            except sqlite3.Error:
                return _default_category_rules_list(include_inactive=True if include_inactive else False)

    def get_category_rule(self, category: str) -> dict[str, Any] | None:
        if not category:
            return None
        if not _table_exists(self.con, "category_rules"):
            return None

        # Prefer new schema
        try:
            cur = self.con.execute(
                """
                SELECT category,
                       effect_model,
                       savings_model,
                       overlay_targets,
                       requires_scope_link,
                       is_active,
                       description,
                       updated_at
                FROM category_rules
                WHERE category = ?
                """,
                (category,),
            )
            row = cur.fetchone()
            return self._normalize_rule_row(dict(row)) if row else None
        except sqlite3.Error:
            # Old schema
            try:
                cur = self.con.execute(
                    """
                    SELECT category,
                           effect_model,
                           savings_model,
                           requires_scope_link,
                           is_active,
                           description,
                           updated_at
                    FROM category_rules
                    WHERE category = ?
                    """,
                    (category,),
                )
                row = cur.fetchone()
                return self._normalize_rule_row(dict(row)) if row else None
            except sqlite3.Error:
                return None

    def upsert_category_rule(self, category: str, payload: dict[str, Any]) -> None:
        clean_category = (category or "").strip()
        if not clean_category:
            raise ValueError("Nazwa kategorii jest wymagana.")

        rule = self._normalize_rule_payload(clean_category, payload)

        # Prefer new schema (overlay_targets)
        try:
            self.con.execute(
                """
                INSERT INTO category_rules (
                    category,
                    effect_model,
                    savings_model,
                    overlay_targets,
                    requires_scope_link,
                    is_active,
                    description,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(category) DO UPDATE SET
                    effect_model = excluded.effect_model,
                    savings_model = excluded.savings_model,
                    overlay_targets = excluded.overlay_targets,
                    requires_scope_link = excluded.requires_scope_link,
                    is_active = excluded.is_active,
                    description = excluded.description,
                    updated_at = excluded.updated_at
                """,
                (
                    rule["category"],
                    rule["effect_model"],
                    rule["savings_model"],
                    rule.get("overlay_targets"),
                    1 if rule["requires_scope_link"] else 0,
                    1 if rule["is_active"] else 0,
                    rule.get("description"),
                    rule["updated_at"],
                ),
            )
            self.con.commit()
            return
        except sqlite3.Error:
            # Old schema (no overlay_targets column)
            self.con.execute(
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
                ON CONFLICT(category) DO UPDATE SET
                    effect_model = excluded.effect_model,
                    savings_model = excluded.savings_model,
                    requires_scope_link = excluded.requires_scope_link,
                    is_active = excluded.is_active,
                    description = excluded.description,
                    updated_at = excluded.updated_at
                """,
                (
                    rule["category"],
                    rule["effect_model"],
                    rule["savings_model"],
                    1 if rule["requires_scope_link"] else 0,
                    1 if rule["is_active"] else 0,
                    rule.get("description"),
                    rule["updated_at"],
                ),
            )
            self.con.commit()

    # --------------------------
    # Normalizers
    # --------------------------
    def _normalize_rule_row(self, row: dict[str, Any]) -> dict[str, Any]:
        overlay_targets_raw = row.get("overlay_targets")
        row["overlay_targets"] = parse_overlay_targets(overlay_targets_raw)
        row["overlay_targets_configured"] = overlay_targets_raw not in (None, "")
        row["requires_scope_link"] = bool(row.get("requires_scope_link"))
        row["is_active"] = bool(row.get("is_active"))
        return row

    def _normalize_category_rule_row(self, row: dict[str, Any]) -> dict[str, Any]:
        category_label = row.get("category_label") or row.get("category") or ""
        overlay_targets_raw = row.get("overlay_targets")
        return {
            "category_label": category_label,
            "effectiveness_model": row.get("effectiveness_model") or row.get("effect_model") or "NONE",
            "savings_model": row.get("savings_model") or "NONE",
            "overlay_targets": parse_overlay_targets(overlay_targets_raw),
            "overlay_targets_configured": overlay_targets_raw not in (None, ""),
            "requires_scope_link": bool(row.get("requires_scope_link")),
            "description": row.get("description"),
            "is_active": bool(row.get("is_active", True)),
        }

    def _normalize_rule_payload(self, category: str, payload: dict[str, Any]) -> dict[str, Any]:
        description = (payload.get("description") or "").strip() or None
        if description and len(description) > 500:
            raise ValueError("Opis metodologii nie może przekraczać 500 znaków.")
        return {
            "category": category,
            "effect_model": payload.get("effect_model") or "NONE",
            "savings_model": payload.get("savings_model") or "NONE",
            "overlay_targets": serialize_overlay_targets(payload.get("overlay_targets")),
            "requires_scope_link": bool(payload.get("requires_scope_link")),
            "is_active": bool(payload.get("is_active", True)),
            "description": description,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }


# =====================================================
# NOTIFICATIONS (email log)
# =====================================================

class NotificationRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def was_sent(self, unique_key: str) -> bool:
        if not unique_key:
            return False
        if not _table_exists(self.con, "email_notifications_log"):
            return False
        cols = _table_columns(self.con, "email_notifications_log")
        if "unique_key" not in cols:
            return False
        try:
            cur = self.con.execute(
                """
                SELECT 1
                FROM email_notifications_log
                WHERE unique_key = ?
                LIMIT 1
                """,
                (unique_key,),
            )
            return cur.fetchone() is not None
        except sqlite3.Error:
            return False

    def log_sent(
        self,
        notification_type: str,
        recipient_email: str,
        action_id: str | None,
        payload: dict[str, Any] | None,
        unique_key: str,
    ) -> None:
        if not unique_key:
            return
        if not _table_exists(self.con, "email_notifications_log"):
            return
        cols = _table_columns(self.con, "email_notifications_log")
        required = {
            "id",
            "created_at",
            "notification_type",
            "recipient_email",
            "action_id",
            "payload_json",
            "unique_key",
        }
        if not required.issubset(cols):
            return
        payload_json = json.dumps(payload, ensure_ascii=False) if payload else None
        try:
            self.con.execute(
                """
                INSERT INTO email_notifications_log (
                    id,
                    created_at,
                    notification_type,
                    recipient_email,
                    action_id,
                    payload_json,
                    unique_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    datetime.now(timezone.utc).isoformat(),
                    notification_type,
                    recipient_email,
                    action_id,
                    payload_json,
                    unique_key,
                ),
            )
            self.con.commit()
        except sqlite3.Error:
            return

    def list_recent(self, limit: int = 50) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "email_notifications_log"):
            return []
        cols = _table_columns(self.con, "email_notifications_log")
        select_cols = [
            c
            for c in (
                "id",
                "created_at",
                "notification_type",
                "recipient_email",
                "action_id",
                "payload_json",
                "unique_key",
            )
            if c in cols
        ]
        if not select_cols:
            return []
        order_clause = "ORDER BY created_at DESC" if "created_at" in cols else "ORDER BY rowid DESC"
        try:
            cur = self.con.execute(
                f"""
                SELECT {", ".join(select_cols)}
                FROM email_notifications_log
                {order_clause}
                LIMIT ?
                """,
                (int(limit),),
            )
            return [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []


# =====================================================
# ACTIONS
# =====================================================

class ActionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        _configure_sqlite_connection(self.con)
        _ensure_column(self.con, "actions", "area", "TEXT")

    def list_actions(
        self,
        status: str | None = None,
        project_id: str | None = None,
        champion_id: str | None = None,
        is_draft: bool | None = None,
        overdue_only: bool = False,
        search_text: str | None = None,
    ) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "actions"):
            return []

        action_cols = _table_columns(self.con, "actions")
        if not action_cols:
            return []

        select_cols = [f"a.{c}" for c in action_cols]
        if "id" not in action_cols:
            select_cols.append("a.rowid AS id")

        joins: list[str] = []
        project_name_select = "NULL AS project_name"
        if _table_exists(self.con, "projects") and "project_id" in action_cols:
            project_cols = _table_columns(self.con, "projects")
            if "id" in project_cols and "name" in project_cols:
                joins.append("LEFT JOIN projects p ON p.id = a.project_id")
                project_name_select = "p.name AS project_name"

        owner_name_select = "NULL AS owner_name"
        if _table_exists(self.con, "champions") and "owner_champion_id" in action_cols:
            champion_cols = _table_columns(self.con, "champions")
            if "id" in champion_cols:
                joins.append("LEFT JOIN champions ch ON ch.id = a.owner_champion_id")
                owner_name_select = (
                    "TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, '')) AS owner_name"
                )

        select_sql = ", ".join(select_cols + [project_name_select, owner_name_select])
        base_query = f"""
            SELECT {select_sql}
            FROM actions a
            {' '.join(joins)}
        """
        filters: list[str] = []
        params: list[Any] = []
        today = date.today().isoformat()

        if status and "status" in action_cols:
            filters.append("a.status = ?")
            params.append(status)
        if project_id and "project_id" in action_cols:
            filters.append("a.project_id = ?")
            params.append(project_id)
        if champion_id and "owner_champion_id" in action_cols:
            filters.append("a.owner_champion_id = ?")
            params.append(champion_id)
        if is_draft is not None and "is_draft" in action_cols:
            filters.append("a.is_draft = ?")
            params.append(1 if is_draft else 0)
        if overdue_only and "due_date" in action_cols and "status" in action_cols:
            filters.append(
                "a.due_date IS NOT NULL AND a.due_date < ? AND a.status NOT IN ('done','cancelled')"
            )
            params.append(today)
        if search_text and "title" in action_cols:
            filters.append("a.title LIKE ?")
            params.append(f"%{search_text.strip()}%")

        if filters:
            base_query += " WHERE " + " AND ".join(filters)

        if "due_date" in action_cols and "status" in action_cols:
            base_query += """
                ORDER BY
                    CASE
                        WHEN a.due_date IS NOT NULL
                             AND a.due_date < ?
                             AND a.status NOT IN ('done','cancelled')
                        THEN 0 ELSE 1 END,
                    a.due_date IS NULL,
                    a.due_date,
                    a.created_at DESC
            """
            params.append(today)
        elif "created_at" in action_cols:
            base_query += " ORDER BY a.created_at DESC"
        else:
            base_query += " ORDER BY a.rowid DESC"

        try:
            cur = self.con.execute(base_query, params)
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []

        for row in rows:
            row.setdefault("project_name", None)
            row.setdefault("owner_name", None)
        return rows

    def list_open_actions(self, project_ids: list[str] | None = None) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "actions"):
            return []

        action_cols = _table_columns(self.con, "actions")
        if not action_cols or "status" not in action_cols:
            return []

        select_cols = [f"a.{c}" for c in action_cols]
        if "id" not in action_cols:
            select_cols.append("a.rowid AS id")

        joins: list[str] = []
        project_name_select = "NULL AS project_name"
        if _table_exists(self.con, "projects") and "project_id" in action_cols:
            project_cols = _table_columns(self.con, "projects")
            if "id" in project_cols and "name" in project_cols:
                joins.append("LEFT JOIN projects p ON p.id = a.project_id")
                project_name_select = "p.name AS project_name"

        owner_name_select = "NULL AS owner_name"
        if _table_exists(self.con, "champions") and "owner_champion_id" in action_cols:
            champion_cols = _table_columns(self.con, "champions")
            if "id" in champion_cols:
                joins.append("LEFT JOIN champions ch ON ch.id = a.owner_champion_id")
                owner_name_select = (
                    "TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, '')) AS owner_name"
                )

        select_sql = ", ".join(select_cols + [project_name_select, owner_name_select])
        base_query = f"""
            SELECT {select_sql}
            FROM actions a
            {' '.join(joins)}
        """
        filters: list[str] = ["a.status NOT IN ('done','cancelled')"]
        params: list[Any] = []

        if "is_draft" in action_cols:
            filters.append("a.is_draft = 0")

        if project_ids and "project_id" in action_cols:
            placeholders = ", ".join(["?"] * len(project_ids))
            filters.append(f"a.project_id IN ({placeholders})")
            params.extend(project_ids)

        if filters:
            base_query += " WHERE " + " AND ".join(filters)

        if "created_at" in action_cols:
            base_query += " ORDER BY a.created_at ASC"
        else:
            base_query += " ORDER BY a.rowid ASC"

        try:
            cur = self.con.execute(base_query, params)
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []

        for row in rows:
            row.setdefault("project_name", None)
            row.setdefault("owner_name", None)
        return rows

    def create_action(self, data: dict[str, Any]) -> str:
        action_id = data.get("id") or str(uuid4())
        if not _table_exists(self.con, "actions"):
            return action_id
        action_cols = _table_columns(self.con, "actions")
        if not action_cols:
            return action_id
        payload = self._normalize_action_payload(action_id, data)

        cols = [
            "id",
            "project_id",
            "analysis_id",
            "title",
            "description",
            "owner_champion_id",
            "priority",
            "status",
            "is_draft",
            "due_date",
            "created_at",
            "closed_at",
            "impact_type",
            "impact_value",
            "impact_aspects",
            "category",
            "area",
            "manual_savings_amount",
            "manual_savings_currency",
            "manual_savings_note",
            "source",
            "source_message_id",
            "submitted_by_email",
            "submitted_at",
        ]
        insert_cols = [c for c in cols if c in action_cols]
        if not insert_cols:
            return action_id
        vals = [payload.get(c) for c in insert_cols]
        placeholders = ", ".join(["?"] * len(insert_cols))
        _configure_sqlite_connection(self.con)
        try:
            self.con.execute(
                f"INSERT INTO actions ({', '.join(insert_cols)}) VALUES ({placeholders})",
                vals,
            )
            self.con.commit()
        except sqlite3.Error:
            return action_id
        return action_id

    def update_action(self, action_id: str, data: dict[str, Any]) -> None:
        """
        Update action row in DB.
        Keeps created_at stable unless explicitly provided.
        Enforces closed_at logic consistent with _normalize_action_payload:
          - status == 'done' => closed_at set (default today)
          - else => closed_at cleared
        """
        if not action_id:
            return
        if not _table_exists(self.con, "actions"):
            return

        action_cols = _table_columns(self.con, "actions")
        if not action_cols:
            return

        try:
            cur = self.con.execute("SELECT * FROM actions WHERE id = ?", (action_id,))
            existing_row = cur.fetchone()
        except sqlite3.Error:
            return
        if not existing_row:
            return

        existing = dict(existing_row)

        # Preserve created_at unless user explicitly passes it
        merged: dict[str, Any] = dict(existing)
        merged.update(data or {})
        if "created_at" not in (data or {}) or not (data or {}).get("created_at"):
            merged["created_at"] = existing.get("created_at") or date.today().isoformat()

        try:
            payload = self._normalize_action_payload(action_id, merged)
        except ValueError:
            return

        allowed_cols = [
            "project_id",
            "analysis_id",
            "title",
            "description",
            "owner_champion_id",
            "priority",
            "status",
            "is_draft",
            "due_date",
            "created_at",
            "closed_at",
            "impact_type",
            "impact_value",
            "impact_aspects",
            "category",
            "area",
            "manual_savings_amount",
            "manual_savings_currency",
            "manual_savings_note",
            "source",
            "source_message_id",
            "submitted_by_email",
            "submitted_at",
        ]

        sets: list[str] = []
        params: list[Any] = []
        for col in allowed_cols:
            if col in payload and col in action_cols:
                sets.append(f"{col} = ?")
                params.append(payload.get(col))

        if not sets:
            return

        params.append(action_id)
        sql = f"UPDATE actions SET {', '.join(sets)} WHERE id = ?"
        _configure_sqlite_connection(self.con)
        try:
            self.con.execute(sql, params)
            self.con.commit()
        except sqlite3.Error:
            return

    def delete_action(self, action_id: str) -> None:
        if not action_id:
            return
        if not _table_exists(self.con, "actions"):
            return
        _configure_sqlite_connection(self.con)
        try:
            self.con.execute("BEGIN")

            dependent_tables = [
                "action_effectiveness",
                "action_changelog",
                "actions_changelog",
                "email_notifications_log",
                "analysis_actions",
                "analysis_action_links",
            ]
            for table in dependent_tables:
                if not _table_exists(self.con, table):
                    continue
                cols = _table_columns(self.con, table)
                for col in ("action_id", "added_action_id"):
                    if col not in cols:
                        continue
                    self.con.execute(f"DELETE FROM {table} WHERE {col} = ?", (action_id,))

            self.con.execute("DELETE FROM actions WHERE id = ?", (action_id,))
            self.con.commit()
        except sqlite3.Error:
            _rollback_safely(self.con)
            return

    def list_action_changelog(
        self, limit: int = 50, project_id: str | None = None, action_id: str | None = None
    ) -> list[dict[str, Any]]:
        """
        UI in actions.py calls list_action_changelog(...).
        Under the hood we use generic reader.
        Note: project_id filter is ignored here unless your changelog schema supports it.
        """
        _ = project_id  # reserved for future filtering
        return self.list_changelog(limit=limit, action_id=action_id)

    def list_changelog(self, limit: int = 50, action_id: str | None = None) -> list[dict[str, Any]]:
        return _list_changelog_generic(
            self.con,
            table_candidates=[
                "action_changelog",
                "actions_changelog",
                "changelog_actions",
                "changelog",
                "change_log",
                "audit_log",
            ],
            limit=limit,
            entity_id=action_id,
        )

    # ============================
    # Existing logic
    # ============================

    def _normalize_action_payload(self, action_id: str, data: dict[str, Any]) -> dict[str, Any]:
        created_at = data.get("created_at") or date.today().isoformat()
        created_date = self._parse_date(created_at, "created_at")
        status = data.get("status") or "open"
        closed_at = data.get("closed_at") or None
        if status == "done":
            closed_at = closed_at or date.today().isoformat()
            closed_date = self._parse_date(closed_at, "closed_at")
            if closed_date < created_date:
                raise ValueError("closed_at < created_at")
        else:
            closed_at = None

        due_date = data.get("due_date") or None
        if due_date:
            due_date = self._parse_date(due_date, "due_date").isoformat()

        return {
            "id": action_id,
            "project_id": (data.get("project_id") or "").strip() or None,
            "analysis_id": (data.get("analysis_id") or "").strip() or None,
            "title": (data.get("title") or "").strip(),
            "description": (data.get("description") or "").strip() or None,
            "owner_champion_id": (data.get("owner_champion_id") or "").strip() or None,
            "priority": data.get("priority") or "med",
            "status": status,
            "is_draft": 1 if bool(data.get("is_draft")) else 0,
            "due_date": due_date,
            "created_at": created_date.isoformat(),
            "closed_at": closed_at,
            "impact_type": data.get("impact_type"),
            "impact_value": data.get("impact_value"),
            "impact_aspects": _normalize_impact_aspects_payload(data.get("impact_aspects")),
            "category": data.get("category"),
            "area": (data.get("area") or "").strip() or None,
            "manual_savings_amount": data.get("manual_savings_amount"),
            "manual_savings_currency": data.get("manual_savings_currency"),
            "manual_savings_note": data.get("manual_savings_note"),
            "source": data.get("source"),
            "source_message_id": data.get("source_message_id"),
            "submitted_by_email": data.get("submitted_by_email"),
            "submitted_at": data.get("submitted_at"),
        }

    @staticmethod
    def _parse_date(value: Any, field_name: str) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            try:
                return datetime.fromisoformat(str(value)).date()
            except ValueError as exc_two:
                raise ValueError(f"Invalid date for {field_name}") from exc_two

    def list_actions_for_kpi(
        self,
        project_id: str | None = None,
        champion_id: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "actions"):
            return []

        action_cols = _table_columns(self.con, "actions")
        if not action_cols:
            return []

        select_fields = []
        for col in (
            "id",
            "title",
            "created_at",
            "closed_at",
            "due_date",
            "status",
            "owner_champion_id",
            "category",
            "project_id",
            "impact_aspects",
        ):
            if col in action_cols:
                select_fields.append(f"a.{col}")
            else:
                select_fields.append(f"NULL AS {col}")

        query = f"""
            SELECT {", ".join(select_fields)}
            FROM actions a
        """
        filters: list[str] = []
        params: list[Any] = []
        if "is_draft" in action_cols:
            filters.append("a.is_draft = 0")
        if project_id and "project_id" in action_cols:
            filters.append("project_id = ?")
            params.append(project_id)
        if champion_id and "owner_champion_id" in action_cols:
            filters.append("owner_champion_id = ?")
            params.append(champion_id)
        if category and "category" in action_cols:
            filters.append("category = ?")
            params.append(category)
        if filters:
            query += " WHERE " + " AND ".join(filters)
        try:
            cur = self.con.execute(query, params)
            return [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []

    def list_actions_for_ranking(
        self,
        project_id: str | None = None,
        category: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "actions"):
            return []

        action_cols = _table_columns(self.con, "actions")
        if not action_cols:
            return []

        select_fields = []
        for col in (
            "id",
            "title",
            "owner_champion_id",
            "project_id",
            "category",
            "status",
            "created_at",
            "due_date",
            "closed_at",
            "manual_savings_amount",
            "manual_savings_currency",
            "manual_savings_note",
        ):
            if col in action_cols:
                select_fields.append(f"a.{col}")
            else:
                select_fields.append(f"NULL AS {col}")

        joins: list[str] = []
        project_name_select = "NULL AS project_name"
        if _table_exists(self.con, "projects") and "project_id" in action_cols:
            project_cols = _table_columns(self.con, "projects")
            if "id" in project_cols and "name" in project_cols:
                joins.append("LEFT JOIN projects p ON p.id = a.project_id")
                project_name_select = "p.name AS project_name"

        effectiveness_select = ["NULL AS effectiveness_metric", "NULL AS effectiveness_delta"]
        if _table_exists(self.con, "action_effectiveness"):
            eff_cols = _table_columns(self.con, "action_effectiveness")
            if "action_id" in eff_cols:
                joins.append("LEFT JOIN action_effectiveness ae ON ae.action_id = a.id")
                metric_select = "ae.metric AS effectiveness_metric" if "metric" in eff_cols else "NULL AS effectiveness_metric"
                delta_select = "ae.delta AS effectiveness_delta" if "delta" in eff_cols else "NULL AS effectiveness_delta"
                effectiveness_select = [metric_select, delta_select]

        select_sql = ", ".join(select_fields + [project_name_select] + effectiveness_select)
        query = f"""
            SELECT {select_sql}
            FROM actions a
            {' '.join(joins)}
        """
        filters: list[str] = []
        params: list[Any] = []
        if "created_at" in action_cols:
            filters.append("a.created_at IS NOT NULL")
        if "is_draft" in action_cols:
            filters.append("a.is_draft = 0")
        if project_id and "project_id" in action_cols:
            filters.append("a.project_id = ?")
            params.append(project_id)
        if category and "category" in action_cols:
            filters.append("a.category = ?")
            params.append(category)
        if date_to and "created_at" in action_cols:
            filters.append("date(a.created_at) <= date(?)")
            params.append(date_to.isoformat())
        if date_from and "created_at" in action_cols:
            if "closed_at" in action_cols:
                filters.append(
                    "(date(a.created_at) >= date(?) OR (a.closed_at IS NOT NULL AND date(a.closed_at) >= date(?)))"
                )
                params.extend([date_from.isoformat(), date_from.isoformat()])
            else:
                filters.append("date(a.created_at) >= date(?)")
                params.append(date_from.isoformat())
        if filters:
            query += " WHERE " + " AND ".join(filters)
        try:
            cur = self.con.execute(query, params)
            return [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []

    def list_actions_for_project_outcome(
        self,
        project_id: str,
        date_from: date | str | None = None,
        date_to: date | str | None = None,
    ) -> list[dict[str, Any]]:
        if not project_id:
            return []
        if not _table_exists(self.con, "actions"):
            return []
        action_cols = _table_columns(self.con, "actions")
        if not action_cols or "project_id" not in action_cols:
            return []

        def _d(v: date | str | None) -> str | None:
            if v is None:
                return None
            return v.isoformat() if isinstance(v, date) else str(v)

        df = _d(date_from)
        dt = _d(date_to)

        select_fields = []
        for col in (
            "id",
            "title",
            "description",
            "category",
            "status",
            "created_at",
            "due_date",
            "closed_at",
            "owner_champion_id",
            "impact_type",
            "impact_value",
            "impact_aspects",
            "manual_savings_amount",
            "manual_savings_currency",
            "manual_savings_note",
        ):
            if col in action_cols:
                select_fields.append(f"a.{col}")
            else:
                select_fields.append(f"NULL AS {col}")

        joins: list[str] = []
        owner_name_select = "NULL AS owner_name"
        if _table_exists(self.con, "champions") and "owner_champion_id" in action_cols:
            champion_cols = _table_columns(self.con, "champions")
            if "id" in champion_cols:
                joins.append("LEFT JOIN champions ch ON ch.id = a.owner_champion_id")
                owner_name_select = (
                    "TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, '')) AS owner_name"
                )

        select_sql = ", ".join(select_fields + [owner_name_select])
        query = f"""
            SELECT {select_sql}
            FROM actions a
            {' '.join(joins)}
            WHERE a.project_id = ?
        """
        params: list[Any] = [project_id]

        if "is_draft" in action_cols:
            query += " AND a.is_draft = 0"

        if df or dt:
            df = df or "0001-01-01"
            dt = dt or "9999-12-31"
            date_filters = []
            if "created_at" in action_cols:
                date_filters.append("(a.created_at IS NOT NULL AND date(a.created_at) BETWEEN date(?) AND date(?))")
                params.extend([df, dt])
            if "closed_at" in action_cols:
                date_filters.append("(a.closed_at IS NOT NULL AND date(a.closed_at) BETWEEN date(?) AND date(?))")
                params.extend([df, dt])
            if "due_date" in action_cols:
                date_filters.append("(a.due_date IS NOT NULL AND date(a.due_date) BETWEEN date(?) AND date(?))")
                params.extend([df, dt])
            if date_filters:
                query += " AND (" + " OR ".join(date_filters) + ")"

        if "closed_at" in action_cols and "created_at" in action_cols:
            query += " ORDER BY a.closed_at DESC, a.created_at DESC"
        elif "created_at" in action_cols:
            query += " ORDER BY a.created_at DESC"
        else:
            query += " ORDER BY a.rowid DESC"

        try:
            cur = self.con.execute(query, params)
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []
        for r in rows:
            r.setdefault("owner_name", None)
            r.setdefault("impact_aspects", None)
            r.setdefault("manual_savings_amount", None)
            r.setdefault("manual_savings_currency", None)
            r.setdefault("manual_savings_note", None)
        return rows


class AnalysisRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        _configure_sqlite_connection(self.con)
        _ensure_column(self.con, "analyses", "area", "TEXT")

    def list_analyses(self) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "analyses"):
            return []
        analysis_cols = _table_columns(self.con, "analyses")
        if not analysis_cols:
            return []

        select_cols = [f"a.{c}" for c in analysis_cols]
        if "id" not in analysis_cols:
            select_cols.append("a.rowid AS id")

        joins: list[str] = []
        project_name_select = "NULL AS project_name"
        work_center_select = "NULL AS work_center"
        if _table_exists(self.con, "projects") and "project_id" in analysis_cols:
            project_cols = _table_columns(self.con, "projects")
            if "id" in project_cols:
                joins.append("LEFT JOIN projects p ON p.id = a.project_id")
                if "name" in project_cols:
                    project_name_select = "p.name AS project_name"
                if "work_center" in project_cols:
                    work_center_select = "p.work_center AS work_center"

        champion_name_select = "NULL AS champion_name"
        if _table_exists(self.con, "champions") and "champion_id" in analysis_cols:
            champion_cols = _table_columns(self.con, "champions")
            if "id" in champion_cols:
                joins.append("LEFT JOIN champions ch ON ch.id = a.champion_id")
                champion_name_select = (
                    "TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, '')) AS champion_name"
                )

        select_sql = ", ".join(
            select_cols + [project_name_select, work_center_select, champion_name_select]
        )
        base_query = f"""
            SELECT {select_sql}
            FROM analyses a
            {' '.join(joins)}
        """

        order_clauses: list[str] = []
        if "status" in analysis_cols:
            order_clauses.append("CASE WHEN a.status = 'closed' THEN 1 ELSE 0 END")
        if "created_at" in analysis_cols:
            order_clauses.append("a.created_at DESC")
        if not order_clauses:
            order_clauses.append("a.rowid DESC")
        base_query += " ORDER BY " + ", ".join(order_clauses)

        try:
            cur = self.con.execute(base_query)
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []

        for row in rows:
            row.setdefault("project_name", None)
            row.setdefault("work_center", None)
            row.setdefault("champion_name", None)
        return rows

    def create_analysis(self, data: dict[str, Any]) -> str:
        analysis_id = data.get("id") or str(uuid4())
        if not _table_exists(self.con, "analyses"):
            return analysis_id
        analysis_cols = _table_columns(self.con, "analyses")
        if not analysis_cols:
            return analysis_id

        payload = self._normalize_analysis_payload(analysis_id, data)

        cols = [
            "id",
            "project_id",
            "champion_id",
            "tool_type",
            "status",
            "created_at",
            "closed_at",
            "area",
            "template_json",
        ]
        insert_cols = [c for c in cols if c in analysis_cols]
        if not insert_cols:
            return analysis_id

        vals = [payload.get(c) for c in insert_cols]
        placeholders = ", ".join(["?"] * len(insert_cols))
        _configure_sqlite_connection(self.con)
        try:
            self.con.execute(
                f"INSERT INTO analyses ({', '.join(insert_cols)}) VALUES ({placeholders})",
                vals,
            )
            self.con.commit()
        except sqlite3.Error:
            return analysis_id

        self._log_changelog(analysis_id, "CREATE", payload)
        return analysis_id

    def update_analysis(self, analysis_id: str, data: dict[str, Any]) -> None:
        if not analysis_id:
            return
        if not _table_exists(self.con, "analyses"):
            return
        analysis_cols = _table_columns(self.con, "analyses")
        if not analysis_cols:
            return

        try:
            cur = self.con.execute("SELECT * FROM analyses WHERE id = ?", (analysis_id,))
            existing_row = cur.fetchone()
        except sqlite3.Error:
            return
        if not existing_row:
            return

        existing = dict(existing_row)
        merged: dict[str, Any] = dict(existing)
        merged.update(data or {})
        payload = self._normalize_analysis_payload(analysis_id, merged)

        allowed_cols = [
            "project_id",
            "champion_id",
            "tool_type",
            "status",
            "created_at",
            "closed_at",
            "area",
            "template_json",
        ]
        updates = {k: payload.get(k) for k in allowed_cols if k in analysis_cols}
        if not updates:
            return

        changes: dict[str, Any] = {}
        for key, value in updates.items():
            if existing.get(key) != value:
                changes[key] = {"from": existing.get(key), "to": value}

        if not changes:
            return

        sets = [f"{col} = ?" for col in updates.keys()]
        params = list(updates.values())
        params.append(analysis_id)
        try:
            self.con.execute(
                f"UPDATE analyses SET {', '.join(sets)} WHERE id = ?",
                params,
            )
            self.con.commit()
        except sqlite3.Error:
            return

        self._log_changelog(analysis_id, "UPDATE", changes)

    def delete_analysis(self, analysis_id: str) -> None:
        if not analysis_id:
            return
        if not _table_exists(self.con, "analyses"):
            return
        with self.con:
            if _table_exists(self.con, "analysis_actions"):
                self.con.execute(
                    "DELETE FROM analysis_actions WHERE analysis_id = ?",
                    (analysis_id,),
                )
            if _table_exists(self.con, "analysis_changelog"):
                self.con.execute(
                    "DELETE FROM analysis_changelog WHERE analysis_id = ?",
                    (analysis_id,),
                )
            self.con.execute("DELETE FROM analyses WHERE id = ?", (analysis_id,))
        self._log_changelog(analysis_id, "DELETE", {"id": analysis_id})

    def list_analysis_actions(self, analysis_id: str) -> list[dict[str, Any]]:
        if not analysis_id:
            return []
        if not _table_exists(self.con, "analysis_actions"):
            return []
        action_cols = _table_columns(self.con, "analysis_actions")
        if not action_cols:
            return []

        select_cols = [f"aa.{c}" for c in action_cols]
        if "id" not in action_cols:
            select_cols.append("aa.rowid AS id")

        joins: list[str] = []
        owner_name_select = "NULL AS owner_name"
        if _table_exists(self.con, "champions") and "owner_champion_id" in action_cols:
            champion_cols = _table_columns(self.con, "champions")
            if "id" in champion_cols:
                joins.append("LEFT JOIN champions ch ON ch.id = aa.owner_champion_id")
                owner_name_select = (
                    "TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, '')) AS owner_name"
                )

        select_sql = ", ".join(select_cols + [owner_name_select])
        base_query = f"""
            SELECT {select_sql}
            FROM analysis_actions aa
            {' '.join(joins)}
            WHERE aa.analysis_id = ?
        """
        if "created_at" in action_cols:
            base_query += " ORDER BY aa.created_at ASC"
        else:
            base_query += " ORDER BY aa.rowid ASC"

        try:
            cur = self.con.execute(base_query, (analysis_id,))
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []

        for row in rows:
            row.setdefault("owner_name", None)
        return rows

    def create_analysis_action(self, analysis_id: str, data: dict[str, Any]) -> str:
        action_id = data.get("id") or str(uuid4())
        if not analysis_id:
            return action_id
        if not _table_exists(self.con, "analysis_actions"):
            return action_id
        cols = _table_columns(self.con, "analysis_actions")
        if not cols:
            return action_id

        title = (data.get("title") or "").strip()
        if "title" in cols and not title:
            raise ValueError("title is required")
        action_type = (data.get("action_type") or "").strip()
        if "action_type" in cols and not action_type:
            raise ValueError("action_type is required")

        due_date = data.get("due_date") or None
        if due_date:
            due_date = self._parse_date(due_date, "due_date").isoformat()

        payload = {
            "id": action_id,
            "analysis_id": analysis_id,
            "action_type": action_type,
            "title": title,
            "description": (data.get("description") or "").strip() or None,
            "due_date": due_date,
            "owner_champion_id": (data.get("owner_champion_id") or "").strip() or None,
            "added_action_id": (data.get("added_action_id") or "").strip() or None,
            "created_at": self._parse_date(
                data.get("created_at") or date.today().isoformat(), "created_at"
            ).isoformat(),
        }

        insert_cols = [c for c in payload.keys() if c in cols]
        if not insert_cols:
            return action_id
        vals = [payload.get(c) for c in insert_cols]
        placeholders = ", ".join(["?"] * len(insert_cols))
        _configure_sqlite_connection(self.con)
        try:
            self.con.execute(
                f"INSERT INTO analysis_actions ({', '.join(insert_cols)}) VALUES ({placeholders})",
                vals,
            )
            self.con.commit()
        except sqlite3.Error:
            return action_id
        return action_id

    def mark_analysis_action_added(self, analysis_action_id: str, action_id: str) -> None:
        if not analysis_action_id or not action_id:
            return
        if not _table_exists(self.con, "analysis_actions"):
            return
        cols = _table_columns(self.con, "analysis_actions")
        if "added_action_id" not in cols:
            return
        try:
            self.con.execute(
                "UPDATE analysis_actions SET added_action_id = ? WHERE id = ?",
                (action_id, analysis_action_id),
            )
            self.con.commit()
        except sqlite3.Error:
            return

    def list_changelog(self, limit: int = 50, analysis_id: str | None = None) -> list[dict[str, Any]]:
        return _list_changelog_generic(
            self.con,
            [
                "analysis_changelog",
                "analyses_changelog",
                "changelog_analyses",
                "changelog",
            ],
            "analysis_id",
            analysis_id,
            limit,
        )

    def _normalize_analysis_payload(self, analysis_id: str, data: dict[str, Any]) -> dict[str, Any]:
        created_at = data.get("created_at") or date.today().isoformat()
        created_date = self._parse_date(created_at, "created_at")
        status = (data.get("status") or "open").strip() or "open"
        closed_at = data.get("closed_at") or None
        if status == "closed":
            closed_at = closed_at or date.today().isoformat()
            closed_date = self._parse_date(closed_at, "closed_at")
            if closed_date < created_date:
                raise ValueError("closed_at < created_at")
        else:
            closed_at = None

        template_json = data.get("template_json")
        if isinstance(template_json, (dict, list)):
            template_json = json.dumps(template_json, ensure_ascii=False)
        template_json = (template_json or "").strip()
        if not template_json:
            template_json = json.dumps({}, ensure_ascii=False)

        return {
            "id": analysis_id,
            "project_id": (data.get("project_id") or "").strip() or None,
            "champion_id": (data.get("champion_id") or "").strip() or None,
            "tool_type": (data.get("tool_type") or "").strip() or "5WHY",
            "status": status,
            "created_at": created_date.isoformat(),
            "closed_at": closed_at,
            "area": (data.get("area") or "").strip() or None,
            "template_json": template_json,
        }

    @staticmethod
    def _parse_date(value: Any, field_name: str) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            try:
                return datetime.fromisoformat(str(value)).date()
            except ValueError as exc_two:
                raise ValueError(f"Invalid date for {field_name}") from exc_two

    def _log_changelog(self, analysis_id: str, event_type: str, changes: dict[str, Any]) -> None:
        if not _table_exists(self.con, "analysis_changelog"):
            return
        columns = _table_columns(self.con, "analysis_changelog")
        required = {"id", "analysis_id", "event_type", "event_at", "changes_json"}
        if not required.issubset(columns):
            return
        payload = {
            "id": str(uuid4()),
            "analysis_id": analysis_id,
            "event_type": event_type,
            "event_at": datetime.now(timezone.utc).isoformat(),
            "changes_json": json.dumps(changes, ensure_ascii=False),
        }
        try:
            self.con.execute(
                """
                INSERT INTO analysis_changelog (id, analysis_id, event_type, event_at, changes_json)
                VALUES (:id, :analysis_id, :event_type, :event_at, :changes_json)
                """,
                payload,
            )
            self.con.commit()
        except sqlite3.Error:
            _rollback_safely(self.con)

    def list_changelog(self, limit: int = 50, action_id: str | None = None) -> list[dict[str, Any]]:
        return _list_changelog_generic(
            self.con,
            table_candidates=[
                "action_changelog",
                "actions_changelog",
                "changelog_actions",
                "changelog",
                "change_log",
                "audit_log",
            ],
            limit=limit,
            entity_id=action_id,
        )


# =====================================================
# EFFECTIVENESS
# =====================================================

class EffectivenessRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_effectiveness(self, action_id: str, payload: dict[str, Any]) -> None:
        if not _table_exists(self.con, "action_effectiveness"):
            return
        eff_cols = _table_columns(self.con, "action_effectiveness")
        if not eff_cols or "action_id" not in eff_cols:
            return

        record_id = payload.get("id") or str(uuid4())
        now = datetime.now(timezone.utc).isoformat()
        full_payload = {
            "id": record_id,
            "action_id": action_id,
            "metric": payload.get("metric"),
            "baseline_from": payload.get("baseline_from"),
            "baseline_to": payload.get("baseline_to"),
            "after_from": payload.get("after_from"),
            "after_to": payload.get("after_to"),
            "baseline_days": int(payload.get("baseline_days") or 0),
            "after_days": int(payload.get("after_days") or 0),
            "baseline_avg": payload.get("baseline_avg"),
            "after_avg": payload.get("after_avg"),
            "delta": payload.get("delta"),
            "pct_change": payload.get("pct_change"),
            "classification": payload.get("classification"),
            "computed_at": payload.get("computed_at") or now,
        }

        insert_cols = [c for c in full_payload.keys() if c in eff_cols]
        if not insert_cols:
            return
        values = [full_payload[c] for c in insert_cols]
        update_cols = [c for c in insert_cols if c not in {"id", "action_id"}]
        update_clause = ", ".join([f"{col} = excluded.{col}" for col in update_cols])
        placeholders = ", ".join(["?"] * len(insert_cols))

        self.con.execute(
            f"""
            INSERT INTO action_effectiveness ({', '.join(insert_cols)})
            VALUES ({placeholders})
            ON CONFLICT(action_id) DO UPDATE SET
                {update_clause}
            """,
            values,
        )
        self.con.commit()

    def get_effectiveness_for_actions(self, action_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not action_ids:
            return {}
        if not _table_exists(self.con, "action_effectiveness"):
            return {}
        eff_cols = _table_columns(self.con, "action_effectiveness")
        if not eff_cols or "action_id" not in eff_cols:
            return {}
        placeholders = ", ".join(["?"] * len(action_ids))
        select_cols = ", ".join(eff_cols)
        cur = self.con.execute(
            f"""
            SELECT {select_cols}
            FROM action_effectiveness
            WHERE action_id IN ({placeholders})
            """,
            action_ids,
        )
        rows = [dict(r) for r in cur.fetchall()]
        return {row["action_id"]: row for row in rows}

    def list_effectiveness_for_actions(self, action_ids: list[str]) -> dict[str, dict[str, Any]]:
        return self.get_effectiveness_for_actions(action_ids)


# =====================================================
# PROJECTS
# =====================================================

class ProjectRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def ensure_projects_full_project_column(self) -> None:
        _ensure_column(self.con, "projects", "full_project", "TEXT")

    def create_project(self, data: dict[str, Any]) -> str:
        if not _table_exists(self.con, "projects"):
            return ""

        self.ensure_projects_full_project_column()
        project_id = data.get("id") or str(uuid4())
        cols = _table_columns(self.con, "projects")
        if not cols:
            return ""

        if "name" in cols and not (data.get("name") or "").strip():
            raise ValueError("name is required")
        if "work_center" in cols and not (data.get("work_center") or "").strip():
            raise ValueError("work_center is required")

        if "name" in cols and "work_center" in cols:
            payload = self._normalize_project_payload(project_id, data)
        else:
            payload = {"id": project_id}
            if "name" in cols:
                payload["name"] = (data.get("name") or "").strip() or None
            if "work_center" in cols:
                payload["work_center"] = (data.get("work_center") or "").strip() or None
            if "project_code" in cols:
                payload["project_code"] = (data.get("project_code") or "").strip() or None
            if "full_project" in cols:
                payload["full_project"] = (data.get("full_project") or "").strip() or None
            if "status" in cols:
                payload["status"] = (data.get("status") or "active").strip() or "active"
            if "created_at" in cols:
                payload["created_at"] = datetime.now(timezone.utc).isoformat()
            if "importance" in cols:
                importance = (data.get("importance") or "").strip()
                payload["importance"] = importance or None

        insert_cols = [c for c in payload.keys() if c in cols]
        if not insert_cols:
            return project_id

        placeholders = ", ".join(["?"] * len(insert_cols))
        values = [payload[c] for c in insert_cols]
        try:
            self.con.execute(
                f"INSERT INTO projects ({', '.join(insert_cols)}) VALUES ({placeholders})",
                values,
            )
            self.con.commit()
        except sqlite3.Error:
            return project_id
        return project_id

    def update_project(self, project_id: str, data: dict[str, Any]) -> None:
        if not project_id:
            return
        if not _table_exists(self.con, "projects"):
            return

        self.ensure_projects_full_project_column()
        cols = _table_columns(self.con, "projects")
        if not cols:
            return

        data_keys = set(data.keys())
        name = (data.get("name") or "").strip()
        work_center = (data.get("work_center") or "").strip()
        if "name" in data_keys and "name" in cols and not name:
            raise ValueError("name is required")
        if "work_center" in data_keys and "work_center" in cols and not work_center:
            raise ValueError("work_center is required")
        if "full_project" in data_keys and "full_project" not in cols:
            raise ValueError("full_project column missing; run migration")

        payload: dict[str, Any] = {}

        if "name" in data_keys and "name" in cols:
            payload["name"] = name
        if "work_center" in data_keys and "work_center" in cols:
            payload["work_center"] = work_center
        if "project_code" in data_keys and "project_code" in cols:
            payload["project_code"] = (data.get("project_code") or "").strip() or None
        if "full_project" in data_keys and "full_project" in cols:
            payload["full_project"] = (data.get("full_project") or "").strip() or None
        if "project_sop" in data_keys and "project_sop" in cols:
            project_sop = data.get("project_sop") or None
            if project_sop:
                project_sop = self._parse_date(project_sop, "project_sop").isoformat()
            payload["project_sop"] = project_sop
        if "project_eop" in data_keys and "project_eop" in cols:
            project_eop = data.get("project_eop") or None
            if project_eop:
                project_eop = self._parse_date(project_eop, "project_eop").isoformat()
            payload["project_eop"] = project_eop
        if "related_work_center" in data_keys and "related_work_center" in cols:
            payload["related_work_center"] = (data.get("related_work_center") or "").strip() or None
        if "type" in data_keys and "type" in cols:
            payload["type"] = (data.get("type") or "custom").strip() or "custom"
        if "importance" in data_keys and "importance" in cols:
            importance = (data.get("importance") or "").strip()
            payload["importance"] = importance or None
        if "owner_champion_id" in data_keys and "owner_champion_id" in cols:
            payload["owner_champion_id"] = (data.get("owner_champion_id") or "").strip() or None
        if "created_at" in data_keys and "created_at" in cols:
            created_at = data.get("created_at") or None
            if created_at:
                created_at = self._parse_date(created_at, "created_at").isoformat()
            payload["created_at"] = created_at
        if "status" in data_keys and "status" in cols:
            status = (data.get("status") or "active").strip() or "active"
            payload["status"] = status
            if "closed_at" in cols:
                if status == "closed":
                    existing_closed_at = None
                    cur = self.con.execute("SELECT closed_at FROM projects WHERE id = ?", (project_id,))
                    row = cur.fetchone()
                    if row:
                        existing_closed_at = row["closed_at"]
                    payload["closed_at"] = existing_closed_at or date.today().isoformat()
                else:
                    payload["closed_at"] = None

        if not payload:
            return

        sets = [f"{col} = ?" for col in payload.keys()]
        params = list(payload.values())
        params.append(project_id)
        self.con.execute(
            f"UPDATE projects SET {', '.join(sets)} WHERE id = ?",
            params,
        )
        self.con.commit()

    def delete_project(self, project_id: str) -> bool:
        if not project_id:
            raise ValueError("project_id is required")
        if not _table_exists(self.con, "projects"):
            return False

        with self.con:
            if _table_exists(self.con, "actions"):
                cur = self.con.execute(
                    """
                    SELECT 1
                    FROM actions
                    WHERE project_id = ?
                    LIMIT 1
                    """,
                    (project_id,),
                )
                if cur.fetchone():
                    return False

            if _table_exists(self.con, "champion_projects"):
                self.con.execute(
                    "DELETE FROM champion_projects WHERE project_id = ?",
                    (project_id,),
                )

            if _table_exists(self.con, "wc_inbox"):
                inbox_cols = _table_columns(self.con, "wc_inbox")
                if "linked_project_id" in inbox_cols:
                    if "status" in inbox_cols:
                        self.con.execute(
                            """
                            UPDATE wc_inbox
                            SET linked_project_id = NULL,
                                status = CASE
                                    WHEN linked_project_id = ? AND status IN ('linked', 'created') THEN 'open'
                                    ELSE status
                                END
                            WHERE linked_project_id = ?
                            """,
                            (project_id, project_id),
                        )
                    else:
                        self.con.execute(
                            "UPDATE wc_inbox SET linked_project_id = NULL WHERE linked_project_id = ?",
                            (project_id,),
                        )

            cur = self.con.execute(
                "DELETE FROM projects WHERE id = ?",
                (project_id,),
            )
            return cur.rowcount > 0

    def list_projects(self, include_counts: bool = True) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "projects"):
            return []
        project_cols = _table_columns(self.con, "projects")
        if not project_cols:
            return []

        if include_counts and _table_exists(self.con, "actions") and "id" in project_cols:
            action_cols = _table_columns(self.con, "actions")
            select_fields = []
            for col in (
                "id",
                "name",
                "type",
                "importance",
                "owner_champion_id",
                "status",
                "created_at",
                "closed_at",
                "work_center",
                "project_code",
                "full_project",
                "project_sop",
                "project_eop",
                "related_work_center",
            ):
                if col in project_cols:
                    select_fields.append(f"p.{col}")
                else:
                    select_fields.append(f"NULL AS {col}")

            status_col = "a.status" if "status" in action_cols else "NULL"
            is_draft_filter = "AND a.is_draft = 0" if "is_draft" in action_cols else ""
            query = f"""
                SELECT {", ".join(select_fields)},
                       COUNT(a.id) AS actions_total,
                       COALESCE(SUM(CASE WHEN {status_col} IN ('done','cancelled') THEN 1 ELSE 0 END), 0) AS actions_closed,
                       COALESCE(SUM(CASE WHEN {status_col} NOT IN ('done','cancelled') THEN 1 ELSE 0 END), 0) AS actions_open
                FROM projects p
                LEFT JOIN actions a ON a.project_id = p.id {is_draft_filter}
                GROUP BY p.id
            """
            if "name" in project_cols:
                query += " ORDER BY p.name"
            cur = self.con.execute(query)
            return [dict(r) for r in cur.fetchall()]

        select_fields = list(project_cols)
        if "importance" not in project_cols:
            select_fields.append("NULL AS importance")
        query = f"SELECT {', '.join(select_fields)} FROM projects"
        if "name" in project_cols:
            query += " ORDER BY name"
        cur = self.con.execute(query)
        return [dict(r) for r in cur.fetchall()]

    def list_project_work_centers_norms(self, include_related: bool = True) -> set[str]:
        try:
            from action_tracking.services.effectiveness import normalize_wc, parse_work_centers
        except Exception:
            def normalize_wc(v: Any) -> str:
                return normalize_key(str(v or ""))

            def parse_work_centers(_: Any, value: Any) -> list[str]:
                if not value:
                    return []
                return [t.strip() for t in str(value).split(",") if t.strip()]

        if not _table_exists(self.con, "projects"):
            return set()

        cols = _table_columns(self.con, "projects")
        if not cols:
            return set()
        select_cols = [c for c in ("work_center", "related_work_center") if c in cols]
        if not select_cols:
            return set()
        cur = self.con.execute(f"SELECT {', '.join(select_cols)} FROM projects")
        norms: set[str] = set()
        for row in cur.fetchall():
            rowd = dict(row)
            primary = normalize_wc(rowd.get("work_center"))
            if primary:
                norms.add(primary)
            if include_related:
                for token in parse_work_centers(None, rowd.get("related_work_center")):
                    n = normalize_wc(token)
                    if n:
                        norms.add(n)
        return norms

    def list_changelog(self, limit: int = 50, project_id: str | None = None) -> list[dict[str, Any]]:
        return _list_changelog_generic(
            self.con,
            table_candidates=[
                "project_changelog",
                "projects_changelog",
                "changelog_projects",
                "changelog",
                "change_log",
                "audit_log",
            ],
            limit=limit,
            entity_id=project_id,
        )

    def _normalize_project_payload(self, project_id: str, data: dict[str, Any]) -> dict[str, Any]:
        name = (data.get("name") or "").strip()
        work_center = (data.get("work_center") or "").strip()
        if not name:
            raise ValueError("name is required")
        if not work_center:
            raise ValueError("work_center is required")

        created_at = data.get("created_at") or date.today().isoformat()
        created_date = self._parse_date(created_at, "created_at")
        status = (data.get("status") or "active").strip() or "active"
        closed_at = data.get("closed_at") or None
        if status == "closed":
            closed_at = closed_at or date.today().isoformat()
            closed_date = self._parse_date(closed_at, "closed_at")
            if closed_date < created_date:
                raise ValueError("closed_at < created_at")
            closed_at = closed_date.isoformat()
        else:
            closed_at = None

        project_sop = data.get("project_sop") or None
        if project_sop:
            project_sop = self._parse_date(project_sop, "project_sop").isoformat()

        project_eop = data.get("project_eop") or None
        if project_eop:
            project_eop = self._parse_date(project_eop, "project_eop").isoformat()

        return {
            "id": project_id,
            "name": name,
            "type": (data.get("type") or "custom").strip() or "custom",
            "importance": (data.get("importance") or "Mid Runner").strip() or "Mid Runner",
            "owner_champion_id": (data.get("owner_champion_id") or "").strip() or None,
            "status": status,
            "created_at": created_date.isoformat(),
            "closed_at": closed_at,
            "work_center": work_center,
            "project_code": (data.get("project_code") or "").strip() or None,
            "full_project": (data.get("full_project") or "").strip() or None,
            "project_sop": project_sop,
            "project_eop": project_eop,
            "related_work_center": (data.get("related_work_center") or "").strip() or None,
        }

    @staticmethod
    def _parse_date(value: Any, field_name: str) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            try:
                return datetime.fromisoformat(str(value)).date()
            except ValueError as exc_two:
                raise ValueError(f"Invalid date for {field_name}") from exc_two


# =====================================================
# CHAMPIONS
# =====================================================

class ChampionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        _configure_sqlite_connection(self.con)

    def list_champions(self) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "champions"):
            return []

        cols = _table_columns(self.con, "champions")
        if not cols:
            return []

        base_cols = ["id", "first_name", "last_name", "email", "active"]
        select_cols = [c for c in base_cols if c in cols]
        if "hire_date" in cols:
            select_cols.append("hire_date")
        if "position" in cols:
            select_cols.append("position")
        if "team" in cols:
            select_cols.append("team")

        if not select_cols:
            return []

        cur = self.con.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM champions
            ORDER BY last_name, first_name
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r.setdefault("id", None)
            r.setdefault("first_name", None)
            r.setdefault("last_name", None)
            r.setdefault("email", None)
            r.setdefault("active", 1)
            r["display_name"] = (
                f"{(r.get('first_name') or '').strip()} {(r.get('last_name') or '').strip()}".strip()
                or r.get("email")
                or r.get("id")
            )
            r["active"] = bool(r.get("active"))
            r.setdefault("hire_date", None)
            r.setdefault("position", None)
            r.setdefault("team", None)
        return rows

    def has_champion_projects_table(self) -> bool:
        return _table_exists(self.con, "champion_projects")

    def get_assigned_projects(self, champion_id: str) -> list[str]:
        if not _table_exists(self.con, "champion_projects"):
            return []
        cols = _table_columns(self.con, "champion_projects")
        if "champion_id" not in cols or "project_id" not in cols:
            return []
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

    def get_assigned_projects_with_fallback(self, champion_id: str) -> list[str]:
        assigned = self.get_assigned_projects(champion_id)
        if assigned:
            return assigned

        # Fallback for legacy data: use projects.owner_champion_id when no explicit assignments exist.
        if not _table_exists(self.con, "projects"):
            return []
        project_cols = _table_columns(self.con, "projects")
        if "owner_champion_id" not in project_cols or "id" not in project_cols:
            return []

        order_by = "name" if "name" in project_cols else "id"
        cur = self.con.execute(
            f"""
            SELECT id
            FROM projects
            WHERE owner_champion_id = ?
            ORDER BY {order_by}
            """,
            (champion_id,),
        )
        return [row["id"] for row in cur.fetchall()]

    def set_assigned_projects(self, champion_id: str, project_ids: list[str]) -> None:
        if not _table_exists(self.con, "champion_projects"):
            return

        try:
            cur = self.con.execute("PRAGMA table_info(champion_projects)")
            columns_info = cur.fetchall()
        except sqlite3.Error:
            return

        column_names = {row[1] for row in columns_info}
        if "champion_id" not in column_names or "project_id" not in column_names:
            return

        required_columns = [
            row[1]
            for row in columns_info
            if row[1] not in {"champion_id", "project_id"}
            and bool(row[3])
            and row[4] is None
            and not bool(row[5])
        ]
        if required_columns:
            return

        cleaned_ids = [
            str(project_id).strip()
            for project_id in project_ids
            if project_id is not None and str(project_id).strip()
        ]
        unique_ids = list(dict.fromkeys(cleaned_ids))

        _configure_sqlite_connection(self.con)
        try:
            self.con.execute("BEGIN IMMEDIATE")
            self.con.execute(
                "DELETE FROM champion_projects WHERE champion_id = ?",
                (champion_id,),
            )
            if unique_ids:
                self.con.executemany(
                    """
                    INSERT INTO champion_projects (champion_id, project_id)
                    VALUES (?, ?)
                    """,
                    [(champion_id, project_id) for project_id in unique_ids],
                )
            self.con.execute("COMMIT")
        except Exception:
            _rollback_safely(self.con)
            return

    def list_changelog(self, limit: int = 50, champion_id: str | None = None) -> list[dict[str, Any]]:
        return _list_changelog_generic(
            self.con,
            table_candidates=[
                "champion_changelog",
                "champions_changelog",
                "changelog_champions",
                "changelog",
                "change_log",
                "audit_log",
            ],
            limit=limit,
            entity_id=champion_id,
        )

    def create_champion(self, data: dict[str, Any]) -> str:
        if not _table_exists(self.con, "champions"):
            return ""

        cols = _table_columns(self.con, "champions")
        if not cols:
            return ""

        champion_id = data.get("id") or str(uuid4())
        first_name = (data.get("first_name") or "").strip() or None
        last_name = (data.get("last_name") or "").strip() or None
        email = (data.get("email") or "").strip() or None
        display_name = (data.get("display_name") or "").strip() or None
        active = data.get("active")
        active_value = int(True if active is None else bool(active))
        hire_date = data.get("hire_date") or None
        if isinstance(hire_date, datetime):
            hire_date = hire_date.date().isoformat()
        elif isinstance(hire_date, date):
            hire_date = hire_date.isoformat()
        elif hire_date:
            hire_date = str(hire_date)

        payload = {
            "id": champion_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "active": active_value,
            "hire_date": hire_date,
            "position": (data.get("position") or "").strip() or None,
            "team": (data.get("team") or "").strip() or None,
        }
        if "name" in cols:
            # Backfill NOT NULL name column using UI fields (display/first/last/email/id).
            full_name = " ".join(part for part in [first_name, last_name] if part).strip()
            derived_name = display_name or full_name or email or champion_id
            payload["name"] = derived_name

        insert_cols = [c for c in payload.keys() if c in cols]
        if not insert_cols:
            return champion_id

        placeholders = ", ".join(["?"] * len(insert_cols))
        values = [payload[c] for c in insert_cols]
        _configure_sqlite_connection(self.con)
        try:
            # IMMEDIATE transaction reduces "database is locked" during concurrent writes.
            self.con.execute("BEGIN IMMEDIATE")
            self.con.execute(
                f"INSERT INTO champions ({', '.join(insert_cols)}) VALUES ({placeholders})",
                values,
            )
            self.con.execute("COMMIT")
        except Exception:
            _rollback_safely(self.con)
            return champion_id
        return champion_id

    def update_champion(self, champion_id: str, data: dict[str, Any]) -> None:
        if not champion_id:
            return
        if not _table_exists(self.con, "champions"):
            return

        try:
            cur = self.con.execute("PRAGMA table_info(champions)")
            columns_info = cur.fetchall()
        except sqlite3.Error:
            columns_info = []
        if not columns_info:
            return

        cols = {row[1] for row in columns_info}
        name_not_null = any(row[1] == "name" and bool(row[3]) for row in columns_info)

        data_keys = set(data.keys())
        payload: dict[str, Any] = {}

        existing: dict[str, Any] | None = None
        if "name" in cols and (name_not_null or "name" in data_keys):
            cur = self.con.execute(
                "SELECT name, first_name, last_name, email FROM champions WHERE id = ?",
                (champion_id,),
            )
            row = cur.fetchone()
            if row:
                existing = dict(row)

        if "first_name" in data_keys and "first_name" in cols:
            payload["first_name"] = (data.get("first_name") or "").strip() or None
        if "last_name" in data_keys and "last_name" in cols:
            payload["last_name"] = (data.get("last_name") or "").strip() or None
        if "email" in data_keys and "email" in cols:
            payload["email"] = (data.get("email") or "").strip() or None
        if "active" in data_keys and "active" in cols:
            payload["active"] = int(bool(data.get("active")))
        if "hire_date" in data_keys and "hire_date" in cols:
            hire_date = data.get("hire_date") or None
            if isinstance(hire_date, datetime):
                hire_date = hire_date.date().isoformat()
            elif isinstance(hire_date, date):
                hire_date = hire_date.isoformat()
            elif hire_date:
                hire_date = str(hire_date)
            payload["hire_date"] = hire_date
        if "position" in data_keys and "position" in cols:
            payload["position"] = (data.get("position") or "").strip() or None
        if "team" in data_keys and "team" in cols:
            payload["team"] = (data.get("team") or "").strip() or None

        if "name" in cols:
            name_value = None
            if "name" in data_keys:
                name_value = (data.get("name") or "").strip() or None
            if name_not_null or "name" in data_keys:
                first_name = (data.get("first_name") if "first_name" in data_keys else None)
                last_name = (data.get("last_name") if "last_name" in data_keys else None)
                email = (data.get("email") if "email" in data_keys else None)
                if existing:
                    if first_name is None:
                        first_name = existing.get("first_name")
                    if last_name is None:
                        last_name = existing.get("last_name")
                    if email is None:
                        email = existing.get("email")
                full_name = " ".join(part for part in [first_name, last_name] if part).strip()
                derived_name = full_name or email or (existing.get("name") if existing else None) or champion_id
                payload["name"] = name_value or derived_name

        if not payload:
            return

        sets = [f"{col} = ?" for col in payload.keys()]
        params = list(payload.values())
        params.append(champion_id)
        with self.con:
            try:
                self.con.execute(
                    f"UPDATE champions SET {', '.join(sets)} WHERE id = ?",
                    params,
                )
            except sqlite3.Error:
                return

    def delete_champion(self, champion_id: str) -> bool:
        if not champion_id:
            return False
        if not _table_exists(self.con, "champions"):
            return False

        with self.con:
            if _table_exists(self.con, "champion_projects"):
                self.con.execute(
                    "DELETE FROM champion_projects WHERE champion_id = ?",
                    (champion_id,),
                )

            if _table_exists(self.con, "actions"):
                action_cols = _table_columns(self.con, "actions")
                if "owner_champion_id" in action_cols:
                    self.con.execute(
                        "UPDATE actions SET owner_champion_id = NULL WHERE owner_champion_id = ?",
                        (champion_id,),
                    )

            if _table_exists(self.con, "projects"):
                project_cols = _table_columns(self.con, "projects")
                if "owner_champion_id" in project_cols:
                    self.con.execute(
                        "UPDATE projects SET owner_champion_id = NULL WHERE owner_champion_id = ?",
                        (champion_id,),
                    )

            cur = self.con.execute(
                "DELETE FROM champions WHERE id = ?",
                (champion_id,),
            )
            return cur.rowcount > 0


# =====================================================
# PRODUCTION DATA (SCRAP / KPI)
# =====================================================

class ProductionDataRepository:
    """
    API CONTRACT used by:
    - production_import.py: upsert_scrap_daily, upsert_production_kpi_daily
    - production_explorer.py: list_scrap_daily, list_kpi_daily, list_distinct_work_centers, list_work_centers
    - projects.py (WC inbox): list_production_work_centers_with_stats
    """

    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_scrap_daily(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if not _table_exists(self.con, "scrap_daily"):
            return
        cols = _table_columns(self.con, "scrap_daily")
        if not cols or "metric_date" not in cols or "work_center" not in cols:
            return
        now = datetime.now(timezone.utc).isoformat()
        payload = []
        for r in rows:
            payload.append(
                {
                    "id": r.get("id") or str(uuid4()),
                    "metric_date": str(r["metric_date"]),
                    "work_center": str(r["work_center"]),
                    "full_project": r.get("full_project"),
                    "scrap_qty": _normalize_int(r.get("scrap_qty"), default=0),
                    "scrap_cost_amount": _normalize_float(r.get("scrap_cost_amount")),
                    "scrap_cost_currency": (r.get("scrap_cost_currency") or "PLN"),
                    "created_at": r.get("created_at") or now,
                }
            )

        insert_cols = [
            c
            for c in (
                "id",
                "metric_date",
                "work_center",
                "full_project",
                "scrap_qty",
                "scrap_cost_amount",
                "scrap_cost_currency",
                "created_at",
            )
            if c in cols
        ]
        if not insert_cols:
            return
        values = [[row.get(col) for col in insert_cols] for row in payload]
        placeholders = ", ".join(["?"] * len(insert_cols))
        update_cols = [
            c
            for c in (
                "full_project",
                "scrap_qty",
                "scrap_cost_amount",
                "scrap_cost_currency",
            )
            if c in cols
        ]
        update_clause = ", ".join([f"{col} = excluded.{col}" for col in update_cols])
        _configure_sqlite_connection(self.con)
        self.con.executemany(
            f"""
            INSERT INTO scrap_daily ({', '.join(insert_cols)})
            VALUES ({placeholders})
            ON CONFLICT(metric_date, work_center, scrap_cost_currency) DO UPDATE SET
                {update_clause}
            """,
            values,
        )
        self.con.commit()

    def upsert_production_kpi_daily(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if not _table_exists(self.con, "production_kpi_daily"):
            return
        cols = _table_columns(self.con, "production_kpi_daily")
        if not cols or "metric_date" not in cols or "work_center" not in cols:
            return
        now = datetime.now(timezone.utc).isoformat()
        payload = []
        for r in rows:
            payload.append(
                {
                    "id": r.get("id") or str(uuid4()),
                    "metric_date": str(r["metric_date"]),
                    "work_center": str(r["work_center"]),
                    "full_project": r.get("full_project"),
                    "worktime_min": _normalize_float(r.get("worktime_min")),
                    "performance_pct": _normalize_percent(r.get("performance_pct")),
                    "oee_pct": _normalize_percent(r.get("oee_pct")),
                    "availability_pct": _normalize_percent(r.get("availability_pct")),
                    "quality_pct": _normalize_percent(r.get("quality_pct")),
                    "source_file": r.get("source_file"),
                    "imported_at": r.get("imported_at") or now,
                    "created_at": r.get("created_at") or r.get("imported_at") or now,
                }
            )

        insert_cols = [
            c
            for c in (
                "id",
                "metric_date",
                "work_center",
                "full_project",
                "worktime_min",
                "performance_pct",
                "oee_pct",
                "availability_pct",
                "quality_pct",
                "source_file",
                "imported_at",
                "created_at",
            )
            if c in cols
        ]
        if not insert_cols:
            return
        values = [[row.get(col) for col in insert_cols] for row in payload]
        placeholders = ", ".join(["?"] * len(insert_cols))
        update_cols = [
            c
            for c in (
                "full_project",
                "worktime_min",
                "performance_pct",
                "oee_pct",
                "availability_pct",
                "quality_pct",
                "source_file",
                "imported_at",
                "created_at",
            )
            if c in cols
        ]
        update_clause = ", ".join([f"{col} = excluded.{col}" for col in update_cols])
        _configure_sqlite_connection(self.con)
        self.con.executemany(
            f"""
            INSERT INTO production_kpi_daily ({', '.join(insert_cols)})
            VALUES ({placeholders})
            ON CONFLICT(metric_date, work_center) DO UPDATE SET
                {update_clause}
            """,
            values,
        )
        self.con.commit()

    def list_scrap_daily(
        self,
        work_centers: str | list[str] | None,
        date_from: date | str | None,
        date_to: date | str | None,
        currency: str | None = "PLN",
        full_project: str | list[str] | None = None,
        workcenter_areas: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "scrap_daily"):
            return []
        cols = _table_columns(self.con, "scrap_daily")
        if not cols:
            return []
        select_fields = []
        for col in ("metric_date", "work_center", "scrap_qty", "scrap_cost_amount", "scrap_cost_currency"):
            if col in cols:
                select_fields.append(col)
            else:
                select_fields.append(f"NULL AS {col}")
        query = f"""
            SELECT {", ".join(select_fields)}
            FROM scrap_daily
        """
        filters: list[str] = []
        params: list[Any] = []

        if full_project is not None:
            if "full_project" in cols:
                if isinstance(full_project, str):
                    project_value = full_project.strip()
                    if project_value:
                        filters.append("full_project = ?")
                        params.append(project_value)
                else:
                    if not full_project:
                        return []
                    placeholders = ", ".join(["?"] * len(full_project))
                    filters.append(f"full_project IN ({placeholders})")
                    params.extend([str(project) for project in full_project])
            elif work_centers is None:
                work_centers = full_project

        if work_centers is not None:
            if isinstance(work_centers, str):
                wc = work_centers.strip()
                if wc:
                    filters.append("work_center = ?")
                    params.append(wc)
            else:
                if not work_centers:
                    return []
                placeholders = ", ".join(["?"] * len(work_centers))
                filters.append(f"work_center IN ({placeholders})")
                params.extend([str(wc) for wc in work_centers])

        if date_from:
            filters.append("metric_date >= ?")
            params.append(self._normalize_date_filter(date_from))
        if date_to:
            filters.append("metric_date <= ?")
            params.append(self._normalize_date_filter(date_to))

        if currency and "scrap_cost_currency" in cols:
            filters.append("scrap_cost_currency = ?")
            params.append(str(currency))

        if filters:
            query += " WHERE " + " AND ".join(filters)

        query += " ORDER BY metric_date ASC, work_center ASC"
        try:
            cur = self.con.execute(query, params)
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []
        if workcenter_areas:
            rows = filter_rows_by_areas(rows, workcenter_areas)
        for row in rows:
            row["scrap_qty"] = _normalize_int(row.get("scrap_qty"), default=0)
            row["scrap_cost_amount"] = _normalize_float(row.get("scrap_cost_amount"))
            if row.get("scrap_cost_currency") in (None, "") and currency:
                row["scrap_cost_currency"] = str(currency)
        return rows

    def list_kpi_daily(
        self,
        work_centers: str | list[str] | None,
        date_from: date | str | None,
        date_to: date | str | None,
        full_project: str | list[str] | None = None,
        workcenter_areas: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "production_kpi_daily"):
            return []
        cols = _table_columns(self.con, "production_kpi_daily")
        if not cols:
            return []
        select_fields = []
        for col in (
            "metric_date",
            "work_center",
            "worktime_min",
            "performance_pct",
            "oee_pct",
            "availability_pct",
            "quality_pct",
        ):
            if col in cols:
                select_fields.append(col)
            else:
                select_fields.append(f"NULL AS {col}")
        query = f"""
            SELECT {", ".join(select_fields)}
            FROM production_kpi_daily
        """
        filters: list[str] = []
        params: list[Any] = []

        if full_project is not None:
            if "full_project" in cols:
                if isinstance(full_project, str):
                    project_value = full_project.strip()
                    if project_value:
                        filters.append("full_project = ?")
                        params.append(project_value)
                else:
                    if not full_project:
                        return []
                    placeholders = ", ".join(["?"] * len(full_project))
                    filters.append(f"full_project IN ({placeholders})")
                    params.extend([str(project) for project in full_project])
            elif work_centers is None:
                work_centers = full_project

        if work_centers is not None:
            if isinstance(work_centers, str):
                wc = work_centers.strip()
                if wc:
                    filters.append("work_center = ?")
                    params.append(wc)
            else:
                if not work_centers:
                    return []
                placeholders = ", ".join(["?"] * len(work_centers))
                filters.append(f"work_center IN ({placeholders})")
                params.extend([str(wc) for wc in work_centers])

        if date_from:
            filters.append("metric_date >= ?")
            params.append(self._normalize_date_filter(date_from))
        if date_to:
            filters.append("metric_date <= ?")
            params.append(self._normalize_date_filter(date_to))

        if filters:
            query += " WHERE " + " AND ".join(filters)

        query += " ORDER BY metric_date ASC, work_center ASC"
        try:
            cur = self.con.execute(query, params)
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.Error:
            return []
        if workcenter_areas:
            rows = filter_rows_by_areas(rows, workcenter_areas)
        for row in rows:
            row["worktime_min"] = _normalize_float(row.get("worktime_min"))
            row["performance_pct"] = _normalize_percent(row.get("performance_pct"))
            row["oee_pct"] = _normalize_percent(row.get("oee_pct"))
            row["availability_pct"] = _normalize_percent(row.get("availability_pct"))
            row["quality_pct"] = _normalize_percent(row.get("quality_pct"))
        kpi_cols = ("performance_pct", "oee_pct", "availability_pct", "quality_pct")
        rows = [row for row in rows if any(row.get(col) is not None for col in kpi_cols)]
        return rows

    def has_full_project_column(self, table: str) -> bool:
        if not _table_exists(self.con, table):
            return False
        return "full_project" in _table_columns(self.con, table)

    def count_full_project_matches(self, table: str, project_key: str) -> int | None:
        if not self.has_full_project_column(table):
            return None
        if not project_key:
            return 0
        try:
            cur = self.con.execute(
                f"""
                SELECT COUNT(1) AS row_count
                FROM {table}
                WHERE TRIM(full_project) = TRIM(?)
                """,
                (project_key,),
            )
            row = cur.fetchone()
        except sqlite3.Error:
            return None
        if not row:
            return 0
        return int(row[0] or 0)

    def list_distinct_full_project(self, table: str, limit: int = 20) -> list[str] | None:
        if not self.has_full_project_column(table):
            return None
        try:
            cur = self.con.execute(
                f"""
                SELECT DISTINCT full_project
                FROM {table}
                ORDER BY full_project
                LIMIT ?
                """,
                (limit,),
            )
            rows = cur.fetchall()
        except sqlite3.Error:
            return None
        return [row[0] for row in rows if row and row[0] is not None]

    def rescale_kpi_daily_percent(self) -> dict[str, int]:
        if not _table_exists(self.con, "production_kpi_daily"):
            return {}
        cols = _table_columns(self.con, "production_kpi_daily")
        if not cols:
            return {}
        _configure_sqlite_connection(self.con)
        scale_targets = [
            col
            for col in ("performance_pct", "oee_pct", "availability_pct", "quality_pct")
            if col in cols
        ]
        if not scale_targets:
            return {}
        stats: dict[str, int] = {}
        for col in scale_targets:
            cur = self.con.execute(
                f"""
                SELECT COUNT(*) AS count_rows
                FROM production_kpi_daily
                WHERE {col} IS NOT NULL
                  AND {col} BETWEEN 0 AND 1.5
                """
            )
            row = cur.fetchone()
            to_scale = int(row[0]) if row else 0
            stats[col] = to_scale
            if to_scale:
                self.con.execute(
                    f"""
                    UPDATE production_kpi_daily
                    SET {col} = {col} * 100
                    WHERE {col} IS NOT NULL
                      AND {col} BETWEEN 0 AND 1.5
                    """
                )
        self.con.commit()
        return stats

    def list_distinct_work_centers(self) -> dict[str, list[str]]:
        scrap_work_centers: list[str] = []
        kpi_work_centers: list[str] = []

        if _table_exists(self.con, "scrap_daily"):
            scrap_cols = _table_columns(self.con, "scrap_daily")
            if "work_center" in scrap_cols:
                cur = self.con.execute(
                    """
                    SELECT DISTINCT work_center
                    FROM scrap_daily
                    ORDER BY work_center
                    """
                )
                scrap_work_centers = [row["work_center"] for row in cur.fetchall()]

        if _table_exists(self.con, "production_kpi_daily"):
            kpi_cols = _table_columns(self.con, "production_kpi_daily")
            if "work_center" in kpi_cols:
                cur = self.con.execute(
                    """
                    SELECT DISTINCT work_center
                    FROM production_kpi_daily
                    ORDER BY work_center
                    """
                )
                kpi_work_centers = [row["work_center"] for row in cur.fetchall()]

        return {"scrap_work_centers": scrap_work_centers, "kpi_work_centers": kpi_work_centers}

    def list_work_centers(self) -> list[str]:
        if not _table_exists(self.con, "scrap_daily") and not _table_exists(self.con, "production_kpi_daily"):
            return []
        scrap_cols = _table_columns(self.con, "scrap_daily") if _table_exists(self.con, "scrap_daily") else set()
        kpi_cols = _table_columns(self.con, "production_kpi_daily") if _table_exists(self.con, "production_kpi_daily") else set()
        if "work_center" not in scrap_cols and "work_center" not in kpi_cols:
            return []
        if "work_center" in scrap_cols and "work_center" in kpi_cols:
            cur = self.con.execute(
                """
                SELECT work_center FROM scrap_daily
                UNION
                SELECT work_center FROM production_kpi_daily
                ORDER BY work_center ASC
                """
            )
        elif "work_center" in scrap_cols:
            cur = self.con.execute("SELECT work_center FROM scrap_daily ORDER BY work_center ASC")
        else:
            cur = self.con.execute("SELECT work_center FROM production_kpi_daily ORDER BY work_center ASC")
        return [row["work_center"] for row in cur.fetchall()]

    def full_project_exists(self, value: str) -> bool:
        project_key = (value or "").strip()
        if not project_key:
            return False
        for table in ("scrap_daily", "production_kpi_daily"):
            if not self.has_full_project_column(table):
                continue
            cur = self.con.execute(
                f"""
                SELECT 1
                FROM {table}
                WHERE TRIM(full_project) = TRIM(?)
                LIMIT 1
                """,
                (project_key,),
            )
            if cur.fetchone():
                return True
        return False

    def list_full_project_candidates_by_wc(self) -> dict[str, list[dict[str, Any]]]:
        from action_tracking.services.effectiveness import normalize_wc  # type: ignore

        candidates: dict[str, dict[str, dict[str, Any]]] = {}

        def _accumulate(table: str) -> None:
            if not _table_exists(self.con, table):
                return
            cols = _table_columns(self.con, table)
            if "work_center" not in cols or "full_project" not in cols:
                return
            if "metric_date" not in cols:
                return
            cur = self.con.execute(
                f"""
                SELECT work_center,
                       full_project,
                       COUNT(*) AS row_count,
                       MAX(metric_date) AS last_seen
                FROM {table}
                WHERE full_project IS NOT NULL
                  AND TRIM(full_project) != ''
                GROUP BY work_center, full_project
                """
            )
            for row in cur.fetchall():
                wc_raw = row["work_center"]
                wc_norm = normalize_wc(wc_raw)
                full_project = (row["full_project"] or "").strip()
                if not wc_norm or not full_project:
                    continue
                wc_entries = candidates.setdefault(wc_norm, {})
                entry = wc_entries.setdefault(
                    full_project,
                    {
                        "full_project": full_project,
                        "row_count": 0,
                        "last_seen": None,
                        "wc_raw": wc_raw,
                    },
                )
                entry["row_count"] += int(row["row_count"] or 0)
                if entry["last_seen"] is None or (
                    row["last_seen"] and row["last_seen"] > entry["last_seen"]
                ):
                    entry["last_seen"] = row["last_seen"]
                if not entry.get("wc_raw"):
                    entry["wc_raw"] = wc_raw

        _accumulate("scrap_daily")
        _accumulate("production_kpi_daily")

        result: dict[str, list[dict[str, Any]]] = {}
        for wc_norm, entries in candidates.items():
            result[wc_norm] = list(entries.values())
        return result

    def list_production_work_centers_with_stats(self) -> list[dict[str, Any]]:
        from action_tracking.services.effectiveness import normalize_wc  # type: ignore

        stats: dict[str, dict[str, Any]] = {}

        if _table_exists(self.con, "scrap_daily"):
            scrap_cols = _table_columns(self.con, "scrap_daily")
            if "work_center" not in scrap_cols or "metric_date" not in scrap_cols:
                return list(stats.values())
            cur = self.con.execute(
                """
                SELECT work_center,
                       MIN(metric_date) AS first_seen_date,
                       MAX(metric_date) AS last_seen_date,
                       COUNT(DISTINCT metric_date) AS count_days_present
                FROM scrap_daily
                GROUP BY work_center
                """
            )
            for row in cur.fetchall():
                wc_raw = row["work_center"]
                wc_norm = normalize_wc(wc_raw)
                if not wc_norm:
                    continue
                entry = stats.setdefault(
                    wc_norm,
                    {
                        "wc_raw": wc_raw,
                        "wc_norm": wc_norm,
                        "has_scrap": False,
                        "has_kpi": False,
                        "first_seen_date": row["first_seen_date"],
                        "last_seen_date": row["last_seen_date"],
                        "count_days_present": 0,
                    },
                )
                entry["has_scrap"] = True
                entry["count_days_present"] += int(row["count_days_present"] or 0)
                if entry.get("first_seen_date") is None or (
                    row["first_seen_date"] and row["first_seen_date"] < entry["first_seen_date"]
                ):
                    entry["first_seen_date"] = row["first_seen_date"]
                    entry["wc_raw"] = wc_raw
                if entry.get("last_seen_date") is None or (
                    row["last_seen_date"] and row["last_seen_date"] > entry["last_seen_date"]
                ):
                    entry["last_seen_date"] = row["last_seen_date"]

        if _table_exists(self.con, "production_kpi_daily"):
            kpi_cols = _table_columns(self.con, "production_kpi_daily")
            if "work_center" not in kpi_cols or "metric_date" not in kpi_cols:
                return list(stats.values())
            cur = self.con.execute(
                """
                SELECT work_center,
                       MIN(metric_date) AS first_seen_date,
                       MAX(metric_date) AS last_seen_date,
                       COUNT(DISTINCT metric_date) AS count_days_present
                FROM production_kpi_daily
                GROUP BY work_center
                """
            )
            for row in cur.fetchall():
                wc_raw = row["work_center"]
                wc_norm = normalize_wc(wc_raw)
                if not wc_norm:
                    continue
                entry = stats.setdefault(
                    wc_norm,
                    {
                        "wc_raw": wc_raw,
                        "wc_norm": wc_norm,
                        "has_scrap": False,
                        "has_kpi": False,
                        "first_seen_date": row["first_seen_date"],
                        "last_seen_date": row["last_seen_date"],
                        "count_days_present": 0,
                    },
                )
                entry["has_kpi"] = True
                entry["count_days_present"] += int(row["count_days_present"] or 0)
                if entry.get("first_seen_date") is None or (
                    row["first_seen_date"] and row["first_seen_date"] < entry["first_seen_date"]
                ):
                    entry["first_seen_date"] = row["first_seen_date"]
                    entry["wc_raw"] = wc_raw
                if entry.get("last_seen_date") is None or (
                    row["last_seen_date"] and row["last_seen_date"] > entry["last_seen_date"]
                ):
                    entry["last_seen_date"] = row["last_seen_date"]

        return list(stats.values())

    @staticmethod
    def _normalize_date_filter(value: date | str) -> str:
        return value.isoformat() if isinstance(value, date) else str(value)


# =====================================================
# WC INBOX
# =====================================================

class WcInboxRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        _configure_sqlite_connection(self.con)
        _ensure_column(self.con, "wc_inbox", "full_project", "TEXT")

    def upsert_from_production(
        self,
        work_centers_stats: list[dict[str, Any]],
        existing_project_wc_norms: set[str],
        full_project_by_wc_norm: dict[str, str] | None = None,
    ) -> None:
        if not _table_exists(self.con, "wc_inbox"):
            return
        cols = _table_columns(self.con, "wc_inbox")
        required_cols = {
            "id",
            "wc_raw",
            "wc_norm",
            "sources",
            "first_seen_date",
            "last_seen_date",
            "status",
            "linked_project_id",
            "created_at",
            "updated_at",
        }
        if not required_cols.issubset(cols):
            return

        try:
            from action_tracking.services.effectiveness import normalize_wc  # type: ignore
        except Exception:
            def normalize_wc(v: Any) -> str:
                return normalize_key(str(v or ""))

        existing_rows: dict[str, dict[str, Any]] = {}
        cur = self.con.execute(
            """
            SELECT wc_norm, wc_raw, sources, status, first_seen_date, last_seen_date
            FROM wc_inbox
            """
        )
        for r in cur.fetchall():
            existing_rows[r["wc_norm"]] = dict(r)

        now = datetime.now(timezone.utc).isoformat()

        _configure_sqlite_connection(self.con)
        try:
            # IMMEDIATE transaction avoids lock contention during batch upsert.
            self.con.execute("BEGIN IMMEDIATE")
            for row in work_centers_stats or []:
                wc_norm = normalize_wc(row.get("wc_norm") or row.get("wc_raw"))
                if not wc_norm:
                    continue

                existing = existing_rows.get(wc_norm)

                if wc_norm in (existing_project_wc_norms or set()):
                    if existing and existing.get("status") == "open":
                        self._set_status(wc_norm, "linked", None, commit=False)
                    continue

                sources: list[str] = []
                if row.get("has_scrap"):
                    sources.append("scrap")
                if row.get("has_kpi"):
                    sources.append("kpi")

                if existing:
                    try:
                        prev_sources = json.loads(existing.get("sources") or "[]")
                    except json.JSONDecodeError:
                        prev_sources = []
                    sources = sorted(set(prev_sources) | set(sources))

                wc_raw_value = (row.get("wc_raw") or "").strip()
                if existing and existing.get("wc_raw"):
                    wc_raw_value = existing.get("wc_raw") or wc_raw_value

                full_project_value = None
                if "full_project" in cols:
                    full_project_value = row.get("full_project") or None
                    if full_project_by_wc_norm:
                        full_project_value = full_project_by_wc_norm.get(wc_norm) or full_project_value
                    if full_project_value is not None:
                        full_project_value = str(full_project_value).strip() or None

                insert_cols = [
                    "id",
                    "wc_raw",
                    "wc_norm",
                    "sources",
                    "first_seen_date",
                    "last_seen_date",
                    "status",
                    "linked_project_id",
                    "created_at",
                    "updated_at",
                ]
                if "full_project" in cols:
                    insert_cols.insert(3, "full_project")
                placeholders = ", ".join(["?"] * len(insert_cols))
                update_sets = [
                    "wc_raw = excluded.wc_raw",
                    "sources = excluded.sources",
                    "first_seen_date = COALESCE("
                    "MIN(wc_inbox.first_seen_date, excluded.first_seen_date),"
                    "excluded.first_seen_date,"
                    "wc_inbox.first_seen_date"
                    ")",
                    "last_seen_date = COALESCE("
                    "MAX(wc_inbox.last_seen_date, excluded.last_seen_date),"
                    "excluded.last_seen_date,"
                    "wc_inbox.last_seen_date"
                    ")",
                    "updated_at = excluded.updated_at",
                ]
                if "full_project" in cols:
                    update_sets.insert(
                        2,
                        "full_project = CASE "
                        "WHEN excluded.full_project IS NOT NULL "
                        "AND TRIM(excluded.full_project) != '' "
                        "THEN excluded.full_project "
                        "ELSE wc_inbox.full_project END",
                    )

                values = [
                    str(uuid4()),
                    wc_raw_value,
                    wc_norm,
                ]
                if "full_project" in cols:
                    values.append(full_project_value)
                values.extend(
                    [
                        json.dumps(sources, ensure_ascii=False),
                        row.get("first_seen_date"),
                        row.get("last_seen_date"),
                        "open",
                        None,
                        now,
                        now,
                    ]
                )

                self.con.execute(
                    f"""
                    INSERT INTO wc_inbox ({', '.join(insert_cols)})
                    VALUES ({placeholders})
                    ON CONFLICT(wc_norm) DO UPDATE SET
                        {", ".join(update_sets)}
                    """,
                    values,
                )
            self.con.execute("COMMIT")
        except Exception:
            _rollback_safely(self.con)
            return

    def list_open(self, limit: int = 200) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "wc_inbox"):
            return []
        cols = _table_columns(self.con, "wc_inbox")
        if not cols or "status" not in cols:
            return []
        order_clause = "ORDER BY last_seen_date DESC, wc_raw ASC"
        if "last_seen_date" not in cols or "wc_raw" not in cols:
            order_clause = "ORDER BY rowid DESC"
        cur = self.con.execute(
            f"""
            SELECT *
            FROM wc_inbox
            WHERE status = 'open'
            {order_clause}
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            try:
                r["sources"] = json.loads(r.get("sources") or "[]")
            except json.JSONDecodeError:
                r["sources"] = []
        return rows

    def ignore(self, wc_norm: str) -> None:
        if not _table_exists(self.con, "wc_inbox"):
            return
        self._set_status(wc_norm, "ignored", None)

    def link_to_project(self, wc_norm: str, project_id: str) -> None:
        if not _table_exists(self.con, "wc_inbox"):
            return
        self._set_status(wc_norm, "linked", project_id)

    def mark_created(self, wc_norm: str, project_id: str) -> None:
        if not _table_exists(self.con, "wc_inbox"):
            return
        self._set_status(wc_norm, "created", project_id)

    def _set_status(
        self,
        wc_norm: str,
        status: str,
        project_id: str | None,
        commit: bool = True,
    ) -> None:
        cols = _table_columns(self.con, "wc_inbox")
        required = {"status", "linked_project_id", "updated_at", "wc_norm"}
        if not required.issubset(cols):
            return
        now = datetime.now(timezone.utc).isoformat()
        self.con.execute(
            """
            UPDATE wc_inbox
            SET status = ?,
                linked_project_id = ?,
                updated_at = ?
            WHERE wc_norm = ?
            """,
            (status, project_id, now, wc_norm),
        )
        if commit:
            self.con.commit()
