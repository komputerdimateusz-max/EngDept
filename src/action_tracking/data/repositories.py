from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

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

        query = """
            SELECT id, name, is_active, sort_order, created_at
            FROM action_categories
        """
        params: list[Any] = []
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY sort_order ASC, name ASC"
        cur = self.con.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["is_active"] = bool(r.get("is_active"))
        return rows


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
        try:
            cur = self.con.execute(
                """
                SELECT id,
                       created_at,
                       notification_type,
                       recipient_email,
                       action_id,
                       payload_json,
                       unique_key
                FROM email_notifications_log
                ORDER BY created_at DESC
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

    def list_actions(
        self,
        status: str | None = None,
        project_id: str | None = None,
        champion_id: str | None = None,
        is_draft: bool | None = None,
        overdue_only: bool = False,
        search_text: str | None = None,
    ) -> list[dict[str, Any]]:
        base_query = """
            SELECT a.*,
                   p.name AS project_name,
                   TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, '')) AS owner_name
            FROM actions a
            LEFT JOIN projects p ON p.id = a.project_id
            LEFT JOIN champions ch ON ch.id = a.owner_champion_id
        """
        filters: list[str] = []
        params: list[Any] = []
        today = date.today().isoformat()

        if status:
            filters.append("a.status = ?")
            params.append(status)
        if project_id:
            filters.append("a.project_id = ?")
            params.append(project_id)
        if champion_id:
            filters.append("a.owner_champion_id = ?")
            params.append(champion_id)
        if is_draft is not None:
            filters.append("a.is_draft = ?")
            params.append(1 if is_draft else 0)
        if overdue_only:
            filters.append(
                "a.due_date IS NOT NULL AND a.due_date < ? AND a.status NOT IN ('done','cancelled')"
            )
            params.append(today)
        if search_text:
            filters.append("a.title LIKE ?")
            params.append(f"%{search_text.strip()}%")

        if filters:
            base_query += " WHERE " + " AND ".join(filters)

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

        cur = self.con.execute(base_query, params)
        return [dict(r) for r in cur.fetchall()]

    def create_action(self, data: dict[str, Any]) -> str:
        action_id = data.get("id") or str(uuid4())
        payload = self._normalize_action_payload(action_id, data)

        cols = [
            "id",
            "project_id",
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
            "manual_savings_amount",
            "manual_savings_currency",
            "manual_savings_note",
            "source",
            "source_message_id",
            "submitted_by_email",
            "submitted_at",
        ]
        vals = [payload.get(c) for c in cols]
        placeholders = ", ".join(["?"] * len(cols))
        self.con.execute(
            f"INSERT INTO actions ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        self.con.commit()
        return action_id

    # ============================
    # ✅ MISSING METHODS (FIX)
    # ============================

    def update_action(self, action_id: str, data: dict[str, Any]) -> None:
        """
        Update action row in DB.
        Keeps created_at stable unless explicitly provided.
        Enforces closed_at logic consistent with _normalize_action_payload:
          - status == 'done' => closed_at set (default today)
          - else => closed_at cleared
        """
        if not action_id:
            raise ValueError("action_id is required")
        if not _table_exists(self.con, "actions"):
            raise ValueError("actions table missing")

        cur = self.con.execute("SELECT * FROM actions WHERE id = ?", (action_id,))
        existing_row = cur.fetchone()
        if not existing_row:
            raise ValueError("Action not found")

        existing = dict(existing_row)

        # Preserve created_at unless user explicitly passes it
        merged: dict[str, Any] = dict(existing)
        merged.update(data or {})
        if "created_at" not in (data or {}) or not (data or {}).get("created_at"):
            merged["created_at"] = existing.get("created_at") or date.today().isoformat()

        payload = self._normalize_action_payload(action_id, merged)

        # Build UPDATE dynamically
        allowed_cols = [
            "project_id",
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
            if col in payload:
                sets.append(f"{col} = ?")
                params.append(payload.get(col))

        if not sets:
            return

        params.append(action_id)
        sql = f"UPDATE actions SET {', '.join(sets)} WHERE id = ?"
        self.con.execute(sql, params)
        self.con.commit()

    def delete_action(self, action_id: str) -> None:
        if not action_id:
            return
        if not _table_exists(self.con, "actions"):
            return
        self.con.execute("DELETE FROM actions WHERE id = ?", (action_id,))
        self.con.commit()

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
        query = """
            SELECT id,
                   title,
                   created_at,
                   closed_at,
                   due_date,
                   status,
                   owner_champion_id,
                   category,
                   project_id,
                   impact_aspects
            FROM actions
            WHERE is_draft = 0
        """
        filters: list[str] = []
        params: list[Any] = []
        if project_id:
            filters.append("project_id = ?")
            params.append(project_id)
        if champion_id:
            filters.append("owner_champion_id = ?")
            params.append(champion_id)
        if category:
            filters.append("category = ?")
            params.append(category)
        if filters:
            query += " AND " + " AND ".join(filters)
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def list_actions_for_ranking(
        self,
        project_id: str | None = None,
        category: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT a.id,
                   a.title,
                   a.owner_champion_id,
                   a.project_id,
                   p.name AS project_name,
                   a.category,
                   a.status,
                   a.created_at,
                   a.due_date,
                   a.closed_at,
                   a.manual_savings_amount,
                   a.manual_savings_currency,
                   a.manual_savings_note,
                   ae.metric AS effectiveness_metric,
                   ae.delta AS effectiveness_delta
            FROM actions a
            LEFT JOIN projects p ON p.id = a.project_id
            LEFT JOIN action_effectiveness ae ON ae.action_id = a.id
            WHERE a.created_at IS NOT NULL AND a.is_draft = 0
        """
        filters: list[str] = []
        params: list[Any] = []
        if project_id:
            filters.append("a.project_id = ?")
            params.append(project_id)
        if category:
            filters.append("a.category = ?")
            params.append(category)
        if date_to:
            filters.append("date(a.created_at) <= date(?)")
            params.append(date_to.isoformat())
        if date_from:
            filters.append(
                "(date(a.created_at) >= date(?) OR (a.closed_at IS NOT NULL AND date(a.closed_at) >= date(?)))"
            )
            params.extend([date_from.isoformat(), date_from.isoformat()])
        if filters:
            query += " AND " + " AND ".join(filters)
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

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

        def _d(v: date | str | None) -> str | None:
            if v is None:
                return None
            return v.isoformat() if isinstance(v, date) else str(v)

        df = _d(date_from)
        dt = _d(date_to)

        query = """
            SELECT a.id,
                   a.title,
                   a.description,
                   a.category,
                   a.status,
                   a.created_at,
                   a.due_date,
                   a.closed_at,
                   a.owner_champion_id,
                   TRIM(COALESCE(ch.first_name, '') || ' ' || COALESCE(ch.last_name, '')) AS owner_name,
                   a.impact_type,
                   a.impact_value,
                   a.impact_aspects,
                   a.manual_savings_amount,
                   a.manual_savings_currency,
                   a.manual_savings_note
            FROM actions a
            LEFT JOIN champions ch ON ch.id = a.owner_champion_id
            WHERE a.project_id = ?
              AND a.is_draft = 0
        """
        params: list[Any] = [project_id]

        if df or dt:
            df = df or "0001-01-01"
            dt = dt or "9999-12-31"
            query += """
              AND (
                    (a.created_at IS NOT NULL AND date(a.created_at) BETWEEN date(?) AND date(?))
                 OR (a.closed_at  IS NOT NULL AND date(a.closed_at)  BETWEEN date(?) AND date(?))
                 OR (a.due_date   IS NOT NULL AND date(a.due_date)   BETWEEN date(?) AND date(?))
              )
            """
            params.extend([df, dt, df, dt, df, dt])

        query += " ORDER BY a.closed_at DESC, a.created_at DESC"

        cur = self.con.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r.setdefault("owner_name", None)
            r.setdefault("impact_aspects", None)
            r.setdefault("manual_savings_amount", None)
            r.setdefault("manual_savings_currency", None)
            r.setdefault("manual_savings_note", None)
        return rows

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
        record_id = payload.get("id") or str(uuid4())
        self.con.execute(
            """
            INSERT INTO action_effectiveness (
                id,
                action_id,
                metric,
                baseline_from,
                baseline_to,
                after_from,
                after_to,
                baseline_days,
                after_days,
                baseline_avg,
                after_avg,
                delta,
                pct_change,
                classification,
                computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(action_id) DO UPDATE SET
                metric = excluded.metric,
                baseline_from = excluded.baseline_from,
                baseline_to = excluded.baseline_to,
                after_from = excluded.after_from,
                after_to = excluded.after_to,
                baseline_days = excluded.baseline_days,
                after_days = excluded.after_days,
                baseline_avg = excluded.baseline_avg,
                after_avg = excluded.after_avg,
                delta = excluded.delta,
                pct_change = excluded.pct_change,
                classification = excluded.classification,
                computed_at = excluded.computed_at
            """,
            (
                record_id,
                action_id,
                payload.get("metric"),
                payload.get("baseline_from"),
                payload.get("baseline_to"),
                payload.get("after_from"),
                payload.get("after_to"),
                int(payload.get("baseline_days") or 0),
                int(payload.get("after_days") or 0),
                payload.get("baseline_avg"),
                payload.get("after_avg"),
                payload.get("delta"),
                payload.get("pct_change"),
                payload.get("classification"),
                payload.get("computed_at") or datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.con.commit()

    def get_effectiveness_for_actions(self, action_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not action_ids:
            return {}
        if not _table_exists(self.con, "action_effectiveness"):
            return {}
        placeholders = ", ".join(["?"] * len(action_ids))
        cur = self.con.execute(
            f"""
            SELECT *
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

    def create_project(self, data: dict[str, Any]) -> str:
        if not _table_exists(self.con, "projects"):
            raise ValueError("projects table missing")

        project_id = data.get("id") or str(uuid4())
        payload = self._normalize_project_payload(project_id, data)

        try:
            cur = self.con.execute("PRAGMA table_info(projects)")
            cols = {r[1] for r in cur.fetchall()}
        except sqlite3.Error:
            cols = set()
        if not cols:
            raise ValueError("projects table has no columns")

        insert_cols = [c for c in payload.keys() if c in cols]
        if not insert_cols:
            raise ValueError("projects table has no insertable columns")

        placeholders = ", ".join(["?"] * len(insert_cols))
        values = [payload[c] for c in insert_cols]
        self.con.execute(
            f"INSERT INTO projects ({', '.join(insert_cols)}) VALUES ({placeholders})",
            values,
        )
        self.con.commit()
        return project_id

    def update_project(self, project_id: str, data: dict[str, Any]) -> None:
        if not project_id:
            raise ValueError("project_id is required")
        if not _table_exists(self.con, "projects"):
            raise ValueError("projects table missing")

        cols = _table_columns(self.con, "projects")
        if not cols:
            raise ValueError("projects table has no columns")

        name = (data.get("name") or "").strip()
        work_center = (data.get("work_center") or "").strip()
        if not name:
            raise ValueError("name is required")
        if not work_center:
            raise ValueError("work_center is required")

        data_keys = set(data.keys())
        payload: dict[str, Any] = {}

        if "name" in data_keys and "name" in cols:
            payload["name"] = name
        if "work_center" in data_keys and "work_center" in cols:
            payload["work_center"] = work_center
        if "project_code" in data_keys and "project_code" in cols:
            payload["project_code"] = (data.get("project_code") or "").strip() or None
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
                       COALESCE(SUM(CASE WHEN a.status IN ('done','cancelled') THEN 1 ELSE 0 END), 0) AS actions_closed,
                       COALESCE(SUM(CASE WHEN a.status NOT IN ('done','cancelled') THEN 1 ELSE 0 END), 0) AS actions_open
                FROM projects p
                LEFT JOIN actions a ON a.project_id = p.id AND a.is_draft = 0
                GROUP BY p.id
                ORDER BY p.name
                """
            )
            return [dict(r) for r in cur.fetchall()]

        cur = self.con.execute("SELECT * FROM projects ORDER BY name")
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

        cur = self.con.execute("SELECT work_center, related_work_center FROM projects")
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
            "owner_champion_id": (data.get("owner_champion_id") or "").strip() or None,
            "status": status,
            "created_at": created_date.isoformat(),
            "closed_at": closed_at,
            "work_center": work_center,
            "project_code": (data.get("project_code") or "").strip() or None,
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

        cols: set[str] = set()
        try:
            cur = self.con.execute("PRAGMA table_info(champions)")
            cols = {r[1] for r in cur.fetchall()}
        except sqlite3.Error:
            cols = set()

        select_cols = ["id", "first_name", "last_name", "email", "active"]
        if "hire_date" in cols:
            select_cols.append("hire_date")
        if "position" in cols:
            select_cols.append("position")
        if "team" in cols:
            select_cols.append("team")

        cur = self.con.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM champions
            ORDER BY last_name, first_name
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
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

    def get_assigned_projects(self, champion_id: str) -> list[str]:
        if not _table_exists(self.con, "champion_projects"):
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
            raise ValueError("champions table missing")

        cols = _table_columns(self.con, "champions")
        if not cols:
            raise ValueError("champions table has no columns")

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
            raise ValueError("champions table has no insertable columns")

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
            raise
        return champion_id


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
            raise sqlite3.OperationalError("scrap_daily table missing; migration required")
        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                r.get("id") or str(uuid4()),
                str(r["metric_date"]),
                str(r["work_center"]),
                int(r.get("scrap_qty") or 0),
                r.get("scrap_cost_amount"),
                (r.get("scrap_cost_currency") or "PLN"),
                r.get("created_at") or now,
            )
            for r in rows
        ]
        self.con.executemany(
            """
            INSERT INTO scrap_daily (
                id, metric_date, work_center,
                scrap_qty, scrap_cost_amount,
                scrap_cost_currency, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_date, work_center, scrap_cost_currency) DO UPDATE SET
                scrap_qty = excluded.scrap_qty,
                scrap_cost_amount = excluded.scrap_cost_amount,
                scrap_cost_currency = excluded.scrap_cost_currency
            """,
            payload,
        )
        self.con.commit()

    def upsert_production_kpi_daily(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if not _table_exists(self.con, "production_kpi_daily"):
            raise sqlite3.OperationalError("production_kpi_daily table missing; migration required")
        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                r.get("id") or str(uuid4()),
                str(r["metric_date"]),
                str(r["work_center"]),
                r.get("worktime_min"),
                r.get("performance_pct"),
                r.get("oee_pct"),
                r.get("availability_pct"),
                r.get("quality_pct"),
                r.get("source_file"),
                r.get("imported_at") or now,
                r.get("created_at") or r.get("imported_at") or now,
            )
            for r in rows
        ]
        self.con.executemany(
            """
            INSERT INTO production_kpi_daily (
                id, metric_date, work_center,
                worktime_min, performance_pct, oee_pct,
                availability_pct, quality_pct,
                source_file, imported_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric_date, work_center) DO UPDATE SET
                worktime_min = excluded.worktime_min,
                performance_pct = excluded.performance_pct,
                oee_pct = excluded.oee_pct,
                availability_pct = excluded.availability_pct,
                quality_pct = excluded.quality_pct,
                source_file = excluded.source_file,
                imported_at = excluded.imported_at,
                created_at = excluded.created_at
            """,
            payload,
        )
        self.con.commit()

    def list_scrap_daily(
        self,
        work_centers: str | list[str] | None,
        date_from: date | str | None,
        date_to: date | str | None,
        currency: str | None = "PLN",
    ) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "scrap_daily"):
            return []
        query = """
            SELECT metric_date,
                   work_center,
                   scrap_qty,
                   scrap_cost_amount,
                   scrap_cost_currency
            FROM scrap_daily
        """
        filters: list[str] = []
        params: list[Any] = []

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

        if currency:
            filters.append("scrap_cost_currency = ?")
            params.append(str(currency))

        if filters:
            query += " WHERE " + " AND ".join(filters)

        query += " ORDER BY metric_date ASC, work_center ASC"
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def list_kpi_daily(
        self,
        work_centers: str | list[str] | None,
        date_from: date | str | None,
        date_to: date | str | None,
    ) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "production_kpi_daily"):
            return []
        query = """
            SELECT metric_date,
                   work_center,
                   worktime_min,
                   performance_pct,
                   oee_pct,
                   availability_pct,
                   quality_pct
            FROM production_kpi_daily
        """
        filters: list[str] = []
        params: list[Any] = []

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
        cur = self.con.execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def list_distinct_work_centers(self) -> dict[str, list[str]]:
        scrap_work_centers: list[str] = []
        kpi_work_centers: list[str] = []

        if _table_exists(self.con, "scrap_daily"):
            cur = self.con.execute(
                """
                SELECT DISTINCT work_center
                FROM scrap_daily
                ORDER BY work_center
                """
            )
            scrap_work_centers = [row["work_center"] for row in cur.fetchall()]

        if _table_exists(self.con, "production_kpi_daily"):
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
        cur = self.con.execute(
            """
            SELECT work_center FROM scrap_daily
            UNION
            SELECT work_center FROM production_kpi_daily
            ORDER BY work_center ASC
            """
        )
        return [row["work_center"] for row in cur.fetchall()]

    def list_production_work_centers_with_stats(self) -> list[dict[str, Any]]:
        from action_tracking.services.effectiveness import normalize_wc  # type: ignore

        stats: dict[str, dict[str, Any]] = {}

        if _table_exists(self.con, "scrap_daily"):
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

    def upsert_from_production(
        self,
        work_centers_stats: list[dict[str, Any]],
        existing_project_wc_norms: set[str],
    ) -> None:
        if not _table_exists(self.con, "wc_inbox"):
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

                self.con.execute(
                    """
                    INSERT INTO wc_inbox (
                        id, wc_raw, wc_norm, sources,
                        first_seen_date, last_seen_date,
                        status, linked_project_id,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(wc_norm) DO UPDATE SET
                        wc_raw = excluded.wc_raw,
                        sources = excluded.sources,
                        first_seen_date = COALESCE(
                            MIN(wc_inbox.first_seen_date, excluded.first_seen_date),
                            excluded.first_seen_date,
                            wc_inbox.first_seen_date
                        ),
                        last_seen_date = COALESCE(
                            MAX(wc_inbox.last_seen_date, excluded.last_seen_date),
                            excluded.last_seen_date,
                            wc_inbox.last_seen_date
                        ),
                        updated_at = excluded.updated_at
                    """,
                    (
                        str(uuid4()),
                        wc_raw_value,
                        wc_norm,
                        json.dumps(sources, ensure_ascii=False),
                        row.get("first_seen_date"),
                        row.get("last_seen_date"),
                        "open",
                        None,
                        now,
                        now,
                    ),
                )
            self.con.execute("COMMIT")
        except Exception:
            _rollback_safely(self.con)
            raise

    def list_open(self, limit: int = 200) -> list[dict[str, Any]]:
        if not _table_exists(self.con, "wc_inbox"):
            return []
        cur = self.con.execute(
            """
            SELECT *
            FROM wc_inbox
            WHERE status = 'open'
            ORDER BY last_seen_date DESC, wc_raw ASC
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
