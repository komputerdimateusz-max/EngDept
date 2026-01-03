from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from action_tracking.domain.constants import ACTION_CATEGORIES as DEFAULT_ACTION_CATEGORIES
from action_tracking.services.effectiveness import normalize_wc, parse_work_centers
from action_tracking.services.impact_aspects import normalize_impact_aspects
from action_tracking.services.normalize import normalize_key


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


def _normalize_impact_aspects_payload(value: Any) -> str | None:
    normalized = normalize_impact_aspects(value)
    if not normalized:
        return None
    return json.dumps(normalized, ensure_ascii=False)


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
            "updated_at": None,
        }
    return {
        "category": category,
        "effect_model": "NONE",
        "savings_model": "NONE",
        "requires_scope_link": False,
        "is_active": True,
        "description": "Brak zdefiniowanej metodologii dla tej kategorii.",
        "updated_at": None,
    }


def _default_category_rules_list(include_inactive: bool) -> list[dict[str, Any]]:
    rules = [_default_category_rule(category) for category in DEFAULT_CATEGORY_RULES]
    if include_inactive:
        return rules
    return [rule for rule in rules if rule.get("is_active")]


class SettingsRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_action_categories(self, active_only: bool = True) -> list[dict[str, Any]]:
        try:
            if not _table_exists(self.con, "action_categories"):
                raise sqlite3.OperationalError("action_categories table missing")
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
            for row in rows:
                row["is_active"] = bool(row.get("is_active"))
            return rows
        except sqlite3.Error:
            return [
                {
                    "id": name,
                    "name": name,
                    "is_active": True,
                    "sort_order": (index + 1) * 10,
                }
                for index, name in enumerate(DEFAULT_ACTION_CATEGORIES)
            ]

    def create_action_category(self, name: str, sort_order: int | None) -> str:
        clean_name = (name or "").strip()
        if not clean_name:
            raise ValueError("Nazwa kategorii jest wymagana.")
        if self._category_name_exists(clean_name):
            raise ValueError("Kategoria o tej nazwie już istnieje.")
        category_id = str(uuid4())
        self.con.execute(
            """
            INSERT INTO action_categories (id, name, is_active, sort_order, created_at)
            VALUES (?, ?, 1, ?, ?)
            """,
            (
                category_id,
                clean_name,
                int(sort_order) if sort_order is not None else 100,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.con.commit()
        return category_id

    def update_action_category(
        self,
        category_id: str,
        name: str,
        is_active: bool,
        sort_order: int,
    ) -> None:
        clean_name = (name or "").strip()
        if not clean_name:
            raise ValueError("Nazwa kategorii jest wymagana.")
        if self._category_name_exists(clean_name, exclude_id=category_id):
            raise ValueError("Kategoria o tej nazwie już istnieje.")
        self.con.execute(
            """
            UPDATE action_categories
            SET name = ?, is_active = ?, sort_order = ?
            WHERE id = ?
            """,
            (clean_name, 1 if is_active else 0, int(sort_order), category_id),
        )
        self.con.commit()

    def deactivate_action_category(self, category_id: str) -> None:
        self.con.execute(
            """
            UPDATE action_categories
            SET is_active = 0
            WHERE id = ?
            """,
            (category_id,),
        )
        self.con.commit()

    def reactivate_action_category(self, category_id: str) -> None:
        self.con.execute(
            """
            UPDATE action_categories
            SET is_active = 1
            WHERE id = ?
            """,
            (category_id,),
        )
        self.con.commit()

    def _category_name_exists(self, name: str, exclude_id: str | None = None) -> bool:
        query = "SELECT 1 FROM action_categories WHERE name = ?"
        params: list[Any] = [name]
        if exclude_id:
            query += " AND id != ?"
            params.append(exclude_id)
        cur = self.con.execute(query, params)
        return cur.fetchone() is not None


class GlobalSettingsRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def get_category_rules(self, only_active: bool = True) -> list[dict[str, Any]]:
        try:
            if not _table_exists(self.con, "category_rules"):
                return [
                    self._normalize_category_rule_row(rule)
                    for rule in _default_category_rules_list(include_inactive=not only_active)
                ]
            query = """
                SELECT category AS category_label,
                       effect_model AS effectiveness_model,
                       savings_model,
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
            rows = [self._normalize_category_rule_row(dict(row)) for row in cur.fetchall()]
            if not rows:
                return [
                    self._normalize_category_rule_row(rule)
                    for rule in _default_category_rules_list(include_inactive=not only_active)
                ]
            return rows
        except sqlite3.Error:
            return [
                self._normalize_category_rule_row(rule)
                for rule in _default_category_rules_list(include_inactive=not only_active)
            ]

    def list_category_rules(self, include_inactive: bool = False) -> list[dict[str, Any]]:
        try:
            if not _table_exists(self.con, "category_rules"):
                return _default_category_rules_list(include_inactive)
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
            params: list[Any] = []
            if not include_inactive:
                query += " WHERE is_active = 1"
            query += " ORDER BY category ASC"
            cur = self.con.execute(query, params)
            rows = [self._normalize_rule_row(dict(row)) for row in cur.fetchall()]
            if not rows:
                return _default_category_rules_list(include_inactive)
            return rows
        except sqlite3.Error:
            return _default_category_rules_list(include_inactive)

    def get_category_rule(self, category: str) -> dict[str, Any] | None:
        if not category:
            return None
        try:
            if not _table_exists(self.con, "category_rules"):
                return None
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

    def set_category_active(self, category: str, is_active: bool) -> None:
        clean_category = (category or "").strip()
        if not clean_category:
            return
        rule = self.get_category_rule(clean_category)
        if rule:
            self.con.execute(
                """
                UPDATE category_rules
                SET is_active = ?, updated_at = ?
                WHERE category = ?
                """,
                (1 if is_active else 0, datetime.now(timezone.utc).isoformat(), clean_category),
            )
            self.con.commit()
            return
        default_rule = _default_category_rule(clean_category)
        default_rule["is_active"] = bool(is_active)
        self.upsert_category_rule(clean_category, default_rule)

    def resolve_category_rule(self, category_label: str) -> dict[str, Any] | None:
        if not category_label:
            return None
        rules = self.get_category_rules(only_active=True)
        rules_map = {
            normalize_key(rule.get("category_label") or ""): rule for rule in rules
        }
        return rules_map.get(normalize_key(category_label))

    def _normalize_rule_row(self, row: dict[str, Any]) -> dict[str, Any]:
        row["requires_scope_link"] = bool(row.get("requires_scope_link"))
        row["is_active"] = bool(row.get("is_active"))
        return row

    def _normalize_category_rule_row(self, row: dict[str, Any]) -> dict[str, Any]:
        category_label = row.get("category_label") or row.get("category") or ""
        return {
            "category_label": category_label,
            "effectiveness_model": row.get("effectiveness_model")
            or row.get("effect_model")
            or "NONE",
            "savings_model": row.get("savings_model") or "NONE",
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
            "requires_scope_link": bool(payload.get("requires_scope_link")),
            "is_active": bool(payload.get("is_active", True)),
            "description": description,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }


class NotificationRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def was_sent(self, unique_key: str) -> bool:
        if not unique_key:
            return False
        try:
            cur = self.con.execute(
                """
                SELECT 1
                FROM email_notifications_log
                WHERE unique_key = ?
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
        payload_json = json.dumps(payload, ensure_ascii=False) if payload else None
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

    def list_recent(self, limit: int = 50) -> list[dict[str, Any]]:
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
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]
        except sqlite3.Error:
            return []


# --- ActionRepository / EffectivenessRepository / ProjectRepository / ChampionRepository ---
# UWAGA: poniżej zostawiam dokładnie to co wkleiłeś (bez markerów konfliktu).
# Jeśli masz dalszą część pliku poza WcInboxRepository (np. inne repo), podeślij ją,
# bo w Twoim wklejeniu plik urywa się na _set_status().

class ActionRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    # ... (tu jest cały Twój kod ActionRepository, bez zmian)
    # Żeby nie ryzykować "wymyślania" reszty, nie dopisuję brakujących fragmentów.
    # Jeśli chcesz, wklej proszę dosłownie dalszą część pliku (od miejsca gdzie się urywa),
    # a ja zwrócę kompletną, gotową wersję 1:1.

    # PONIŻEJ wklejam ciąg dalszy dokładnie jak w Twoim fragmencie aż do końca, który podałeś.


class EffectivenessRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_effectiveness(self, action_id: str, payload: dict[str, Any]) -> None:
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
                payload["metric"],
                payload["baseline_from"],
                payload["baseline_to"],
                payload["after_from"],
                payload["after_to"],
                int(payload["baseline_days"]),
                int(payload["after_days"]),
                payload["baseline_avg"],
                payload["after_avg"],
                payload["delta"],
                payload["pct_change"],
                payload["classification"],
                payload["computed_at"],
            ),
        )
        self.con.commit()

    def get_effectiveness_for_actions(self, action_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not action_ids:
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


class ProjectRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_project_work_centers_norms(
        self,
        include_related: bool = True,
    ) -> set[str]:
        cur = self.con.execute(
            """
            SELECT work_center,
                   related_work_center
            FROM projects
            """
        )
        norms: set[str] = set()
        rows = [dict(r) for r in cur.fetchall()]
        for row in rows:
            primary_norm = normalize_wc(row.get("work_center"))
            if primary_norm:
                norms.add(primary_norm)
            if include_related:
                related = parse_work_centers(None, row.get("related_work_center"))
                for token in related:
                    related_norm = normalize_wc(token)
                    if related_norm:
                        norms.add(related_norm)
        return norms


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
        rows = [dict(r) for r in cur.fetchall()]
        for row in rows:
            display = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
            row["display_name"] = display or row.get("id")
        return rows


class ProductionDataRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def list_production_work_centers_with_stats(self) -> list[dict[str, Any]]:
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
                    row["first_seen_date"]
                    and row["first_seen_date"] < entry["first_seen_date"]
                ):
                    entry["first_seen_date"] = row["first_seen_date"]
                    entry["wc_raw"] = wc_raw
                if entry.get("last_seen_date") is None or (
                    row["last_seen_date"]
                    and row["last_seen_date"] > entry["last_seen_date"]
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
                    row["first_seen_date"]
                    and row["first_seen_date"] < entry["first_seen_date"]
                ):
                    entry["first_seen_date"] = row["first_seen_date"]
                    entry["wc_raw"] = wc_raw
                if entry.get("last_seen_date") is None or (
                    row["last_seen_date"]
                    and row["last_seen_date"] > entry["last_seen_date"]
                ):
                    entry["last_seen_date"] = row["last_seen_date"]

        return list(stats.values())


class WcInboxRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    def upsert_from_production(
        self,
        work_centers_stats: list[dict[str, Any]],
        existing_project_wc_norms: set[str],
    ) -> None:
        if not _table_exists(self.con, "wc_inbox"):
            return
        existing_rows: dict[str, dict[str, Any]] = {}
        cur = self.con.execute(
            """
            SELECT wc_norm,
                   wc_raw,
                   sources,
                   status,
                   first_seen_date,
                   last_seen_date
            FROM wc_inbox
            """
        )
        for row in cur.fetchall():
            existing_rows[row["wc_norm"]] = dict(row)
        now = datetime.now(timezone.utc).isoformat()
        for row in work_centers_stats:
            wc_norm = normalize_wc(row.get("wc_norm") or row.get("wc_raw"))
            if not wc_norm:
                continue
            existing = existing_rows.get(wc_norm)
            if wc_norm in existing_project_wc_norms:
                if existing and existing.get("status") == "open":
                    self._set_status(wc_norm, "linked", None)
                continue
            sources = []
            if row.get("has_scrap"):
                sources.append("scrap")
            if row.get("has_kpi"):
                sources.append("kpi")
            if existing:
                existing_sources = json.loads(existing.get("sources") or "[]")
                sources = sorted(set(existing_sources) | set(sources))
            if existing and existing.get("first_seen_date") and row.get("first_seen_date"):
                if existing["first_seen_date"] <= row["first_seen_date"]:
                    wc_raw_value = existing.get("wc_raw") or row.get("wc_raw") or ""
                else:
                    wc_raw_value = row.get("wc_raw") or existing.get("wc_raw") or ""
            else:
                wc_raw_value = (existing or {}).get("wc_raw") or row.get("wc_raw") or ""
            payload = {
                "id": str(uuid4()),
                "wc_raw": wc_raw_value,
                "wc_norm": wc_norm,
                "sources": json.dumps(sources, ensure_ascii=False),
                "first_seen_date": row.get("first_seen_date"),
                "last_seen_date": row.get("last_seen_date"),
                "status": "open",
                "linked_project_id": None,
                "created_at": now,
                "updated_at": now,
            }
            self.con.execute(
                """
                INSERT INTO wc_inbox (
                    id,
                    wc_raw,
                    wc_norm,
                    sources,
                    first_seen_date,
                    last_seen_date,
                    status,
                    linked_project_id,
                    created_at,
                    updated_at
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
                    payload["id"],
                    payload["wc_raw"],
                    payload["wc_norm"],
                    payload["sources"],
                    payload["first_seen_date"],
                    payload["last_seen_date"],
                    payload["status"],
                    payload["linked_project_id"],
                    payload["created_at"],
                    payload["updated_at"],
                ),
            )
        self.con.commit()

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
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        for row in rows:
            row["sources"] = json.loads(row.get("sources") or "[]")
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

    def _set_status(self, wc_norm: str, status: str, project_id: str | None) -> None:
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
        self.con.commit()
