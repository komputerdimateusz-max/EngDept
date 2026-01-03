# === HOTFIX: ProductionDataRepository missing ===
# Wklej TEN BLOK do: src/action_tracking/data/repositories.py
# Najlepiej NA SAM KONIEC pliku (albo w sekcji z innymi *Repository).
# To naprawi: ImportError: cannot import name 'ProductionDataRepository' ...

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
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


class ProductionDataRepository:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    # ---------------------------
    # UPSERTS (used by import UI)
    # ---------------------------

    def upsert_scrap_daily(self, rows: list[dict[str, Any]]) -> None:
        """
        Rows schema expected (per day, per work_center, per currency):
        - metric_date (YYYY-MM-DD)
        - work_center (str)
        - scrap_qty (int)
        - scrap_cost_amount (float|None)
        - scrap_cost_currency (str, default PLN)
        """
        if not rows:
            return
        if not _table_exists(self.con, "scrap_daily"):
            raise sqlite3.OperationalError("scrap_daily table missing; database migration required")

        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                row.get("id") or str(uuid4()),
                row["metric_date"],
                row["work_center"],
                int(row["scrap_qty"]),
                row.get("scrap_cost_amount"),
                (row.get("scrap_cost_currency") or "PLN"),
                row.get("created_at") or now,
            )
            for row in rows
        ]

        self.con.executemany(
            """
            INSERT INTO scrap_daily (
                id,
                metric_date,
                work_center,
                scrap_qty,
                scrap_cost_amount,
                scrap_cost_currency,
                created_at
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
        """
        Rows schema expected (per day, per work_center):
        - metric_date, work_center
        - worktime_min (float|None)
        - performance_pct (float|None)
        - oee_pct (float|None)
        - availability_pct (float|None)
        - quality_pct (float|None)
        - source_file (str|None)
        - imported_at (iso|None)
        """
        if not rows:
            return
        if not _table_exists(self.con, "production_kpi_daily"):
            raise sqlite3.OperationalError(
                "production_kpi_daily table missing; database migration required"
            )

        now = datetime.now(timezone.utc).isoformat()
        payload = [
            (
                row.get("id") or str(uuid4()),
                row["metric_date"],
                row["work_center"],
                row.get("worktime_min"),
                row.get("performance_pct"),
                row.get("oee_pct"),
                row.get("availability_pct"),
                row.get("quality_pct"),
                row.get("source_file"),
                row.get("imported_at") or now,
                row.get("created_at") or row.get("imported_at") or now,
            )
            for row in rows
        ]

        self.con.executemany(
            """
            INSERT INTO production_kpi_daily (
                id,
                metric_date,
                work_center,
                worktime_min,
                performance_pct,
                oee_pct,
                availability_pct,
                quality_pct,
                source_file,
                imported_at,
                created_at
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

    # ---------------------------
    # QUERIES (used by Explorer/Projects)
    # ---------------------------

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
                if work_centers.strip():
                    filters.append("work_center = ?")
                    params.append(work_centers)
            else:
                if not work_centers:
                    return []
                placeholders = ", ".join(["?"] * len(work_centers))
                filters.append(f"work_center IN ({placeholders})")
                params.extend(work_centers)

        if date_from:
            filters.append("metric_date >= ?")
            params.append(self._normalize_date_filter(date_from))
        if date_to:
            filters.append("metric_date <= ?")
            params.append(self._normalize_date_filter(date_to))

        if currency:
            filters.append("scrap_cost_currency = ?")
            params.append(currency)

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
                if work_centers.strip():
                    filters.append("work_center = ?")
                    params.append(work_centers)
            else:
                if not work_centers:
                    return []
                placeholders = ", ".join(["?"] * len(work_centers))
                filters.append(f"work_center IN ({placeholders})")
                params.extend(work_centers)

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
        if not _table_exists(self.con, "scrap_daily") and not _table_exists(
            self.con, "production_kpi_daily"
        ):
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
        """
        Used by WC inbox feature: aggregates per normalized WC.
        """
        # import lokalny żeby nie robić cykli importów
        from action_tracking.services.effectiveness import normalize_wc

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

        # uzupełnij first/last jako min/max z obu źródeł (opcjonalnie)
        for wc_norm, entry in stats.items():
            entry["wc_norm"] = wc_norm
        return list(stats.values())

    @staticmethod
    def _normalize_date_filter(value: date | str) -> str:
        return value.isoformat() if isinstance(value, date) else str(value)

