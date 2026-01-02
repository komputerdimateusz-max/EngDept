from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, sep=None, engine="python", decimal=",")
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]
    return df


def _upsert_df(con: sqlite3.Connection, table: str, df: pd.DataFrame, key: str = "id") -> None:
    if df.empty:
        return
    if key not in df.columns:
        raise ValueError(f"Seed for {table} requires column '{key}'")

    cols = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)

    update_cols = [c for c in cols if c != key]
    set_clause = ", ".join([f"{c}=excluded.{c}" for c in update_cols])

    sql = f"""
    INSERT INTO {table} ({col_list})
    VALUES ({placeholders})
    ON CONFLICT({key}) DO UPDATE SET {set_clause};
    """

    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df[cols].itertuples(index=False, name=None)
    ]
    con.executemany(sql, rows)
    con.commit()


def seed_from_csv(con: sqlite3.Connection, sample_dir: Path) -> None:
    champions = _read_csv(sample_dir / "champions.csv")
    projects = _read_csv(sample_dir / "projects.csv")
    actions = _read_csv(sample_dir / "actions.csv")

    # kolejność ważna (FK)
    _upsert_df(con, "champions", champions)
    _upsert_df(con, "projects", projects)
    _upsert_df(con, "actions", actions)
