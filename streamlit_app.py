from __future__ import annotations

from pathlib import Path
import os

import streamlit as st

from action_tracking.data.db import connect, init_db, table_count
from action_tracking.data.seed import seed_from_csv

st.set_page_config(page_title="engdept", layout="wide")
st.title("engdept — Action Tracker (SQLite + CSV seed)")

DATA_DIR = Path(os.getenv("ACTION_TRACKING_DATA_DIR", "./data"))
DB_PATH = Path(os.getenv("ACTION_TRACKING_DB_PATH", DATA_DIR / "app.db"))
SAMPLE_DIR = Path(os.getenv("ACTION_TRACKING_SAMPLE_DIR", DATA_DIR / "sample"))

con = connect(DB_PATH)
init_db(con)

if table_count(con, "actions") == 0 and SAMPLE_DIR.exists():
    seed_from_csv(con, SAMPLE_DIR)
    st.success("Database seeded from CSV sample data.")
else:
    st.info("Database ready (seed not needed).")

st.write("DB path:", str(DB_PATH.resolve()))
