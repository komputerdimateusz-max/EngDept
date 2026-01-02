from __future__ import annotations

import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if SRC.exists() and str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import streamlit as st

from action_tracking.data.db import connect, init_db
from action_tracking.app.pages import explorer, kpi, actions, champions, projects, settings

st.set_page_config(page_title="engdept", layout="wide")

# --- DB init (globalnie, raz na start aplikacji) ---
DATA_DIR = Path(os.getenv("ACTION_TRACKING_DATA_DIR", "./data"))
DB_PATH = Path(os.getenv("ACTION_TRACKING_DB_PATH", DATA_DIR / "app.db"))

con = connect(DB_PATH)
init_db(con)

# --- Sidebar navigation ---
st.sidebar.title("engdept")

PAGES = {
    "Explorer": explorer.render,
    "KPI": lambda: kpi.render(con),
    "Akcje": lambda: actions.render(con),
    "Champions": lambda: champions.render(con),
    "Projekty": lambda: projects.render(con),
    "Ustawienia Globalne": settings.render,
}

selected = st.sidebar.radio("Strony", list(PAGES.keys()), index=2)

# --- Render selected page ---
PAGES[selected]()
