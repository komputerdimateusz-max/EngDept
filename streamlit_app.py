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
from action_tracking.app.pages import (
    explorer,
    kpi,
    actions,
    champions,
    champions_ranking,
    projects,
    settings,
    production_import,
)

st.set_page_config(page_title="engdept", layout="wide")

# --- DB init (globalnie, raz na start aplikacji) ---
DATA_DIR = Path(os.getenv("ACTION_TRACKING_DATA_DIR", "./data"))
DB_PATH = Path(os.getenv("ACTION_TRACKING_DB_PATH", DATA_DIR / "app.db"))

con = connect(DB_PATH)
init_db(con)

# --- Sidebar navigation ---
st.sidebar.title("engdept")

PAGES = {
    "Explorer (Produkcja)": lambda: explorer.render(con),
    "Import danych produkcyjnych": lambda: production_import.render(con),
    "KPI": lambda: kpi.render(con),
    "Akcje": lambda: actions.render(con),
    "Champions": lambda: champions.render(con),
    "Champion Ranking v2": lambda: champions_ranking.render(con),
    "Projekty": lambda: projects.render(con),
    "Ustawienia Globalne": lambda: settings.render(con),
}

selected = st.sidebar.radio("Strony", list(PAGES.keys()), index=2)

# --- Render selected page ---
PAGES[selected]()
