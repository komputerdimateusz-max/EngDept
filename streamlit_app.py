from __future__ import annotations

import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if SRC.exists() and str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import streamlit as st

import action_tracking
from action_tracking.data.db import connect, init_db
from action_tracking.app.pages import (
    kpi,
    actions,
    champions,
    champions_ranking,
    analizy,
    projects,
    settings,
    production_import,
    production_explorer,
)

st.set_page_config(page_title="engdept", layout="wide")

# --- DB init (globalnie, raz na start aplikacji) ---
DATA_DIR = Path(os.getenv("ACTION_TRACKING_DATA_DIR", "./data"))
DB_PATH = Path(os.getenv("ACTION_TRACKING_DB_PATH", DATA_DIR / "app.db"))

con = connect(DB_PATH)
init_db(con)

# --- Sidebar navigation ---
st.sidebar.title("Magna PE Database")

build_number = (
    os.getenv("APP_BUILD")
    or os.getenv("BUILD_NUMBER")
    or action_tracking.__version__
)
st.sidebar.markdown(
    f"""
    <style>
    [data-testid="stSidebar"] .build-info {{
        position: fixed;
        bottom: 0.5rem;
        left: 1rem;
        color: #6c757d;
        font-size: 0.75rem;
    }}
    </style>
    <div class="build-info">Build: {build_number}</div>
    """,
    unsafe_allow_html=True,
)

PAGES = {
    "Production Explorer": lambda: production_explorer.render(con),
    "KPI": lambda: kpi.render(con),
    "Akcje": lambda: actions.render(con),
    "Champions ranking": lambda: champions_ranking.render(con),
    "Champions": lambda: champions.render(con),
    "Analizy": lambda: analizy.render(con),
    "Projekty": lambda: projects.render(con),
    "Ustawienia Globalne": lambda: settings.render(con),
    "Import danych produkcyjnych": lambda: production_import.render(con),
}

selected = st.sidebar.radio("Strony", list(PAGES.keys()), index=0)

# --- Render selected page ---
PAGES[selected]()
