from __future__ import annotations

import sqlite3
import streamlit as st

from action_tracking.data.repositories import ActionRepository


def render(con: sqlite3.Connection) -> None:
    st.header("Akcje")

    repo = ActionRepository(con)

    statuses = ["(wszystkie)", "open", "in_progress", "blocked", "done", "cancelled"]
    selected = st.selectbox("Filtr statusu", statuses, index=0)

    status_filter = None if selected == "(wszystkie)" else selected
    rows = repo.list_actions(status=status_filter)

    st.caption(f"Liczba akcji: {len(rows)}")
    st.dataframe(rows, use_container_width=True)
