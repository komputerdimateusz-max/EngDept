from __future__ import annotations

import sqlite3
import streamlit as st

from action_tracking.data.repositories import ActionRepository
from action_tracking.domain.constants import ACTION_CATEGORIES


def render(con: sqlite3.Connection) -> None:
    st.header("Akcje")

    repo = ActionRepository(con)

    statuses = ["(wszystkie)", "open", "in_progress", "blocked", "done", "cancelled"]
    selected = st.selectbox("Filtr statusu", statuses, index=0)
    categories = ["(wszystkie)"] + list(ACTION_CATEGORIES)
    selected_category = st.selectbox("Filtr kategorii", categories, index=0)

    status_filter = None if selected == "(wszystkie)" else selected
    category_filter = None if selected_category == "(wszystkie)" else selected_category
    rows = repo.list_actions(status=status_filter)
    if category_filter:
        rows = [
            row
            for row in rows
            if row.get("category") == category_filter
            or row.get("impact_type") == category_filter
        ]

    st.caption(f"Liczba akcji: {len(rows)}")
    st.dataframe(rows, use_container_width=True)
