from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import SettingsRepository


def render(con: sqlite3.Connection) -> None:
    st.header("Ustawienia Globalne")
    st.caption("Zarządzaj globalnymi słownikami dla aplikacji.")

    repo = SettingsRepository(con)

    st.subheader("Action Categories")
    categories = repo.list_action_categories(active_only=False)
    if not categories:
        st.info("Brak zdefiniowanych kategorii akcji.")
    else:
        df = pd.DataFrame(categories)
        df = df[["id", "name", "is_active", "sort_order"]].sort_values("sort_order")
        edited = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "id": st.column_config.TextColumn("ID", disabled=True, width="small"),
                "name": st.column_config.TextColumn("Nazwa", required=True),
                "is_active": st.column_config.CheckboxColumn("Aktywna"),
                "sort_order": st.column_config.NumberColumn("Kolejność", min_value=1, step=1),
            },
            num_rows="fixed",
            key="action_categories_editor",
        )

        if st.button("Zapisz zmiany kategorii"):
            original_rows = {row["id"]: row for row in categories}
            changes = 0
            for _, row in edited.iterrows():
                category_id = row["id"]
                name = str(row["name"]).strip()
                if not name:
                    st.error("Nazwa kategorii nie może być pusta.")
                    return
                original = original_rows.get(category_id, {})
                if (
                    name != original.get("name")
                    or bool(row["is_active"]) != bool(original.get("is_active"))
                    or int(row["sort_order"]) != int(original.get("sort_order"))
                ):
                    repo.update_action_category(
                        category_id=category_id,
                        name=name,
                        is_active=bool(row["is_active"]),
                        sort_order=int(row["sort_order"]),
                    )
                    changes += 1
            if changes:
                st.success("Zapisano zmiany kategorii.")
                st.rerun()
            else:
                st.info("Brak zmian do zapisania.")

    st.divider()
    st.subheader("Dodaj kategorię")
    with st.form("add_action_category"):
        name = st.text_input("Nazwa kategorii")
        sort_order = st.number_input("Kolejność", min_value=1, step=1, value=1)
        submitted = st.form_submit_button("Dodaj")
        if submitted:
            if not name.strip():
                st.error("Nazwa kategorii jest wymagana.")
            else:
                repo.create_action_category(name.strip(), int(sort_order))
                st.success("Dodano kategorię.")
                st.rerun()

    st.divider()
    st.subheader("Dezaktywuj kategorię")
    active_categories = repo.list_action_categories(active_only=True)
    if not active_categories:
        st.caption("Brak aktywnych kategorii do dezaktywacji.")
    else:
        options = {row["name"]: row["id"] for row in active_categories}
        selected_name = st.selectbox("Wybierz kategorię", list(options.keys()))
        if st.button("Dezaktywuj"):
            repo.delete_action_category(options[selected_name])
            st.success("Kategoria została dezaktywowana.")
            st.rerun()
