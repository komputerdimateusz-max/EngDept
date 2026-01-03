from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import SettingsRepository


def render(con: sqlite3.Connection) -> None:
    st.header("Ustawienia Globalne")
    st.caption("Zarządzaj globalnymi słownikami dla aplikacji.")

    repo = SettingsRepository(con)

    st.subheader("Kategorie akcji")
    categories = repo.list_action_categories(active_only=False)
    if not categories:
        st.info("Brak zdefiniowanych kategorii akcji.")
    else:
        df = pd.DataFrame(categories)
        df = df[["name", "is_active", "sort_order"]].rename(
            columns={
                "name": "Nazwa",
                "is_active": "Aktywna",
                "sort_order": "Kolejność",
            }
        )
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Dodaj kategorię")
    with st.form("add_action_category"):
        name = st.text_input("Nazwa kategorii")
        sort_order = st.number_input("Kolejność", min_value=1, step=1, value=10)
        submitted = st.form_submit_button("Dodaj")
        if submitted:
            try:
                repo.create_action_category(name.strip(), int(sort_order))
                st.success("Dodano kategorię.")
                st.rerun()
            except (ValueError, sqlite3.Error) as exc:
                st.error(str(exc))

    st.divider()
    st.subheader("Edytuj kategorię")
    if not categories:
        st.caption("Brak kategorii do edycji.")
    else:
        categories_by_name = {row["name"]: row for row in categories}
        selected_name = st.selectbox("Wybierz kategorię", list(categories_by_name.keys()))
        selected = categories_by_name[selected_name]
        with st.form("edit_action_category"):
            edit_name = st.text_input("Nazwa", value=selected.get("name", ""))
            edit_active = st.checkbox("Aktywna", value=bool(selected.get("is_active")))
            edit_order = st.number_input(
                "Kolejność",
                min_value=1,
                step=1,
                value=int(selected.get("sort_order") or 100),
            )
            submitted_edit = st.form_submit_button("Zapisz")
            if submitted_edit:
                try:
                    repo.update_action_category(
                        category_id=selected["id"],
                        name=edit_name,
                        is_active=edit_active,
                        sort_order=int(edit_order),
                    )
                    st.success("Zapisano zmiany kategorii.")
                    st.rerun()
                except (ValueError, sqlite3.Error) as exc:
                    st.error(str(exc))

        st.divider()
        st.subheader("Dezaktywuj kategorię")
        confirm = st.checkbox("Potwierdzam dezaktywację kategorii")
        if st.button("Dezaktywuj", disabled=not confirm):
            repo.deactivate_action_category(selected["id"])
            st.success("Kategoria została dezaktywowana.")
            st.rerun()
