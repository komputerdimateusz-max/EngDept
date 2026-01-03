from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import GlobalSettingsRepository, SettingsRepository
from action_tracking.services.normalize import normalize_key


def render(con: sqlite3.Connection) -> None:
    st.header("Ustawienia Globalne")
    st.caption("Zarządzaj globalnymi słownikami dla aplikacji.")

    repo = SettingsRepository(con)
    rules_repo = GlobalSettingsRepository(con)

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

    st.divider()
    st.subheader("Reguły kategorii akcji (effectiveness + savings)")
    rules = rules_repo.get_category_rules(only_active=False)
    if not rules:
        st.info("Brak zdefiniowanych reguł kategorii.")
    else:
        rules_df = pd.DataFrame(rules)
        rules_df = rules_df[
            [
                "category_label",
                "effectiveness_model",
                "savings_model",
                "requires_scope_link",
                "is_active",
            ]
        ].rename(
            columns={
                "category_label": "Kategoria",
                "effectiveness_model": "Model skuteczności",
                "savings_model": "Model oszczędności",
                "requires_scope_link": "Wymaga WC",
                "is_active": "Aktywna",
            }
        )
        rules_df["Klucz (normalized)"] = rules_df["Kategoria"].map(normalize_key)
        st.dataframe(rules_df, use_container_width=True, hide_index=True)

    st.subheader("Edytor reguły kategorii")
    category_names = sorted(
        {row["name"] for row in categories} | {row["category_label"] for row in rules}
    )
    if not category_names:
        st.caption("Brak kategorii do konfiguracji.")
    else:
        selected_category = st.selectbox("Kategoria", category_names)
        selected_rule = rules_repo.resolve_category_rule(selected_category) or {
            "category_label": selected_category,
            "effectiveness_model": "NONE",
            "savings_model": "NONE",
            "requires_scope_link": False,
            "is_active": True,
            "description": None,
        }
        effect_options = ["SCRAP", "OEE", "PERFORMANCE", "NONE"]
        savings_options = ["AUTO_SCRAP_COST", "MANUAL_REQUIRED", "AUTO_TIME_TO_PLN", "NONE"]
        st.caption(
            f"Zapisana etykieta: '{selected_category}' | normalized: '{normalize_key(selected_category)}'"
        )
        with st.form("edit_category_rule"):
            effect_model = st.selectbox(
                "Model skuteczności",
                effect_options,
                index=effect_options.index(selected_rule.get("effectiveness_model") or "NONE"),
            )
            savings_model = st.selectbox(
                "Model oszczędności",
                savings_options,
                index=savings_options.index(selected_rule.get("savings_model") or "NONE"),
            )
            requires_scope_link = st.checkbox(
                "Wymaga powiązania z projektem (WC)",
                value=bool(selected_rule.get("requires_scope_link")),
            )
            is_active = st.checkbox(
                "Aktywna",
                value=bool(selected_rule.get("is_active")),
            )
            description = st.text_area(
                "Opis metodologii",
                value=selected_rule.get("description") or "",
                max_chars=500,
            )
            submitted_rule = st.form_submit_button("Zapisz regułę")
            if submitted_rule:
                try:
                    rules_repo.upsert_category_rule(
                        selected_category,
                        {
                            "effect_model": effect_model,
                            "savings_model": savings_model,
                            "requires_scope_link": requires_scope_link,
                            "is_active": is_active,
                            "description": description,
                        },
                    )
                    st.success("Zapisano regułę kategorii.")
                    st.rerun()
                except (ValueError, sqlite3.Error) as exc:
                    st.error(str(exc))

    with st.expander("Metodologia (dla użytkowników)", expanded=False):
        if not rules:
            st.caption("Brak opisów metodologii.")
        else:
            for rule in rules:
                st.markdown(
                    f"**{rule['category_label']}**: {rule.get('description') or 'Brak opisu.'}"
                )
        st.markdown(
            """
- Okno bazowe i po zmianie obejmuje ostatnie 14 dni przed/po zamknięciu akcji.
- Brak powiązania z projektem lub work center może uniemożliwić automatyczną ocenę.
- Wyceny oszczędności automatycznych w v1 są w PLN, a ręczne wprowadzamy jako PLN/EUR.
"""
        )
