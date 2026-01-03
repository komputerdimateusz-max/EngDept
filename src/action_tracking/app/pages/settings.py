from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

import pandas as pd
import streamlit as st

from action_tracking.data.repositories import (
    ChampionRepository,
    GlobalSettingsRepository,
    NotificationRepository,
    SettingsRepository,
)
from action_tracking.integrations.email_sender import smtp_config_status
from action_tracking.services.normalize import normalize_key


def _truthy_env(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_json_loads(value: str | None) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def render(con: sqlite3.Connection) -> None:
    st.header("Ustawienia Globalne")
    st.caption("Zarządzaj globalnymi słownikami i regułami dla aplikacji.")

    repo = SettingsRepository(con)
    rules_repo = GlobalSettingsRepository(con)
    champion_repo = ChampionRepository(con)
    notification_repo = NotificationRepository(con)

    # =========================
    # EMAIL NOTIFICATIONS
    # =========================
    st.subheader("Powiadomienia email")

    notifications_enabled = os.getenv("ACTION_TRACKING_EMAIL_NOTIFICATIONS_ENABLED")
    enabled_flag = _truthy_env(notifications_enabled)
    st.checkbox(
        "Włącz powiadomienia email (env)",
        value=enabled_flag,
        disabled=True,
        help="Ustaw ACTION_TRACKING_EMAIL_NOTIFICATIONS_ENABLED w środowisku.",
    )

    status = smtp_config_status()
    if status.get("configured"):
        smtp_config = status.get("config") or {}
        masked_user = (smtp_config.get("user") or "").replace("@", " [at] ")
        masked_from = (smtp_config.get("from_address") or "").replace("@", " [at] ")
        st.success("SMTP skonfigurowane.")
        st.markdown(
            "\n".join(
                [
                    f"- Host: {smtp_config.get('host')}",
                    f"- Port: {smtp_config.get('port')}",
                    f"- TLS: {smtp_config.get('tls')}",
                    f"- User: {masked_user or 'brak'}",
                    f"- From: {masked_from or 'brak'}",
                ]
            )
        )
    else:
        missing = ", ".join(status.get("missing") or [])
        st.warning(f"SMTP nie jest skonfigurowane. Braki: {missing or 'nieznane'}")

    champions = champion_repo.list_champions()
    missing_email = [ch for ch in champions if ch.get("active") and not ch.get("email")]
    if missing_email:
        st.info("Champions bez adresu email (pomijani w powiadomieniach):")
        st.write([row.get("display_name") or row.get("id") for row in missing_email])

    logs = notification_repo.list_recent(limit=50)
    if logs:
        def _payload_count(payload_json: str | None) -> int:
            payload = _safe_json_loads(payload_json)
            if payload is None:
                return 0
            if isinstance(payload, dict):
                if payload.get("action_ids"):
                    try:
                        return len(payload["action_ids"])
                    except TypeError:
                        return 0
                try:
                    return int(payload.get("overdue_count") or 0) + int(payload.get("open_count") or 0)
                except (TypeError, ValueError):
                    return 0
            return 0

        for row in logs:
            row["action_count"] = _payload_count(row.get("payload_json"))

        logs_df = pd.DataFrame(logs)
        cols = ["created_at", "notification_type", "recipient_email", "action_count"]
        existing_cols = [c for c in cols if c in logs_df.columns]
        logs_df = logs_df[existing_cols].rename(
            columns={
                "created_at": "Data",
                "notification_type": "Typ",
                "recipient_email": "Odbiorca",
                "action_count": "Liczba akcji",
            }
        )
        st.dataframe(logs_df, use_container_width=True, hide_index=True)
    else:
        st.caption("Brak logów powiadomień email.")

    st.divider()

    # =========================
    # ACTION CATEGORIES (DB)
    # =========================
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

    # =========================
    # CATEGORY RULES (EFFECTIVENESS + SAVINGS)
    # =========================
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
