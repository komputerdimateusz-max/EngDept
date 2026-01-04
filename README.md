# engdept

**engdept** is a lightweight engineering department tool for tracking projects, actions, ownership, and progress — with a strong focus on data quality, auditability, and future analytics.

The application is built incrementally:
- first as a **database-backed action & project tracker**
- later as a **management reporting and analytics platform**
- eventually as a **production data & ML analysis tool** (scrap, performance, trends)

---

## 🎯 Purpose

The goal of engdept is to provide:

- transparency of engineering work
- clear ownership (champions)
- traceability of actions and projects
- reliable data for KPIs and future analytics

This tool is designed for **technical / process engineering teams** working on:
- scrap reduction initiatives
- cost savings
- ECR / PDP projects
- continuous improvement actions

---

## 🧩 Core Concepts (Domains)

### Champions
Engineers responsible for projects and actions.

Each champion:
- has a full profile (name, email, position, hire date)
- can be assigned to multiple projects
- has a full audit trail (create / update / delete)

### Projects
Engineering initiatives such as scrap reduction, savings, ECR, PDP, or custom topics.

Each project:
- has a name and required work center
- can store future-facing fields (SOP, EOP, related work centers)
- groups multiple actions
- maintains a full changelog

### Actions
Atomic units of work performed within projects.

Each action:
- belongs to a project and a champion
- has a status lifecycle and due dates
- is the base for KPI computation

---

## 🏗 Architecture Overview

- **UI:** Streamlit
- **Backend:** Python
- **Persistence:** SQLite (single source of truth)
- **Seed data:** CSV → SQLite (for local bootstrap)
- **Structure:** `src/`-based Python package

The application follows a clear separation of concerns:
- `app/` – Streamlit pages (UI)
- `data/` – repositories, DB schema, migrations
- `domain/` – domain concepts (gradually introduced)

---

## 📂 Project Structure

```
engdept/
├── data/
│   └── sample/ # sample CSV seed data
├── docs/ # architecture, roadmap (grows over time)
├── src/
│   └── action_tracking/
│       ├── app/ # Streamlit pages
│       ├── data/ # DB, repositories, migrations
│       └── domain/ # domain models (future)
├── streamlit_app.py # application entrypoint
├── pyproject.toml
├── AGENT_CODEX.md # operating contract for Codex agent
└── README.md
```

---

## 🚀 Getting Started (Local)

### Requirements
- Python **3.11+**
- Windows / macOS / Linux

### Setup
```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS/Linux

pip install -e .
```

This repository is actively developed with a dedicated AI coding agent.

Rules, responsibilities, and architectural constraints are defined in:
👉 AGENT_CODEX.md

Any automated changes must follow that contract.

---

## 📌 Philosophy

- start simple, but structurally correct
- database is the source of truth
- auditability > convenience
- no premature optimization
- learning-friendly codebase

---

## 🧑‍💻 Ownership

The repository owner remains the final decision maker.

The Codex Agent acts as a senior engineer and architect — not autonomously, but by delegation.

---

## Run the app

```bash
streamlit run streamlit_app.py
```

On first run:

- SQLite database is created in data/app.db
- sample CSV data is automatically seeded

---

## ⚠️ Running without installation (fallback)

For environments where `pip install -e .` is not possible,
`streamlit_app.py` includes a small fallback that adds `src/` to `sys.path`.

Recommended approach is always:

```bash
pip install -e .
```

Fallback exists only for robustness.

---

## 🧪 Current Features

- Sidebar navigation (Explorer, Actions, Champions, Projects, KPI)
- Actions list (DB-backed)
- Champions CRUD with:
  - full profile
  - project assignment
  - audit changelog
- Projects CRUD with:
  - required work center
  - future analytics fields (SOP, EOP, related WC)
  - importance classification (High/Mid/Low Runner, Spare parts)
  - audit changelog
- SQLite schema versioning using PRAGMA user_version
- High risk WorkCenter view with trend flags + deep-links to Production Explorer/Actions

---

## 🗺 Roadmap (High Level)

### Phase 1 – Foundation (DONE / IN PROGRESS)

- Database-first design
- Actions, Champions, Projects
- Auditability and changelogs

### Phase 2 – Management KPIs

- time-to-close
- on-time close rate
- backlog health
- champion and project views

### Phase 3 – Production Data & Analytics

- scrap and performance imports
- feature engineering
- trend analysis

### Phase 4 – ML

- anomaly detection
- scrap prediction
- action impact analysis
