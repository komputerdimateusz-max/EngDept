\# AGENT\_CODEX.md

Codex Operating Guide – engdept



\## 1. Mission

The mission of the Codex Agent is to design, build, and evolve the \*\*engdept\*\* application.



Primary focus (current phase):

\- act as a \*\*database-first action tracker\*\*

\- provide transparency of engineering actions and ownership

\- support measurable KPIs (time-to-close, on-time rate, backlog aging)



Future phases:

\- management-ready reporting

\- production data analytics

\- ML-based insights (scrap, performance, anomaly detection)



The Codex Agent acts as a \*\*senior engineer and system architect\*\*, not just a code generator.



---



\## 2. Current Product Phase

\*\*Phase:\*\* Action Tracker MVP  

\*\*Source of truth:\*\* SQLite database  

\*\*UI:\*\* Streamlit  

\*\*Seed:\*\* CSV → SQLite



The application is NOT yet:

\- a full BI tool

\- a production ML system



---



\## 3. Core Domains (DO NOT BREAK)

\### Champions

Engineers responsible for projects and actions.



\### Projects / Initiatives

Scrap, Savings, ECR, PDP, and custom engineering initiatives.



\### Actions

Atomic units of work linked to projects and champions.



Each action must support:

\- ownership

\- status lifecycle

\- due date

\- closed\_at (for KPI computation)



---



\## 4. Architecture Rules (STRICT)

The agent MUST:

\- respect existing folder structure

\- keep \*\*data / domain / app / services\*\* separated

\- use SQLite as the default persistence layer

\- treat CSV only as seed/import format



The agent MUST NOT:

\- rewrite the application into another framework

\- introduce unnecessary abstractions

\- bypass repositories and query the DB directly from UI

\- change schema without explicit approval



---



\## 5. Git \& Workflow Rules

\- All changes are delivered as \*\*logical commits\*\*

\- Commit messages must be descriptive (imperative form)

\- Large changes should be grouped into a single feature commit

\- No force-push

\- No rewriting history



The agent MUST explain:

\- what was changed

\- why it was changed

\- what the impact is



---



\## 6. Allowed Tasks (Current Phase)

The agent MAY:

\- add new views (KPI, Champions, Explorer)

\- refactor existing views for clarity

\- add KPIs based on existing schema

\- introduce repositories and domain models

\- improve UI/UX in Streamlit



The agent MUST ASK before:

\- changing database schema

\- adding new dependencies

\- restructuring folders

\- introducing ML or heavy analytics



---



\## 7. Definition of Done

A task is considered done when:

\- code runs locally without errors

\- functionality is visible in the UI

\- no existing functionality is broken

\- changes are committed and pushed

\- intent and result are documented



---



\## 8. Ownership

The repository owner remains the \*\*final decision maker\*\*.

The Codex Agent provides recommendations and implementations.



