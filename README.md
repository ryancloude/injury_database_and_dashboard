# MLB Injury Database & Dashboard

This project builds a structured database of **MLB injured list (IL) stints** and a **public Streamlit dashboard** that visualizes:

- How often players get hurt, and where (by body part, role, team, and season).
- How many **games** and **days** teams lose to the injured list.
- The **financial impact** (injured salary) and **on-field impact** (WAR missed) of those injuries.

The codebase includes:

- A **data pipeline** that ingests MLB, salary, and projection data into PostgreSQL.
- A **dbt project** that cleans and aggregates that data into analysis-ready tables.
- **Airflow DAGs** that orchestrate daily and yearly runs.
- A **Streamlit dashboard** for exploring the results.

---

## Live dashboard

> 🔗 **Dashboard:** _https://baseballinjurydatabase.streamlit.app/_  

---

## High-level architecture

At a high level:

> Raw feeds → Bronze tables → Silver tables → Gold tables → Streamlit dashboard

**Data sources**

- MLB Stats API (transactions, players, rosters, games, Statcast)
- Cot’s Contracts (salary)
- FanGraphs ZiPS preseason projections (WAR)

**Core stack**

- **PostgreSQL on AWS RDS** (production database)
- **PostgreSQL in Docker** (local dev)
- **Python** for ingestion scripts
- **dbt** for transformations (silver & gold layers)
- **Apache Airflow** for orchestration
- **Docker + Amazon ECR + AWS ECS Fargate** for running pipeline/dbt/Airflow in the cloud
- **Streamlit** for the dashboard (hosted on Streamlit Community Cloud)
- **GitHub** for version control

---

## Repository layout

Your exact structure may differ slightly, but the project is organized roughly as:

```text
.
├── pipeline/             # Python ingestion scripts (bronze layer)
│   ├── bronze_statcast.py
│   ├── bronze_games.py
│   ├── bronze_players.py
│   ├── bronze_transactions.py
│   └── ... (other scripts)
├── dbt/                  # dbt project (silver & gold models)
│   ├── models/
│   ├── macros/
│   ├── profiles.yml      # dbt profiles file (not committed in real repo)
│   └── dbt_project.yml
├── airflow/ or dags/     # Airflow DAGs (e.g., pipeline_dag.py, bronze_yearly.py)
│   └── database/
├── dashboard/            # Streamlit app
│   ├── League_Injuries.py
│   ├── pages/
│   │   ├── 01_Team_Injuries.py
│   │   ├── 02_Player_Injuries.py
│   │   └── 03_About.py
│   └── requirements.txt
├── docker-compose.yaml   # Local stack (Postgres, Airflow, etc.)
├── .env                  # Local environment variables (NOT committed)
├── .env.example          # Example env file
├── requirements-airflow.txt
├── requirements-pipeline.txt
└── README.md