import streamlit as st

st.set_page_config(page_title="About", layout="wide")

# ----------------- Section markdown strings -----------------

contents_markdown = """
## Contents

- [What is this dashboard?](#what-is-this-dashboard)
- [Data sources](#data-sources)
- [From raw data to dashboard](#from-raw-data-to-dashboard)
- [Key definitions and metrics](#key-definitions-and-metrics)
- [Update frequency and coverage](#update-frequency-and-coverage)
- [Data quality, limitations, and caveats](#data-quality-limitations-and-caveats)
- [Tech stack and implementation](#tech-stack-and-implementation)
- [Contact](#contact)

---
"""

what_is_this_markdown = """
### What is this dashboard?

This dashboard summarizes Major League Baseball injured list stints and helps quantify the on-field and financial impact of those injuries. It brings together injury, salary, and performance data to show how much time (and value) teams lose to injured players.

The goal is to make injury impact:
- **Visible** – how often players get hurt, and where (by body part, role, team, and season).
- **Comparable** – across teams and seasons using consistent definitions and metrics.
- **Actionable** – by tying injuries to estimated games missed, WAR missed, and salary committed to injured players.

In its current form, the dashboard:
- Covers **MLB teams** across recent seasons (2010–present). 
- Focuses on metrics like **injury counts**, **games missed**, **WAR missed**, and **injured salary**.
"""

data_sources_markdown = """
### Data sources

This dashboard combines data from the MLB Stats API, salary data from Cot’s Contracts, and ZiPS projections from FanGraphs.

**Statcast data**

- **Source:** Statcast pitch- and game-level data (MLB Stats API accessed via the `pybaseball` Python package).
- **What it provides:** Game dates, teams, players, and performance metrics (pitches thrown, playing time, etc.).
- **How it’s used:** Checks for player appearances in games where transactions activating players from the injured list may be missing. This data will also be used in the future to attempt to predict pitcher injuries.  

**Transaction data**

- **Source:** Public MLB transaction data from the MLB Stats API transactions endpoint.
- **What it provides:** MLB transactions from 2010–present.
- **How it’s used:** To build a database of players’ injured list history with descriptions of injuries when they are placed on the injured list.

**Player data**

- **Source:** MLB player data from the MLB Stats API people endpoint.
- **What it provides:** Player IDs, positions, handedness, and basic biographical information.
- **How it’s used:** To classify players by role (e.g., starter vs. reliever, pitcher vs. position player) and to support per-team aggregates.

**Roster data**

- **Source:** MLB Stats API people endpoint with roster entries.
- **What it provides:** Each player’s history of roster membership.
- **How it’s used:** To determine which team a player belongs to at the time of an injury.

**Games data**

- **Source:** MLB Stats API schedule endpoint.
- **What it provides:** MLB game types and dates.
- **How it’s used:** To determine how many games a player missed while on the injured list.

**Teams data**

- **Source:** MLB Stats API teams endpoint.
- **What it provides:** MLB team names and IDs.
- **How it’s used:** To get current team names and IDs used for joins and aggregations.

**Salary data**

- **Source:** Public contract and salary information from Cot’s Contracts on Baseball Prospectus.
- **What it provides:** Player-level salary figures by season.
- **How it’s used:** To estimate the **salary committed to injured players** during their IL stints (shown as “injured salary” in the dashboard).

**Projections data**

- **Source:** Wins Above Replacement (WAR) estimates from FanGraphs ZiPS preseason projections.
- **What it provides:** Player-level estimates of on-field value in wins above replacement.
- **How it’s used:** To estimate **WAR missed** when a player is unavailable due to injury, aggregated to the team–season level.

**Coverage note**

- The injured list data is built from MLB public transaction descriptions. Any injury that is not reported in the MLB transactions feed (for example, injuries that do not result in an IL placement) will not be included in the database. More detail is provided in the **Data quality, limitations, and caveats** section below.
"""

pipeline_markdown = """
### From raw data to dashboard

The numbers in this dashboard come from a multi-step pipeline that turns raw feeds (MLB Stats API, Cot’s, FanGraphs) into clean, aggregated team–season injury metrics.

At a high level:

> Raw feeds → Raw database tables → Cleaned & normalized tables → Injured list stints → Team–season metrics → Dashboard

---

**1. Ingestion: raw tables (“bronze” layer)**

- Nightly scripts call the MLB Stats API (transactions, players, rosters, games, Statcast) and load the results into PostgreSQL raw tables.
- Annual scripts, run near Opening Day, load relatively static data such as Cot’s salary files, FanGraphs ZiPS preseason projections, and team metadata.
- This layer mirrors the source data as closely as possible and preserves the original feeds so transformations can be re-run or adjusted without re-downloading everything.

---

**2. Cleaning and standardization (“silver” layer)**

From the raw tables, the pipeline builds cleaned, consistent tables:

- **Standardized IDs and keys**  
  Players, teams, and seasons are assigned consistent IDs across endpoints and external sources where possible.
- **Clean dimension tables**  
  Cleaned tables for players, teams, games, and rosters are created from the Stats API feeds.
- **Normalized seasons and games**  
  Games are tied to a specific season and game type (e.g., regular season), and rosters are aligned with game dates so that each player–team–date combination is well defined.

---

**3. Building injured list episodes**

Injured list (IL) history is constructed from the MLB transaction feed:

- **Identify IL placements and returns**  
  Transaction descriptions are parsed to find when a player is placed on, activated from, or transferred on the injured list, and to extract a free-text description of the injury.
- **Group into IL stints**  
  Related transactions are grouped into a single “injury episode” with a player, team, start date, end date, and injury description.  
  For example:
  - A player who is placed on the 10-day injured list and later transferred to the 60-day injured list is treated as one continuous episode.
  - A player who is placed on the injured list during a season, activated in the offseason, and then placed back on the injured list before Opening Day of the next season is also treated as the same injury episode if the injured list placements report injuries to body parts in the same body-part group (e.g., forearm and UCL, rotator cuff and shoulder).
- **Handle missing end dates**  
  When an explicit activation transaction is missing, game and roster data are used to approximate the player’s return date (for example, using the first game the player appears in after the IL placement).
- **Classify injury body part**  
  Free-text injury descriptions (e.g., “right elbow soreness”) are mapped into standardized body-part categories (such as elbow, shoulder, back, or more general buckets like arm or leg).
- **Count days and games missed**  
  Using the MLB schedule, the pipeline counts how many calendar days and MLB games each player misses during an IL stint and assigns those missed games to the appropriate team and season.
- **Compute injured salary and WAR missed**  
  Injured salary and WAR missed are computed at the IL-stint level using the formulas described in the **Key definitions and metrics** section below, and then passed to the gold layer for aggregation.

---

**4. Aggregating to team–season metrics (“gold” layer)**

The final step is to aggregate individual IL stints into the team–season table that powers this dashboard (for example, the `gold.team_season_injuries` table):

- **Core volume metrics**
  - Number of IL stints.
  - Total days missed.
  - Total games missed.
- **Financial impact**
  - Injured salary, summed across all IL stints for each team and season.
- **On-field impact**
  - WAR missed, summed across all IL stints for each team and season.
- **Breakdowns used in the dashboard**
  - By body part (e.g., elbow, shoulder, back).
  - By player group (pitcher vs. position player, and starter vs. reliever).
  - Additional summary statistics used in charts and tables.

The dashboard pages read from these precomputed tables rather than doing heavy calculations on the fly. That keeps the UI fast and puts all of the complex logic inside a versioned, testable data pipeline.
"""

definitions_markdown = """
### Key definitions and metrics

This section explains how the main concepts in the dashboard are defined and calculated.

---

#### Injured list stint / injury episode

- An **injured list (IL) stint** (or **injury episode**) is a continuous period where a player is on the injured list.
- It is built from one or more related transactions for the same player and injury.
- Examples:
  - A player who is placed on the 10-day IL and later **transferred to the 60-day IL** is treated as **one** continuous injury episode.
  - A player who is placed on the IL during a season, activated in the offseason, and then **placed back on the IL before Opening Day** of the next season is also treated as the **same** injury episode **if** the injured list placements report injuries to body parts in the same body-part group (e.g., forearm and UCL, rotator cuff and shoulder).

An episode has a player, team, start date, end date (actual or estimated), and an injury description.

---

#### Days missed and games missed

- **Days missed**  
  The number of calendar days between the start and end of an IL stint.

- **Games missed**  
  The number of MLB games on the schedule that occur between the start and end of an IL stint.
  - Only games in the relevant MLB game types (for example, regular season) are counted.
  - Games are assigned to the team and season the player belongs to during the stint, based on roster and schedule data.

---

#### Injured salary

- A player’s **salary per game** is derived from their seasonal salary (from Cot’s) divided by 162.
- For each IL stint, **injured salary** is calculated as:

> salary per game × games missed during that stint

- At the **team–season** level, injured salary is the **sum of injured salary** across all IL stints for that team in that season.

This gives an estimate of how much salary was committed to players while they were unavailable due to injury.

---

#### WAR missed

- For each player, ZiPS **preseason projected WAR** is converted to a **WAR per game** estimate.
- For each IL stint, **WAR missed** is calculated as:

> projected WAR per game × games missed

- For starting pitchers and relievers, an additional scaling factor is applied to reflect typical usage over a full season:
  - **Starting pitchers:** scaled by 33/162  
  - **Relievers:** scaled by 65/162  
- A pitcher’s role is determined to be a **starter** if they have the same number of projected games and games started, and a **reliever** if they are projected to have no games started.
- If a pitcher is projected to have a different number of games started and games (they are projected to be both a starter and reliever), their scaling factors are weighted by their ratio of games started to total games.
- If a player does not have a preseason projection for that year, their full-season WAR is estimated to be slightly above replacement level at **0.5 WAR**.
- Some players have preseason projections as both a hitter and a pitcher despite not being two-way players in the majors (for example, Noah McLean); only their projected WAR at their primary position is used.
- Projected playing time beyond these simple usage assumptions is **not** taken into account.

At the **team–season** level, WAR missed is the **sum of WAR missed** across all IL stints for that team in that season. This provides an estimate of the on-field value lost to injuries.

---

#### Body part categories

- Injury descriptions from the transaction feed (for example, “right elbow soreness”) are treated as **free text**.
- These descriptions are mapped into a set of standardized **body-part categories** and subcategories, such as:

  - **Elbow**
    - Elbow  
    - Forearm  
    - UCL  
    - Nerve  

  - **Shoulder**
    - Shoulder  
    - Rotator cuff  
    - AC joint  
    - Labrum  
    - Teres major  
    - Scapular  
    - SC joint  

  - **Other arm**
    - Arm  
    - Bicep  
    - Tricep  
    - Lat  
    - Chest  
    - Trap  

  - **Hand**
    - Hand  
    - Wrist  
    - Finger  
    - Thumb  

  - **Torso**
    - Abdomen  
    - Oblique  
    - Rib  
    - Back  

  - **Knee**
    - Knee  
    - Achilles  
    - Meniscus  
    - Patellar  

  - **Upper leg**
    - Hamstring  
    - Groin  
    - Quad  
    - SI joint  
    - Hip  
    - Adductor  
    - Abductor  
    - Glute  

  - **Lower leg**
    - Calf  
    - Shin  
    - Ankle  
    - Foot  
    - Toe  

- These are the categories for post-normalized injuries. To see the full mapping from transaction descriptions to injuries, use the `il_placements.py` script on GitHub.
- These categories are used for the body-part group breakdowns in the dashboard.

Because the source text is imperfect, some injuries will inevitably be classified into broader or approximate categories.

---

#### Player groups and roles

Players are grouped to make comparisons more meaningful:

- **Pitcher vs. position player**  
  Determined using the player’s position metadata from the Stats API.
- **Starter vs. reliever (for pitchers)**  
  Determined using a combination of role/position information and usage/projection data (for example, whether the player is projected to start games or appear only in relief).

These definitions are applied consistently across all teams and seasons so that comparisons in the dashboard are as fair and interpretable as possible.
"""

update_markdown = """
### Update frequency and coverage

This dashboard is built from data that is refreshed on a regular schedule, but different sources update on different timelines.

---

#### Update cadence

- **Daily updates (in-season and offseason):**  
  The core MLB data is updated **daily** using automated scripts, including:
  - Transactions and injured list moves  
  - Rosters  
  - Games and schedules  
  - Statcast pitch- and game-level data  

- **Yearly updates (around Opening Day):**  
  The following sources are updated **once per year**, typically near the start of a new season:
  - Teams metadata  
  - Cot’s salary data  
  - FanGraphs ZiPS preseason projections  

You may occasionally see short lags between real-world events and their appearance in the dashboard, depending on when new data is published by the original sources and when the daily scripts run.

---

#### Season coverage

- Injured list, salary, and projection data are available from **2010** through the **most recent season loaded into the database**.
- Older seasons may have less detailed or less consistent transaction descriptions, which can affect injury classification and body-part mapping.
- For current and recent seasons, coverage is generally complete for:
  - Regular-season injured list stints.
  - Standard MLB IL designations (10-day, 15-day, 60-day, 7-day concussion IL, etc.).

---

#### Source-specific freshness

- **MLB Stats API (transactions, players, rosters, games, Statcast):**  
  Polled daily and reflects MLB’s public data within a day.
- **Cot’s Contracts (salary data):**  
  Loaded and refreshed once per year around the start of each season, or when updated contract information becomes available.
- **FanGraphs ZiPS projections:**  
  Loaded once per year when preseason projections are released and treated as fixed for that season.
- **Teams metadata:**  
  Updated annually near Opening Day or when league-level changes require it.

Because all downstream tables are built from these sources, any corrections or updates to the raw feeds will flow through on the next daily or yearly pipeline run, depending on the source.
"""

quality_markdown = """
### Data quality, limitations, and caveats

This dashboard is built on public data sources and several modeling assumptions. The goal of this section is to highlight where the data is strong and where it is incomplete or approximate.

---

#### Injuries vs. injured list stints

- The database tracks **injured list stints reported in MLB transactions**, not every physical issue a player might have.
- Injuries that never result in an IL placement are **not included**.
- As a result, the dashboard focuses on **documented IL stints**, which generally correspond to more serious or longer-term injuries.

---

#### Reliance on transaction text

- Injury descriptions come from the **free-text transaction notes** published by MLB.
- These notes can be:
  - Vague (e.g., “arm soreness”, “lower body discomfort”),
  - Inconsistent across teams and seasons,
  - Or missing entirely.
- Body-part categories are derived by mapping these descriptions into standardized groups. While the mapping is systematic, some injuries will inevitably be classified into **broader or approximate categories** due to limited information.

---

#### Approximated return dates

- When a player has a clear activation transaction, the end of an IL stint is well defined.
- When an explicit activation is **missing**, the pipeline approximates the player’s return date using:
  - Roster data, and  
  - The first game in which the player appears after being placed on the IL.
- In these cases, **days missed** and **games missed** are estimates. They are generally close to reality, but edge cases (e.g., brief rehab assignments, late-season roster moves) may introduce small inaccuracies.

---

#### WAR missed is an estimate

- WAR missed is based on **ZiPS preseason projected WAR per game**, not realized performance.
- It uses the same role and scaling assumptions described in the **WAR missed** definition above (starter/reliever usage factors, blended roles, default 0.5 WAR when no projection exists).
- Projected playing time beyond these simple usage assumptions is **not modeled**.  
  As a result, WAR missed should be interpreted as a **reasonable approximation of value lost**, not a precise measurement.

---

#### Salary estimates

- Injured salary uses the same method described in the **Injured salary** definition above (seasonal salary spread over 162 games × games missed).
- This approach does **not** account for:
  - Performance bonuses or incentives,
  - Option years or buyouts,
  - Retained salary or money sent between teams in trades,
  - Minor league time or split contracts.
- The result is a **clean, consistent estimate** of salary committed to injured players, but it may differ from exact team payroll accounting.

---

#### What this dashboard is (and isn’t)

- This dashboard is designed to make **team–season injury burden** visible and comparable using consistent rules.
- It is **not**:
  - A medical database,
  - A definitive accounting of every nagging injury,
  - Or a perfect reconstruction of team payroll obligations.
- Instead, it provides a **standardized view** of publicly reported IL stints, their estimated impact on team performance (WAR missed), and their estimated financial cost (injured salary).

These limitations are kept in mind when designing metrics and visualizations, and the same rules are applied consistently across teams and seasons so that comparisons remain meaningful.
"""

tech_stack_markdown = """
### Tech stack and implementation

Under the hood, this dashboard is powered by a modern data engineering and analytics stack.

---

#### Data storage

- **PostgreSQL on AWS RDS (production)**  
  In production, all data lives in a PostgreSQL database running on **Amazon RDS**. The database is organized into schemas that roughly follow a bronze / silver / gold pattern:

  - **Bronze:** Raw source tables from the MLB Stats API, Cot’s Contracts, and FanGraphs ZiPS.
  - **Silver:** Cleaned and standardized tables for players, teams, games, rosters, and individual IL episodes.
  - **Gold:** Final analytical tables (for example, `gold.team_season_injuries`) that power this dashboard.

- **PostgreSQL in Docker (local development)**  
  For local development, the same schemas and tables run in a Postgres container via Docker Compose. The code uses an `APP_ENV` environment variable (`local` vs `aws`) to switch between the local and RDS databases without changing SQL.

---

#### Data ingestion and transformations

- **Python ingestion pipeline (containerized)**  
  A set of Python scripts (for example, the `bronze_*.py` scripts) handle:

  - Calling the MLB Stats API (transactions, players, rosters, games, Statcast).
  - Loading salary data from Cot’s Contracts.
  - Loading preseason ZiPS projections from FanGraphs.
  - Writing the results into the bronze layer in PostgreSQL.

  These scripts are packaged into a **Docker image** (the “pipeline” image), which is:

  - Run locally via Docker Compose for development and testing.
  - Run in production on **AWS ECS Fargate** as on-demand tasks triggered by Airflow.

- **dbt (data build tool)**  
  Downstream transformations are defined as a **dbt project**, which:

  - Builds and maintains the silver and gold tables.
  - Uses incremental models so only new or changed data is processed.
  - Keeps all SQL logic versioned and testable.

  The dbt project is also containerized (a “dbt” image) and:

  - Runs locally via `docker compose` (or directly with dbt).
  - Runs in production as an ECS Fargate task that Airflow triggers after the bronze ingestion completes.

---

#### Orchestration and automation

- **Apache Airflow (local + AWS)**  

  Airflow coordinates the entire pipeline:

  - **DAGs** schedule and orchestrate:
    - Daily refreshes of MLB data (transactions, rosters, games, Statcast).
    - Yearly loads of teams, salary, and ZiPS projection data.
    - dbt runs for the silver and gold layers.

  There are two environments:

  - **Local development:**  
    Airflow runs in Docker (webserver + scheduler) using the local Postgres instance for both metadata and baseball data. Tasks can run either as local Docker containers (`DockerOperator`) or as ECS tasks (via `EcsRunTaskOperator`) when testing the cloud workflow.

  - **Production on AWS ECS:**  
    Airflow’s webserver and scheduler run as long-lived **ECS services** using Docker images stored in **Amazon ECR** and an Airflow metadata database on RDS.  
    Heavy work (bronze ingestion and dbt runs) is offloaded to short-lived ECS Fargate tasks using `EcsRunTaskOperator`, which run the pipeline and dbt images against the RDS baseball database.

---

#### Containerization and deployment

- **Docker + Amazon ECR + AWS ECS Fargate**

  - All components (pipeline, dbt, Airflow) are packaged as Docker images.
  - Images are pushed to **Amazon ECR** for use in the AWS environment.
  - **AWS ECS Fargate** runs:
    - The long-running Airflow webserver and scheduler services.
    - On-demand tasks for the pipeline (bronze ingestion) and dbt (silver/gold transformations).

  This keeps local and cloud environments aligned: the same images and code paths run in Docker on your machine and in ECS in production, with only configuration and environment variables (such as `APP_ENV`) changing between them.

---

#### Dashboard and presentation

- **Streamlit dashboard (Streamlit Community Cloud)**  
  The interactive dashboard is built with **Streamlit** and deployed to **Streamlit Community Cloud**. It:

  - Connects to the **gold** schema in the RDS database using a **read-only** connection string.
  - Powers the team and player injury pages, charts, and tables.
  - Hosts this About page alongside the analytical views.

- **Local dashboard development**  
  For local development, the same Streamlit app can be run against the local Postgres instance simply by changing environment variables (for example, pointing `READONLY_PG_DSN` at the local database instead of RDS).

---

#### Configuration, environments, and version control

- **Environment management**  
  Configuration is handled via environment variables, typically loaded from a `.env` file in local development and set in ECS task definitions / Streamlit secrets in production.  
  A few important variables:

  - `APP_ENV` to switch between `local` and `aws`.
  - `LOCAL_PG_DSN` / `AWS_PG_DSN` for the main database.
  - Read-only DSNs for the dashboard.
  - AWS region and ECS/RDS settings in the AWS environment.

- **Git and GitHub**  
  All code for ingestion, transformations, orchestration, and the dashboard lives in a single GitHub repository. This includes:

  - Pipeline scripts (bronze ingestion).
  - dbt models (silver/gold).
  - Airflow DAGs.
  - Streamlit dashboard pages.

  This makes it straightforward to review changes, track the evolution of the pipeline, and keep the local and cloud setups in sync.
"""
contact_markdown = """
### Contact

If you have questions, notice an issue in the data, or want to discuss the project, feel free to reach out.

- **Email:** [ryancloude@gmail.com](mailto:ryancloude@gmail.com)  
- **GitHub repository:** [injury_databse_and_dashboard](https://github.com/ryancloude/injury_databse_and_dashboard)

I’m always open to feedback, bug reports, and ideas for additional features or analyses.
"""

# ----------------- Layout -----------------

st.markdown(contents_markdown)

with st.expander("What is this dashboard?", expanded=True):
    st.markdown(what_is_this_markdown)

with st.expander("Data sources", expanded=True):
    st.markdown(data_sources_markdown)

with st.expander("From raw data to dashboard", expanded=False):
    st.markdown(pipeline_markdown)

with st.expander("Key definitions and metrics", expanded=False):
    st.markdown(definitions_markdown)

with st.expander("Update frequency and coverage", expanded=False):
    st.markdown(update_markdown)

with st.expander("Data quality, limitations, and caveats", expanded=False):
    st.markdown(quality_markdown)

with st.expander("Tech stack and implementation", expanded=False):
    st.markdown(tech_stack_markdown)

with st.expander("Contact", expanded=False):
    st.markdown(contact_markdown)