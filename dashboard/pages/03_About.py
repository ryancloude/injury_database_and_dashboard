import streamlit as st

st.set_page_config(page_title="About", layout="wide")

st.title("About This Dashboard")

st.markdown(
    """
### What is this dashboard?

This dashboard summarizes Major League Baseball injured list stints and helps quantify the on-field and financial impact of those injuries. It brings together injury, salary, and performance data to show how much time (and value) teams lose to injured players.

The goal is to make injury impact:
- **Visible** – how often players get hurt, and where (by body part, role, team, and season).
- **Comparable** – across teams and seasons using consistent definitions and metrics.
- **Actionable** – by tying injuries to estimated games missed, WAR missed, and salary committed to injured players.

In its current form, the dashboard:
- Covers **MLB teams** across recent seasons (2010–present). 
- Focuses on metrics like **injury counts**, **games missed**, **WAR missed**, and **injured salary**.
+
+---
+
### Data sources

This dashboard combines data from the MLB Stats API, salary data from Cot’s Contracts, and ZiPS projections from FanGraphs

**Statcast data**

- **Source:** Statcast pitch- and game-level data (MLB Stats API accessed via the `pybaseball` Python package).
- **What it provides:** Game dates, teams, players, and performance metrics (pitches thrown, playing time, etc.).
- **How it’s used:** Checks for player appearances in games where transactions activating players from the injured list may be missing. This data will also be used in the future to make an attempt at predicting pitcher injuries.  

**Transaction data**

- **Source:** Public MLB transaction data from the MLB Stats API transactions endpoint.
- **What it provides:** MLB transactions from 2010-Present.
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
- **What it provides:** MLB team names and IDs
- **How it’s used:** To get current team names and IDs used for joins and aggregations.

**Salary data**

- **Source:** Public contract and salary information from Cot’s Contracts on Baseball Prospectus
- **What it provides:** Player-level salary figures by season.
- **How it’s used:** To estimate the **salary committed to injured players** during their IL stints (shown as “injured salary” in the dashboard).

**Projections data**

- **Source:** Wins Above Replacement (WAR) estimates from FanGraphs ZiPS preseason projections.
- **What it provides:** Player-level estimates of on-field value in wins above replacement.
- **How it’s used:** To estimate **WAR missed** when a player is unavailable due to injury, aggregated to the team–season level.

**Coverage note**
- The injured list data is built from MLB public transaction descriptions. Any injury that is not reported in the MLB transactions feed (for example,injuries that do not result in an IL placement) will not be included in the database.


### From raw data to dashboard

The numbers in this dashboard come from a multi-step pipeline that turns raw feeds (MLB Stats API, Cot’s, FanGraphs) into clean, aggregated team–season injury metrics.

At a high level:

> Raw feeds → Raw database tables → Cleaned & normalized tables → Injured list stints → Team–season metrics → Dashboard

---

**1. Ingestion: raw tables (“bronze” layer)**

-Nightly scripts call the MLB Stats API (transactions, players, rosters, games, Statcast) and load the results into PostgreSQL raw tables.
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

- **Transaction descriptions are parsed to find when a player is placed on, activated from, or transferred on the injured list, and to extract a free-text description of the injury.
- **Group into IL stints**  
  Related transactions are grouped into a single “injury episode” with a player, team, start date, end date, and injury description.
- A player who is placed on the 10-day injured list and later transferred to the 60-day injured list is treated as one continuous episode.
- A player who is placed on the injured list during a season, activated in the offseason, and then placed back on the injured list before Opening Day of the next season is also treated as the same injury episode.

- **Handle missing end dates**  
  When an explicit activation transaction is missing, game and roster data are used to approximate the player’s return date (for example, using the first game the player appears in after the IL placement).
- **Classify injury body part**  
  Free-text injury descriptions (e.g., “right elbow soreness”) are mapped into standardized body-part categories (such as elbow, shoulder, back, or more general buckets like arm or leg).
- **Count days and games missed**  
  Using the MLB schedule, the pipeline counts how many calendar days and MLB games each player misses during an IL stint and assigns those missed games to the appropriate team and season.
- **Injured Salary and WAR missed**  
 - **Injured salary** is calculated by multiplying the player’s salary per game by the number of games the player missed while on the injured list.
- **WAR missed** is calculated by multiplying the player’s ZiPS preseason projected WAR per game by the number of games missed. For starting pitchers this is additionally scaled by 33/162, and for relievers by 65/162, to reflect typical games appeared in over a full season. Projected playing time is not taken into account.

---

**4. Aggregating to team–season metrics (“gold” layer)**

The final step is to aggregate individual IL stints into the team–season table that powers this dashboard (for example, the `gold.team_season_injuries` table):

- **Core volume metrics**
  - Number of IL stints.
  - Total days missed.
  - Total games missed.
- **Financial impact**
  -Injured salary
- **On-field impact**
  - WAR missed
- **Breakdowns used in the dashboard**
  - By body part (e.g., elbow, shoulder, back).
  - By player group (pitcher vs. position player, and starter vs. reliever).
  - Additional summary statistics used in charts and tables.

The dashboard pages read from these precomputed tables rather than doing heavy calculations on the fly. That keeps the UI fast and puts all of the complex logic inside a versioned, testable data pipeline.

""",
)