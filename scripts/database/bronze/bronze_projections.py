"""
Bronze-layer ingestion for FanGraphs player projections (hitters & pitchers) CSVs.

Responsibilities
- Read pre-downloaded FanGraphs projections CSVs for hitters and pitchers across a season range
- Normalize and standardize the minimal set of columns used downstream
- Tag each row with position and season; cast key fields to nullable numeric types
- Concatenate across seasons into a tidy DataFrame
- Write to bronze via the staging/merge helpers from `bronze_statcast`:
  - ensure_bronze_tables_from_df → fast_copy_from → merge_staging_into_target → truncate_staging
"""

import pandas as pd
import os
from bronze_statcast import fast_copy_from, ensure_bronze_tables_from_df,  truncate_staging, merge_staging_into_target
from sqlalchemy import create_engine, text
import datetime


def create_projections_df(start_year, end_year, path):
    """
    Build a tidy projections DataFrame from local FanGraphs CSVs.

    Strategy
    - For each season in [start_year, end_year], read two files expected at:
        * fg_projections_hitters_<YEAR>.csv
        * fg_projections_pitchers_<YEAR>.csv
    - Select a consistent subset of columns present in both files (pitchers/hitters differ slightly)
    - Add a 'position' label ('pitcher' vs 'position player'), and align column names
    - Cast id/count-like columns to nullable Int64 where appropriate
    - Stamp the 'season' column; concatenate all seasons

    Args
    start_year  First season to include (int).
    end_year    Last season to include (int), inclusive.
    path        Directory containing the projections CSVs (string ending with a slash, e.g., "/app/projections/").

    Returns
    A pandas DataFrame with standardized columns:
      ['name', 'player_id', 'games', 'games_started', 'war', 'nameascii', 'position', 'season']
    """
    # Accumulator for all seasons
    projections = pd.DataFrame()

    # Iterate season-by-season and append to the accumulator
    for year in range(start_year, end_year+1):
        # Raw reads (filenames are assumed to follow the given convention)
        hitters = pd.read_csv(path + f"fg_projections_hitters_{year}.csv")
        pitchers = pd.read_csv(path + f"fg_projections_pitchers_{year}.csv")

        # Pitchers: keep a small, consistent subset and annotate role
        pitchers = pitchers[['Name', 'MLBAMID','G','GS','WAR', 'NameASCII']]
        pitchers['position'] = 'pitcher'
        pitchers['PA'] = None

        # Hitters: keep overlapping columns; GS not present → will be filled with None later
        hitters = hitters[['Name', 'MLBAMID','G', 'PA', 'WAR','NameASCII']]
        hitters['position'] = 'position player'
        hitters['GS'] = None  # keep schema aligned with pitchers

        # Stack pitchers + hitters for the given season
        df = pd.concat([pitchers, hitters], axis=0)

        # Standardize column names for downstream consistency
        df = df.rename(columns={
            'MLBAMID': 'player_id',
            'G': 'games',
            'GS': 'games_started',
            'PA':'pa',
            'WAR': 'war',
            'Name': 'name',
            'NameASCII': 'nameascii'
        })

        # Cast id/count-like columns to nullable integer for robustness
        df[['player_id','games']] = df[['player_id','games']].astype('Int64')

        # Stamp the season
        df['season'] = year

        # Append this season's block
        projections = pd.concat([projections, df])

    # Drop any rows that somehow lack a player_id or season (defensive)
    projections = projections.dropna(subset=['player_id','season'], axis=0, how='any')
    return projections


def create_projections_table(start_year, end_year, path, primary_keys, schema, base, engine):
    """
    End-to-end creation/merge of the bronze projections table using staging helpers.

    Steps
      1) Materialize the in-memory projections DataFrame across the requested season range
      2) Ensure the bronze target exists with an aligned schema (based on the DataFrame)
      3) Bulk-load to the staging table (fast copy)
      4) Merge staging → target on the specified primary keys
      5) Truncate staging to leave it clean

    Args
    start_year    First season to include (int).
    end_year      Last season to include (int), inclusive.
    path          Directory containing projections CSVs.
    primary_keys  List of column names used as natural keys for the merge (e.g., ['player_id','season','position']).
    schema        Target schema name (e.g., 'bronze').
    base          Base table name (e.g., 'projections').
    engine        SQLAlchemy engine for the Postgres connection.

    Returns
    None (performs I/O side effects: ensures tables, loads, merges, truncates).
    """
    # Build the projections DataFrame for the specified window
    projections = create_projections_df(start_year, end_year, path)

    # Ensure target & staging tables exist with the correct schema based on the DataFrame
    ensure_bronze_tables_from_df(projections, base=base, schema=schema, engine=engine, primary_keys=primary_keys)

    # Bulk insert into staging for performance
    fast_copy_from(projections, schema, base, engine)

    # Merge staging rows into the target table on primary keys
    merge_staging_into_target(schema, base, engine, primary_keys, projections.columns)

    # Clean up staging
    truncate_staging(engine, schema,base)
    return


def create_projections_table(start_year, end_year, path, primary_keys, schema, base, engine):
    """
    End-to-end creation/merge of the bronze projections table using staging helpers.

    (Duplicate definition preserved intentionally; no behavioral changes requested.)

    Steps
      1) Materialize the in-memory projections DataFrame across the requested season range
      2) Ensure the bronze target exists with an aligned schema (based on the DataFrame)
      3) Bulk-load to the staging table (fast copy)
      4) Merge staging → target on the specified primary keys
      5) Truncate staging to leave it clean

    Args
    start_year    First season to include (int).
    end_year      Last season to include (int), inclusive.
    path          Directory containing projections CSVs.
    primary_keys  List of column names used as natural keys for the merge (e.g., ['player_id','season','position']).
    schema        Target schema name (e.g., 'bronze').
    base          Base table name (e.g., 'projections').
    engine        SQLAlchemy engine for the Postgres connection.

    Returns
    None (performs I/O side effects: ensures tables, loads, merges, truncates).
    """
    projections = create_projections_df(start_year, end_year, path)
    ensure_bronze_tables_from_df(projections, base=base, schema=schema, engine=engine, primary_keys=primary_keys)
    fast_copy_from(projections, schema, base, engine)
    merge_staging_into_target(schema, base, engine, primary_keys, projections.columns)
    truncate_staging(engine, schema,base)
    return


if __name__ == "__main__":
    # Load PostgreSQL connection string from environment (docker-compose/.env)
    BASEBALL_URL = os.environ["BASEBALL_URL"]
    engine = create_engine(BASEBALL_URL)

    # Define the season window and local CSV path
    start_year = 2010
    end_year = datetime.datetime.today().year
    path = "/app/projections/"  # expects fg_projections_hitters_<YEAR>.csv and fg_projections_pitchers_<YEAR>.csv

    # Bronze destination and merge keys
    base = 'projections'
    schema = 'bronze'
    primary_keys = ['player_id', 'season', 'position']

    # Smoke test DB connectivity before loading
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()

    # Kick off ingestion
    create_projections_table(start_year, end_year, path, primary_keys, schema, base, engine)