"""
Bronze-layer ingestion for MLB Teams via the MLB Stats API.

Responsibilities
- Request the current list of MLB teams (sportId=1)
- Normalize JSON → DataFrame and infer/cast a first-pass schema
- Create/alter Postgres bronze tables (and matching _staging tables)
- Bulk load with COPY; then MERGE staging -> target (idempotent upsert)
"""

import requests
from sqlalchemy import create_engine, text
import os
import pandas as pd
from bronze_statcast import fast_copy_from, alter_table, ensure_bronze_tables_from_df, \
merge_staging_into_target, truncate_staging, get_table_columns_and_types, infer_and_cast_dtypes_psql
import datetime

def get_teams_data(primary_keys: list, start_season:int, end_season:int):
    """
    Retrieve and normalize MLB teams data.
    Strategy:
    - Call MLB Stats API endpoint `/api/v1/teams?sportId=1`
    - Flatten nested JSON with pandas.json_normalize
    - Lower-case column names
    - Deduplicate on provided primary keys; drop rows missing PKs
    - Drop columns that are entirely null

    Args
    primary_keys Columns to de-duplicate on (ex. ['id','season']).

    Returns: Clean, flat DataFrame ready for staging (may be empty).
    """
    teams_and_seasons_df = pd.DataFrame()
    url = "https://statsapi.mlb.com/api/v1/teams"
    for season in range(start_season, end_season+1):
        params = {"season":season, 'sportId':1}

        response = requests.get(url, params=params, timeout=60)
        try:
            teams = response.json().get('teams', [])
        except Exception as e:
            print(f"[WARNING] Failed to parse team data: {e}")
            return pd.DataFrame()

        
        if not teams:
            print("[INFO] No teams found")
            return pd.DataFrame()
        
        teams_df = pd.json_normalize(teams, sep='_')
        teams_df.columns = teams_df.columns.str.lower()
        teams_df = teams_df.drop_duplicates(subset=primary_keys, keep='last')
        teams_df = teams_df.dropna(subset=primary_keys, axis=0)
        teams_df['season'] = season
        teams_and_seasons_df = pd.concat([teams_and_seasons_df, teams_df])
    return teams_and_seasons_df


def create_teams(engine, base: str, schema: str, primary_keys: list, start_season: int, end_season:int):
    """
    End-to-end Teams ingestion into bronze (single snapshot load).

    Steps:
      1) Extract teams into a DataFrame
      2) If target is missing then infer types and create base/staging
         else infer types for the chunk and compare with existing schema
      3) If schema drift is detected, ALTER TABLE to reconcile
      4) Align DataFrame column order to target table order
      5) COPY into staging; MERGE staging -> target (idempotent upsert)
      6) TRUNCATE staging (cleanu
    Args: 
    engine : Database engine connected to Postgres.
    base : Base table name (ex. 'teams').
    schema :Schema name (ex. 'bronze').
    primary_keys : Primary key columns for idempotent merges (ex. ['id']).

    Returns: None
    """
    df = get_teams_data(primary_keys=primary_keys, start_season=start_season, end_season=end_season)
    existing_columns_and_types = get_table_columns_and_types(engine=engine, base=base, schema=schema)
    if existing_columns_and_types is None:
            df, existing_columns_and_types = infer_and_cast_dtypes_psql(df=df)
            chunk_columns_and_types = existing_columns_and_types
    else:
            df, chunk_columns_and_types = infer_and_cast_dtypes_psql(df=df)
    ensure_bronze_tables_from_df(df, base=base, schema=schema, engine=engine, primary_keys=primary_keys)
    truncate_staging(engine, schema=schema, base=base)
    if chunk_columns_and_types != existing_columns_and_types:
        df, existing_columns_and_types = alter_table(df, base=base, schema=schema, engine=engine, \
                                                    chunk_columns_and_types=chunk_columns_and_types, \
                                                    existing_columns_and_types=existing_columns_and_types)
    try:
        df = df[list(existing_columns_and_types.keys())]
        fast_copy_from(df, schema=schema, base=base, engine=engine)
        merge_staging_into_target(base=base, engine=engine, primary_keys=primary_keys, \
                                columns=list(existing_columns_and_types.keys()), schema=schema)
        print(f"Inserted {len(df)} rows")
        truncate_staging(engine, schema=schema, base=base)
    except Exception as e:
        print(f"[WARNING] fast_copy_from failed: {e}")
        truncate_staging(engine, schema=schema, base=base)
    return

if __name__ == "__main__":
    # Load PostgreSQL connection string from environment variable
    BASEBALL_URL = os.environ['BASEBALL_URL']
    engine = create_engine(BASEBALL_URL)

    base_table_name = 'teams'
    schema = 'bronze'
    primary_key = ['id', 'season']
    start_season = 2010
    end_season = datetime.datetime.today().year
    
    # Test DB connection before starting extraction
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()
    create_teams(engine=engine, base=base_table_name, schema=schema, primary_keys=primary_key, start_season=start_season, end_season=end_season)