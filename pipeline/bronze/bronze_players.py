"""
Bronze-layer ingestion for MLB Players via the MLB Stats API.


Responsibilities
- Determine a seasonal starting point (resume from existing table or full load)
- Request per-season player lists from the public MLB Stats API
- Normalize JSON → DataFrame and infer/cast a first-pass schema
- Create/alter Postgres bronze tables (+ matching _staging tables)
- Bulk load with COPY; then MERGE staging → target (idempotent upsert)
"""
import requests
from sqlalchemy import create_engine, text, inspect
import os
import pandas as pd
from bronze_statcast import fast_copy_from, alter_table, ensure_bronze_tables_from_df, \
merge_staging_into_target, truncate_staging, get_table_columns_and_types, infer_and_cast_dtypes_psql, get_baseball_engine
from datetime import datetime

def get_starting_season(engine, base: str, column_name: str, schema:str, default_start=2010):
    """
    Compute the first season to request for an incremental load.
    Looks up the max season already present in the target table. If the table
    is missing or empty, returns `default_start`.

    Args
    ----
    engine : database engine connected to Postgres.
    base :Base table name (without schema), ex. 'players'.
    column_name : Column used to determine progress, ex. 'season'.
    schema : Schema name, e.g. 'bronze'.
    default_start : Fallback first season for a first-ever load, by default 2015.

    Returns: The season to start (as int) from (see note above).
    """

    with engine.connect() as conn:
        inspector = inspect(conn)
        table_exist = inspector.has_table(base, schema)
        if table_exist:
            result = conn.execute(text(f"select max({column_name}) from {schema}.{base}"))
            value =  result.scalar()
            if value is not None:
                return value
            else:
                return default_start
        else:
            return default_start


def get_players_data(primary_keys: list, season: int):
    """
    Retrieve and normalize MLB players data for a single season.
    Strategy
    - Call MLB Stats API endpoint `/api/v1/sports/1/players?season={season}`
    - Flatten nested JSON with pandas.json_normalize
    - Lower-case column names and attach the requested `season`
    - Deduplicate on provided primary keys; drop rows missing PKs
    - Drop columns that are entirely null

    Args:
    primary_keys : Columns to de-duplicate on (ex., ['id', 'season']).
    season : Season year to fetch.

    Returns: 
    Clean, flat DataFrame ready for staging (may be empty).
    """

    params = {'season': season}
    url = "https://statsapi.mlb.com/api/v1/sports/1/players"
    response = requests.get(url, params=params,timeout=60)
    
     # Parse JSON payload safely; default to empty if parsing fails
    try:
        players = response.json().get('people', [])
    except Exception as e:
        print(f"[WARNING] Failed to parse players data: {e}")
        return pd.DataFrame()

    
    if not players:
        print("[INFO] No players found")
        return pd.DataFrame()
    
    players_df = pd.json_normalize(players, sep='_')
    players_df.columns = players_df.columns.str.lower()
    players_df['season'] = season
    # De-dup by PKs (keep last), drop rows missing PKs, remove all-null columns.
    players_df = players_df.drop_duplicates(subset=primary_keys, keep='last')
    players_df = players_df.dropna(subset=primary_keys, axis=0)
    players_df = players_df.dropna(axis=1, how='all')
    return players_df

def create_players(engine, base: str, schema: str, primary_keys: list, start_year: int, end_year: int):
    """
    End-to-end Player ingestion into bronze (season-by-season).

    For each season in [start_year, end_year]:
      1) Extract players for the season into a DataFrame
      2) On first non-empty chunk:
           - If table missing, infer types and create base/staging
           - If table exists, infer types for the chunk to detect/schema-compare
      3) If schema drift is detected, ALTER TABLE to reconcile
      4) Align DataFrame column order to target table order
      5) COPY into staging; MERGE staging -> target (idempotent upsert)
      6) TRUNCATE staging (cleanup)

    Args:
    engine : Database engine connected to Postgres.
    base : Base table name (ex., 'players').
    schema : Schema name (ex., 'bronze').
    primary_keys : Primary key columns for idempotent merges (ex., ['id', 'season']).
    start_year : First season to load (inclusive).
    end_year : Last season to load (inclusive).

    Returns: None
    """
    first_chunk = True
    for season in range(start_year, end_year + 1):
        print(f"\n Fetching: {season} season")
        try:
            df = get_players_data(primary_keys=primary_keys, season=season)
        except Exception as e:
                print(f"Failed for {season} season: {e}")
        if df.empty:
            continue
        if first_chunk == True:
            existing_columns_and_types = get_table_columns_and_types(engine=engine, base=base, schema=schema)
            if existing_columns_and_types is None:
                df, existing_columns_and_types = infer_and_cast_dtypes_psql(df=df)
                chunk_columns_and_types = existing_columns_and_types
            else:
                df, chunk_columns_and_types = infer_and_cast_dtypes_psql(df=df)
            
            ensure_bronze_tables_from_df(df, base=base, schema=schema, engine=engine, primary_keys=primary_keys)
            truncate_staging(engine, schema=schema, base=base)
            first_chunk = False
            
        else:
            df, chunk_columns_and_types = infer_and_cast_dtypes_psql(df=df)
        if chunk_columns_and_types != existing_columns_and_types:
            df, existing_columns_and_types = alter_table(df, base=base, schema=schema, engine=engine, \
                                                        chunk_columns_and_types=chunk_columns_and_types, \
                                                        existing_columns_and_types=existing_columns_and_types)
        try:
            df = df[list(existing_columns_and_types.keys())]
            fast_copy_from(df, schema=schema, base=base, engine=engine)
            merge_staging_into_target(base=base, engine=engine, primary_keys=primary_keys, \
                                    columns=list(existing_columns_and_types.keys()), schema=schema)
            print(f"Inserted {len(df)} rows from for the {season} season")
            truncate_staging(engine, schema=schema, base=base)
        except Exception as e:
            print(f"[WARNING] fast_copy_from failed: {e}")
            truncate_staging(engine, schema=schema, base=base)

if __name__ == "__main__":
    # Load PostgreSQL connection string from environment variable
    engine = get_baseball_engine()

    base_table_name = 'players'
    schema = 'bronze'
    primary_key = ['id']

    starting_season = get_starting_season(engine=engine, base=base_table_name, column_name='season', schema='bronze')
    end_season = datetime.today().year

    
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
    create_players(engine=engine, base=base_table_name, schema=schema, primary_keys=primary_key,\
                   start_year=starting_season, end_year=end_season)