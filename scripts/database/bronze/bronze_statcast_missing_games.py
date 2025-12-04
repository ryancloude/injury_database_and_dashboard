"""
Bronze-layer backfill for Statcast pitch data using pybaseball.

- Finds finalized MLB games present in bronze.games but missing in bronze.statcast
- Pulls one-day Statcast snapshots via pybaseball.statcast
- Aligns dtypes/columns to Postgres, stages via COPY, and MERGEs into target
"""
import pandas as pd
from pybaseball import statcast
from sqlalchemy import create_engine, text
import warnings
import os
from bronze_statcast import fast_copy_from, alter_table, merge_staging_into_target, truncate_staging,\
      get_table_columns_and_types, infer_and_cast_dtypes_psql, get_baseball_engine

def get_missing_games_dates(engine):
    """Return dates of finalized games that have no rows in bronze.statcast.

    Logic:
    - Looks for MLB regular and postseason games that are finalized
    - Filter to games whose gamepk does not exist in bronze.statcast but exists in bronze.games
    -
    Return ISO 'YYYY-MM-DD' strings in ascending order (stable processing)

    Args: SQLAlchemy engine connected to the target Postgres.

    Returns: list of dates as strings to fetch (inclusive day windows for statcast()).
    """
    sql_query = """
    SELECT distinct CAST(gamedate as date) as game_date
    from bronze.games g
    WHERE gametype in ('R','F', 'D', 'L', 'W') and status_statuscode = 'F'
    and not exists (select 1 from bronze.statcast s where s.game_pk = g.gamepk)
    order by game_date asc;
    """
    df = pd.read_sql(sql_query, engine)
    return df['game_date'].astype(str).to_list()


def get_missing_statcast_games(dates: list, engine, base: str, schema: str, primary_keys: list):
    existing_columns_and_types = get_table_columns_and_types(engine=engine, base=base, schema=schema)
    for date in dates:
        try:
        # Silence known warnings from pybaseball
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                df = statcast(date)
        except Exception as e:
            print(f"Pybaseball failed for {date}: {e}")
            continue
        df, chunk_columns_and_types = infer_and_cast_dtypes_psql(df=df)
        if chunk_columns_and_types != existing_columns_and_types:
            df, existing_columns_and_types = alter_table(df, base=base, schema=schema, engine=engine, \
                                            chunk_columns_and_types=chunk_columns_and_types, \
                                            existing_columns_and_types=existing_columns_and_types)
        try:
            #Orders the columns to match order in table
            df = df[list(existing_columns_and_types.keys())]    
            fast_copy_from(df, schema=schema, base=base, engine=engine)
            merge_staging_into_target(base=base, engine=engine, primary_keys=primary_keys, \
                                        columns=list(existing_columns_and_types.keys()), schema=schema)
            print(f"Inserted {len(df)} rows for {date}")
            truncate_staging(engine, schema=schema, base=base)
        except Exception as e:
            print(f"[WARNING] fast_copy_from failed: {e}")
            truncate_staging(engine, schema=schema, base=base)
    return

if __name__ == "__main__":
    # Load PostgreSQL connection string from environment variable
    engine = get_baseball_engine()

    base_table_name = 'statcast'
    schema = 'bronze'
    primary_key = ['game_pk', 'at_bat_number', 'pitch_number']

    dates = get_missing_games_dates(engine=engine)

    
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
    # Begin Statcast data ingestion
    get_missing_statcast_games( engine=engine, base=base_table_name, schema=schema, primary_keys=primary_key, dates=dates)