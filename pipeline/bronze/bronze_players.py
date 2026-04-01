"""
Bronze-layer ingestion for MLB Players via the MLB Stats API.

Responsibilities
- Determine a seasonal starting point (resume from existing table or full load)
- Request per-season player lists from the public MLB Stats API
- Normalize JSON into a DataFrame and infer/cast a first-pass schema
- Create/alter Postgres bronze tables (+ matching _staging tables)
- Bulk load with COPY; then MERGE staging into target (idempotent upsert)
"""

from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import inspect, text

from bronze_statcast import (
    alter_table,
    ensure_bronze_tables_from_df,
    fast_copy_from,
    get_baseball_engine,
    get_table_columns_and_types,
    infer_and_cast_dtypes_psql,
    merge_staging_into_target,
    truncate_staging,
)


def get_starting_season(engine, base: str, column_name: str, schema: str, default_start=2010):
    """
    Compute the first season to request for an incremental load.
    Looks up the max season already present in the target table. If the table
    is missing or empty, returns `default_start`.
    """
    with engine.connect() as conn:
        inspector = inspect(conn)
        table_exist = inspector.has_table(base, schema)
        if table_exist:
            result = conn.execute(text(f"select max({column_name}) from {schema}.{base}"))
            value = result.scalar()
            if value is not None:
                return value
            return default_start
        return default_start


def get_players_data(primary_keys: list, season: int) -> pd.DataFrame:
    """
    Retrieve and normalize MLB players data for a single season.
    """
    params = {"season": season}
    url = "https://statsapi.mlb.com/api/v1/sports/1/players"
    response = requests.get(url, params=params, timeout=60)

    try:
        players = response.json().get("people", [])
    except Exception as exc:
        print(f"[WARNING] Failed to parse players data: {exc}")
        return pd.DataFrame()

    if not players:
        print("[INFO] No players found")
        return pd.DataFrame()

    players_df = pd.json_normalize(players, sep="_")
    players_df.columns = players_df.columns.str.lower()
    players_df["season"] = season
    players_df = players_df.drop_duplicates(subset=primary_keys, keep="last")
    players_df = players_df.dropna(subset=primary_keys, axis=0)
    players_df = players_df.dropna(axis=1, how="all")
    return players_df


def normalize_players_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep code-like player fields as strings so schema inference stays stable
    across seasons and matches the existing bronze.players table.
    """
    text_columns = [
        "primaryposition_code",
        "batside_code",
        "pitchhand_code",
        "primarynumber",
    ]

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


def create_players(engine, base: str, schema: str, primary_keys: list, start_year: int, end_year: int):
    """
    End-to-end Player ingestion into bronze (season-by-season).
    """
    first_chunk = True
    for season in range(start_year, end_year + 1):
        print(f"\n Fetching: {season} season")
        try:
            df = get_players_data(primary_keys=primary_keys, season=season)
        except Exception as exc:
            print(f"Failed for {season} season: {exc}")
            continue

        if df.empty:
            continue

        df = normalize_players_schema(df)

        if first_chunk:
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
            df, existing_columns_and_types = alter_table(
                df,
                base=base,
                schema=schema,
                engine=engine,
                chunk_columns_and_types=chunk_columns_and_types,
                existing_columns_and_types=existing_columns_and_types,
            )

        try:
            df = df[list(existing_columns_and_types.keys())]
            fast_copy_from(df, schema=schema, base=base, engine=engine)
            merge_staging_into_target(
                base=base,
                engine=engine,
                primary_keys=primary_keys,
                columns=list(existing_columns_and_types.keys()),
                schema=schema,
            )
            print(f"Inserted {len(df)} rows for the {season} season")
            truncate_staging(engine, schema=schema, base=base)
        except Exception as exc:
            print(f"[WARNING] fast_copy_from failed: {exc}")
            truncate_staging(engine, schema=schema, base=base)


if __name__ == "__main__":
    engine = get_baseball_engine()

    base_table_name = "players"
    schema = "bronze"
    primary_key = ["id"]

    starting_season = get_starting_season(
        engine=engine,
        base=base_table_name,
        column_name="season",
        schema="bronze",
    )
    end_season = datetime.today().year

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as exc:
        print("Failed to connect to PostgreSQL:")
        print(exc)
        raise SystemExit(1)

    create_players(
        engine=engine,
        base=base_table_name,
        schema=schema,
        primary_keys=primary_key,
        start_year=starting_season,
        end_year=end_season,
    )
