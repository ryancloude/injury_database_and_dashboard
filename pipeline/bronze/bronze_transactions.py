"""
Bronze-layer ingestion for MLB transactions from statsapi.mlb.com.


- Fetches transactions by date range
- Normalizes response -> flat DataFrame; de-duplicates and removes empty columns
- Ensures bronze tables exist and upserts via MERGE
"""
import requests
from sqlalchemy import create_engine, text
import os
import pandas as pd
from datetime import datetime, timedelta
from bronze_statcast import get_start_date, get_date_chunks, fast_copy_from, \
    alter_table, ensure_bronze_tables_from_df, merge_staging_into_target, truncate_staging, get_table_columns_and_types,\
    infer_and_cast_dtypes_psql, get_baseball_engine
from tqdm import tqdm
import time


def get_transactions_data(start_date: str, end_date: str, primary_keys: list):
    """Retrieve and normalize MLB transactions between two dates (inclusive).

    Args:
    start_date: ISO date string (YYYY-MM-DD).
    end_date: ISO date string (YYYY-MM-DD).
    primary_keys: Columns to de-duplicate on (e.g., ['id']).

    Returns:
    pd.DataFrame: Flattened transaction rows, de-duplicated, all-null columns dropped.
    """
    params = {"startDate": start_date, "endDate": end_date, 'sportId':1}
    url = "https://statsapi.mlb.com/api/v1/transactions"
    response = requests.get(url, params=params, timeout=60)
    # Try to extract JSON
    try:
        transactions = response.json().get('transactions', [])
    except Exception as e:
        print(f"[WARNING] Failed to parse transactions data: {e}")
        return pd.DataFrame()

    # Return early if nothing found
    if not transactions:
        print(f"[INFO] No transactions found for {start_date} to {end_date}")
        return pd.DataFrame()
    transactions_df = pd.DataFrame()
    for t in transactions:
        transactions_df = pd.concat([transactions_df, pd.json_normalize(t, sep='_')])

    transactions_df.columns = transactions_df.columns.str.lower()
    #If any of the primary keys are null, drop the row
    transactions_df = transactions_df.dropna(subset=primary_keys, axis=0)
    #Drops rows with duplicates primary key, keeps the row with the least nulls
    transactions_df['null_count'] = transactions_df.isnull().sum(axis=1)
    transactions_df = transactions_df.sort_values(by='null_count', ascending=True)
    transactions_df = transactions_df.drop_duplicates(subset=primary_keys, keep='first')
    transactions_df = transactions_df.drop(labels='null_count', axis=1)
    #If a column is all nulls it dropped
    transactions_df = transactions_df.dropna(axis=1, how='all')

    return transactions_df


def create_transactions(start_date: datetime.date, end_date:datetime.date, chunk_size:int, \
                    engine, base: str, schema: str, primary_keys=None):
    """Ingest MLB transactions into bronze in small date chunks.

    Mirrors the games/statcast ingestion pattern: infer/ensure schema on the first chunk,
    then COPY -> MERGE for idempotent upserts.
    """
    first_chunk = True
    for year in range(start_date.year, end_date.year + 1):
        print(f"\n Year: {year}")
        season_start = datetime(year, 2, 1)
        year_start = max(season_start, start_date) if year == start_date.year else season_start
        year_end = end_date if year == end_date.year else datetime(year, 11, 30)
        date_chunks = get_date_chunks(year_start, year_end, chunk_size)
        for chunk_start, chunk_end in tqdm(date_chunks, desc=f"fetching {year}"):
            try:
                df = get_transactions_data(start_date=chunk_start, end_date=chunk_end, primary_keys=primary_key)
                if df.empty:
                    continue
                if first_chunk:
                    existing_columns_and_types = get_table_columns_and_types(engine=engine, base=base, schema=schema)
                    if existing_columns_and_types is None:
                        df, existing_columns_and_types = infer_and_cast_dtypes_psql(df=df)
                        chunk_columns_and_types = existing_columns_and_types
                    else:
                        df, chunk_columns_and_types = infer_and_cast_dtypes_psql(df=df)
                    # Create or replace table with initial chunk
                    ensure_bronze_tables_from_df(df, base=base, schema=schema, engine=engine, primary_keys=primary_keys)
                    truncate_staging(engine, schema=schema, base=base)
                    first_chunk = False
                    # If schema has changed, update table structure
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
                    print(f"Inserted {len(df)} rows from {chunk_start} to {chunk_end}")
                    truncate_staging(engine, schema=schema, base=base)
                except Exception as e:
                    print(f"[WARNING] fast_copy_from failed: {e}")
                    truncate_staging(engine, schema=schema, base=base)
            except Exception as e:
                print(f"Failed for {chunk_start} to {chunk_end}: {e}")
                time.sleep(2)
    return



if __name__ == "__main__":
    # Load PostgreSQL connection string from environment variable
    engine = get_baseball_engine()

    base_table_name = 'transactions'
    schema = 'bronze'
    primary_key = ['id']

    # Define parameters for data extraction
    start_date = get_start_date(engine=engine, base=base_table_name, column_name='date',schema=schema, window=7)
    end_date = datetime.today() - timedelta(days=1)
    chunk_size = 14

    
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
    create_transactions(start_date=start_date, end_date=end_date, chunk_size=chunk_size,\
                    engine=engine, base=base_table_name, schema=schema, primary_keys=primary_key)