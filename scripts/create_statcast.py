# Loads necessary packages for data extraction, manipulation, and database interaction
from pybaseball import statcast
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import time
import os
import warnings
import io
import psycopg2

def get_date_chunks(start_date, end_date, chunk_size):
    """
    Splits a date range into equal-sized chunks for API calls.

    Returns:
        List of (start_date, end_date) tuples in 'YYYY-MM-DD' format
    """
    chunks = []
    current_date = start_date
    while current_date <= end_date:
        chunk_end = min(current_date + timedelta(days=chunk_size-1), end_date)
        chunks.append((current_date.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
        current_date += timedelta(days=chunk_size)
    return chunks

def fast_copy_from(df: pd.DataFrame, table_name: str, engine):
    """
    Fast bulk insert into PostgreSQL using COPY FROM STDIN.

    Falls back to quoted table name if initial insert fails.
    """
    conn = engine.raw_connection()
    cursor = conn.cursor()
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    try:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[❌] fast_copy_from failed: {e}")
        cursor.copy_expert(f'COPY "{table_name}" FROM STDIN WITH CSV', buffer)
        # Retry with quoted table name as fallback for edge cases
        
    finally:
        cursor.close()
        conn.close()

def alter_table(df, existing_columns, table_name, engine):
    """
    Adds new columns to an existing PostgreSQL table if the DataFrame contains unseen fields.

    Also reorders and fills missing columns for consistent schema.
    """
        
    new_columns = [col for col in df.columns if col not in existing_columns]


    for col in new_columns:
        # Infer SQL data type based on pandas dtype    
        if str(df[col].dtype) == "int64":
            col_type = "INTEGER"
        elif str(df[col].dtype) == "float64":
            col_type = "FLOAT"
        elif str(df[col].dtype) == "datetime64[s]":
            col_type = "TIMESTAMP"
        else:
            col_type = "TEXT"

        print(f"[INFO] Adding new column '{col}' ({col_type}) to table '{table_name}'")
        alter_stmt = text(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{col}" {col_type};')
        with engine.begin() as conn:
            conn.execute(alter_stmt)
        existing_columns.append(col)

    # Fill in missing columns with None and reorder
    missing_cols = set(existing_columns) - set(df.columns)
    for col in missing_cols:
        df[col] = None
    df = df[existing_columns]
    return existing_columns, df

def create_statcast(start_year, end_year, chunk_size, table_name, engine):
    """
    Downloads Statcast data from pybaseball and stores it into PostgreSQL.

    For each season, splits data into manageable chunks and:
        - Creates table from first chunk
        - Adds new columns dynamically
        - Uses COPY for fast ingestion
    """
    first_chunk = True
    for year in range(start_year, end_year + 1):
        print(f"\n Year: {year}")
        date_chunks = get_date_chunks(datetime(year, 3, 1), datetime(year, 11, 30),chunk_size)        
        for start_date, end_date in tqdm(date_chunks, desc=f"fetching {year}"):
            try:
                # Silence known warnings from pybaseball
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=FutureWarning)
                    df = statcast(start_dt=start_date, end_dt=end_date)
                    df = df.dropna(axis=1, how='all')
                if not df.empty:
                    if first_chunk:
                        # Create or replace table with initial chunk
                        df.to_sql(table_name, engine, if_exists="replace", index=False, method="multi")
                        existing_columns = df.columns.tolist()
                        first_chunk = False
                        print(f"Created table with {len(df)} rows from {start_date} to {end_date}")
                    else:
                        # If schema has changed, update table structure
                        if df.columns.to_list() != existing_columns:
                            existing_columns, df = alter_table(df, existing_columns, table_name, engine=engine)
                        try:
                            fast_copy_from(df, table_name, engine)
                            print(f"Inserted {len(df)} rows from {start_date} to {end_date}")
                        except Exception as e:
                             print(f"[WARNING] fast_copy_from failed: {e}")
            except Exception as e:
                print(f"Failed for {start_date} to {end_date}: {e}")
                time.sleep(2)
    return

if __name__ == "__main__":
    # Load PostgreSQL connection string from environment variable
    DATABASE_URL = os.environ['DATABASE_URL']
    engine = create_engine(DATABASE_URL)

    # Define parameters for data extraction
    start_year = 2015
    end_year = datetime.today().year
    chunk_size = 7 # Days per chunk (Statcast has request limits)
    table_name = "statcast"
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
    create_statcast(start_year = start_year, end_year = end_year, chunk_size = chunk_size, table_name = table_name, engine=engine)