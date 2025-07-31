# Import necessary modules
import os
import pandas as pd
from pybaseball import statcast
from sqlalchemy import create_engine, text, inspect
from urllib.parse import quote_plus
from datetime import date, timedelta
# Import shared utility functions from create_statcast
from create_statcast import get_date_chunks, alter_table, fast_copy_from

def get_start_date(engine, table_name, column_name):
    """
    Queries the database for the most recent game date and returns the next day.

    Ensures that updates resume from the correct point without duplicating data.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"select max({column_name}) from {table_name}"))
        value =  result.scalar()
        try:
            start_date = value.date() + timedelta(days=1)
        except:
            start_date = value + timedelta(days=1)
        return start_date
    

def get_existing_columns(engine):
    """
    Retrieves the current column names of the statcast table from PostgreSQL.

    Used to detect schema changes in newly pulled data.
    """
    with engine.connect() as conn:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        return [col["name"] for col in columns]

def update_statcast(engine, today, chunk_size, table_name):
    """
    Increments the statcast table with the latest pitch-level data.

    - Skips execution during offseason months (Dec–Feb).
    - Uses chunks for batch downloads if multiple days of data are available.
    - Automatically handles schema changes.
    """

    # Avoid updating if not in the regular season (March to November)
    if today.month < 3 or today.month > 11:
        print('Offseason - skipping update')
        return
    
    # Determine date range to update
    start_date = get_start_date(engine=engine, column_name="game_date", table_name="statcast")
    end_date = today - timedelta(days=1)
    existing_columns = get_existing_columns(engine)

    # Break into chunks if the gap is large
    if end_date - start_date > timedelta(days=chunk_size):
        chunks = get_date_chunks(start_date, end_date, chunk_size)
        for start, end in chunks:
            df = statcast(start, end)
            #Drops column if all values ar na
            df = df.dropna(axis=1, how='all')
            # Update table schema if new columns appear
            if df.columns.to_list() != existing_columns:
                existing_columns, df = alter_table(df, existing_columns, table_name, engine=engine)
            # Load chunk into PostgreSQL
            fast_copy_from(df, table_name=table_name, engine=engine)
    else:
        # If gap is small, download in one batch
        df = statcast(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        df = df.dropna(axis=1, how='all')
        if df.columns.to_list() != existing_columns:
            existing_columns, df = alter_table(df, existing_columns, table_name, engine=engine)
        fast_copy_from(df, table_name=table_name, engine=engine)


if __name__ == "__main__":
    # Initialize database connection from environment variable
    DATABASE_URL = os.environ['DATABASE_URL']
    engine = create_engine(DATABASE_URL)

    # Define constants
    chunk_size = 7 # Days per API call
    table_name = "statcast"
    today = date.today()
    # Sanity check: test DB connection before running update
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()
    # Run the update
    update_statcast(engine=engine, today=today, chunk_size = chunk_size, table_name = table_name)