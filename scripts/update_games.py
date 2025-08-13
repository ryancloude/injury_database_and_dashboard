from create_statcast import fast_copy_from, get_date_chunks
from update_statcast import get_start_date
from datetime import date, timedelta
from sqlalchemy import create_engine, text
import os
from create_games import get_games_data

def update_games(engine, today, chunk_size, table_name):
    start_date = get_start_date(engine=engine, column_name='game_date', table_name=table_name)
    end_date = today - timedelta(days=1)
    if end_date - start_date > timedelta(days= chunk_size):
        chunks =  get_date_chunks(start_date, end_date, chunk_size)
        for start, end in chunks:
            df = get_games_data(start, end)
            try:
                fast_copy_from(df, table_name, engine)
                print(f"Inserted {len(df)} rows from {start} to {end} in {table_name}")
            except Exception as e:
                print(f"[WARNING] fast_copy_from failed: {e}")
    else:
        df = get_games_data(start_date, end_date)
        try:
            fast_copy_from(df, table_name, engine)
            print(f"Inserted {len(df)} rows from {start_date} to {end_date} in {table_name}")
        except Exception as e:
            print(f"[WARNING] fast_copy_from failed: {e}")
    return

if __name__ == "__main__":
    # Load PostgreSQL connection string from environment variable
    DATABASE_URL = os.environ['DATABASE_URL']
    engine = create_engine(DATABASE_URL)

    # Define parameters for data extraction
    today = date.today()
    chunk_size = 14 # Days per chunk
    table_name = "games"
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
    update_games(engine, today, chunk_size, table_name)