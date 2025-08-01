# Import required libraries
import requests
import sqlalchemy
from sqlalchemy import create_engine, text
import io
import os
import pandas as pd
from datetime import datetime
from update_statcast import get_date_chunks, fast_copy_from
from tqdm import tqdm


def get_transactions_data(start_date, end_date):
    # Build query
    params = {"startDate": start_date, "endDate": end_date, "sportId": 1}
    url = "https://statsapi.mlb.com/api/v1/transactions"
    response = requests.get(url, params=params, timeout=15)
    # Try to extract JSON
    try:
        transactions = response.json().get('transactions', [])
    except Exception as e:
        print(f"[WARNING] Failed to parse transaction data: {e}")
        return pd.DataFrame()

    # Return early if nothing found
    if not transactions:
        print(f"[INFO] No transactions found for {start_date} to {end_date}")
        return pd.DataFrame()
    
    data= []
    for t in transactions:
        date = t.get('date', None)
        typeCode = t.get('typeCode', None)
        typeDesc = t.get('typeDesc', None)
        effective_date = t.get('effectiveDate', None)
        descr = t.get('description', None)
        player = t.get('person', {}).get('fullName', None)
        player_id = t.get('person', {}).get('id', None)
        team = t.get('toTeam', {}).get('name', None)
        data.append([effective_date, date, typeCode, typeDesc,team, player, player_id, descr])
    # Create DataFrame
    df = pd.DataFrame(data, columns=['effective_date', 'date', 'typecode', 'typedesc','team', 'player', 'player_id', 'descr'])
    if df.empty:
        return df
    df['player_id'] = pd.to_numeric(df['player_id'], errors='coerce').astype('Int64')
    return df

def create_transactions_table(start_year, end_date, chunk_size, table_name, engine):
    dtypes = {
    "team": sqlalchemy.Text,
    "descr": sqlalchemy.Text,
    "date": sqlalchemy.Date,
    'effective_date': sqlalchemy.Date,
    "player": sqlalchemy.Text,
    "player_id": sqlalchemy.Integer,
    "typecode": sqlalchemy.Text,
    "typedesc": sqlalchemy.Text,
    }
    first_chunk = True
    for year in range(start_year, end_date.year + 1):
        # Split year into smaller chunks
        date_chunks = get_date_chunks(datetime(year, 1, 1), datetime(year, 12, 31), chunk_size=chunk_size)
        print(f"\n Year: {year}")
        for chunk_start, chunk_end in tqdm(date_chunks, desc=f"fetching: {year}"):
            df = get_transactions_data(chunk_start, chunk_end)
            if not df.empty:
                if first_chunk:
                    # Create or replace table with initial schema
                    df.to_sql(table_name, engine, if_exists="replace",index=False, method="multi", dtype=dtypes)
                    first_chunk = False
                    print(f"Created {table_name} with {len(df)} rows from {chunk_start} to {chunk_end}")
                else:
                    try:
                        # Append data efficiently
                        fast_copy_from(df, table_name, engine)
                        print(f"Inserted {len(df)} rows from {chunk_start} to {chunk_end} in {table_name}")
                    except Exception as e:
                        print(f"[WARNING] fast_copy_from failed: {e}")
    return

if __name__ == "__main__":
    # Load PostgreSQL connection string from environment variable
    DATABASE_URL = os.environ['DATABASE_URL']
    engine = create_engine(DATABASE_URL)

    # Define parameters for data extraction
    start_year = 2015
    end_date = datetime.today()
    chunk_size = 14 # Days per chunk
    table_name = "transactions"
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()
    # Run the IL transaction pipeline
    create_transactions_table(start_year = start_year, end_date = end_date, chunk_size = chunk_size, table_name = table_name, engine=engine)