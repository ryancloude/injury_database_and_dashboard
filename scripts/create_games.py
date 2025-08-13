# Import required libraries
import requests
import sqlalchemy
from sqlalchemy import create_engine, text
import io
import os
import pandas as pd
from datetime import datetime
from create_statcast import get_date_chunks, fast_copy_from
from tqdm import tqdm
import time


def get_games_data(start_date, end_date):
    # Build query
    params = {"startDate": start_date, "endDate": end_date, 'sportId':1}
    url = "https://statsapi.mlb.com/api/v1/schedule"
    response = requests.get(url, params=params, timeout=60)
    # Try to extract JSON
    try:
        dates = response.json().get('dates', [])
    except Exception as e:
        print(f"[WARNING] Failed to parse schedule data: {e}")
        return pd.DataFrame()

    # Return early if nothing found
    if not dates:
        print(f"[INFO] No games found for {start_date} to {end_date}")
        return pd.DataFrame()
    
    data= []
    for date in dates:
        game_date = date['date']
        for game in date.get('games',[]):
            state = game['status']['detailedState']
            game_type = game['gameType']
            away_team = game['teams']['away']['team']['name']
            away_score = game['teams']['away'].get('score',None)
            home_team = game['teams']['home']['team']['name']
            home_score = game['teams']['home'].get('score',None)
            data.append([game_date, game_type, away_team, away_score, home_team, home_score, state])
    # Create DataFrame
    df = pd.DataFrame(data, columns=['game_date', 'game_type', 'away_team', 'away_score','home_team', 'home_score', 'state'])
    df['home_score'] = pd.to_numeric(df['home_score'], errors='coerce').astype('Int64')
    df['away_score'] = pd.to_numeric(df['away_score'], errors='coerce').astype('Int64')
    if df.empty:
        return df
    return df

def create_games_table(start_year, end_date, chunk_size, table_name, engine):
    dtypes = {
    "game_date": sqlalchemy.Date,
    "game_type": sqlalchemy.Text,
    "away_team": sqlalchemy.Text,
    "away_score": sqlalchemy.Integer,
    "home_team": sqlalchemy.Text,
    "gome_score": sqlalchemy.Integer,
    "state": sqlalchemy.Text
    }
    first_chunk = True
    for year in range(start_year, end_date.year + 1):
        # Split year into smaller chunks
        if end_date.year ==  year:
            date_chunks = get_date_chunks(datetime(year, 2, 1), end_date, chunk_size=chunk_size)
        else:
            date_chunks = get_date_chunks(datetime(year, 2, 1), datetime(year, 11, 30), chunk_size=chunk_size)
        print(f"\n Year: {year}")
        for chunk_start, chunk_end in tqdm(date_chunks, desc=f"fetching: {year}"):
            df = get_games_data(chunk_start, chunk_end)
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
    chunk_size = 15 # Days per chunk
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
    # Run the games pipeline
    create_games_table(start_year = start_year, end_date = end_date, chunk_size = chunk_size, table_name = table_name, engine=engine)