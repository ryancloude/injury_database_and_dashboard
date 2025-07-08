#Loads necessary packages
from pybaseball import statcast
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import time
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

#loads .env variables
load_dotenv()

#establishes db connection
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

#config
start_year = 2015
end_year = datetime.today().year
chunk_size = 7
table_name = "statcast"

#create list of date ranges to use in the statcast function
#inputs: start_date is the first date of the statcast data, end_date is the last date of statcast data, chunk_size is how many days
#of data to pull at once
#Output: list of tuples tuples with the first value being start date and second value being the end date of each chunk
def get_date_chunks(start_date, end_date, chunk_size):
    chunks = []
    current_date = start_date
    while current_date <= end_date:
        #chunk_end will be 6 days after current_date or end_date, whichever occurs first
        chunk_end = min(current_date + timedelta(days=chunk_size-1), end_date)
        #appends the tuple onto chunks
        chunks.append((current_date.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
        #adds chunk_sizes days to current_date
        current_date += timedelta(days=chunk_size)
    return chunks

if __name__ == "__main__":
    #checks sql connection
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()
    #loops through each year from start year to the current year

    for year in range(start_year, end_year + 1):
        #prints the year that is being processed
        print(f"\n Year: {year}")
        #gets date chunks for the year being proccessed
        date_chunks = get_date_chunks(datetime(year, 3, 1), datetime(year, 11, 30),chunk_size)
        
        for start_date, end_date in tqdm(date_chunks, desc=f"fetching {year}"):
            try:
                df = statcast(start_dt=start_date, end_dt=end_date)
                if not df.empty:
                    df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
            except Exception as e:
                print(f"Failed for {start_date} to {end_date}: {e}")
                time.sleep(2)