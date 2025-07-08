import os
import pandas as pd
from pybaseball import statcast
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus
from datetime import date, timedelta
from create_statcast import get_date_chunks

# Load env vars (DB creds)
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

chunk_size = 7
table_name = "statcast"


DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

end_date = date.today() - timedelta(days=1)

with engine.connect() as conn:
    result = conn.execute(text("select max(game_date) from statcast"))
    start_date = result.scalar().date()

if end_date - start_date > timedelta(days=7):
    chunks = get_date_chunks(start_date,end_date)
    for start, end in chunks:
        df = statcast(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if not df.empty:
            df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
else:
    df = statcast(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    if not df.empty:
        df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")