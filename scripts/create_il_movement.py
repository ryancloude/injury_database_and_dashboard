# Import required libraries
import requests
import sqlalchemy
from sqlalchemy import create_engine, text
import io
import os
import pandas as pd
from datetime import datetime
import spacy
from spacy.pipeline import EntityRuler
from update_statcast import get_date_chunks, fast_copy_from
from tqdm import tqdm




def setup_injury_parser():
    """
    Build a ready-to-use spaCy NLP pipeline for parsing injury descriptions.
    Returns:
        nlp: spaCy NLP pipeline with custom EntityRuler
        parse_injury_text: function(text) -> dict with structured fields
    """
    # Dictionary to normalize different injury terms into canonical labels
    injury_normalizer = {
        "epicondylitis":"inflammation",
        "stiffness": "stiffness",
        "spasms":"spasms",
        "fatigue":"fatigue",
        "broken":" broken",
        "blister":"blister",
        "reconstruction":"surgery",
        "replacement":"surgery",
        "tommy john":"surgery",
        "surgery":"surgery",
        "bruise":"bruise",
        "contusion":"bruise",
        "impingement":"impingement",
        "strain": "strain",
        "strained": "strain",
        "sprain": "sprain",
        "sprained": "sprain",
        "fracture": "fracture",
        "fractured": "fracture",
        "rupture":"tear",
        "tear": "tear",
        "torn": "tear",
        "irritation":"inflammation",
        "inflammation": "inflammation",
        "inflamed": "inflammation",
        "tendinitis": "tendinitis",
        "tendonitis": "tendinitis",
        "bursitis": "bursitis",
        "tightness": "tightness",
        "soreness": "soreness",
        "pain": "pain",
        "discomfort": "discomfort",
        "dislocation": "dislocation",
        "concussion": "concussion"
    }

    # Dictionary to normalize different body part names
    body_part_normalizer = {
        # Upper body
        'head':'head',
        "shoulder": "shoulder",
        "rotator cuff": "shoulder",
        "ac joint": "shoulder",
        "labrum": "shoulder",
        "teres major":"shoulder",
        "scapular":"shoulder",

        "elbow": "elbow",
        "forearm": "elbow",
        "flexor":"elbow",
        "ucl":"elbow",
        "ulnar collateral ligament":"elbow",
        "tommy john":"elbow",
        
        "wrist": "wrist",
        "hand": "hand",
        "finger": "hand",
        "thumb": "hand",
        
        "nerve":"nerve",
        "biceps": "biceps",
        "triceps": "triceps",
        "pec": "chest",
        "pectoral": "chest",
        "abdomen":"oblique",
        "oblique": "oblique",
        "costochondral":"rib",
        "ribcage":"rib",
        "rib": "rib", 
        "back": "back",
        "neck": "neck",
        "lat":"lat",
        
        # Lower body
        "hamstring": "hamstring",
        "groin": "groin",
        "quad": "quad",
        "quadriceps": "quad",
        "hip": "hip",
        "adductor":"hip",
        "abductor":"hip",
        "glute": "glute", 
        "calf": "calf",
        "achilles": "achilles",
        "acl":"achilles",
        "knee":"knee",
        "mcl":"knee",
        "ankle": "ankle",
        "foot": "foot",
        "toe": "foot",
        "plantar fascia": "foot"
    }
    

    # Build lists of recognized terms
    injury_terms = list(injury_normalizer.keys())
    body_parts = list(body_part_normalizer.keys())
    sides = ["left", "right"]

    # ✅ Load spaCy and add EntityRuler
    nlp = spacy.load("en_core_web_sm")
    ruler = nlp.add_pipe("entity_ruler", before="ner")

    # Create patterns for each term
    patterns = []
    # Sides
    for s in sides:
        patterns.append({"label": "SIDE", "pattern": s})
    # Body parts
    for bp in body_parts:
        patterns.append({"label": "BODY_PART", "pattern": bp})
    # Injury types
    for it in injury_terms:
        patterns.append({"label": "INJURY_TYPE", "pattern": it})

    # Define function to apply parser to text
    def parse_injury_text(text: str):
        doc = nlp(text.lower())
        side, body_part, injury_type = None, None, None

        for ent in doc.ents:
            if ent.label_ == "SIDE":
                side = ent.text
            elif ent.label_ == "BODY_PART":
                body_part = body_part_normalizer.get(ent.text, ent.text)
            elif ent.label_ == "INJURY_TYPE":
                injury_type = injury_normalizer.get(ent.text, ent.text)

        return {
            "side": side,
            "body_part": body_part,
            "injury_type": injury_type
            }

    ruler.add_patterns(patterns)
    return nlp, parse_injury_text

def get_il_data(start_date, end_date, parser):
    """
    Query MLB Stats API for injured list transactions between start_date and end_date.
    Filters to only include IL placements (not transfers or activations).
    Applies NLP parser to extract injury metadata from the description.
    Returns:
        DataFrame with parsed IL transactions.
    """

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
        effective_date = t.get('effectiveDate', None)
        descr = t.get('description', None)
        if descr is None:
            continue
        # Extract IL placement rows only
        if any(x in descr.lower() for x in ["injured list", "disabled list"]) and not any(x in descr.lower() for x in ["transferred", "activated"]):
            player = t.get('person', {}).get('fullName', None)
            player_id = t.get('person', {}).get('id', None)
            team = t.get('toTeam', {}).get('name', None)
            if player is None or player_id is None:
                continue
            data.append([effective_date, date, team, player, player_id,descr])
    # Create DataFrame
    df = pd.DataFrame(data, columns=['effective_date', 'date', 'team', 'player', 'player_id','descr'])
    if df.empty:
        return df
    # Choose resolved date from effective or log date
    df['date'] = df.apply(lambda row: row.date if row.date == row.effective_date else row.effective_date, axis=1)
    df = df.drop(columns=['effective_date'])
    # Apply NLP parser
    injury_parsed = df["descr"].apply(parser)  # Series of dicts
    injury_df = pd.DataFrame(injury_parsed.tolist())
    df[['injury_side','injury_body_part','injury_type']] = injury_df[['side','body_part','injury_type']]
    # Score each row based on how many injury fields are not null
    df["injury_score"] = df[["injury_side", "injury_body_part", "injury_type"]].notnull().sum(axis=1)
    # Keep only the row with the highest injury_score per player/date
    df = df.sort_values("injury_score", ascending=False).drop_duplicates(subset=["player", "date"])
    # Drop the temporary score column
    df = df.drop(columns=["injury_score"])
    #Drops rows where date is before 2015
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'] > datetime(2015, 1, 1)]



    return df



def create_il_movement(start_year, end_date, chunk_size, table_name, engine):
    """
    Main ingestion function that loops over years and time chunks,
    downloads IL data, parses it, and loads it into the PostgreSQL table.
    """
    # Define column types for SQL table
    dtypes = {
    "team": sqlalchemy.Text,
    "descr": sqlalchemy.Text,
    "date": sqlalchemy.Date,
    "player": sqlalchemy.Text,
    "player_id": sqlalchemy.Integer,
    "injury_side": sqlalchemy.Text,
    "injury_type": sqlalchemy.Text,
    "injury_body_part":sqlalchemy.Text
    }

    # Load NLP pipeline
    nlp, parse_injury_text = setup_injury_parser()
    
    first_chunk = True
    for year in range(start_year, end_date.year + 1):
        # Split year into smaller chunks
        date_chunks = get_date_chunks(datetime(year, 1, 1), datetime(year, 12, 31), chunk_size=chunk_size)
        print(f"\n Year: {year}")
        for chunk_start, chunk_end in tqdm(date_chunks, desc=f"fetching: {year}"):
            df = get_il_data(chunk_start, chunk_end, parse_injury_text)
            if not df.empty:
                if first_chunk:
                    # Create or replace table with initial schema
                    df.to_sql(table_name, engine, if_exists="replace",index=False, method="multi",dtype=dtypes)
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
    table_name = "il_mov"
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()
    # Run the IL ingestion pipeline
    create_il_movement(start_year = start_year, end_date = end_date, chunk_size = chunk_size, table_name = table_name, engine=engine)