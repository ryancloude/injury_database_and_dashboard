import os
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine, text
import requests
from bronze_statcast import ensure_bronze_tables_from_df, truncate_staging, fast_copy_from, merge_staging_into_target

def get_roster_entries(players, teams, primary_keys):
    players_ids = ",".join(players)
    params = {"hydrate": "rosterEntries", "personIds": players_ids}
    url = "https://statsapi.mlb.com/api/v1/people"
    response = requests.get(url, params=params, timeout=60)
    # Try to extract JSON
    try:
        data = response.json()
    except Exception as e:
        print(f"[WARNING] Failed to parse data: {e}")
        return pd.DataFrame()

    all_rows = []
    for person in (data.get("people") or []):
        pid = person.get("id")
        for e in (person.get("rosterEntries") or []):
            row = {
                    "person_id": pid,
                    "is_active": e.get("isActive") or None,
                    "team_id": e.get("team" or {}).get("id") or None,
                    "start_date": e.get("startDate"),
                    "status_date": e.get("statusDate") or None,
                    "end_date": e.get("endDate") or None,
                    "status_code": (e.get("status") or {}).get("code"),
                    "status_desc": (e.get("status") or {}).get("description"),
                    "parent_org_id": e.get("team" or {}).get("parentOrgId") or None,
                }
            if row["team_id"] in teams or row["status_code"] == "RA" or row["team_id"] is None:
                all_rows.append(row)

    df = pd.DataFrame(all_rows, columns=[
        "person_id", "team_id", "start_date", "status_date", "end_date", "is_active",
        "status_code", "status_desc", "parent_org_id"])
    if not df.empty:
        # de-dupe on the natural stint key
        df = df.drop_duplicates(subset=primary_keys, keep="last")
    return df

def create_roster_entries(players, teams, chunk_size, base, schema, engine, primary_keys):
    first_chunk = True
    column_dtypes = {'person_id':"int",
                     'is_active':"boolean",
                     "team_id":"Int64",
                     "status_code":"str",
                     "status_desc":"str",
                     "parent_org_id":"Int64"
    }
    for i in range(0, len(players), chunk_size):
        roster_df = get_roster_entries(players[i:i+chunk_size], teams, primary_keys=primary_keys)
        if roster_df.empty:
            continue
        roster_df = roster_df.astype(column_dtypes)
        roster_df[['start_date', 'status_date','end_date']] = roster_df[['start_date', 'status_date','end_date']].apply(\
            pd.to_datetime, errors='coerce')
        if first_chunk:
            ensure_bronze_tables_from_df(roster_df, base=base, schema=schema, engine=engine, primary_keys=primary_keys)
            first_chunk = False
        fast_copy_from(roster_df, schema, base, engine)
        merge_staging_into_target(base=base, engine=engine, primary_keys=primary_keys, \
                                              columns=list(roster_df.columns), schema=schema)
        truncate_staging(engine, schema=schema, base=base)
    return
        
if __name__ == "__main__":
    BASEBALL_URL = os.environ["BASEBALL_URL"]
    engine = create_engine(BASEBALL_URL)
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()
    players_query = "select distinct id from bronze.players;"
    players = pd.read_sql_query(players_query, engine)
    players = players['id'].astype(str).tolist()
    teams_query= "select distinct id from bronze.teams;"
    teams = pd.read_sql_query(teams_query, engine)
    teams = teams['id'].tolist()

    chunk_size = 150
    base = 'roster_entries'
    schema = 'bronze'
    primary_keys = ['person_id', 'status_code', 'team_id','start_date']
    create_roster_entries(players, teams, chunk_size, base, schema, engine, primary_keys)