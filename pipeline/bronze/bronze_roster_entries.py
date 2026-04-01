import pandas as pd
import requests
from sqlalchemy import inspect, text

from bronze_statcast import (
    ensure_bronze_tables_from_df,
    fast_copy_from,
    get_baseball_engine,
    merge_staging_into_target,
    truncate_staging,
)


def _normalize_player_ids(values) -> list[str]:
    """
    Convert a mixed list/Series of ids into clean string ids, dropping nulls and decimals.
    """
    series = pd.Series(values, dtype="object")
    series = series.dropna()
    series = pd.to_numeric(series, errors="coerce").dropna().astype("Int64").astype(str)
    return series.tolist()


def get_roster_entries(players, teams, primary_keys):
    players = _normalize_player_ids(players)
    if not players:
        return pd.DataFrame()

    players_ids = ",".join(players)
    params = {"hydrate": "rosterEntries", "personIds": players_ids}
    url = "https://statsapi.mlb.com/api/v1/people"
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    try:
        data = response.json()
    except Exception as exc:
        print(f"[WARNING] Failed to parse data: {exc}")
        return pd.DataFrame()

    all_rows = []
    for person in (data.get("people") or []):
        pid = person.get("id")
        for entry in (person.get("rosterEntries") or []):
            team = entry.get("team") or {}
            status = entry.get("status") or {}
            row = {
                "person_id": pid,
                "is_active": entry.get("isActive"),
                "team_id": team.get("id"),
                "start_date": entry.get("startDate"),
                "status_date": entry.get("statusDate"),
                "end_date": entry.get("endDate"),
                "status_code": status.get("code"),
                "status_desc": status.get("description"),
                "parent_org_id": team.get("parentOrgId"),
            }
            if row["team_id"] in teams or row["status_code"] == "RA" or row["team_id"] is None:
                all_rows.append(row)

    df = pd.DataFrame(
        all_rows,
        columns=[
            "person_id",
            "team_id",
            "start_date",
            "status_date",
            "end_date",
            "is_active",
            "status_code",
            "status_desc",
            "parent_org_id",
        ],
    )

    column_dtypes = {
        "person_id": "Int64",
        "is_active": "boolean",
        "team_id": "Int64",
        "status_code": "string",
        "status_desc": "string",
        "parent_org_id": "Int64",
    }

    if not df.empty:
        df = df.drop_duplicates(subset=primary_keys, keep="last")
        df = df.astype(column_dtypes)
        df[["start_date", "status_date", "end_date"]] = df[
            ["start_date", "status_date", "end_date"]
        ].apply(pd.to_datetime, errors="coerce")

    return df


def create_roster_entries(players, teams, chunk_size, base, schema, engine, primary_keys):
    players = _normalize_player_ids(players)
    first_chunk = True

    for i in range(0, len(players), chunk_size):
        roster_df = get_roster_entries(players[i:i + chunk_size], teams, primary_keys=primary_keys)
        if roster_df.empty:
            continue

        if first_chunk:
            ensure_bronze_tables_from_df(
                roster_df,
                base=base,
                schema=schema,
                engine=engine,
                primary_keys=primary_keys,
            )
            first_chunk = False

        fast_copy_from(roster_df, schema, base, engine)
        merge_staging_into_target(
            base=base,
            engine=engine,
            primary_keys=primary_keys,
            columns=list(roster_df.columns),
            schema=schema,
        )
        print(f"Merged {len(roster_df)} rows")
        truncate_staging(engine, schema=schema, base=base)


if __name__ == "__main__":
    engine = get_baseball_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as exc:
        print("Failed to connect to PostgreSQL:")
        print(exc)
        raise SystemExit(1)

    base = "roster_entries"
    schema = "bronze"
    table_exists = inspect(engine).has_table(table_name=base, schema=schema)

    if table_exists:
        players_query = """
        select distinct person_id as id
        from bronze.transactions
        where date >= (
            select max(status_date)::date - INTERVAL '7 days'
            from bronze.roster_entries
        );
        """
    else:
        players_query = "select distinct id from bronze.players;"

    players = pd.read_sql_query(players_query, engine)["id"].tolist()

    teams_query = "select distinct id from bronze.teams;"
    teams = pd.read_sql_query(teams_query, engine)["id"].tolist()

    chunk_size = 150
    primary_keys = ["person_id", "team_id", "start_date"]

    create_roster_entries(players, teams, chunk_size, base, schema, engine, primary_keys)
