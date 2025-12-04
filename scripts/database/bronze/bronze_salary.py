"""
Bronze-layer ingestion for MLB payroll via Cot's / Google Sheets.

Responsibilities
- I scrape Cot's team pages to collect per-season payroll sheet links.
- I normalize team/division names to the Cot's URL slug format.
- I convert mixed Google links (edit/pubhtml/drive) into direct CSV export URLs.
- I read each sheet, standardize player/salary columns, and tag season/team.
- I write two bronze tables: `bronze.salary` (flattened rows) and `bronze.payroll_urls` (lineage).
"""

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import pandas as pd
import re
from urllib.parse import urlparse, parse_qs
import time
import os
import datetime
import unicodedata
from bronze_statcast import get_baseball_engine

# I reuse the bronze I/O helpers I wrote for Statcast to stage/merge data efficiently.
from bronze_statcast import fast_copy_from, ensure_bronze_tables_from_df,  truncate_staging, merge_staging_into_target


def create_payroll_urls_df(teams_df: pd.DataFrame, current_year: int, start_year: int) -> pd.DataFrame:
    """
    Build a (team, year, url) table by scraping Cot's per-team pages.

    Strategy
    - I compute Cot's slugs from team/division, GET the per-team page, and parse the second table.
    - I take a bounded number of body rows based on (current_year - start_year).
    - I return one row per team-season with the Google Sheets (or published) link.

    Args
    teams_df:      DataFrame with ['name','division_name'] for the target season.
    current_year:  Upper bound year for scraping (I slice rows accordingly).
    start_year:    Lower bound year to determine how far back I reach.

    Returns
    pd.DataFrame with columns ['team','year','url'].
    """
    def _get_payroll_urls(team: str, div: str, current_year: int, start_year: int) -> pd.DataFrame:
        # Cot's pages have multiple tables; empirically the season links live in the 2nd table.
        start_row = 2
        years = current_year - start_year  # I cap how many season rows I pull.
        url = f"https://legacy.baseballprospectus.com/compensation/cots/{div}/{team}/"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "lxml")

        # This index is fragile if the page layout changes, but works today.
        table = soup.find_all("table")[1]

        data = []
        # I skip headers and slice to avoid over-scraping the page.
        for row in table.find_all("tr")[start_row:years + start_row + 1]:
            data.append([
                team.replace("-", " "),                              # keep a human-readable team name
                int(row.find_all("td")[0].get_text()),               # season (first column)
                row.find_all("a")[0].get("href")                     # Google Sheets (or published) link
            ])
        return pd.DataFrame(data, columns=["team", "year", "url"])

    # I normalize team/division to Cot's slug format (e.g., 'new-york-yankees', 'al-east').
    teams_df["formatted_name"] = teams_df["name"].str.replace(" ", "-", regex=False).str.lower()
    replacements = {"American League": "al", "National League": "nl", " ": "-"}  # 'al-east' / 'nl-west'
    teams_df["formatted_div"] = teams_df["division_name"].replace(replacements, regex=True).str.lower()

    # I iterate teams, scrape, and concatenate; I throttle requests to be polite.
    payroll_urls_df = pd.DataFrame()
    for name, div in teams_df[["formatted_name", "formatted_div"]].values:
        payroll_urls_df = pd.concat(
            [payroll_urls_df, _get_payroll_urls(name, div, current_year, start_year)],
            axis=0,
            ignore_index=True
        )
        time.sleep(2)  # throttle requests

    return payroll_urls_df


def salary_person_ids(salary_df, players_df):
    """
    Map salary sheet names to MLB `person_id` using multiple name variants.

    Strategy
    - I generate cleaned variants of the salary player name (with/without 'jr', accents stripped).
    - I build join keys against players_dim columns (fullname, secondary_name, nameascii, first_last).
    - I perform an OR-style set of merges and deduplicate back to the salary natural key.

    Args
    salary_df:  DataFrame with ['player','salary','season','team'] (plus helper columns I add here).
    players_df: Players dimension with person_id and name variants.

    Returns
    pd.DataFrame with ['player','salary','season','team','person_id'].
    """
    def _or_merges(salary_df, players_df, salary_cols, player_cols):
        """I try all (salary_col × player_col) combinations and collapse to unique matches."""
        salary_cols = ['player','player_clean','player_clean_wo_jr']
        player_cols = ['fullname','secondary_name','nameascii','first_last']
        merge_dfs = []
        for s_col in salary_cols:
            for p_col in player_cols:
                merge_dfs.append(
                    pd.merge(
                        salary_df,
                        players_df[['person_id',p_col]],
                        left_on=s_col,
                        right_on=p_col,
                        how='inner'
                    ).drop(p_col,axis=1)
                )
        # I dedupe on the natural key of this bronze table.
        merged = pd.concat(merge_dfs).drop_duplicates(subset=['player','salary','team','season']).reset_index(drop=True)
        merged = merged[['player', 'salary','season','team','person_id']]
        # I left-join the resolved person_id back to the original salary rows.
        return salary_df.merge(merged, on=['player','salary','season','team'], how='left')
    
    def _clean_player_names(player_series: pd.Series) -> pd.Series:
        """
        Clean player names the way I want:
        - I remove nicknames in quotes and any parenthetical text.
        - I collapse whitespace, strip accents to ASCII, and drop periods.
        - I move any 'Jr.' appearing mid-name to a single trailing 'jr'.
        - I keep apostrophes, and I standardize hyphens to spaces.
        """
        def _strip_accents(text: str):
            # I convert to NFKD and drop combining marks; preserves ASCII only.
            if pd.isna(text):
                return text
            return "".join(
                ch for ch in unicodedata.normalize("NFKD", str(text))
                if not unicodedata.combining(ch)
            )

        def _move_jr_to_end(name: str) -> str:
            # If 'Jr' shows up in the middle, I normalize to a single trailing 'jr'.
            if pd.isna(name):
                return name
            parts = str(name).strip().split()
            norm = [p.lower().rstrip('.,') for p in parts]
            has_jr = any(p == 'jr' for p in norm)
            if not has_jr:
                return " ".join(parts).strip()
            kept = [t for t, n in zip(parts, norm) if n != 'jr']
            kept.append('jr')
            return " ".join(kept).strip()

        s = player_series.astype("string")

        s = (
            # I remove text in straight or curly double quotes.
            s.str.replace(r'"[^"]*"', '', regex=True)
            .str.replace(r'“[^”]*”', '', regex=True)
            # I remove any parenthetical text.
            .str.replace(r'\([^)]*\)', '', regex=True)
            # I drop any leftover quote characters.
            .str.replace('"|“|”', '', regex=True)
            # I normalize whitespace.
            .str.replace(r'\s+', ' ', regex=True).str.strip()
            # I strip accents.
            .map(_strip_accents)
            # I tidy again post-accent removal.
            .str.replace(r'\s+', ' ', regex=True).str.strip()
            # I remove periods globally (so 'A.J.' -> 'AJ').
            .str.replace('.', '', regex=False)
            # I move any 'Jr' to the end, once.
            .map(_move_jr_to_end)
            # I do a final whitespace tidy and normalize hyphens.
            .str.replace(r'\s+', ' ', regex=True)
            .str.replace('-',' ').str.strip()
        )

        return s
    
    # I patch common aliases/renames that pop up in salary sheets.
    replacements = {'jon niese':'jonathon niese', 'dan vogelbach':'daniel vogelbach','jake lemoine':'jacob lemoine',\
        'tom milone':'tommy milone','danny otero':'dan otero','felipe rivero':'felipe vázquez','dee gordon':'dee strange-gordon',\
        'matt boyd': 'matthew boyd', 'daniel poncedeleon':'daniel ponce de leon','shed long':'shed long jr.','nori aoki':'norichika aoki',\
        'chase bradford':'chasen bradford','hong-chih kuo':'hung-chih kuo','dany jimeenz':'dany jimenez','seung-hwan oh' : 'seunghwan oh',\
        'nate eovaldi':'nathan eovaldi', 'yuniel escobar':'yunel escobar','dan murphy':'daniel murphy','francisco rodney':'fernando rodney',\
        'byung ho park':'byungho park','mike soroka':'michael soroka','zach britton':'zack britton.', 'kiké hernández':'enrique hernández',\
        'yolmer carlos sanchez':'yolmer sanchez','mike gonzalez':'michael gonzalez','erisbel arruebarruena':'erisbel arruebarrena',\
        'vicente padillia':'vicente padilla','jose martinez':'jose a. martinez','ronald acuna':'ronald acuña jr.','shin soo choo':'shin-soo choo',\
        'robbie ross':'robbie ross jr.','albert almora':'albert almora jr.', 'christian guzman':'cristian guzman','fausto carmona':'roberto hernandez',\
        'bobby howry':'bob howry','kendry morales': 'kendrys morales', 'tony pena':'ramon pena','benji molina':'benjie molina'}

    # Candidate columns for cross-joining names.
    salary_cols = ['player','player_clean','player_clean_wo_jr']
    player_cols = ['fullname','secondary_name','nameascii','first_last']

    # I create a first/last convenience column on the players dimension.
    players_df['first_last'] = players_df['firstname'] + ' ' + players_df['lastname']
    players_df = players_df[['person_id','fullname','secondary_name','nameascii','first_last']].copy()
    for col in player_cols:
        players_df[col] = players_df[col].str.lower().str.strip()

    # I normalize salary names, then create a no-jr variant as a fallback.
    salary_df['player'] = salary_df['player'].str.lower().replace(replacements)
    salary_df['player_clean'] = _clean_player_names(salary_df['player']).str.strip()
    salary_df['player_clean_wo_jr'] = salary_df['player_clean'].str.replace('jr', '').str.strip()

    # I run the OR-style merge and return only the essential columns.
    salary_df = _or_merges(salary_df, players_df, salary_cols, player_cols)
    return salary_df[['player','salary','season','team','person_id']]


def format_name_and_salary(
    df: pd.DataFrame,
    player_column_idx: int,
    salary_column_idx: int,
    season: int,
    team: str
) -> pd.DataFrame:
    """
    Standardize one payroll sheet to: player, salary, season, team.

    Strategy
    - I select player/salary columns by position (the sheet format changes by era).
    - I drop obvious non-player rows and accounting parentheses in salary.
    - I convert "Last, First" → "First Last" and drop a simple middle initial.
    - I strip currency and commas and cast to nullable Int64.
    - I annotate the rows with the season and team.

    Args
    df:                 Raw sheet DataFrame.
    player_column_idx:  Positional index for the player column in the sheet.
    salary_column_idx:  Positional index for the salary column in the sheet.
    season:             Season year to stamp on the rows.
    team:               Team name to stamp on the rows.

    Returns
    Tidy DataFrame with standardized columns and types.
    """
    def _drop_middle_initial(name: str) -> str:
        """'José O. Berríos' → 'José Berríos' (I leave other shapes alone)."""
        if not isinstance(name, str):
            return name
        parts = name.strip().split()
        if len(parts) == 3 and parts[1].endswith(".") and len(parts[1]) == 2:
            return f"{parts[0]} {parts[2]}"
        return name

    # I keep only the two columns I need and rename them.
    df = df.iloc[:, [player_column_idx, salary_column_idx]]
    df = df.rename(columns={df.columns[0]: "player", df.columns[1]: "salary"})

    # I drop missing players and switch to string so .str operations are safe.
    df = df[~df["player"].isna()]
    df = df[["player", "salary"]].astype(str)

    # I remove obvious non-player lines (digits in the name usually means totals/headers).
    df = df[~df["player"].str.contains(r"[0-9]", na=False)]

    # I convert accounting negatives "(1,234)" → "-1,234".
    df["salary"] = df["salary"].str.replace("(", "-").str.replace(")", "")

    # I require "Last, First"; then I normalize to "First Last".
    df = df[df["player"].str.contains(",", regex=True, na=False)]
    df["player"] = df["player"].astype("string").str.replace("*", "", regex=False)
    df["player"] = df["player"].apply(lambda x: " ".join(reversed(x.split(", ")))).str.replace("  ", " ")
    df["player"] = df["player"].map(_drop_middle_initial)

    # I coerce salary to nullable Int64 after stripping $ and commas.
    df["salary"] = pd.to_numeric(
        df["salary"].str.replace(r"\$|,", "", regex=True),
        errors="coerce"
    ).astype("Int64")

    # I annotate with season and team.
    df["season"] = season
    df["team"] = team
    return df


def sheets_csv_url(u: str) -> str:
    """
    Normalize Google Sheets/Drive links into a direct CSV export URL.

    Strategy
    - If it's already an export CSV, I no-op.
    - I convert Drive 'open?id=...' and 'file/d/...' into Sheets '/edit'.
    - I convert '/pubhtml' → '/pub?output=csv' (scraping gid when available).
    - I flip '?output=html' → '?output=csv' for published links.
    - I extract the spreadsheet id and preserve gid from query/fragment if present.
    - I build 'https://docs.google.com/spreadsheets/d/<id>/export?format=csv[&gid=...]'.

    Args
    u:  Original link (any edit/pubhtml/drive variant).

    Returns
    CSV export URL; if pattern unrecognized, I return the input unchanged.
    """
    def _csv_url_from_pubhtml(url: str) -> str:
        # If the link is a published HTML page, I fetch once to extract gid and return published CSV.
        UA = {"User-Agent": "Mozilla/5.0"}
        if "/pubhtml" not in url:
            return url
        h = requests.get(url, headers=UA, timeout=20).text
        m = re.search(r'(?:gid=|data-gid=")(\d+)', h)
        gid = m.group(1) if m else "0"
        return f"{url.replace('/pubhtml','/pub').split('?')[0]}?output=csv&gid={gid}"

    u = (u or "").strip()
    if not u:
        return u
    # Already a CSV export? I return as-is.
    if "/export?format=csv" in u or "/pub?output=csv" in u:
        return u

    # Drive links → I normalize to a standard Sheets URL.
    m_drive = re.search(r"drive\.google\.com/(?:open\?id=|file/d/)([A-Za-z0-9-_]+)", u)
    if m_drive:
        u = f"https://docs.google.com/spreadsheets/d/{m_drive.group(1)}/edit"

    # Published HTML → I return the published CSV, scraping gid if needed.
    if "/pubhtml" in u:
        return _csv_url_from_pubhtml(u)

    # Published link with output=html → I switch to csv.
    if "output=html" in u:
        u = u.replace("output=html", "output=csv")

    # I extract the spreadsheet id; if it's not a recognizable Sheets URL, I bail.
    m = re.search(r"/spreadsheets/d/([A-Za-z0-9-_]+)", u)
    if not m:
        return u
    doc_id = m.group(1)

    # I preserve gid from query or fragment if available.
    parsed = urlparse(u)
    gid = parse_qs(parsed.query).get("gid", [None])[-1]
    if not gid and parsed.fragment:
        fm = re.search(r"gid=(\d+)", parsed.fragment)
        gid = fm.group(1) if fm else None

    # I build the CSV export URL (append gid when present).
    base = f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv"
    return f"{base}&gid={gid}" if gid else base


def create_salary_table(
    engine,
    teams_df: pd.DataFrame,
    players_df: pd.DataFrame,
    current_year: int,
    start_year: int,
    base: str,
    schema: str,
    primary_keys: list):
    """
    End-to-end bronze creation for salary data from Cot's / Google Sheets.

    Steps
      1) I build (team, year, url) from Cot's per-team pages.
      2) I normalize mixed Google links into direct CSV export URLs (and persist lineage).
      3) I read each season sheet and standardize player/salary fields
         - Pre-2020 sheets use one set of column positions.
         - 2020+ sheets use another set of column positions.
      4) I concatenate rows across teams/seasons and resolve person_id.
      5) I bulk-stage, MERGE into bronze target, and truncate staging.

    Args
    engine:        SQLAlchemy engine.
    teams_df:      Current-season `bronze.teams` (name, division_name).
    players_df:    Players dimension (for name → person_id resolution).
    current_year:  Upper bound for scraping.
    start_year:    Lower bound for scraping.
    base:          Base table name ('salary').
    schema:        Schema name ('bronze').
    primary_keys:  Natural key columns for this bronze table.

    Returns
    None (I write tables and exit).
    """
    # Column positions differ by era; I set them explicitly.
    old_format_player_idx = 0
    old_format_salary_idx = 5
    new_format_player_idx = 0
    new_format_salary_idx = 12

    # 1) I scrape Cot's for team-season URLs.
    payroll_urls_df = create_payroll_urls_df(teams_df, current_year, start_year)

    # 2) I normalize to CSV export URLs and persist lineage for transparency.
    payroll_urls_df["csv_url"] = payroll_urls_df["url"].apply(sheets_csv_url)
    payroll_urls_df.to_sql(name="payroll_urls", con=engine, schema="bronze", if_exists="replace")
    
    # 3) I read, standardize, and append.
    salary_df = pd.DataFrame()
    for team, season, csv_url in payroll_urls_df[["team", "year", "csv_url"]].values:
        # If a link returns HTML (permissions / wrong gid), pd.read_csv will error; I can add a fallback later.
        df_raw = pd.read_csv(csv_url)

        if season <= 2019:
            df_tidy = format_name_and_salary(
                df_raw,
                player_column_idx=old_format_player_idx,
                salary_column_idx=old_format_salary_idx,
                season=season,
                team=team,
            )
        else:
            df_tidy = format_name_and_salary(
                df_raw,
                player_column_idx=new_format_player_idx,
                salary_column_idx=new_format_salary_idx,
                season=season,
                team=team,
            )
        salary_df = pd.concat([salary_df, df_tidy], axis=0, ignore_index=True)

    # I attach MLB person_ids using my resolver.
    salary_df = salary_person_ids(salary_df, players_df) 

    # I defensively dedupe on the natural key.
    salary_df = salary_df.drop_duplicates(subset=['player','salary','season','team'])

    # I ensure tables exist, stage, MERGE, and clear staging.
    ensure_bronze_tables_from_df(salary_df, base=base, schema=schema, engine=engine, primary_keys=primary_keys)
    fast_copy_from(salary_df, schema, base, engine)
    merge_staging_into_target(schema, base, engine, primary_keys, salary_df)
    truncate_staging(engine, schema,base)
    return


if __name__ == "__main__":
    # I load the PostgreSQL connection string from the environment (docker-compose/.env).
    engine = get_baseball_engine()

    # I define the scrape window.
    start_year = 2010
    current_year = datetime.datetime.today().year
    base = 'salary'
    schema = 'bronze'
    primary_keys = ['player','salary','team','season']

    # I smoke-test DB connectivity before extracting.
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()

    # I pull current-season teams to drive scraping scope.
    teams_query = "select name, id, division_name from bronze.teams where season = (select max(season) from bronze.teams);"
    teams_df = pd.read_sql_query(teams_query, engine)

    # I read the players dimension to power name → person_id mapping.
    players_df = pd.read_sql_table('players', engine, schema='silver')

    # I kick off ingestion.
    create_salary_table(engine, teams_df, players_df,current_year=current_year, start_year=start_year, base=base, schema=schema, primary_keys=primary_keys)
