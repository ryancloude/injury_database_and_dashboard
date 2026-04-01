"""
Bronze-layer ingestion for MLB payroll via Cot's / Google Sheets.

Responsibilities
-scrape Cot's team pages to collect per-season payroll sheet links.
-normalize team/division names to the Cot's URL slug format.
-convert mixed Google links (edit/pubhtml/drive) into direct CSV export URLs.
-read each sheet, standardize player/salary columns, and tag season/team.
-write two bronze tables: `bronze.salary` (flattened rows) and `bronze.payroll_urls` (lineage).
"""

import datetime
import io
import re
import time
import unicodedata
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from bronze_statcast import (
    ensure_bronze_tables_from_df,
    fast_copy_from,
    get_baseball_engine,
    merge_staging_into_target,
    truncate_staging,
)


def create_payroll_urls_df(teams_df: pd.DataFrame, current_year: int, start_year: int) -> pd.DataFrame:
    """
    Build (team, year, url) payroll-sheet lineage from Cot's team pages.

    Behavior
    - For years <= 2025: use the historical payroll table rows on each team page.
    - For years >= 2026: discover team pages from the Cot's index page, then pull the
      top-of-page "2026-30 payroll obligations" / "tracker" style links instead of
      relying on the empty 2026 payroll-table row.
    """
    index_url = "https://legacy.baseballprospectus.com/compensation/cots/"
    index_cutoff_year = 2026
    user_agent = {"User-Agent": "Mozilla/5.0"}

    work = teams_df.copy()
    work["team_name"] = work["name"].str.lower().str.strip()
    work["formatted_name"] = work["name"].str.replace(" ", "-", regex=False).str.lower()
    replacements = {"American League": "al", "National League": "nl", " ": "-"}
    work["formatted_div"] = work["division_name"].replace(replacements, regex=True).str.lower()

    session = requests.Session()
    session.headers.update(user_agent)

    def _build_index_team_page_map() -> dict:
        out = {}
        try:
            r = session.get(index_url, timeout=30)
            r.raise_for_status()
        except Exception:
            return out

        soup = BeautifulSoup(r.text, "lxml")
        pattern = re.compile(r"/compensation/cots/(american|national)-league/[^/]+/?$")

        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if not pattern.search(href):
                continue
            abs_url = requests.compat.urljoin(index_url, href)
            slug = abs_url.rstrip("/").split("/")[-1].lower().strip()
            if slug and slug not in out:
                out[slug] = abs_url

        return out

    def _parse_historical_table(team_display: str, page_url: str, min_year: int, max_year: int) -> list:
        rows = []
        try:
            r = session.get(page_url, timeout=30)
            r.raise_for_status()
        except Exception:
            return rows

        soup = BeautifulSoup(r.text, "lxml")
        tables = soup.find_all("table")
        if len(tables) < 2:
            return rows

        table = tables[1]
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue

            year_txt = tds[0].get_text(strip=True)
            if not year_txt.isdigit():
                continue

            year = int(year_txt)
            if year < min_year or year > max_year:
                continue

            link = tr.find("a", href=True)
            if link is None:
                continue

            rows.append([team_display, year, link.get("href")])

        return rows

    def _parse_projected_links(team_display: str, page_url: str, min_year: int, max_year: int) -> list:
        """
        For 2026+ pages, Cot's often places links like:
        - 2026-30 payroll obligations
        - 2026-30 payroll and tax tracker
        near the top of the page, outside the year-by-year payroll table.
        """
        rows = []
        try:
            r = session.get(page_url, timeout=30)
            r.raise_for_status()
        except Exception:
            return rows

        soup = BeautifulSoup(r.text, "lxml")
        seen_years = set()

        for anchor in soup.find_all("a", href=True):
            text_value = anchor.get_text(" ", strip=True).lower()
            href = anchor.get("href")

            if not href:
                continue
            if "docs.google.com" not in href and "google.com" not in href:
                continue
            if "payroll" not in text_value and "tracker" not in text_value and "obligation" not in text_value:
                continue

            year_match = re.search(r"\b(20\d{2})\b", text_value)
            if not year_match:
                continue

            year = int(year_match.group(1))
            if year < min_year or year > max_year:
                continue
            if year in seen_years:
                continue

            seen_years.add(year)
            rows.append([team_display, year, href])

        return rows

    index_team_pages = _build_index_team_page_map()
    collected = []

    for team_slug, div_slug, team_display in work[["formatted_name", "formatted_div", "team_name"]].values:
        legacy_url = f"https://legacy.baseballprospectus.com/compensation/cots/{div_slug}/{team_slug}/"
        team_page_url = index_team_pages.get(team_slug, legacy_url)

        legacy_end = min(current_year, index_cutoff_year - 1)
        if start_year <= legacy_end:
            collected.extend(_parse_historical_table(team_display, legacy_url, start_year, legacy_end))

        modern_start = max(start_year, index_cutoff_year)
        if modern_start <= current_year:
            collected.extend(_parse_projected_links(team_display, team_page_url, modern_start, current_year))

        time.sleep(1)

    payroll_urls_df = pd.DataFrame(collected, columns=["team", "year", "url"])
    if payroll_urls_df.empty:
        return pd.DataFrame(columns=["team", "year", "url"])

    payroll_urls_df = payroll_urls_df.dropna(subset=["team", "year", "url"])
    payroll_urls_df["year"] = payroll_urls_df["year"].astype(int)
    payroll_urls_df = (
        payroll_urls_df.sort_values(["team", "year", "url"])
        .drop_duplicates(subset=["team", "year"], keep="first")
        .reset_index(drop=True)
    )
    return payroll_urls_df


def salary_person_ids(salary_df: pd.DataFrame, players_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map salary sheet names to MLB `person_id` using multiple name variants.
    """

    def _or_merges(salary_df: pd.DataFrame, players_df: pd.DataFrame, salary_cols: list, player_cols: list) -> pd.DataFrame:
        merge_dfs = []
        for s_col in salary_cols:
            for p_col in player_cols:
                merge_dfs.append(
                    pd.merge(
                        salary_df,
                        players_df[["person_id", p_col]],
                        left_on=s_col,
                        right_on=p_col,
                        how="inner",
                    ).drop(p_col, axis=1)
                )

        merged = pd.concat(merge_dfs).drop_duplicates(subset=["player", "salary", "team", "season"]).reset_index(drop=True)
        merged = merged[["player", "salary", "season", "team", "person_id"]]
        return salary_df.merge(merged, on=["player", "salary", "season", "team"], how="left")

    def _clean_player_names(player_series: pd.Series) -> pd.Series:
        def _strip_accents(value: str):
            if pd.isna(value):
                return value
            return "".join(
                ch for ch in unicodedata.normalize("NFKD", str(value))
                if not unicodedata.combining(ch)
            )

        def _move_jr_to_end(name: str) -> str:
            if pd.isna(name):
                return name

            parts = str(name).strip().split()
            norm = [p.lower().rstrip(".,") for p in parts]
            has_jr = any(p == "jr" for p in norm)
            if not has_jr:
                return " ".join(parts).strip()

            kept = [token for token, normalized in zip(parts, norm) if normalized != "jr"]
            kept.append("jr")
            return " ".join(kept).strip()

        cleaned = player_series.astype("string")
        cleaned = (
            cleaned.str.replace(r'"[^"]*"', "", regex=True)
            .str.replace(r"\([^)]*\)", "", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .map(_strip_accents)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.replace(".", "", regex=False)
            .map(_move_jr_to_end)
            .str.replace(r"\s+", " ", regex=True)
            .str.replace("-", " ", regex=False)
            .str.strip()
        )
        return cleaned

    replacements = {
        "jon niese": "jonathon niese",
        "dan vogelbach": "daniel vogelbach",
        "jake lemoine": "jacob lemoine",
        "tom milone": "tommy milone",
        "danny otero": "dan otero",
        "felipe rivero": "felipe vazquez",
        "dee gordon": "dee strange-gordon",
        "matt boyd": "matthew boyd",
        "daniel poncedeleon": "daniel ponce de leon",
        "shed long": "shed long jr.",
        "nori aoki": "norichika aoki",
        "chase bradford": "chasen bradford",
        "hong-chih kuo": "hung-chih kuo",
        "dany jimeenz": "dany jimenez",
        "seung-hwan oh": "seunghwan oh",
        "nate eovaldi": "nathan eovaldi",
        "yuniel escobar": "yunel escobar",
        "dan murphy": "daniel murphy",
        "francisco rodney": "fernando rodney",
        "byung ho park": "byungho park",
        "mike soroka": "michael soroka",
        "zach britton": "zack britton.",
        "kike hernandez": "enrique hernandez",
        "yolmer carlos sanchez": "yolmer sanchez",
        "mike gonzalez": "michael gonzalez",
        "erisbel arruebarruena": "erisbel arruebarrena",
        "vicente padillia": "vicente padilla",
        "jose martinez": "jose a. martinez",
        "ronald acuna": "ronald acuna jr.",
        "shin soo choo": "shin-soo choo",
        "robbie ross": "robbie ross jr.",
        "albert almora": "albert almora jr.",
        "christian guzman": "cristian guzman",
        "fausto carmona": "roberto hernandez",
        "bobby howry": "bob howry",
        "kendry morales": "kendrys morales",
        "tony pena": "ramon pena",
        "benji molina": "benjie molina",
    }

    salary_cols = ["player", "player_clean", "player_clean_wo_jr"]
    player_cols = ["fullname", "secondary_name", "nameascii", "first_last"]

    players_df["first_last"] = players_df["firstname"] + " " + players_df["lastname"]
    players_df = players_df[["person_id", "fullname", "secondary_name", "nameascii", "first_last"]].copy()
    for col in player_cols:
        players_df[col] = players_df[col].str.lower().str.strip()

    salary_df["player"] = salary_df["player"].str.lower().replace(replacements)
    salary_df["player_clean"] = _clean_player_names(salary_df["player"]).str.strip()
    salary_df["player_clean_wo_jr"] = salary_df["player_clean"].str.replace("jr", "", regex=False).str.strip()

    salary_df = _or_merges(salary_df, players_df, salary_cols, player_cols)
    return salary_df[["player", "salary", "season", "team", "person_id"]]


def format_name_and_salary(
    df: pd.DataFrame,
    player_column_idx: int,
    salary_column_idx: int,
    season: int,
    team: str,
) -> pd.DataFrame:
    """
    Standardize one payroll sheet to: player, salary, season, team.
    """

    def _drop_middle_initial(name: str) -> str:
        if not isinstance(name, str):
            return name
        parts = name.strip().split()
        if len(parts) == 3 and parts[1].endswith(".") and len(parts[1]) == 2:
            return f"{parts[0]} {parts[2]}"
        return name

    df = df.iloc[:, [player_column_idx, salary_column_idx]]
    df = df.rename(columns={df.columns[0]: "player", df.columns[1]: "salary"})

    df = df[~df["player"].isna()]
    df = df[["player", "salary"]].astype(str)
    df = df[~df["player"].str.contains(r"[0-9]", na=False)]
    df["salary"] = df["salary"].str.replace("(", "-", regex=False).str.replace(")", "", regex=False)

    df = df[df["player"].str.contains(",", regex=True, na=False)]
    df["player"] = df["player"].astype("string").str.replace("*", "", regex=False)
    df["player"] = df["player"].apply(lambda x: " ".join(reversed(x.split(", ")))).str.replace("  ", " ")
    df["player"] = df["player"].map(_drop_middle_initial)

    df["salary"] = pd.to_numeric(
        df["salary"].str.replace(r"\$|,", "", regex=True),
        errors="coerce",
    ).astype("Int64")

    df["season"] = season
    df["team"] = team
    return df


def sheets_csv_url(url: str) -> str:
    """
    Normalize Google Sheets/Drive links into a direct CSV export URL.
    """

    def _csv_url_from_pubhtml(pubhtml_url: str) -> str:
        if "/pubhtml" not in pubhtml_url:
            return pubhtml_url

        html = requests.get(pubhtml_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20).text
        match = re.search(r'(?:gid=|data-gid=")(\d+)', html)
        gid = match.group(1) if match else None

        base = f"{pubhtml_url.replace('/pubhtml', '/pub').split('?')[0]}?output=csv"
        return f"{base}&gid={gid}" if gid else base

    url = (url or "").strip()
    if not url:
        return url

    if "/export?format=csv" in url or "/pub?output=csv" in url:
        return url

    drive_match = re.search(r"drive\.google\.com/(?:open\?id=|file/d/)([A-Za-z0-9-_]+)", url)
    if drive_match:
        url = f"https://docs.google.com/spreadsheets/d/{drive_match.group(1)}/edit"

    if "/pubhtml" in url:
        return _csv_url_from_pubhtml(url)

    if "output=html" in url:
        url = url.replace("output=html", "output=csv")

    match = re.search(r"/spreadsheets/(?:u/\d+/)?d/([A-Za-z0-9-_]+)", url)
    if not match:
        return url
    doc_id = match.group(1)

    parsed = urlparse(url)
    gid = parse_qs(parsed.query).get("gid", [None])[-1]
    if not gid and parsed.fragment:
        fragment_match = re.search(r"gid=(\d+)", parsed.fragment)
        gid = fragment_match.group(1) if fragment_match else None

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
    primary_keys: list,
):
    """
    End-to-end bronze creation for salary data from Cot's / Google Sheets.
    """

    def _read_csv_with_requests(url: str) -> pd.DataFrame:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
        response.raise_for_status()
        return pd.read_csv(io.StringIO(response.text))

    old_format_player_idx = 0
    old_format_salary_idx = 5
    new_format_player_idx = 0
    new_format_salary_idx = 12

    payroll_urls_df = create_payroll_urls_df(teams_df, current_year, start_year)
    payroll_urls_df["csv_url"] = payroll_urls_df["url"].apply(sheets_csv_url)
    payroll_urls_df.to_sql(name="payroll_urls", con=engine, schema="bronze", if_exists="replace")

    salary_df = pd.DataFrame()
    for team, season, source_url, csv_url in payroll_urls_df[["team", "year", "url", "csv_url"]].values:
        df_raw = None
        tried_urls = [csv_url]

        if "/pub?output=csv&gid=" in csv_url:
            tried_urls.append(csv_url.split("&gid=")[0])

        if "output=html" in source_url:
            tried_urls.append(source_url.replace("output=html", "output=csv"))

        last_error = None
        for candidate_url in dict.fromkeys(tried_urls):
            try:
                df_raw = _read_csv_with_requests(candidate_url)
                break
            except Exception as exc:
                last_error = exc

        if df_raw is None:
            print(f"[salary] read_csv failed team={team} season={season}")
            print(f"[salary] source_url={source_url}")
            print(f"[salary] tried_urls={tried_urls}")
            raise last_error

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

    salary_df = salary_person_ids(salary_df, players_df)
    salary_df = salary_df.drop_duplicates(subset=["player", "salary", "season", "team"])

    ensure_bronze_tables_from_df(salary_df, base=base, schema=schema, engine=engine, primary_keys=primary_keys)
    fast_copy_from(salary_df, schema, base, engine)
    merge_staging_into_target(schema, base, engine, primary_keys, salary_df.columns.tolist())
    truncate_staging(engine, schema, base)


if __name__ == "__main__":
    engine = get_baseball_engine()

    start_year = 2010
    current_year = datetime.datetime.today().year
    base = "salary"
    schema = "bronze"
    primary_keys = ["player", "salary", "team", "season"]

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as exc:
        print("Failed to connect to PostgreSQL:")
        print(exc)
        raise SystemExit(1)

    teams_query = """
        select name, id, division_name
        from bronze.teams
        where season = (select max(season) from bronze.teams);
    """
    teams_df = pd.read_sql_query(teams_query, engine)
    players_df = pd.read_sql_table("players", engine, schema="silver")

    create_salary_table(
        engine,
        teams_df,
        players_df,
        current_year=current_year,
        start_year=start_year,
        base=base,
        schema=schema,
        primary_keys=primary_keys,
    )
