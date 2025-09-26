"""Builds consolidated injury spans from IL placements, transactions, and appearances.

This module reads Major League Baseball injured list placement data, transactional
movement logs, and player appearance records, then stitches them together into
continuous injury spans that indicate when a player went on the IL and when they
returned (either via formal activation or on-field appearance). The resulting
spans are written to the silver schema of the warehouse for downstream analysis.
"""

import os
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine, text


def get_activations(il_placements, transactions, season_dates, teams):
    """Attach activation information to IL placements.

    Parameters
    ----------
    il_placements : pandas.DataFrame
        Rows representing player IL placements with ``il_place_date`` and
        ``il_place_team`` columns.
    transactions : pandas.DataFrame
        Player transaction log containing descriptions and type codes used to
        infer activations. Relies on the module-level ``mlb_teams`` lookup that
        is populated when the module is executed as a script.
    season_dates : pandas.DataFrame
        Season calendar that provides ``first_game`` and ``last_game`` dates for
        each franchise-season combination.

    Returns
    -------
    pandas.DataFrame
        The IL placements augmented with the best matched activation
        transaction (if any) and offseason return adjustments.
    """
    off_il_mask = (
        ((transactions["description"].str.contains("activated|reinstated|recalled|returned|status changed"))
            | (transactions["typecode"].isin(["DFA", "REL", "RET", "OUT", "DEI", "DEC", "OPT", "DES", "CLW"])))
        | ((transactions.typecode == "ASG")
            & transactions.toteam_id.isin(teams["id"]))) & ~transactions["description"].str.contains("all-stars")
    # Filter transactions to those representing roster activations, demotions, or
    # other events that indicate a player is leaving the IL.
    off_il = transactions[off_il_mask]
    off_il = off_il.rename(columns={"date": "return_date"})
    off_il = off_il[["trans_id", "return_date", "toteam_id", "person_id"]]
    il_placements = il_placements.sort_values(["il_place_date"]).reset_index(drop=True)
    off_il = off_il.sort_values(["return_date"]).reset_index(drop=True)
    # Use an asof merge so that each IL placement looks forward to the next
    # applicable activation for the same player.
    injury = pd.merge_asof(
        il_placements,
        off_il,
        by="person_id",
        left_on="il_place_date",
        right_on="return_date",
        suffixes=("", "_return"),
        direction="forward",
        allow_exact_matches=False,
    )
    injury = injury.rename(
        columns={"toteam_id": "return_team_id", "trans_id": "return_trans_id"}
    )
    injury["season"] = injury.il_place_date.dt.year
    injury = pd.merge(
        injury,
        season_dates.groupby("season").last_game.max(),
        left_on=["season"],
        right_on=["season"],
    )
    injury["last_game"] = injury["last_game"].dt.date
    offseason_return_mask = (injury.return_date > injury.last_game + timedelta(7)) | (
        (injury.return_date.isnull()) & (~injury.last_game.isnull())
    )
    # If a transaction happens deep into the offseason (or is missing), cap the
    # return date to one week after the team's final game so the span ends in the
    # same competitive season.
    injury.loc[offseason_return_mask, "return_date"] = injury.last_game + timedelta(7)
    injury.loc[offseason_return_mask, "return_team_id"] = injury.il_place_team
    injury = injury.drop(columns=["season", "last_game"])
    # If a player is placed by on the IL again before being activated (can happen with rehab assignemnts)
    # Then the earliest IL placement will have its return_date set to None and IL stints is combined into one
    # in the bridge IL_stints functions
    multi = injury.groupby(['person_id','return_date']).transform("size") > 1
    earliest = injury['il_place_date'].eq(injury.groupby(['person_id','return_date'])['il_place_date'].transform("min"))
    injury.loc[(multi & earliest), 'return_date'] = None
    return injury


def check_for_player_app(injury, player_app):
    """Identify on-field returns that precede transactional activations.

    Parameters
    ----------
    injury : pandas.DataFrame
        DataFrame returned by :func:`get_activations` containing placement and
        activation details.
    player_app : pandas.DataFrame
        Appearance-level data, typically from Statcast, with ``game_date`` and
        ``team_id`` columns. The module expects this to be loaded globally as
        ``player_app`` when run as a script.

    Returns
    -------
    pandas.DataFrame
        The injury table where any appearance that happens before the
        transactional activation overrides the return information.
    """
    injury = injury.sort_values(["il_place_date"]).reset_index(drop=True)
    player_app = player_app.sort_values(["game_date"]).reset_index(drop=True)
    # Look for the first appearance at or after the IL placement; if it occurs
    # before the transactional activation we treat that appearance date/team as
    # the true return.
    injury = pd.merge_asof(
        injury,
        player_app,
        by="person_id",
        left_on="il_place_date",
        right_on="game_date",
        suffixes=("", "_app"),
        direction="forward",
        allow_exact_matches=False,
    )
    return_before_trans_mask = injury.game_date < injury.return_date
    injury.loc[return_before_trans_mask, "return_team_id"] = injury.loc[
        return_before_trans_mask, "team_id"
    ]
    injury.loc[return_before_trans_mask, "return_trans_id"] = None
    injury.loc[return_before_trans_mask, "return_date"] = injury.loc[
        return_before_trans_mask, "game_date"
    ]
    injury = injury.drop(columns=["game_date", "team_id"])
    return injury


def bridge_il_stints(injury, season_dates):
    """Bridge related IL stints into multi-stage injuries.

    Parameters
    ----------
    injury : pandas.DataFrame
        Injury spans after appearance overrides have been applied.
    season_dates : pandas.DataFrame
        Season calendar with first/last game markers used to determine
        offseason windows and season boundaries.

    Returns
    -------
    pandas.DataFrame
        Consolidated injury spans that include secondary injury descriptors and
        account for offseason gaps between related IL placements.
    """
    injury["next_il_placement"] = injury.groupby(["person_id"])["il_place_date"].shift(-1)
    injury["next_il_team"] = injury.groupby(["person_id"])["il_place_team"].shift(-1)
    injury["next_il_return_team"] = injury.groupby(["person_id"])["return_team_id"].shift(-1)
    injury["next_trans_id_return"] = injury.groupby(["person_id"])["return_trans_id"].shift(-1)
    injury["next_il_return_date"] = injury.groupby(["person_id"])["return_date"].shift(-1)
    injury["next_body_part"] = injury.groupby(["person_id"])["body_part"].shift(-1)
    injury["next_body_part_group"] = injury.groupby(["person_id"])["body_part_group"].shift(-1)
    injury["next_injury_type"] = injury.groupby(["person_id"])["injury_type"].shift(-1)
    injury["season"] = injury["il_place_date"].dt.year
    # Attach the last game of the current season for the team that activated the player.
    injury = injury.merge(
        season_dates[["season", "last_game", "team_id"]],
        how="left",
        left_on=["season", "return_team_id"],
        right_on=["season", "team_id"],
    )
    # injury = injury.drop(['team_id_x'],axis=1)
    injury["next_season"] = injury["season"] + 1
    # Bring in the next season's Opening Day to know if the follow-up IL stint occurs before the season starts.
    injury = injury.merge(
        season_dates[["season", "first_game", "team_id"]],
        how="left",
        left_on=["next_season", "next_il_team"],
        right_on=["season", "team_id"],
    )
    injury = injury.drop(["season_x", "next_season", "season_y"], axis=1)
    injury = injury.rename(columns={"first_game": "next_season_first_game"})
    injury["offseason_activation"] = injury.return_date.between(
        injury.last_game,
        pd.to_datetime(pd.Series(injury.last_game.dt.year.astype("Int64"), dtype="string")+ "-12-31"),)
    # A "pre Opening Day" placement is one that falls before the next season's
    # first game for that franchise, signalling an offseason bridge scenario.
    injury["pre_od_placement"] = (
        injury.next_il_placement <= injury.next_season_first_game
    ) & (injury.next_il_placement.dt.year == injury.next_season_first_game.dt.year)
    cu_unknown = injury["body_part_group"].isna() | (
        injury["body_part_group"] == "undisclosed")
    nx_unknown = injury["next_body_part_group"].isna() | (
        injury["next_body_part_group"] == "undisclosed")
    injury["bpg_clean"] = injury["body_part_group"].where(~cu_unknown)
    injury["next_bpg_clean"] = injury["next_body_part_group"].where(~nx_unknown)
    injury["prev_known_bpg"] = injury.groupby("person_id")["bpg_clean"].ffill()
    sev_mask = injury["injury_type"].str.contains(
        "surgery", case=False, na=False
    ) | injury["injury_type"].str.contains(
        "tear", case=False, na=False
    )  # catches 'tear' and 'partial tear'

    mask = (
        # Player never activated but is re-placed on IL.
        ((injury["return_date"].isnull())
            & ~(injury["next_il_placement"].isnull()))
        | (# Returned in offseason and placed back on IL before Opening Day.
            injury["offseason_activation"]
            & injury["pre_od_placement"])) & (
        # same known group → bridge
        (injury["body_part_group"] == injury["next_body_part_group"])
        # known → unknown → always bridge
        | (~cu_unknown & nx_unknown)
        # unknown → known → bridge only if it matches the previous known
        | (cu_unknown
            & ~nx_unknown
            & (injury["prev_known_bpg"] == injury["next_bpg_clean"]))
        # unknown → unknown → still counts as an edge
        | (cu_unknown & nx_unknown))
    injury["bridge"] = mask
    # Start a new bridge group whenever the bridge condition is not satisfied for a placement.
    injury["bridge_break"] = ~injury.groupby("person_id")["bridge"].shift(fill_value=False)
    injury["bridge_group"] = injury.groupby("person_id")["bridge_break"].cumsum()
    sev_rows = (
        injury.loc[sev_mask,["person_id","bridge_group","il_place_date","body_part", "injury_type", "raw_injury"]]
        .sort_values("il_place_date")
        .groupby(["person_id", "bridge_group"], as_index=False)
        .tail(1)  # latest surgery/tear in the bridge
        .rename(
            columns={"body_part": "sev_body_part",
                "injury_type": "sev_injury_type",
                "raw_injury": "sev_raw_injury",}))
    injury = injury.merge(
        sev_rows[["person_id","bridge_group","sev_body_part","sev_injury_type","sev_raw_injury"]],
        on=["person_id", "bridge_group"],
        how="left")
    injury[
        [
            "chain_team_return",
            "chain_date_return",
            "chain_trans_id_return",
            "chain_body_part",
            "chain_injury_type",
            "chain_raw_injury",
            "chain_body_part_group",
        ]
    ] = injury.groupby(["person_id", "bridge_group"])[
        [
            "return_team_id",
            "return_date",
            "return_trans_id",
            "body_part",
            "injury_type",
            "raw_injury",
            "body_part_group",
        ]
    ].transform(
        "last", skipna=False
    )
    # Final picks for "second_*": prefer surgery/tear if present, else fall back to existing chain_*
    injury["final_second_body_part"] = injury["chain_body_part"].where(
        injury["sev_body_part"].isna(), injury["sev_body_part"]
    )
    injury["final_second_injury_type"] = injury["chain_injury_type"].where(
        injury["sev_injury_type"].isna(), injury["sev_injury_type"]
    )
    injury["final_second_raw_injury"] = injury["chain_raw_injury"].where(
        injury["sev_raw_injury"].isna(), injury["sev_raw_injury"]
    )
    # Only rows belonging to a bridge need to inherit the chained return metadata.
    needs_bridge = (
        injury.groupby(["person_id", "bridge_group"])["bridge"]
        .transform("max")
        .astype(bool)
    )
    tgt_cols = [
        "team_return",
        "return_date",
        "return_trans_id",
        "second_body_part",
        "second_injury_type",
        "second_raw_injury",
    ]
    injury.loc[needs_bridge, tgt_cols] = injury.loc[
        needs_bridge,
        [
            "chain_team_return",
            "chain_date_return",
            "chain_trans_id_return",
            "final_second_body_part",
            "final_second_injury_type",
            "final_second_raw_injury",
        ],
    ].to_numpy()
    keep_first = injury.groupby(["person_id", "bridge_group"]).cumcount() == 0
    injury = injury[keep_first]
    # Fill null body part groups with the last known value from the bridge to
    # keep downstream grouping consistent.
    injury.loc[injury.body_part_group.isnull(), "body_part_group"] = injury.loc[
        injury.body_part_group.isnull(), "chain_body_part_group"
    ]
    injury = injury.drop(
        columns=[
            "bridge",
            "bridge_break",
            "bridge_group",
            "offseason_activation",
            "pre_od_placement",
            "chain_team_return",
            "chain_date_return",
            "chain_trans_id_return",
            "bpg_clean",
            "next_bpg_clean",
            "prev_known_bpg",
            "sev_body_part",
            "sev_injury_type",
            "sev_raw_injury",
            "final_second_body_part",
            "final_second_injury_type",
            "final_second_raw_injury",
        ]
    )
    new_columns = {
        "il_place_trans_id": "il_place_trans_id",
        "il_place_date": "il_place_date",
        "person_id": "person_id",
        "il_place_team": "il_place_team",
        "raw_injury": "raw_injury",
        "side": "side",
        "body_part": "body_part",
        "injury_type": "injury_type",
        "body_part_group": "body_part_group",
        "second_raw_injury": "second_raw_injury",
        "second_body_part": "second_body_part",
        "second_injury_type": "second_injury_type",
        "return_trans_id": "return_trans_id",
        "return_date": "return_date",
        "return_team_id": "return_team",
    }
    injury = injury.rename(columns=new_columns)
    injury = injury[list(new_columns.values())]
    current_date = pd.Timestamp.now().normalize()
    injury["injury_length"] = (
        injury.return_date.fillna(current_date) - injury.il_place_date
    )
    injury["injury_length"] = injury["injury_length"].dt.days.astype("Int64")
    injury["currently_injured"] = injury.return_date.isnull()
    injury["return_trans_id"] = injury["return_trans_id"].astype("Int64")
    injury["return_team"] = injury["return_team"].astype("Int64")
    return injury


def create_silver_injury_spans(il_placements, transactions, season_dates, engine):
    """Build the injury spans table and persist it to the database.

    Parameters
    ----------
    il_placements : pandas.DataFrame
        Injured list placement records for the league.
    transactions : pandas.DataFrame
        Full transaction log for filtering activations.
    season_dates : pandas.DataFrame
        Calendar data used to identify offseason and Opening Day boundaries.
    engine : sqlalchemy.engine.Engine
        Database engine where the silver schema resides.

    Notes
    -----
    This function relies on the module-level ``mlb_teams`` and ``player_app``
    globals being populated prior to invocation when executed as a script.
    """
    injury = get_activations(il_placements, transactions, season_dates, mlb_teams)
    injury = check_for_player_app(injury, player_app)
    injury = bridge_il_stints(injury, season_dates)
    injury["next_il_placement"] = injury.groupby(["person_id"])["il_place_date"].shift(-1)
    mask = injury['return_date'].isnull() & ~injury["next_il_placement"].isnull()
    injury.loc[mask, "return_date"] = injury['next_il_placement']
    injury = injury.drop(columns=["next_il_placement"])
    injury.to_sql(
        "injury_spans", engine, schema="silver", index=False, if_exists="replace")


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
    mlb_teams_query = "select id, name from bronze.teams;"
    mlb_teams = pd.read_sql(mlb_teams_query, engine)
    il_place_query = "select * from silver.il_placements;"
    il_placements = pd.read_sql(il_place_query, engine)
    transactions_query = "SELECT * FROM silver.transactions where description is not null and person_id is not null;"
    transactions = pd.read_sql(transactions_query, engine)
    season_dates_query = "select * from silver.season_dates;"
    season_dates = pd.read_sql(season_dates_query, engine)
    player_apperance_query = """select DISTINCT pitcher as person_id, game_date, pitcher_team as team_id
    from silver.statcast
    UNION
    select distinct batter as person_id, game_date, batter_team as team_id
    from silver.statcast;"""
    player_app = pd.read_sql(player_apperance_query, engine)
    create_silver_injury_spans(il_placements, transactions, season_dates, engine)
