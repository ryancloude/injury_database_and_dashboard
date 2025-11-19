import os
import matplotlib
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px

st.set_page_config(page_title="Team Injuries", layout="wide")

# ----------------- Connections & caching -----------------
@st.cache_resource
def get_engine():
    dsn = os.getenv("READONLY_PG_DSN") or st.secrets.get("READONLY_PG_DSN")
    if not dsn:
        st.stop()  # fail fast if no DSN
    return create_engine(dsn, pool_pre_ping=True)

@st.cache_data(ttl=300)
def list_teams():
    sql = "select distinct team from gold.team_season_injuries order by team"
    with get_engine().connect() as con:
        return pd.read_sql_query(sql, con)["team"].tolist()

@st.cache_data(ttl=300)
def list_seasons_for_team(team: str):
    sql = text("""
        select distinct season
        from gold.team_season_injuries
        where team = :team
        order by season desc
    """)
    with get_engine().connect() as con:
        return pd.read_sql_query(sql, con, params={"team": team})["season"].tolist()

@st.cache_data(ttl=300)
def load_games_missed_by_role(team: str, season: int) -> pd.DataFrame:
    sql = text("""
        select position, sum(games_missed)::int as games_missed
        from gold.team_season_injuries
        where team = :team and season = :season
        group by position
    """)
    with get_engine().connect() as con:
        df = pd.read_sql_query(sql, con, params={"team": team, "season": season})
    wanted = pd.DataFrame({"position": ["position player", "pitcher"]})
    df = wanted.merge(df, on="position", how="left").fillna({"games_missed": 0})
    df["games_missed"] = df["games_missed"].astype(int)
    return df

@st.cache_data(ttl=300)
def load_team_injuries(team: str, season: int) -> pd.DataFrame:
    sql = text("""
        select name, position, start_date, end_date,
               games_missed, injured_salary, war_missed ,injury_type, body_part, side, second_injury_type,
               second_body_part, currently_injured
        from gold.team_season_injuries
        where team = :team and season = :season
        order by start_date asc, name asc
    """)
    with get_engine().connect() as con:
        df = pd.read_sql_query(sql, con, params={"team": team, "season": season})
    for c in ("start_date", "end_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in ("games_missed", "injured_salary", "war_missed"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    if "games_missed" in df.columns:
        df["games_missed"] = pd.to_numeric(df["games_missed"], errors="coerce").fillna(0).astype(int)
    return df

# ----------------- Helpers for chart -----------------
def stacked_bar_by_player(
    inj_df: pd.DataFrame,
    group_by: str = "position",          # 'position' | 'body_part' | 'injury_type'
    metric: str = "games_missed",        # 'games_missed' | 'injury_count'
    top_n_players: int = 30,
    largest_bottom: bool = True,
    others_at_top: bool = True
):
    if inj_df.empty:
        return None

    if group_by not in {"position", "body_part", "injury_type"}:
        group_by = "position"
    if metric not in {"games_missed", "injury_count", "injured_salary", "war_missed"}:
        metric = "games_missed"

    need = {group_by, "name", "injury_type", "body_part"}
    if metric in {"games_missed", "injured_salary", "war_missed"}:
        need.add(metric)
    missing = need - set(inj_df.columns)
    if missing:
        raise ValueError(f"Missing columns for chart: {missing}")

    d = inj_df.copy()
    for c in [group_by, "injury_type", "body_part", "name"]:
        if c in d:
            d[c] = d[c].fillna("unknown").astype(str)

        # Per-row "Body — Injury" pair for aligned display in hover
    if {"body_part", "injury_type"}.issubset(d.columns):
        d["body_injury_pair"] = (
            d["body_part"].fillna("unknown").astype(str)
            + " — "
            + d["injury_type"].fillna("unknown").astype(str)
        )
    else:
        d["body_injury_pair"] = "unknown"

    if metric == "games_missed":
        d["value"] = pd.to_numeric(d["games_missed"], errors="coerce").fillna(0).astype(int)
        value_label = "Games missed"
    elif metric == "injured_salary":
        d["value"] = pd.to_numeric(d["injured_salary"], errors="coerce").fillna(0.0)
        value_label = "Injured salary"
    elif metric == "war_missed":
        d["value"] = pd.to_numeric(d["war_missed"], errors="coerce").fillna(0.0)
        value_label = "WAR missed"
    else:  # injury_count
        d["value"] = 1  # each row = one injury episode
        value_label = "Injuries"

    seg = (
        d.groupby([group_by, "name"], as_index=False)
         .agg(
             value=("value", "sum"),
             body_injury_pairs=(
                 "body_injury_pair",
                 lambda s: "; ".join(sorted(set(x for x in s if pd.notna(x)))) or "—",
             ),
         )
    )

    totals = seg.groupby("name", as_index=False)["value"].sum()
    top_players = set(totals.sort_values("value", ascending=False).head(top_n_players)["name"])
    seg["name_for_color"] = seg["name"].where(seg["name"].isin(top_players), other="Others")

    seg_top = seg[seg["name_for_color"] != "Others"].copy()
    seg_oth = (
        seg[seg["name_for_color"] == "Others"]
          .groupby([group_by, "name_for_color"], as_index=False)
          .agg(value=("value", "sum"))
    )
    seg_oth["body_injury_pairs"] = "(multiple)"
    seg = pd.concat([seg_top, seg_oth], ignore_index=True)

    if metric == "injured_salary":
        # value is already in millions
        y_hover = "$%{y:.2f}M"
    elif metric == "war_missed":
        y_hover = "%{y:.1f}"
    else:
        y_hover = "%{y}"


    if group_by == "position":
        cat_order_x = ["position player", "pitcher"]
    else:
        cat_order_x = (seg.groupby(group_by)["value"].sum()
                         .sort_values(ascending=False).index.tolist())
    seg[group_by] = pd.Categorical(seg[group_by], categories=cat_order_x, ordered=True)

    color_order = (seg.groupby("name_for_color")["value"]
                     .sum()
                     .sort_values(ascending=not largest_bottom)
                     .index.tolist())
    if others_at_top and "Others" in color_order:
        color_order = [c for c in color_order if c != "Others"] + ["Others"]

    fig = px.bar(
        seg,
        x=group_by,
        y="value",
        color="name_for_color",
        custom_data=["name_for_color", "body_injury_pairs"],
        category_orders={group_by: cat_order_x, "name_for_color": color_order},
        color_discrete_map={"Others": "#9e9e9e"},
    )
    if metric == "injured_salary":
        fig.update_yaxes(tickprefix="$", ticksuffix="M")
    fig.update_layout(
        barmode="stack",
        xaxis_title=None,
        yaxis_title=value_label,
        legend_title="Player",
        hoverlabel=dict(namelength=-1),
    )

    if group_by == "position":
        hover = (
            "<b>%{customdata[0]}</b><br>"
            f"{value_label}: " + y_hover + "<br>"
            "Body part — injury type:<br>"
            "%{customdata[1]}<extra></extra>"
        )
    elif group_by == "injury_type":
        hover = (
            "<b>%{customdata[0]}</b><br>"
            f"{value_label}: " + y_hover +"<br>"
            "Body part — injury type:<br>"
            "%{customdata[1]}<extra></extra>"
        )
    else:  # body_part
        hover = (
            "<b>%{customdata[0]}</b><br>"
            f"{value_label}: " + y_hover + "<br>"
            "Body part — injury type:<br>"
            "%{customdata[1]}<extra></extra>"
        )

    fig.update_traces(
        hovertemplate=hover,
        marker_line_width=0.5,
        marker_line_color="rgba(0,0,0,0.2)",
    )
    return fig

# ----------------- Filter helpers (used by chart, summary, and table) -----------------
def _unique_union(df, a: str, b: str) -> list[str]:
    s1 = df[a] if a in df else pd.Series([], dtype=object)
    s2 = df[b] if b in df else pd.Series([], dtype=object)
    vals = pd.concat([s1, s2], ignore_index=True)
    vals = (vals.dropna().astype(str).str.strip())
    vals = vals[(vals != "") & (~vals.str.lower().isin({"none", "nan"}))]
    return sorted(vals.unique().tolist())

def to_bool(x):
    if isinstance(x, bool): return x
    s = str(x).strip().lower()
    return s in {"t","true","1","yes","y"}

# ----------------- UI controls -----------------
st.title("Team Injuries")

teams = list_teams()
if not teams:
    st.warning("No teams found in gold.team_season_injuries.")
    st.stop()

qp = st.query_params
default_team = qp.get("team", teams[0])
team = st.selectbox("Team", teams, index=max(0, teams.index(default_team)) if default_team in teams else 0)

seasons = list_seasons_for_team(team)
if not seasons:
    st.info("No seasons for this team.")
    st.stop()

default_season = int(qp.get("season", seasons[0]))
season = st.selectbox("Season", seasons, index=max(0, seasons.index(default_season)) if default_season in seasons else 0)

st.query_params["team"] = team
st.query_params["season"] = str(season)
st.markdown(f"### {team} — {season}")

# Load once
inj = load_team_injuries(team, season)

# ----------------- Filters (drive BOTH chart/summary and table) -----------------
df_view = inj.copy()

if "currently_injured" in df_view.columns:
    df_view["currently_injured"] = df_view["currently_injured"].map(to_bool)

for c in ("games_missed", "injured_salary", "war_missed"):
    if c in df_view.columns:
        df_view[c] = pd.to_numeric(df_view[c], errors="coerce").fillna(0)

if "games_missed" in df_view.columns:
    df_view["games_missed"] = df_view["games_missed"].astype(int)

positions_all    = sorted(df_view["position"].dropna().astype(str).str.strip().unique().tolist()) if "position" in df_view else []
body_parts_all   = _unique_union(df_view, "body_part", "second_body_part")
injury_types_all = _unique_union(df_view, "injury_type", "second_injury_type")

c1, c2, c3, c4 = st.columns([1, 1, 1, 1])

with c1:
    pos_sel = st.multiselect("Position", positions_all, default=positions_all)

with c2:
    all_body_parts = st.checkbox("All body parts", value=True, key="team_all_body_parts")
    bp_sel = st.multiselect(
        "Body Part (incl. secondary)",
        body_parts_all,
        default=body_parts_all,
        key="team_bp_sel",
    )

with c3:
    all_injury_types = st.checkbox("All injury types", value=True, key="team_all_injury_types")
    it_sel = st.multiselect(
        "Injury Type (incl. secondary)",
        injury_types_all,
        default=injury_types_all,
        key="team_it_sel",
    )

with c4:
    curr_choice = st.selectbox("Currently Injured", ["Any", "Yes", "No"], index=0)

# Build one mask and reuse it
mask = pd.Series(True, index=df_view.index)

# Position (unchanged)
if positions_all and len(pos_sel) != len(positions_all) and "position" in df_view:
    mask &= df_view["position"].astype(str).isin(pos_sel)

# Body parts with "select all"
bp_active = body_parts_all if all_body_parts or not bp_sel else bp_sel
if body_parts_all and len(bp_active) != len(body_parts_all):
    m1 = df_view["body_part"].astype(str).isin(bp_active) if "body_part" in df_view else False
    m2 = df_view["second_body_part"].astype(str).isin(bp_active) if "second_body_part" in df_view else False
    mask &= (m1 | m2)

# Injury types with "select all"
it_active = injury_types_all if all_injury_types or not it_sel else it_sel
if injury_types_all and len(it_active) != len(injury_types_all):
    m1 = df_view["injury_type"].astype(str).isin(it_active) if "injury_type" in df_view else False
    m2 = df_view["second_injury_type"].astype(str).isin(it_active) if "second_injury_type" in df_view else False
    mask &= (m1 | m2)

if "currently_injured" in df_view.columns and curr_choice in ("Yes", "No"):
    mask &= df_view["currently_injured"].eq(curr_choice == "Yes")

inj_filtered = df_view.loc[mask].copy()

# ----------------- Chart controls -----------------
group_map = {"Position": "position", "Body Part": "body_part", "Injury Type": "injury_type"}
group_label = st.radio("Group bars by", list(group_map.keys()), horizontal=True, index=0)
group_by = group_map[group_label]

metric_map = {
    "Games missed": "games_missed",
    "Injuries": "injury_count",
    "Injured salary": "injured_salary",
    "WAR missed": "war_missed",
}
metric_label = st.radio("Metric", list(metric_map.keys()), horizontal=True, index=0)
metric = metric_map[metric_label]

# ----------------- Chart + Summary (use filtered data) -----------------
left, right = st.columns([2, 1])
with left:
    fig = stacked_bar_by_player(
        inj_filtered, group_by=group_by, metric=metric,
        top_n_players=30, largest_bottom=True, others_at_top=True
    )
    if fig is None:
        st.info("No injuries to plot for this team/season.")
    else:
        st.plotly_chart(fig, use_container_width=True, key=f"stacked_{group_by}_{metric}")

with right:
    if not inj_filtered.empty:
        if metric == "injury_count":
            summary_table = (
                inj_filtered.groupby(group_by, as_index=False).size()
                            .rename(columns={"size": "injuries"})
                            .sort_values("injuries", ascending=False)
            )
            value_col = "injuries"
            total_val = int(summary_table[value_col].sum()) if not summary_table.empty else 0
            total_label = "Total injuries"
        else:
            summary_table = (
                inj_filtered.groupby(group_by, as_index=False)[metric].sum()
                            .sort_values(metric, ascending=False)
            )
            value_col = metric
            total_val = summary_table[value_col].sum() if not summary_table.empty else 0

            total_label_map = {
                "games_missed": "Total games missed",
                "injured_salary": "Total injured salary",
                "war_missed": "Total WAR missed",
            }
            total_label = total_label_map.get(metric, "Total")

            if metric == "games_missed":
                summary_table[value_col] = summary_table[value_col].astype(int)
                total_val = int(total_val)

        if group_by == "position":
            wanted = pd.DataFrame({group_by: ["position player", "pitcher"]})
            summary_table = (
                wanted.merge(summary_table, on=group_by, how="left")
                      .fillna({value_col: 0})
            )
            if metric in ("games_missed", "injury_count"):
                summary_table[value_col] = summary_table[value_col].astype(int)

        label_nice = {"position": "Position", "body_part": "Body Part", "injury_type": "Injury Type"}[group_by]
        pretty_val_name = value_col.replace("_", " ").title()

        display_table = summary_table.rename(
            columns={group_by: label_nice, value_col: pretty_val_name}
        ).copy()

        # Format display values
        if metric == "injured_salary":
            display_table[pretty_val_name] = display_table[pretty_val_name].map(
                lambda v: f"${v:,.2f}M"
            )
        elif metric == "war_missed":
            display_table[pretty_val_name] = display_table[pretty_val_name].map(
                lambda v: f"{v:.1f}"
            )

        # Format the metric at the top too
        if metric == "war_missed":
            st.metric(total_label, f"{total_val:.1f}")
        elif metric == "injured_salary":
            st.metric(total_label, f"${total_val:,.2f}M")
        else:
            st.metric(total_label, total_val)

        st.table(display_table)

    else:
        st.metric("Total", 0)
        st.info("No injury rows for this team/season.")

# ----------------- Table: individual injuries (same filtered rows) -----------------
st.subheader("Injuries (player-level)")
show_cols = [c for c in [
    "name","position","start_date","end_date",
    "games_missed","injured_salary","war_missed",
    "injury_type","body_part","side",
    "second_injury_type","second_body_part","currently_injured"
] if c in inj_filtered.columns]
df_filt = inj_filtered[show_cols].copy()
metric_cols = [c for c in ["games_missed", "injured_salary", "war_missed"] if c in df_filt.columns]
st.caption(f"{len(df_filt):,} rows")

def color_current(val: bool):
    return "background-color: #d1f7c4; color: #0a0;" if val else "background-color: #ffd6d6; color: #a00;"

try:
    styler = (
        df_filt.style
        .applymap(color_current, subset=["currently_injured"])
        .background_gradient(cmap="YlOrRd", subset=metric_cols)
        .format({
            "injured_salary": lambda v: f"${v:,.2f}M",
            "war_missed":     lambda v: f"{v:.1f}",
        })
        .hide(axis="index")
    )
except Exception:
    def _hex(r, g, b): return f"#{r:02x}{g:02x}{b:02x}"
    START, END = (255, 255, 204), (215, 48, 31)

    def colorize_numeric(s: pd.Series):
        v = pd.to_numeric(s, errors="coerce").fillna(0)
        vmin, vmax = v.min(), v.max()
        rng = (vmax - vmin) or 1.0
        out = []
        for x in v:
            t = (x - vmin) / rng
            r = int(START[0] + t * (END[0] - START[0]))
            g = int(START[1] + t * (END[1] - START[1]))
            b = int(START[2] + t * (END[2] - START[2]))
            out.append(f"background-color: {_hex(r,g,b)}")
        return out

    styler = (
        df_filt.style
        .applymap(color_current, subset=["currently_injured"])
        .apply(colorize_numeric, subset=metric_cols)
        .format({
            "injured_salary": lambda v: f"${v:,.2f}M",
            "war_missed":     lambda v: f"{v:.1f}",
        })
        .hide(axis="index")
    )

st.dataframe(styler, use_container_width=True)