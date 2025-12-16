import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px
st.set_page_config(page_title="Player Injuries", layout="wide")

# ----------------- Connections & caching -----------------
@st.cache_resource
def get_engine():
    dsn = os.getenv("LOCAL_READONLY_PG_DSN") or st.secrets.get("READONLY_PG_DSN")
    if not dsn:
        st.stop()
    return create_engine(dsn, pool_pre_ping=True)

@st.cache_data(ttl=300)
def list_players() -> list[str]:
    """
    Get all distinct player names for the search UI.
    """
    sql = """
        select distinct name
        from gold.team_season_injuries
        where name is not null
        order by name
    """
    with get_engine().connect() as con:
        return pd.read_sql_query(sql, con)["name"].tolist()

@st.cache_data(ttl=300)
def load_player_injuries(player_name: str) -> pd.DataFrame:
    """
    Load all injury rows for a single player.
    """
    sql = text("""
        select team, season, name, position, start_date, end_date,
            games_missed, injury_type, body_part, body_part_group, side,
            second_injury_type, second_body_part, currently_injured,
            injured_salary, war_missed,
            mlb_debut_year, last_year,             
            injury_span_sk                        
        from gold.team_season_injuries
        where name = :name
        order by season desc, start_date asc, team asc
    """)
    with get_engine().connect() as con:
        df = pd.read_sql_query(sql, con, params={"name": player_name})

    # Dates
    for c in ("start_date", "end_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    for c in ("games_missed", "injured_salary", "war_missed"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    if "games_missed" in df.columns:
        df["games_missed"] = df["games_missed"].astype(int)

    # NEW: debut / last year as numbers
    for c in ("mlb_debut_year", "last_year"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    
    if "injury_span_sk" in df.columns:
        df["injury_span_sk"] = df["injury_span_sk"].astype("string")

    # Boolean
    if "currently_injured" in df.columns:
        df["currently_injured"] = df["currently_injured"].map(to_bool)

    return df

# ----------------- Helpers -----------------
def to_bool(x):
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in {"t", "true", "1", "yes", "y"}

def color_current(val: bool):
    return "background-color: #d1f7c4; color: #0a0;" if val else "background-color: #ffd6d6; color: #a00;"

# ----------------- Page UI -----------------
st.title("Player Injuries")

players = list_players()
if not players:
    st.warning("No players found in gold.team_season_injuries.")
    st.stop()

player = st.selectbox("Select player", players)

if not player:
    st.stop()

st.markdown(f"### {player}")

# Load player data
df_player = load_player_injuries(player)
if df_player.empty:
    st.info("No injury records for this player.")
    st.stop()

df_player_filt = df_player.copy()

# ----------------- Top metrics -----------------
total_games = int(df_player_filt["games_missed"].sum()) if "games_missed" in df_player_filt else 0
total_salary = float(df_player_filt["injured_salary"].sum()) if "injured_salary" in df_player_filt else 0.0
total_war = float(df_player_filt["war_missed"].sum()) if "war_missed" in df_player_filt else 0.0

m1, m2, m3 = st.columns(3)
m1.metric("Total games missed", total_games)
m2.metric("Total injured salary", f"${total_salary:,.2f}M")
m3.metric("Total WAR missed", f"{total_war:.1f}")

# ----------------- Season summary (bar chart) -----------------
st.subheader("Season summary")

# 1) Choose metric and breakdown
metric_options = {
    "Games missed": "games_missed",
    "Injured salary": "injured_salary",
    "WAR missed": "war_missed",
}
metric_label = st.radio("Metric", list(metric_options.keys()), horizontal=True, index=0)
metric_col = metric_options[metric_label]

# 2) Aggregate by season for the TOTAL view (using filtered rows)
season_summary = (
    df_player_filt
    .groupby("season", as_index=False)
    .agg(
        games_missed=("games_missed", "sum"),
        injured_salary=("injured_salary", "sum"),
        war_missed=("war_missed", "sum"),
    )
)
season_summary["season"] = pd.to_numeric(season_summary["season"], errors="coerce")

# 3) Use the *unfiltered* player frame to get full career span
base_for_career = df_player  # all rows for this player

debut = None
last = None

if "mlb_debut_year" in base_for_career.columns:
    debut_series = pd.to_numeric(
        base_for_career["mlb_debut_year"], errors="coerce"
    ).dropna()
    if not debut_series.empty:
        debut = int(debut_series.min())   # earliest debut we see

if "last_year" in base_for_career.columns:
    last_series = pd.to_numeric(
        base_for_career["last_year"], errors="coerce"
    ).dropna()
    if not last_series.empty:
        last = int(last_series.max())     # latest last_year we see

# Fallback to observed injury seasons if those columns are missing/empty
if debut is None and not season_summary.empty:
    debut = int(season_summary["season"].min())
if last is None and not season_summary.empty:
    last = int(season_summary["season"].max())

# Safety: if still None (no data), bail out
# Safety: if still None (no data), bail out
if debut is None or last is None:
    st.info("No season data available to plot.")
else:
    # Always break down by body part
    d = df_player_filt.copy()
    d["season"] = pd.to_numeric(d["season"], errors="coerce")
    d["body_part"] = d["body_part"].fillna("Unknown").astype(str)

    plot_df = (
        d.groupby(["season", "body_part"], as_index=False)[metric_col].sum()
    )

    fig = px.bar(
        plot_df,
        x="season",
        y=metric_col,
        color="body_part",
    )

    # One tick per year, and axis from debut..last
    fig.update_xaxes(
        tickmode="linear",
        dtick=1,
        range=[debut - 0.5, last + 0.5],
    )

    # Y-axis formatting
    if metric_col == "injured_salary":
        fig.update_yaxes(tickprefix="$", ticksuffix="M")
    elif metric_col == "war_missed":
        fig.update_yaxes(tickformat=".1f")

    fig.update_layout(
        xaxis_title="Season",
        yaxis_title=metric_label,
        hoverlabel=dict(namelength=-1),
    )

    # Hover formatting
    if metric_col == "injured_salary":
        y_hover = "$%{y:.2f}M"
    elif metric_col == "war_missed":
        y_hover = "%{y:.1f}"
    else:
        y_hover = "%{y}"

    hover = (
        "Season: %{x}<br>"
        "Body part: %{fullData.name}<br>"
        f"{metric_label}: " + y_hover + "<extra></extra>"
    )

    fig.update_traces(hovertemplate=hover)

    st.plotly_chart(fig, use_container_width=True)

# ----------------- Injuries table (per span) -----------------
# ----------------- Injuries table (per span) -----------------
st.subheader("Injuries (per span)")

# Only aggregate if we actually have at least one non-null span key
if (
    "injury_span_sk" in df_player_filt.columns 
    and df_player_filt["injury_span_sk"].notna().any()
):
    span_df = (
        df_player_filt
        .sort_values("start_date")
        .groupby("injury_span_sk", as_index=False)
        .agg(
            start_date=("start_date", "min"),
            end_date=("end_date", "max"),
            games_missed=("games_missed", "sum"),
            injured_salary=("injured_salary", "sum"),
            war_missed=("war_missed", "sum"),
            injury_type=("injury_type", "first"),
            body_part=("body_part", "first"),
            body_part_group=("body_part_group", "first"),
            side=("side", "first"),
            currently_injured=("currently_injured", "max"),
        )
    )
else:
    # Fallback: show row-level injuries if span key is missing or all NaN
    span_df = df_player_filt.copy()

# Only show the columns you want (no team, position, or season)
show_cols = [c for c in [
    "start_date", "end_date",
    "games_missed", "injured_salary", "war_missed",
    "injury_type", "body_part", "body_part_group", "side",
    "currently_injured",
] if c in span_df.columns]

df_show = span_df[show_cols].copy()
st.caption(f"{len(df_show):,} spans")

metric_cols = [c for c in ["games_missed", "injured_salary", "war_missed"] if c in df_show.columns]

try:
    styler = (
        df_show.style
        .applymap(
            color_current,
            subset=["currently_injured"] if "currently_injured" in df_show.columns else [],
        )
        .background_gradient(
            cmap="YlOrRd",
            subset=["games_missed"] if "games_missed" in df_show.columns else [],
        )
        .format({
            "injured_salary": lambda v: f"${v:,.2f}M" if pd.notna(v) else "",
            "war_missed":     lambda v: f"{v:.1f}"   if pd.notna(v) else "",
        })
        .hide(axis="index")
    )
except Exception:
    # Fallback coloring if Styler blows up for some reason
    def _hex(r, g, b): return f"#{r:02x}{g:02x}{b:02x}"
    START, END = (255, 255, 204), (215, 48, 31)

    def colorize_games_missed(s: pd.Series):
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
        df_show.style
        .applymap(
            color_current,
            subset=["currently_injured"] if "currently_injured" in df_show.columns else [],
        )
        .apply(
            colorize_games_missed,
            subset=["games_missed"] if "games_missed" in df_show.columns else [],
        )
        .format({
            "injured_salary": lambda v: f"${v:,.2f}M" if pd.notna(v) else "",
            "war_missed":     lambda v: f"{v:.1f}"   if pd.notna(v) else "",
        })
        .hide(axis="index")
    )

st.dataframe(styler, use_container_width=True)