import os
import pandas as pd
import streamlit as st # type: ignore
from sqlalchemy import create_engine

st.set_page_config(page_title="Injury Leaderboards", layout="wide")

TABLE_FQN = "gold.player_injury_spans_values"

BASE_COLS = [
    "name",
    "start_date",
    "end_date",
    "position",
    "body_part",
    "injury_type",
    "side",
    "second_body_part",
    "second_injury_type",
    "games_missed",
    "injured_salary",
    "war_missed",
]


# ----------------- Connections & caching -----------------
@st.cache_resource
def get_engine():
    dsn = os.getenv("LOCAL_READONLY_PG_DSN") or st.secrets.get("READONLY_PG_DSN")
    if not dsn:
        st.stop()
    return create_engine(dsn, pool_pre_ping=True)


@st.cache_data(ttl=300)
def load_spans_value() -> pd.DataFrame:
    sql = f"select {', '.join(BASE_COLS)} from {TABLE_FQN}"
    with get_engine().connect() as con:
        df = pd.read_sql_query(sql, con)

    for c in ("start_date", "end_date"):
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    for c in ("games_missed", "injured_salary", "war_missed"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["games_missed"] = df["games_missed"].astype(int)

    for c in ("name", "position", "body_part", "injury_type", "side", "second_body_part", "second_injury_type"):
        df[c] = df[c].fillna("").astype(str)

    return df


def fmt_money_m(v):
    return f"${v:,.2f}M" if pd.notna(v) else ""


def fmt_war(v):
    return f"{v:.1f}" if pd.notna(v) else ""


# ----------------- Page UI -----------------
st.title("Injury Leaderboards")

df = load_spans_value()
if df.empty:
    st.info(f"No rows found in {TABLE_FQN}.")
    st.stop()

c1, c2, c3 = st.columns([1.2, 1.2, 1.0])

with c1:
    group_mode = st.radio(
        "View",
        ["Single injury span", "Career"],
        horizontal=True,
        index=0,
    )

with c2:
    rank_metric = st.radio(
        "Rank by",
        ["WAR missed", "Injured salary"],
        horizontal=True,
        index=0,
    )

with c3:
    top_n = st.slider("Top N", min_value=10, max_value=200, value=100, step=10)

metric_col = "war_missed" if rank_metric == "WAR missed" else "injured_salary"

# ----------------- Filters -----------------
with st.expander("Filters", expanded=False):
    positions = sorted([p for p in df["position"].unique().tolist() if p.strip() != ""])
    position_filter = st.multiselect("Position", options=positions, default=positions)

d = df.copy()
if position_filter:
    d = d[d["position"].isin(position_filter)]

if d.empty:
    st.info("No rows match your filters.")
    st.stop()

# ----------------- Build output -----------------
if group_mode == "Single injury span":
    out = d.copy()

    out = out[
        [
            "name",
            "start_date",
            "end_date",
            "position",
            "body_part",
            "injury_type",
            "side",
            "second_body_part",
            "second_injury_type",
            "games_missed",
            "injured_salary",
            "war_missed",
        ]
    ]

else:
    out = (
        d.groupby("name", as_index=False)
         .agg(
             number_of_injuries=("name", "size"),
             games_missed=("games_missed", "sum"),
             war_missed=("war_missed", "sum"),
             injured_salary=("injured_salary", "sum"),
         )
    )

    out = out[["name", "number_of_injuries", "games_missed", "war_missed", "injured_salary"]]

# Rank + sort + top N (NO rank column displayed)
out = out.sort_values(metric_col, ascending=False).head(top_n).reset_index(drop=True)

st.caption(f"Showing {len(out):,} rows")

# ----------------- Display with color coding -----------------
formatters = {}
if "injured_salary" in out.columns:
    formatters["injured_salary"] = fmt_money_m
if "war_missed" in out.columns:
    formatters["war_missed"] = fmt_war

# color-coded numeric columns (only those that exist in the current view)
color_cols = [c for c in ["games_missed", "war_missed", "injured_salary"] if c in out.columns]

try:
    styler = (
        out.style
        .format(formatters)
        .background_gradient(subset=color_cols)
        .hide(axis="index")
    )
    st.dataframe(styler, use_container_width=True)
except Exception:
    st.dataframe(out, use_container_width=True)