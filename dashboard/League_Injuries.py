import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px

st.set_page_config(page_title="League Injuries", layout="wide")

# ----------------- Connections & caching -----------------
@st.cache_resource
def get_engine():
    dsn = os.getenv("LOCAL_READONLY_PG_DSN") or st.secrets.get("READONLY_PG_DSN")
    if not dsn:
        st.stop()
    return create_engine(dsn, pool_pre_ping=True)

@st.cache_data(ttl=300)
def load_league_injuries() -> pd.DataFrame:
    sql = text("""
        select team, season, name, position, start_date, end_date,
               games_missed, injury_type, body_part, body_part_group ,side,
               second_injury_type, second_body_part, currently_injured,
               injured_salary, war_missed
        from gold.team_season_injuries
    """)
    with get_engine().connect() as con:
        df = pd.read_sql_query(sql, con)

    # Dates
    for c in ("start_date", "end_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    # Numeric fields
    for c in ("games_missed", "injured_salary", "war_missed"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    if "games_missed" in df.columns:
        df["games_missed"] = df["games_missed"].astype(int)

    return df

# ----------------- Helpers -----------------
def _unique_union(df, a: str, b: str) -> list[str]:
    s1 = df[a] if a in df else pd.Series([], dtype=object)
    s2 = df[b] if b in df else pd.Series([], dtype=object)
    vals = pd.concat([s1, s2], ignore_index=True)
    vals = (vals.dropna().astype(str).str.strip())
    vals = vals[(vals != "") & (~vals.str.lower().isin({"none", "nan"}))]
    return sorted(vals.unique().tolist())

def to_bool(x):
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in {"t", "true", "1", "yes", "y"}

def aggregate_league(inj_df: pd.DataFrame, x_dim: str, color_dim: str, metric: str) -> pd.DataFrame:
    """
    Aggregate the filtered league injuries for the given grouping & metric.

    - x_dim: dimension on the x-axis (group bars by)
    - color_dim: dimension used for color (stack segments)
    - metric: 'games_missed' | 'injury_count' | 'injured_salary' | 'war_missed'
    """
    if inj_df.empty:
        # Keep shape predictable
        return inj_df.assign(value=pd.Series([], dtype="float64"))

    d = inj_df.copy()

    # Metric -> numeric "value"
    if metric == "injury_count":
        d["value"] = 1  # each row = one injury episode
    else:
        d["value"] = pd.to_numeric(d[metric], errors="coerce").fillna(0.0)

    # Grouping columns
    if x_dim == color_dim:
        group_cols = [x_dim]
    else:
        group_cols = [x_dim, color_dim]

    seg = d.groupby(group_cols, as_index=False)["value"].sum()

    # If x and color are the same, duplicate color column so stacked colors still work
    if x_dim == color_dim:
        seg[color_dim] = seg[x_dim]

    return seg


def league_bar(
    inj_df: pd.DataFrame,
    x_dim: str = "team",
    color_dim: str = "season",
    metric: str = "games_missed",
):
    if inj_df.empty:
        return None

    # Sanity guards (though group_by/color_by come from dim_map, so they're already valid)
    if x_dim not in {"team", "season", "injury_type", "body_part", "body_part_group"}:
        x_dim = "team"
    if color_dim not in {"team", "season", "injury_type", "body_part", "body_part_group"}:
        color_dim = "season"
    if metric not in {"games_missed", "injury_count", "injured_salary", "war_missed"}:
        metric = "games_missed"

    # Aggregate according to current settings
    seg = aggregate_league(inj_df, x_dim, color_dim, metric)
    if seg.empty:
        return None

    # Value label + hover formatting
    if metric == "injury_count":
        value_label = "Injuries"
        y_hover = "%{y}"
    else:
        if metric == "games_missed":
            value_label = "Games missed"
            y_hover = "%{y}"
        elif metric == "injured_salary":
            value_label = "Injured salary"
            y_hover = "$%{y:.2f}M"
        else:  # war_missed
            value_label = "WAR missed"
            y_hover = "%{y:.1f}"

    # Category order for x-axis
    if x_dim == "season":
        x_order = sorted(seg[x_dim].unique())
    else:
        x_order = (
            seg.groupby(x_dim)["value"].sum()
               .sort_values(ascending=False).index.tolist()
        )

    # Order legend / color by total value
    color_order = (
        seg.groupby(color_dim)["value"].sum()
           .sort_values(ascending=False).index.tolist()
    )

    # Make season available for hover via custom_data
    custom_data_cols = []
    if "season" in seg.columns:
        custom_data_cols.append("season")

    # Map all possible grouping/coloring dimensions to nice labels
    dim_labels = {
        "team": "Team",
        "season": "Season",
        "injury_type": "Injury Type",
        "body_part": "Body Part",
        "body_part_group": "Body Part Group",
    }

    x_label = dim_labels.get(x_dim, x_dim)
    c_label = dim_labels.get(color_dim, color_dim)

    fig = px.bar(
        seg,
        x=x_dim,
        y="value",
        color=color_dim,
        category_orders={x_dim: x_order, color_dim: color_order},
        custom_data=custom_data_cols,
    )

    # Hover: special handling when coloring by season
    if color_dim == "season":
        hover = (
            f"{x_label}: " + "%{x}<br>"
            "Season: %{customdata[0]}<br>"
            f"{value_label}: " + y_hover + "<extra></extra>"
        )
    else:
        hover = (
            f"{x_label}: " + "%{x}<br>"
            f"{c_label}: " + "%{fullData.name}<br>"
            f"{value_label}: " + y_hover + "<extra></extra>"
        )

    fig.update_traces(
        hovertemplate=hover,
        marker_line_width=0.5,
        marker_line_color="rgba(0,0,0,0.2)",
    )

    fig.update_layout(
        barmode="stack",
        xaxis_title=None,
        yaxis_title=value_label,
        legend_title=c_label,
        hoverlabel=dict(namelength=-1),
    )

    if metric == "injured_salary":
        fig.update_yaxes(tickprefix="$", ticksuffix="M")
    elif metric == "war_missed":
        fig.update_yaxes(tickformat=".1f")

    # Hide legend + color scale when coloring by season
    if color_dim == "season":
        fig.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
        )
        for tr in fig.data:
            tr.showlegend = False
    else:
        fig.update_layout(
            showlegend=True,
            coloraxis_showscale=True,
        )

    return fig

def color_current(val: bool):
    return "background-color: #d1f7c4; color: #0a0;" if val else "background-color: #ffd6d6; color: #a00;"

# ----------------- Page UI -----------------
st.title("League Injuries")

inj = load_league_injuries()
if inj.empty:
    st.warning("No data in gold.team_season_injuries.")
    st.stop()

df_view = inj.copy()

if "currently_injured" in df_view.columns:
    df_view["currently_injured"] = df_view["currently_injured"].map(to_bool)

for c in ("games_missed", "injured_salary", "war_missed"):
    if c in df_view.columns:
        df_view[c] = pd.to_numeric(df_view[c], errors="coerce").fillna(0)
if "games_missed" in df_view.columns:
    df_view["games_missed"] = df_view["games_missed"].astype(int)

teams_all   = sorted(df_view["team"].dropna().astype(str).unique().tolist()) if "team" in df_view else []
seasons_all = sorted(df_view["season"].dropna().astype(int).unique().tolist()) if "season" in df_view else []
positions_all = sorted(df_view["position"].dropna().astype(str).str.strip().unique().tolist()) if "position" in df_view else []

body_parts_all        = _unique_union(df_view, "body_part", "second_body_part")
body_part_groups_all  = _unique_union(df_view, "body_part_group", "second_body_part_group")
injury_types_all      = _unique_union(df_view, "injury_type", "second_injury_type")

# ----------------- Filters -----------------
# ----------------- Filters -----------------
c1, c2, c3, c4, c5, c6 = st.columns(6)

with c1:
    if st.button("All positions", use_container_width=True):
        st.session_state["league_pos_sel"] = positions_all
    pos_sel = st.multiselect(
        "Position(s)",
        positions_all,
        default=positions_all,
        key="league_pos_sel",
    )

with c2:
    if st.button("All seasons", use_container_width=True):
        st.session_state["league_season_sel"] = seasons_all
    season_sel = st.multiselect(
        "Season(s)",
        seasons_all,
        default=seasons_all,
        key="league_season_sel",
    )

with c3:
    if st.button("All teams", use_container_width=True):
        st.session_state["league_team_sel"] = teams_all
    team_sel = st.multiselect(
        "Team(s)",
        teams_all,
        default=teams_all,
        key="league_team_sel",
    )

with c4:
    if st.button("All body part groups", use_container_width=True):
        st.session_state["league_bpg_sel"] = body_part_groups_all
    bpg_sel = st.multiselect(
        "Body Part Group (incl. secondary)",
        body_part_groups_all,
        default=body_part_groups_all,
        key="league_bpg_sel",
    )

with c5:
    if st.button("All body parts", use_container_width=True):
        st.session_state["league_bp_sel"] = body_parts_all
    bp_sel = st.multiselect(
        "Body Part (incl. secondary)",
        body_parts_all,
        default=body_parts_all,
        key="league_bp_sel",
    )

with c6:
    if st.button("All injury types", use_container_width=True):
        st.session_state["league_it_sel"] = injury_types_all
    it_sel = st.multiselect(
        "Injury Type (incl. secondary)",
        injury_types_all,
        default=injury_types_all,
        key="league_it_sel",
    )

mask = pd.Series(True, index=df_view.index)

# Positions: empty selection = all positions
pos_active = pos_sel or positions_all
if positions_all and pos_active and len(pos_active) != len(positions_all):
    mask &= df_view["position"].astype(str).isin(pos_active)

# Seasons: empty selection = all seasons
season_active = season_sel or seasons_all
if seasons_all and season_active and len(season_active) != len(seasons_all):
    mask &= df_view["season"].isin(season_active)

# Teams: empty selection = all teams
team_active = team_sel or teams_all
if teams_all and team_active and len(team_active) != len(teams_all):
    mask &= df_view["team"].isin(team_active)

# Body part group: empty selection = all groups
bpg_active = bpg_sel or body_part_groups_all
if body_part_groups_all and len(bpg_active) != len(body_part_groups_all):
    m1 = df_view["body_part_group"].astype(str).isin(bpg_active) if "body_part_group" in df_view else False
    m2 = df_view["second_body_part_group"].astype(str).isin(bpg_active) if "second_body_part_group" in df_view else False
    mask &= (m1 | m2)

# Body parts: empty selection = all parts
bp_active = bp_sel or body_parts_all
if body_parts_all and len(bp_active) != len(body_parts_all):
    m1 = df_view["body_part"].astype(str).isin(bp_active) if "body_part" in df_view else False
    m2 = df_view["second_body_part"].astype(str).isin(bp_active) if "second_body_part" in df_view else False
    mask &= (m1 | m2)

# Injury types: empty selection = all types
it_active = it_sel or injury_types_all
if injury_types_all and len(it_active) != len(injury_types_all):
    m1 = df_view["injury_type"].astype(str).isin(it_active) if "injury_type" in df_view else False
    m2 = df_view["second_injury_type"].astype(str).isin(it_active) if "second_injury_type" in df_view else False
    mask &= (m1 | m2)

inj_filtered = df_view.loc[mask].copy()

# ----------------- Chart controls -----------------
dim_map = {
    "Team": "team",
    "Season": "season",
    "Injury Type": "injury_type",
    "Body Part": "body_part",
    "Body Part Group": "body_part_group",
}
dim_labels = list(dim_map.keys())

group_label = st.radio("Group bars by", dim_labels, horizontal=True, index=0)
color_label = st.radio(
    "Color bars by",
    dim_labels,
    horizontal=True,
    index=1 if len(dim_labels) > 1 else 0,
)

group_by = dim_map[group_label]
color_by = dim_map[color_label]

metric_map = {
    "Games missed": "games_missed",
    "Injuries": "injury_count",
    "Injured salary": "injured_salary",
    "WAR missed": "war_missed",
}
metric_label = st.radio("Metric", list(metric_map.keys()), horizontal=True, index=0)
metric = metric_map[metric_label]

# ----------------- Chart + Summary -----------------
left, right = st.columns([2, 1])
with left:
    fig = league_bar(
        inj_filtered,
        x_dim=group_by,
        color_dim=color_by,
        metric=metric,
    )
    if fig is None:
        st.info("No injuries to plot for these filters.")
    else:
        # Hide the toolbar when coloring by season
        plot_config = {"displayModeBar": color_by != "season"}
        st.plotly_chart(
            fig,
            use_container_width=True,
            key=f"league_{group_by}_{color_by}_{metric}",
            config=plot_config,
        )

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

        label_nice = {
            "team": "Team",
            "season": "Season",
            "injury_type": "Injury Type",
            "body_part": "Body Part",
            "body_part_group": "Body Part Group",
        }[group_by]
        pretty_val_name = value_col.replace("_", " ").title()

        display_table = (
            summary_table
            .rename(columns={group_by: label_nice, value_col: pretty_val_name})
            .reset_index(drop=True)          # 👈 reset the index here
            .copy()
        )

        # Format display values
        if metric == "injured_salary":
            display_table[pretty_val_name] = display_table[pretty_val_name].map(
                lambda v: f"${v:,.2f}M"
            )
        elif metric == "war_missed":
            display_table[pretty_val_name] = display_table[pretty_val_name].map(
                lambda v: f"{v:.1f}"
            )

        # Metric above the table
        if metric == "war_missed":
            st.metric(total_label, f"{total_val:.1f}")
        elif metric == "injured_salary":
            st.metric(total_label, f"${total_val:,.2f}M")
        else:
            st.metric(total_label, total_val)

        st.table(display_table)
    else:
        st.metric("Total", 0)
        st.info("No injury rows for these filters.")

# ----------------- Aggregated table (matches chart) -----------------
st.subheader("Injuries (aggregated to match chart)")

seg_bottom = aggregate_league(inj_filtered, group_by, color_by, metric)

if seg_bottom.empty:
    st.caption("0 rows")
else:
    dim_nice = {
        "team": "Team",
        "season": "Season",
        "injury_type": "Injury Type",
        "body_part": "Body Part",
        "body_part_group": "Body Part Group",
    }
    row_label = dim_nice[group_by]
    col_label = dim_nice[color_by]

    pretty_val_name = (
        "Injuries" if metric == "injury_count"
        else metric.replace("_", " ").title()
    )

    tbl = seg_bottom.copy()

    # When group_by == color_by, aggregate_league duplicated the column.
    # Drop the duplicate so we only have one grouping col + value.
    if group_by == color_by and color_by in tbl.columns:
        tbl = tbl.drop(columns=[color_by])

    # ---------- SORTING TO MATCH THE CHART ----------
    if group_by == color_by:
        # 1D case: sort by metric descending
        tbl = tbl.sort_values("value", ascending=False)
    else:
        # 2D case: match x_order / color_order logic from league_bar

        # Order for the x-dimension (group_by)
        if group_by == "season":
            g_order = sorted(tbl[group_by].unique())
        else:
            g_order = (
                tbl.groupby(group_by)["value"]
                   .sum()
                   .sort_values(ascending=False)
                   .index
                   .tolist()
            )

        # Order for the color dimension (color_by)
        if color_by == "season":
            c_order = sorted(tbl[color_by].unique())
        else:
            c_order = (
                tbl.groupby(color_by)["value"]
                   .sum()
                   .sort_values(ascending=False)
                   .index
                   .tolist()
            )

        g_rank = {v: i for i, v in enumerate(g_order)}
        c_rank = {v: i for i, v in enumerate(c_order)}

        tbl["_g_order"] = tbl[group_by].map(g_rank)
        tbl["_c_order"] = tbl[color_by].map(c_rank)

        tbl = (
            tbl.sort_values(["_g_order", "_c_order"])
               .drop(columns=["_g_order", "_c_order"])
        )

    # ---------- RENAME COLUMNS ----------
    rename_map = {group_by: row_label, "value": pretty_val_name}
    if group_by != color_by:
        rename_map[color_by] = col_label

    display_bottom = (
        tbl.rename(columns=rename_map)
           .reset_index(drop=True)
    )

    # Keep metric column NUMERIC; only cast games_missed to int
    if metric == "games_missed":
        display_bottom[pretty_val_name] = display_bottom[pretty_val_name].astype(int)

    # ---------- FORMAT FOR DISPLAY USING STYLER (keeps dtype numeric) ----------
    fmt = {}
    if metric == "injured_salary":
        fmt[pretty_val_name] = lambda v: f"${v:,.2f}M"
    elif metric == "war_missed":
        fmt[pretty_val_name] = lambda v: f"{v:.1f}"

    if fmt:
        styler_bottom = display_bottom.style.format(fmt)
        st.dataframe(styler_bottom, use_container_width=True)
    else:
        st.dataframe(display_bottom, use_container_width=True)