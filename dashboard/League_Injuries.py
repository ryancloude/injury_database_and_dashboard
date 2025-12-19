import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px

st.set_page_config(page_title="League Overview", layout="wide")

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
def ensure_checklist_initialized(key_prefix: str, options: list, default_selected: list | None = None):
    """
    Make sure the per-option checkbox keys exist.
    This prevents returning to the page with 'empty' selections because keys are missing.
    """
    default_selected = options if default_selected is None else default_selected

    needs_init = (
        f"{key_prefix}__initialized" not in st.session_state
        or st.session_state.get(f"{key_prefix}__len") != len(options)
        or (len(options) > 0 and f"{key_prefix}__opt_0" not in st.session_state)
    )

    if needs_init:
        _reset_checklist_state(key_prefix, options, default_selected)

    st.session_state[f"{key_prefix}__len"] = len(options)


def _reset_checklist_state(key_prefix: str, options: list, default_selected: list | None = None):
    default_selected = options if default_selected is None else default_selected
    default_set = set(default_selected)

    for i, opt in enumerate(options):
        st.session_state[f"{key_prefix}__opt_{i}"] = (opt in default_set)

    st.session_state[f"{key_prefix}__search"] = ""
    st.session_state[f"{key_prefix}__initialized"] = True
    st.session_state[f"{key_prefix}__cleared"] = False


def popover_multicheck(
    label: str,
    options: list,
    key_prefix: str,
    default_selected: list | None = None,
    help: str | None = None,
    n_cols: int = 2,
    max_visible: int = 120,
    list_height: int = 240,  # <-- controls popover height pressure
):
    default_selected = options if default_selected is None else default_selected
    init_key = f"{key_prefix}__initialized"

    if init_key not in st.session_state:
        _reset_checklist_state(key_prefix, options, default_selected)

    # If options length changed since last run, re-init defaults
    len_key = f"{key_prefix}__len"
    if st.session_state.get(len_key) != len(options):
        _reset_checklist_state(key_prefix, options, default_selected)
    st.session_state[len_key] = len(options)

    selected_count = sum(
        1 for i in range(len(options)) if st.session_state.get(f"{key_prefix}__opt_{i}", False)
    )

    with st.popover(f"{label} ({selected_count})", help=help):
        # Search first (usually still visible), but the key controls will be at the bottom
        q = st.text_input("Search", key=f"{key_prefix}__search")
        q_lower = q.strip().lower()

        filtered = (
            [opt for opt in options if q_lower in str(opt).lower()]
            if q_lower
            else list(options)
        )

        if len(filtered) > max_visible:
            st.info(f"Showing first {max_visible} of {len(filtered)}. Use Search to narrow.")
            filtered = filtered[:max_visible]

        # Scrollable list so the popover never needs to be huge
        with st.container(height=list_height):
            cols = st.columns(n_cols)
            opt_to_index = {opt: i for i, opt in enumerate(options)}  # stable keys

            for j, opt in enumerate(filtered):
                i = opt_to_index[opt]
                with cols[j % n_cols]:
                    st.checkbox(str(opt), key=f"{key_prefix}__opt_{i}")

        st.divider()

        c1, c2 = st.columns(2)
        if c1.button("Select all", use_container_width=True, key=f"{key_prefix}__btn_all"):
            for i in range(len(options)):
                st.session_state[f"{key_prefix}__opt_{i}"] = True
            st.session_state[f"{key_prefix}__cleared"] = False

        if c2.button("Clear all", use_container_width=True, key=f"{key_prefix}__btn_clear"):
            for i in range(len(options)):
                st.session_state[f"{key_prefix}__opt_{i}"] = False
            st.session_state[f"{key_prefix}__cleared"] = True

    return [opt for i, opt in enumerate(options) if st.session_state.get(f"{key_prefix}__opt_{i}", False)]

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
    metric: str = "injured_salary",
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
st.title("League Overview")

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
body_part_groups_all = (
    sorted(df_view["body_part_group"].dropna().astype(str).str.strip().unique().tolist())
    if "body_part_group" in df_view else []
)
injury_types_all      = _unique_union(df_view, "injury_type", "second_injury_type")

ensure_checklist_initialized("league_team", teams_all, teams_all)
ensure_checklist_initialized("league_bpg", body_part_groups_all, body_part_groups_all)
ensure_checklist_initialized("league_season", seasons_all, seasons_all)
ensure_checklist_initialized("league_pos", positions_all, positions_all)
ensure_checklist_initialized("league_bp", body_parts_all, body_parts_all)
ensure_checklist_initialized("league_it", injury_types_all, injury_types_all)

# ----------------- Filters (Sidebar) -----------------
with st.sidebar:
    st.header("Filters")

    if st.button("Reset filters", use_container_width=True):
        _reset_checklist_state("league_team", teams_all, teams_all)
        _reset_checklist_state("league_bpg", body_part_groups_all, body_part_groups_all)
        _reset_checklist_state("league_bp", body_parts_all, body_parts_all)
        _reset_checklist_state("league_it", injury_types_all, injury_types_all)
        _reset_checklist_state("league_season", seasons_all, seasons_all)
        _reset_checklist_state("league_pos", positions_all, positions_all)

    team_sel = popover_multicheck(
    "Team(s)",
    teams_all,
    key_prefix="league_team",
    default_selected=teams_all,
    help="Select one or more teams.",
    n_cols=3,
    max_visible=60,
    list_height=150,
)

    bpg_sel = popover_multicheck(
        "Body Part Group",
        body_part_groups_all,
        key_prefix="league_bpg",
        default_selected=body_part_groups_all,
        help="Matches primary body part group.",
        n_cols=2,
        max_visible=120,
    )

    # Smaller lists can go lower without causing popover clipping
    season_sel = popover_multicheck(
        "Season(s)",
        seasons_all,
        key_prefix="league_season",
        default_selected=seasons_all,
        help="Select one or more seasons.",
        n_cols=1,
        max_visible=40,
    )

    pos_sel = popover_multicheck(
        "Position(s)",
        positions_all,
        key_prefix="league_pos",
        default_selected=positions_all,
        help="Select one or more positions.",
        n_cols=1,
        max_visible=40,
    )

    with st.expander("Advanced filters", expanded=False):
        bp_sel = popover_multicheck(
            "Body Part (incl. secondary)",
            body_parts_all,
            key_prefix="league_bp",
            default_selected=body_parts_all,
        )

        it_sel = popover_multicheck(
            "Injury Type (incl. secondary)",
            injury_types_all,
            key_prefix="league_it",
            default_selected=injury_types_all,
        )

mask = pd.Series(True, index=df_view.index)

# Positions: empty selection = all positions
pos_active = pos_sel or positions_all
pos_cleared = st.session_state.get("league_pos__cleared", False)

if positions_all:
    if len(pos_sel) == 0:
        if pos_cleared:
            mask &= False
    elif len(pos_sel) != len(positions_all):
        mask &= df_view["position"].astype(str).isin(pos_sel)

# Seasons: empty selection = all seasons
season_active = season_sel or seasons_all
season_cleared = st.session_state.get("league_season__cleared", False)

if seasons_all:
    if len(season_sel) == 0:
        if season_cleared:
            mask &= False
    elif len(season_sel) != len(seasons_all):
        mask &= df_view["season"].isin(season_sel)

# Teams: empty selection = all teams
team_active = team_sel or teams_all
team_cleared = st.session_state.get("league_team__cleared", False)

if teams_all:
    if len(team_sel) == 0:
        if team_cleared:
            mask &= False          # user intentionally cleared -> show none
        else:
            pass                  # empty due to state -> treat as ALL (no filter)
    elif len(team_sel) != len(teams_all):
        mask &= df_view["team"].isin(team_sel)

# Body part group: empty selection = all groups
bpg_active = bpg_sel or body_part_groups_all
bpg_cleared = st.session_state.get("league_bpg__cleared", False)

if body_part_groups_all:
    if len(bpg_sel) == 0:
        if bpg_cleared:
            mask &= False
        else:
            pass  # treat empty as ALL
    elif len(bpg_sel) != len(body_part_groups_all):
        mask &= df_view["body_part_group"].astype(str).isin(bpg_sel)

# Body parts: empty selection = all parts
bp_active = bp_sel or body_parts_all
bp_cleared = st.session_state.get("league_bp__cleared", False)

if body_parts_all:
    if len(bp_sel) == 0:
        if bp_cleared:
            mask &= False
        else:
            pass
    elif len(bp_sel) != len(body_parts_all):
        m1 = df_view["body_part"].astype(str).isin(bp_sel) if "body_part" in df_view else False
        m2 = df_view["second_body_part"].astype(str).isin(bp_sel) if "second_body_part" in df_view else False
        mask &= (m1 | m2)

# Injury types: empty selection = all types
it_active = it_sel or injury_types_all
it_cleared = st.session_state.get("league_it__cleared", False)

if injury_types_all:
    if len(it_sel) == 0:
        if it_cleared:
            mask &= False
        else:
            pass
    elif len(it_sel) != len(injury_types_all):
        m1 = df_view["injury_type"].astype(str).isin(it_sel) if "injury_type" in df_view else False
        m2 = df_view["second_injury_type"].astype(str).isin(it_sel) if "second_injury_type" in df_view else False
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

group_label = st.radio(
    "Bars grouped by (x-axis)",
    dim_labels,
    horizontal=True,
    index=0,
    help="What each bar represents along the x-axis.",
)
color_label = st.radio(
    "Bars broken down by (stacked color)",
    dim_labels,
    horizontal=True,
    index=1 if len(dim_labels) > 1 else 0,
    help="What the colored segments within each bar represent.",
)

group_by = dim_map[group_label]
color_by = dim_map[color_label]

metric_map = {
    "Games missed": "games_missed",
    "Injuries": "injury_count",
    "Injured salary": "injured_salary",
    "WAR missed": "war_missed",
}
default_metric_label = "Injured salary"
metric_keys = list(metric_map.keys())
default_metric_index = metric_keys.index(default_metric_label) if default_metric_label in metric_keys else 0

metric_label = st.radio(
    "Value shown",
    metric_keys,
    horizontal=True,
    index=default_metric_index,
    help="The value that gets summed for each stacked segment.",
)
metric = metric_map[metric_label]

# ----------------- Chart + Summary -----------------
left, right = st.columns([2, 1], vertical_alignment="top")
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