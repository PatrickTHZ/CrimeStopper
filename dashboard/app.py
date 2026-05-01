from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "processed" / "bocsar"
RAW_DIR = ROOT / "data" / "raw" / "bocsar"
MONTH_COL = re.compile(r"^\d{4}-\d{2}$")

PALETTE = [
    "#0A84FF",
    "#30D158",
    "#FF9F0A",
    "#BF5AF2",
    "#64D2FF",
    "#FF453A",
    "#FFD60A",
    "#5E5CE6",
    "#FF375F",
    "#32D74B",
]
HEAT_SCALE = ["#050505", "#18202c", "#0A84FF", "#30D158", "#FF9F0A", "#FF453A"]
MAP_DENSITY_COLORS = {"Low": "#FF9F0A", "Medium": "#FF453A", "High": "#BF5AF2"}
SYSTEM_FONT = "-apple-system, BlinkMacSystemFont, 'SF Pro Display', 'SF Pro Text', Inter, sans-serif"
pio.templates["crimestoppers_apple_dark"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.025)",
        font={"family": SYSTEM_FONT, "color": "#F5F5F7"},
        title={"font": {"size": 20, "color": "#F5F5F7"}},
        colorway=PALETTE,
        xaxis={"gridcolor": "rgba(255,255,255,0.08)", "zerolinecolor": "rgba(255,255,255,0.14)"},
        yaxis={"gridcolor": "rgba(255,255,255,0.08)", "zerolinecolor": "rgba(255,255,255,0.14)"},
        legend={"font": {"color": "#D1D1D6"}},
    )
)
pio.templates.default = "crimestoppers_apple_dark"


st.set_page_config(
    page_title="Crime Analysis Dashboard | CrimeStoppers",
    page_icon="chart_with_upwards_trend",
    layout="wide",
)

st.markdown(
    """
    <style>
    :root {
        --surface: #000000;
        --panel: rgba(28, 28, 30, 0.72);
        --panel-2: rgba(44, 44, 46, 0.78);
        --line: rgba(255, 255, 255, 0.13);
        --ink: #f5f5f7;
        --muted: #a1a1a6;
        --blue: #0A84FF;
        --green: #30D158;
        --orange: #FF9F0A;
        --purple: #BF5AF2;
    }
    .stApp {
        background:
            linear-gradient(180deg, #050505 0%, #000000 42%, #030407 100%);
        color: var(--ink);
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", Inter, sans-serif;
    }
    [data-testid="stHeader"] {
        background: rgba(0, 0, 0, 0.74);
        backdrop-filter: blur(22px);
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
    }
    [data-testid="stSidebar"] {
        background: rgba(12, 12, 14, 0.82);
        backdrop-filter: blur(24px);
        border-right: 1px solid var(--line);
    }
    [data-testid="stSidebar"] * { color: var(--ink); }
    [data-testid="stSidebar"] [data-baseweb="radio"] label,
    [data-testid="stSidebar"] [data-baseweb="slider"] { color: var(--ink); }
    .block-container { padding-top: 2rem; max-width: 1480px; }
    h1, h2, h3 {
        letter-spacing: 0;
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", Inter, sans-serif;
    }
    h1 { font-size: 2rem; font-weight: 720; margin-bottom: .1rem; }
    h2 { font-size: 1.25rem; margin-top: .4rem; }
    h3 { font-size: 1rem; }
    .apple-hero {
        padding: 18px 0 24px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        margin-bottom: 18px;
    }
    .apple-hero h1 {
        font-size: clamp(2.1rem, 5vw, 4.9rem);
        line-height: 1.02;
        font-weight: 760;
        margin: 0;
        color: #f5f5f7;
    }
    .apple-hero h2 {
        font-size: clamp(1.05rem, 2vw, 1.65rem);
        line-height: 1.25;
        font-weight: 560;
        color: #a1a1a6;
        margin: 8px 0 0;
    }
    .apple-hero p {
        max-width: 780px;
        color: #c7c7cc;
        margin: 16px 0 0;
        font-size: 1rem;
        line-height: 1.45;
    }
    p, label, span, div { color: inherit; }
    .stCaption, [data-testid="stCaptionContainer"] { color: var(--muted); }
    [data-testid="stMetric"] {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 14px 16px;
        backdrop-filter: blur(18px);
        box-shadow: 0 22px 44px rgba(0, 0, 0, 0.34);
    }
    [data-testid="stMetricLabel"] { color: var(--muted); }
    [data-testid="stTabs"] {
        background: rgba(28, 28, 30, 0.56);
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 4px;
        backdrop-filter: blur(18px);
    }
    [data-testid="stTabs"] button {
        color: var(--muted);
        border-radius: 7px;
        min-height: 38px;
    }
    [data-testid="stTabs"] button[aria-selected="true"] {
        color: var(--ink);
        background: rgba(255, 255, 255, 0.1);
    }
    [data-baseweb="select"] > div,
    [data-baseweb="input"] > div,
    [data-baseweb="tag"] {
        background: var(--panel-2);
        border-color: var(--line);
        color: var(--ink);
    }
    [data-baseweb="select"] span,
    [data-baseweb="tag"] span { color: var(--ink); }
    div[data-testid="stDataFrame"] {
        border: 1px solid var(--line);
        border-radius: 8px;
        overflow: hidden;
    }
    [data-testid="stPlotlyChart"] {
        background: rgba(28, 28, 30, 0.52);
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 8px;
        backdrop-filter: blur(18px);
        box-shadow: 0 20px 42px rgba(0, 0, 0, 0.28);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def require_processed_data() -> None:
    missing = [
        path
        for path in [
            DATA_DIR / "metadata.json",
            DATA_DIR / "suburb_index.parquet",
            DATA_DIR / "suburb_category_wide.parquet",
            DATA_DIR / "suburb_yearly_by_category.parquet",
            DATA_DIR / "nsw_monthly_by_category.parquet",
            DATA_DIR / "lga_index.parquet",
            DATA_DIR / "lga_trends.parquet",
        ]
        if not path.exists()
    ]
    if missing:
        with st.spinner("Preparing BOCSAR dashboard data for first launch..."):
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "build_dashboard_data.py"),
                    "--download-missing",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=600,
            )
        if result.returncode != 0:
            st.error("Processed dashboard data is missing and automatic setup failed.")
            st.code(result.stderr or result.stdout, language="text")
            st.code("python scripts/build_dashboard_data.py --download-missing", language="bash")
            st.stop()
        st.cache_data.clear()


@st.cache_data(show_spinner=False)
def load_parquet(name: str) -> pd.DataFrame:
    return pd.read_parquet(DATA_DIR / name)


@st.cache_data(show_spinner=False)
def load_metadata() -> dict:
    return json.loads((DATA_DIR / "metadata.json").read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def load_manifest() -> pd.DataFrame:
    path = RAW_DIR / "download_manifest.tsv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, sep="\t")


@st.cache_data(show_spinner=False)
def load_geojson(relative_path: str) -> dict:
    return json.loads((ROOT / relative_path).read_text(encoding="utf-8"))


def format_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{int(round(float(value))):,}"


def format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:+.1f}%"


def month_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if MONTH_COL.match(str(col))]


def annual_line(df: pd.DataFrame, x: str, y: str, color: str, title: str) -> go.Figure:
    fig = px.line(
        df,
        x=x,
        y=y,
        color=color,
        markers=True,
        color_discrete_sequence=PALETTE,
        title=title,
    )
    fig.update_layout(
        height=390,
        margin=dict(l=8, r=8, t=48, b=8),
        legend_title_text="",
        hovermode="x unified",
    )
    fig.update_yaxes(title="Incidents", rangemode="tozero")
    return fig


def compact_bar(df: pd.DataFrame, x: str, y: str, title: str, color: str | None = None) -> go.Figure:
    fig = px.bar(
        df,
        x=x,
        y=y,
        color=color,
        orientation="h",
        color_discrete_sequence=PALETTE,
        title=title,
    )
    fig.update_layout(height=430, margin=dict(l=8, r=8, t=48, b=8), showlegend=False)
    fig.update_xaxes(title="Incidents")
    fig.update_yaxes(title="")
    return fig


def coordinates_iter(geometry: dict):
    if geometry["type"] == "Polygon":
        for ring in geometry["coordinates"]:
            for lon, lat in ring:
                yield lon, lat
    elif geometry["type"] == "MultiPolygon":
        for polygon in geometry["coordinates"]:
            for ring in polygon:
                for lon, lat in ring:
                    yield lon, lat


def geojson_bounds(geojson: dict) -> tuple[float, float, float, float]:
    lons: list[float] = []
    lats: list[float] = []
    for feature in geojson["features"]:
        for lon, lat in coordinates_iter(feature["geometry"]):
            lons.append(lon)
            lats.append(lat)
    return min(lons), min(lats), max(lons), max(lats)


require_processed_data()

metadata = load_metadata()
latest_year = metadata["suburb"]["latest_year"]
month_min = metadata["suburb"]["month_min"]
month_max = metadata["suburb"]["month_max"]

suburb_index = load_parquet("suburb_index.parquet")
nsw_monthly = load_parquet("nsw_monthly_by_category.parquet")

st.markdown(
    """
    <section class="apple-hero">
        <h1>Crime Analysis Dashboard</h1>
        <h2>CrimeStoppers</h2>
        <p>Explore NSW crime trends with suburb drill-downs, LGA comparisons, seasonality heatmaps, and BOCSAR hotspot mapping.</p>
    </section>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Filters")
    data_range = st.caption(f"BOCSAR data: {month_min} to {month_max}")
    year_bounds = (1995, latest_year)
    year_range = st.slider(
        "Year range",
        min_value=year_bounds[0],
        max_value=year_bounds[1],
        value=(2016, latest_year),
    )
    st.divider()
    st.caption("Source: BOCSAR open datasets")


categories = sorted(nsw_monthly["Offence category"].dropna().unique())
latest_nsw = nsw_monthly[nsw_monthly["month"].dt.year == latest_year]
prior_nsw = nsw_monthly[nsw_monthly["month"].dt.year == latest_year - 1]
category_2025 = (
    latest_nsw.groupby("Offence category", as_index=False)["incidents"]
    .sum()
    .sort_values("incidents", ascending=False)
)
default_categories = category_2025.head(5)["Offence category"].tolist()

overview_tab, suburb_tab, lga_tab, map_tab, catalogue_tab = st.tabs(
    ["Overview", "Suburb Analysis", "LGA Rankings", "Hotspot Map", "Data Catalogue"]
)

with overview_tab:
    total_latest = int(latest_nsw["incidents"].sum())
    total_prior = int(prior_nsw["incidents"].sum())
    change_pct = ((total_latest - total_prior) / total_prior * 100) if total_prior else None
    top_category = category_2025.iloc[0]
    top_suburb = suburb_index.iloc[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(f"NSW Incidents {latest_year}", format_number(total_latest), format_pct(change_pct))
    k2.metric("Top Category", top_category["Offence category"], format_number(top_category["incidents"]))
    k3.metric("Top Suburb", top_suburb["Suburb"], format_number(top_suburb[f"incidents_{latest_year}"]))
    k4.metric("Tracked Suburbs", format_number(metadata["suburb"]["area_count"]), f"{metadata['suburb']['category_count']} categories")

    selected_categories = st.multiselect(
        "Offence categories",
        categories,
        default=default_categories,
        key="overview_categories",
    )
    if not selected_categories:
        selected_categories = default_categories

    trend = nsw_monthly[
        (nsw_monthly["Offence category"].isin(selected_categories))
        & (nsw_monthly["month"].dt.year.between(*year_range))
    ]
    fig = px.line(
        trend,
        x="month",
        y="incidents",
        color="Offence category",
        color_discrete_sequence=PALETTE,
        title="NSW Monthly Incidents",
    )
    fig.update_layout(height=420, hovermode="x unified", legend_title_text="", margin=dict(l=8, r=8, t=48, b=8))
    st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns([1, 1])
    with c1:
        cat_bar = category_2025.head(12).sort_values("incidents")
        st.plotly_chart(
            compact_bar(cat_bar, "incidents", "Offence category", f"Top Categories {latest_year}"),
            use_container_width=True,
        )
    with c2:
        top_suburbs = suburb_index.head(15).sort_values(f"incidents_{latest_year}")
        st.plotly_chart(
            compact_bar(top_suburbs, f"incidents_{latest_year}", "Suburb", f"Top Suburbs {latest_year}"),
            use_container_width=True,
        )

    h1, h2 = st.columns([1.15, 1])
    with h1:
        heat = nsw_monthly.copy()
        heat["year"] = heat["month"].dt.year
        heat["month_name"] = heat["month"].dt.strftime("%b")
        heat["month_num"] = heat["month"].dt.month
        heat = (
            heat[heat["year"].between(*year_range)]
            .groupby(["year", "month_num", "month_name"], as_index=False)["incidents"]
            .sum()
            .sort_values(["month_num", "year"])
        )
        matrix = heat.pivot(index="month_name", columns="year", values="incidents")
        matrix = matrix.reindex(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
        heat_fig = px.imshow(
            matrix,
            aspect="auto",
            color_continuous_scale=HEAT_SCALE,
            title="Seasonality Heatmap",
            labels=dict(x="Year", y="Month", color="Incidents"),
        )
        heat_fig.update_layout(height=410, margin=dict(l=8, r=8, t=48, b=8))
        st.plotly_chart(heat_fig, use_container_width=True)
    with h2:
        tree = category_2025.head(14).copy()
        tree_fig = px.treemap(
            tree,
            path=["Offence category"],
            values="incidents",
            color="incidents",
            color_continuous_scale=HEAT_SCALE,
            title=f"Category Composition {latest_year}",
        )
        tree_fig.update_layout(height=410, margin=dict(l=8, r=8, t=48, b=8))
        st.plotly_chart(tree_fig, use_container_width=True)

with suburb_tab:
    suburb_yearly = load_parquet("suburb_yearly_by_category.parquet")
    suburb_wide = load_parquet("suburb_category_wide.parquet")

    top_first = suburb_index["Suburb"].head(250).tolist()
    all_suburbs = sorted(suburb_index["Suburb"].unique())
    suburb_options = list(dict.fromkeys(top_first + all_suburbs))

    selected_suburb = st.selectbox("Suburb", suburb_options, index=0)
    suburb_cats = sorted(
        suburb_yearly.loc[suburb_yearly["Suburb"] == selected_suburb, "Offence category"].unique()
    )
    selected_suburb_categories = st.multiselect(
        "Suburb offence categories",
        suburb_cats,
        default=[cat for cat in default_categories if cat in suburb_cats][:4] or suburb_cats[:4],
        key="suburb_categories",
    )
    if not selected_suburb_categories:
        selected_suburb_categories = suburb_cats[:4]

    suburb_latest = suburb_index[suburb_index["Suburb"] == selected_suburb].iloc[0]
    m1, m2, m3 = st.columns(3)
    m1.metric(f"{selected_suburb} Incidents {latest_year}", format_number(suburb_latest[f"incidents_{latest_year}"]), format_pct(suburb_latest["change_pct_vs_prior"]))
    m2.metric("Largest Category", suburb_latest[f"top_category_{latest_year}"], format_number(suburb_latest[f"top_category_incidents_{latest_year}"]))
    rank = int(suburb_index.index[suburb_index["Suburb"] == selected_suburb][0]) + 1
    m3.metric("NSW Suburb Rank", f"#{rank}", "by total incidents")

    annual = suburb_yearly[
        (suburb_yearly["Suburb"] == selected_suburb)
        & (suburb_yearly["Offence category"].isin(selected_suburb_categories))
        & (suburb_yearly["year"].between(*year_range))
    ]
    annual = annual.groupby(["year", "Offence category"], as_index=False)["incidents"].sum()
    st.plotly_chart(
        annual_line(annual, "year", "incidents", "Offence category", f"{selected_suburb}: Annual Incidents"),
        use_container_width=True,
    )

    compare_defaults = list(dict.fromkeys([selected_suburb] + suburb_index["Suburb"].head(4).tolist()))[:5]
    compare_suburbs = st.multiselect(
        "Compare suburbs",
        suburb_options,
        default=compare_defaults,
        key="compare_suburbs",
    )
    compare = suburb_yearly[
        (suburb_yearly["Suburb"].isin(compare_suburbs))
        & (suburb_yearly["Offence category"].isin(selected_suburb_categories))
        & (suburb_yearly["year"].between(*year_range))
    ]
    compare = compare.groupby(["Suburb", "year"], as_index=False)["incidents"].sum()
    st.plotly_chart(
        annual_line(compare, "year", "incidents", "Suburb", "Selected Category Comparison"),
        use_container_width=True,
    )

    heat_source = suburb_yearly[
        (suburb_yearly["Suburb"] == selected_suburb)
        & (suburb_yearly["year"].between(max(year_range[0], latest_year - 9), year_range[1]))
    ]
    heat_source = (
        heat_source.groupby(["Offence category", "year"], as_index=False)["incidents"]
        .sum()
        .sort_values("incidents", ascending=False)
    )
    top_heat_categories = (
        heat_source.groupby("Offence category")["incidents"]
        .sum()
        .sort_values(ascending=False)
        .head(12)
        .index
    )
    heat_source = heat_source[heat_source["Offence category"].isin(top_heat_categories)]
    suburb_matrix = heat_source.pivot(index="Offence category", columns="year", values="incidents").fillna(0)
    suburb_heat = px.imshow(
        suburb_matrix,
        aspect="auto",
        color_continuous_scale=HEAT_SCALE,
        title=f"{selected_suburb}: Category Intensity",
        labels=dict(x="Year", y="Category", color="Incidents"),
    )
    suburb_heat.update_layout(height=460, margin=dict(l=8, r=8, t=48, b=8))
    st.plotly_chart(suburb_heat, use_container_width=True)

    with st.expander("Monthly detail", expanded=False):
        rows = suburb_wide[
            (suburb_wide["Suburb"] == selected_suburb)
            & (suburb_wide["Offence category"].isin(selected_suburb_categories))
        ]
        months = [
            col
            for col in month_columns(rows)
            if year_range[0] <= int(col[:4]) <= year_range[1]
        ]
        monthly_detail = rows.melt(
            id_vars=["Suburb", "Offence category"],
            value_vars=months,
            var_name="month",
            value_name="incidents",
        )
        monthly_detail["month"] = pd.to_datetime(monthly_detail["month"])
        monthly_detail = monthly_detail.groupby(["month", "Offence category"], as_index=False)["incidents"].sum()
        detail_fig = px.area(
            monthly_detail,
            x="month",
            y="incidents",
            color="Offence category",
            color_discrete_sequence=PALETTE,
            title="Monthly Detail",
        )
        detail_fig.update_layout(height=380, hovermode="x unified", legend_title_text="", margin=dict(l=8, r=8, t=48, b=8))
        st.plotly_chart(detail_fig, use_container_width=True)

with lga_tab:
    lga_trends = load_parquet("lga_trends.parquet")
    lga_index = load_parquet("lga_index.parquet")

    offence_types = sorted(lga_trends["offence_type"].dropna().unique())
    default_offence = (
        offence_types.index("Domestic violence related assault")
        if "Domestic violence related assault" in offence_types
        else 0
    )
    offence = st.selectbox("Offence type", offence_types, index=default_offence)
    metric = st.radio("Rank by", ["rate_2025", "incidents_2025"], horizontal=True)

    offence_rows = lga_trends[lga_trends["offence_type"] == offence].copy()
    ranked = offence_rows.dropna(subset=[metric]).sort_values(metric, ascending=False)

    c1, c2 = st.columns([1.1, 1])
    with c1:
        top_lga = ranked.iloc[0] if not ranked.empty else None
        if top_lga is not None:
            st.metric("Highest LGA", top_lga["lga"], format_number(top_lga[metric]))
        display_cols = [
            "lga",
            "incidents_2025",
            "rate_2025",
            "rank_2025",
            "trend_2y",
            "trend_10y",
        ]
        st.dataframe(
            ranked[display_cols].head(25),
            use_container_width=True,
            hide_index=True,
        )
    with c2:
        top_chart = ranked.head(15).sort_values(metric)
        st.plotly_chart(
            compact_bar(top_chart, metric, "lga", f"Top LGAs: {offence}"),
            use_container_width=True,
        )

    lga_options = sorted(lga_trends["lga"].dropna().unique())
    selected_lgas = st.multiselect(
        "Compare LGAs",
        lga_options,
        default=ranked["lga"].head(5).tolist(),
        key="compare_lgas",
    )
    annual_cols = [f"incidents_{year}" for year in range(year_range[0], latest_year + 1) if f"incidents_{year}" in lga_trends.columns]
    compare_lga = offence_rows[offence_rows["lga"].isin(selected_lgas)]
    compare_lga = compare_lga.melt(
        id_vars=["lga", "offence_type"],
        value_vars=annual_cols,
        var_name="year",
        value_name="incidents",
    )
    compare_lga["year"] = compare_lga["year"].str.extract(r"(\d{4})").astype(int)
    st.plotly_chart(
        annual_line(compare_lga, "year", "incidents", "lga", f"LGA Annual Comparison: {offence}"),
        use_container_width=True,
    )

    scatter = offence_rows.copy()
    scatter["rate_2025"] = pd.to_numeric(scatter["rate_2025"], errors="coerce")
    scatter["incidents_2025"] = pd.to_numeric(scatter["incidents_2025"], errors="coerce")
    scatter = scatter.dropna(subset=["rate_2025", "incidents_2025"])
    scatter_fig = px.scatter(
        scatter,
        x="incidents_2025",
        y="rate_2025",
        size="incidents_2025",
        color="trend_2y",
        hover_name="lga",
        color_discrete_sequence=PALETTE,
        title=f"Rate vs Volume: {offence}",
        labels={"incidents_2025": "Incidents 2025", "rate_2025": "Rate per 100,000"},
    )
    scatter_fig.update_layout(height=430, margin=dict(l=8, r=8, t=48, b=8), legend_title_text="2-year trend")
    st.plotly_chart(scatter_fig, use_container_width=True)

    with st.expander("LGA total incident index", expanded=False):
        st.dataframe(
            lga_index.head(40),
            use_container_width=True,
            hide_index=True,
        )

with map_tab:
    hotspot_layers = load_parquet("hotspot_layers.parquet")
    st.subheader("BOCSAR Hotspot Polygons")
    st.caption("Spatial hotspot layers are available for selected offences and the latest published hotspot period.")

    layer_labels = hotspot_layers.sort_values("crime_label")["crime_label"].tolist()
    selected_layer_label = st.selectbox("Hotspot offence", layer_labels)
    selected_layer = hotspot_layers[hotspot_layers["crime_label"] == selected_layer_label].iloc[0]
    geojson = load_geojson(selected_layer["file"])
    min_lon, min_lat, max_lon, max_lat = geojson_bounds(geojson)
    center = {"lat": (min_lat + max_lat) / 2, "lon": (min_lon + max_lon) / 2}

    map_rows = pd.DataFrame(
        [
            {
                "id": feature["id"],
                "density": feature["properties"]["density"],
                "crime_label": feature["properties"]["crime_label"],
                "period": feature["properties"]["period"],
            }
            for feature in geojson["features"]
        ]
    )
    density_order = [label for label in ["Low", "Medium", "High"] if label in set(map_rows["density"])]
    density_counts = (
        map_rows.groupby("density", as_index=False)
        .size()
        .rename(columns={"size": "polygons"})
        .sort_values("polygons", ascending=False)
    )

    m1, m2, m3 = st.columns(3)
    m1.metric("Layer", selected_layer_label, selected_layer["period"])
    m2.metric("Hotspot Polygons", format_number(len(map_rows)))
    m3.metric("Published Layers", format_number(len(hotspot_layers)))

    map_fig = px.choropleth_mapbox(
        map_rows,
        geojson=geojson,
        locations="id",
        featureidkey="id",
        color="density",
        category_orders={"density": density_order},
        color_discrete_map=MAP_DENSITY_COLORS,
        hover_data={"id": False, "crime_label": True, "period": True, "density": True},
        mapbox_style="carto-darkmatter",
        center=center,
        zoom=5.3,
        opacity=0.62,
        title=f"{selected_layer_label} Hotspots",
    )
    map_fig.update_layout(height=650, margin=dict(l=0, r=0, t=48, b=0), legend_title_text="Density")
    st.plotly_chart(map_fig, use_container_width=True)

    c1, c2 = st.columns([1, 1])
    with c1:
        density_bar = density_counts.sort_values("polygons")
        density_fig = compact_bar(density_bar, "polygons", "density", "Hotspot Density Mix", color="density")
        st.plotly_chart(density_fig, use_container_width=True)
    with c2:
        layer_summary = hotspot_layers.sort_values("features", ascending=False).copy()
        layer_fig = px.bar(
            layer_summary,
            x="features",
            y="crime_label",
            orientation="h",
            color="features",
            color_continuous_scale=HEAT_SCALE,
            title="Available Hotspot Layer Sizes",
        )
        layer_fig.update_layout(height=430, margin=dict(l=8, r=8, t=48, b=8), yaxis_title="", xaxis_title="Polygons")
        st.plotly_chart(layer_fig, use_container_width=True)

with catalogue_tab:
    manifest = load_manifest()
    processed_files = sorted(DATA_DIR.glob("*.parquet"))
    f1, f2, f3 = st.columns(3)
    f1.metric("Raw Files", format_number(len(manifest) if not manifest.empty else 0))
    f2.metric("Processed Tables", format_number(len(processed_files)))
    f3.metric("Built At", metadata["built_at"].split("T")[0])

    st.subheader("Processed Tables")
    processed_summary = []
    for path in processed_files:
        df = load_parquet(path.name)
        processed_summary.append(
            {
                "table": path.name,
                "rows": len(df),
                "columns": len(df.columns),
                "size_mb": round(path.stat().st_size / 1024 / 1024, 2),
            }
        )
    st.dataframe(pd.DataFrame(processed_summary), use_container_width=True, hide_index=True)

    st.subheader("Raw Data Manifest")
    if manifest.empty:
        st.info("No raw data manifest found.")
    else:
        view = manifest.copy()
        view["size_mb"] = (view["bytes"] / 1024 / 1024).round(2)
        manifest_cols = [col for col in ["category", "file", "size_mb", "downloaded_at", "url"] if col in view.columns]
        st.dataframe(
            view[manifest_cols],
            use_container_width=True,
            hide_index=True,
        )
