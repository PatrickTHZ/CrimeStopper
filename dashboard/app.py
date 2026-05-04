from __future__ import annotations

from html import escape
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
    "#32D74B",
    "#FFD60A",
    "#FF453A",
    "#BF5AF2",
    "#64D2FF",
    "#FF9F0A",
    "#5E5CE6",
    "#FF375F",
    "#66D9E8",
]
HEAT_SCALE = ["#050505", "#172033", "#0A84FF", "#32D74B", "#FFD60A", "#FF453A"]
MAP_DENSITY_COLORS = {"Low": "#FFD60A", "Medium": "#FF9F0A", "High": "#FF453A"}
SYSTEM_FONT = "-apple-system, BlinkMacSystemFont, 'SF Pro Display', 'SF Pro Text', Inter, sans-serif"
CHART_BG = "rgba(12,14,20,0.46)"
pio.templates["crimestoppers_premium_dark"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor=CHART_BG,
        font={"family": SYSTEM_FONT, "color": "#F5F5F7"},
        title={"font": {"size": 19, "color": "#F5F5F7"}, "x": 0.02},
        colorway=PALETTE,
        xaxis={
            "gridcolor": "rgba(255,255,255,0.06)",
            "zerolinecolor": "rgba(255,255,255,0.14)",
            "linecolor": "rgba(255,255,255,0.1)",
            "tickfont": {"color": "#B8B8C2"},
        },
        yaxis={
            "gridcolor": "rgba(255,255,255,0.06)",
            "zerolinecolor": "rgba(255,255,255,0.14)",
            "linecolor": "rgba(255,255,255,0.1)",
            "tickfont": {"color": "#B8B8C2"},
        },
        legend={"font": {"color": "#D1D1D6"}, "orientation": "h", "y": -0.2},
        hoverlabel={
            "bgcolor": "rgba(18,20,28,0.96)",
            "bordercolor": "rgba(255,255,255,0.18)",
            "font": {"family": SYSTEM_FONT, "color": "#F5F5F7"},
        },
    )
)
pio.templates.default = "crimestoppers_premium_dark"


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
        --panel: rgba(19, 21, 30, 0.82);
        --panel-2: rgba(31, 34, 46, 0.88);
        --line: rgba(255, 255, 255, 0.14);
        --ink: #f5f5f7;
        --muted: #aeb0bb;
        --soft: #747987;
        --blue: #0A84FF;
        --green: #32D74B;
        --orange: #FF9F0A;
        --purple: #BF5AF2;
        --rose: #FF375F;
        --gold: #FFD60A;
        --uts-blue: #0f4beb;
        --uts-red: #ff2305;
        --uts-dark-grey: #323232;
        --uts-light-grey: #ebebeb;
        --uts-mid-grey: #b2b2b2;
    }
    .stApp {
        background:
            linear-gradient(135deg, rgba(10,132,255,0.16) 0%, transparent 22%),
            linear-gradient(225deg, rgba(255,159,10,0.11) 0%, transparent 26%),
            linear-gradient(180deg, #030305 0%, #07080d 42%, #000000 100%);
        color: var(--ink);
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", Inter, sans-serif;
    }
    [data-testid="stHeader"],
    [data-testid="stToolbar"],
    [data-testid="stDecoration"],
    [data-testid="stStatusWidget"],
    #MainMenu,
    footer {
        display: none !important;
        visibility: hidden !important;
        height: 0 !important;
    }
    [data-testid="stSidebar"] {
        background:
            linear-gradient(180deg, rgba(18, 20, 28, 0.94), rgba(4, 5, 8, 0.96));
        backdrop-filter: blur(24px);
        border-right: 1px solid var(--line);
    }
    [data-testid="stSidebar"] * { color: var(--ink); }
    [data-testid="stSidebar"] [data-baseweb="radio"] label,
    [data-testid="stSidebar"] [data-baseweb="slider"] { color: var(--ink); }
    .block-container { padding-top: 0.9rem; max-width: 1500px; }
    h1, h2, h3 {
        letter-spacing: 0;
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", Inter, sans-serif;
    }
    h1 { font-size: 2rem; font-weight: 720; margin-bottom: .1rem; }
    h2 { font-size: 1.25rem; margin-top: .4rem; }
    h3 { font-size: 1rem; }
    .premium-hero {
        position: relative;
        padding: 24px 28px 28px;
        border: 1px solid transparent;
        border-radius: 8px;
        background:
            radial-gradient(circle at 10% 0%, rgba(15, 75, 235, 0.42), transparent 36%) padding-box,
            radial-gradient(circle at 98% 18%, rgba(255, 35, 5, 0.18), transparent 28%) padding-box,
            linear-gradient(135deg, rgba(50, 50, 50, 0.97), rgba(9, 11, 18, 0.99) 48%, rgba(0, 0, 0, 0.98)) padding-box,
            linear-gradient(135deg, var(--uts-blue), rgba(235,235,235,0.5) 56%, var(--uts-red)) border-box;
        box-shadow:
            inset 0 1px 0 rgba(255,255,255,0.18),
            0 34px 90px rgba(0, 0, 0, 0.5),
            0 0 0 1px rgba(15, 75, 235, 0.12);
        margin-bottom: 22px;
        overflow: hidden;
    }
    .premium-hero:before {
        content: "";
        position: absolute;
        inset: 0;
        background:
            linear-gradient(110deg, rgba(15,75,235,0.18) 0%, transparent 36%, rgba(255,35,5,0.09) 100%),
            repeating-linear-gradient(90deg, rgba(235,235,235,0.035) 0 1px, transparent 1px 82px);
        opacity: 0.72;
        pointer-events: none;
    }
    .premium-hero > * { position: relative; z-index: 1; }
    .hero-kicker {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-bottom: 18px;
    }
    .hero-chip {
        display: inline-flex;
        align-items: center;
        min-height: 28px;
        padding: 5px 10px;
        border: 1px solid rgba(235,235,235,0.16);
        border-radius: 999px;
        background: rgba(0,0,0,0.34);
        color: var(--uts-light-grey);
        font-size: 0.78rem;
        font-weight: 650;
        letter-spacing: 0;
    }
    .premium-hero h1 {
        font-size: clamp(2.45rem, 5.8vw, 5.6rem);
        line-height: 1.02;
        font-weight: 780;
        margin: 0;
        color: #f5f5f7;
        text-wrap: balance;
    }
    .premium-hero h2 {
        font-size: clamp(1.12rem, 2vw, 1.75rem);
        line-height: 1.25;
        font-weight: 620;
        color: #d8d9df;
        margin: 10px 0 0;
    }
    .premium-hero p {
        max-width: 860px;
        color: #c7cad4;
        margin: 16px 0 0;
        font-size: 1.05rem;
        line-height: 1.5;
    }
    .premium-metric {
        --tone: var(--blue);
        min-height: 138px;
        margin-bottom: 28px;
        padding: 18px 18px 16px;
        border: 1px solid transparent;
        border-radius: 8px;
        background:
            linear-gradient(150deg, rgba(255,255,255,0.12), rgba(255,255,255,0.035)) padding-box,
            linear-gradient(135deg, color-mix(in srgb, var(--tone) 84%, white 8%), rgba(255,255,255,0.08) 46%, rgba(255,255,255,0.02)) border-box;
        box-shadow:
            inset 0 1px 0 rgba(255,255,255,0.16),
            0 24px 54px rgba(0, 0, 0, 0.4);
        backdrop-filter: blur(22px);
        overflow: hidden;
        position: relative;
    }
    .premium-metric:before {
        content: "";
        position: absolute;
        left: 0;
        right: 0;
        top: 0;
        height: 2px;
        background: linear-gradient(90deg, transparent, var(--tone), transparent);
        opacity: 0.85;
    }
    .premium-metric.tone-green { --tone: var(--green); }
    .premium-metric.tone-gold { --tone: var(--gold); }
    .premium-metric.tone-rose { --tone: var(--rose); }
    .premium-metric.tone-purple { --tone: var(--purple); }
    .metric-label {
        margin: 0 0 12px;
        color: var(--muted);
        font-size: 0.78rem;
        font-weight: 650;
        text-transform: uppercase;
        letter-spacing: .04em;
    }
    .metric-value {
        color: var(--ink);
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", Inter, sans-serif;
        font-size: clamp(1.65rem, 3.3vw, 2.65rem);
        font-weight: 760;
        line-height: 1.02;
        overflow-wrap: anywhere;
    }
    .metric-delta {
        margin: 12px 0 0;
        color: var(--muted);
        font-size: 0.92rem;
        font-weight: 600;
    }
    .metric-delta.positive { color: #32D74B; }
    .metric-delta.negative { color: #FF453A; }
    .metric-delta.neutral { color: #D1D1D6; }
    .section-title {
        margin: 18px 0 12px;
        color: #f5f5f7;
        font-size: 1.05rem;
        font-weight: 720;
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
        background: rgba(10, 12, 18, 0.72);
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 8px 10px 4px;
        backdrop-filter: blur(22px);
        box-shadow: 0 18px 46px rgba(0,0,0,0.32);
    }
    [data-testid="stTabs"] [role="tablist"] {
        gap: 12px;
        align-items: center;
        overflow-x: auto;
        padding-bottom: 6px;
    }
    [data-testid="stTabs"] button {
        color: var(--muted);
        border-radius: 7px;
        min-height: 52px;
        min-width: max-content;
        padding: 0 24px !important;
        margin: 0 0 6px !important;
        background: rgba(255, 255, 255, 0.035);
        border: 1px solid rgba(255, 255, 255, 0.08);
        box-sizing: border-box;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        transition: background 140ms ease, border-color 140ms ease, color 140ms ease;
    }
    [data-testid="stTabs"] button [data-testid="stMarkdownContainer"] {
        display: flex;
        align-items: center;
        padding: 0 !important;
    }
    [data-testid="stTabs"] button p {
        margin: 0;
        padding: 0;
        line-height: 1.15;
        white-space: nowrap;
        font-weight: 650;
    }
    [data-testid="stTabs"] button:hover {
        color: var(--ink);
        background: rgba(255, 255, 255, 0.08);
        border-color: rgba(255, 255, 255, 0.12);
    }
    [data-testid="stTabs"] button[aria-selected="true"] {
        color: var(--ink);
        background:
            linear-gradient(180deg, rgba(255,255,255,0.18), rgba(255,255,255,0.08));
        border-color: rgba(255, 255, 255, 0.22);
        box-shadow:
            inset 0 1px 0 rgba(255,255,255,0.18),
            0 8px 22px rgba(0, 0, 0, 0.22);
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
        background:
            linear-gradient(150deg, rgba(255,255,255,0.095), rgba(255,255,255,0.025));
        border: 1px solid rgba(255,255,255,0.13);
        border-radius: 8px;
        padding: 10px;
        backdrop-filter: blur(22px);
        box-shadow:
            inset 0 1px 0 rgba(255,255,255,0.12),
            0 28px 64px rgba(0, 0, 0, 0.38);
        overflow: hidden;
    }
    [data-testid="stExpander"] {
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 8px;
        background: rgba(12,14,20,0.66);
    }
    @media (max-width: 720px) {
        .block-container { padding-left: 1rem; padding-right: 1rem; }
        .premium-hero { padding: 20px 18px 22px; }
        .premium-metric { min-height: 124px; }
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


def metric_state(delta: str | None) -> str:
    if not delta:
        return "neutral"
    text = str(delta).strip()
    if text.startswith("+"):
        return "positive"
    if text.startswith("-"):
        return "negative"
    return "neutral"


def render_metric_card(
    label: str,
    value: str | int | float,
    delta: str | int | float | None = None,
    tone: str = "blue",
    state: str | None = None,
) -> None:
    safe_label = escape(str(label))
    safe_value = escape(str(value))
    safe_delta = "" if delta is None else escape(str(delta))
    delta_markup = ""
    if safe_delta:
        delta_class = state or metric_state(safe_delta)
        delta_markup = f'<p class="metric-delta {delta_class}">{safe_delta}</p>'
    st.markdown(
        f"""
        <div class="premium-metric tone-{tone}">
            <p class="metric-label">{safe_label}</p>
            <div class="metric-value">{safe_value}</div>
            {delta_markup}
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_title(title: str) -> None:
    st.markdown(f'<div class="section-title">{escape(title)}</div>', unsafe_allow_html=True)


def month_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if MONTH_COL.match(str(col))]


def polish_figure(fig: go.Figure, height: int | None = None, showlegend: bool | None = None) -> go.Figure:
    layout: dict[str, object] = {
        "margin": dict(l=12, r=12, t=58, b=16),
        "font": {"family": SYSTEM_FONT, "color": "#F5F5F7"},
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": CHART_BG,
        "hoverlabel": {
            "bgcolor": "rgba(16,18,26,0.97)",
            "bordercolor": "rgba(255,255,255,0.18)",
            "font": {"family": SYSTEM_FONT, "color": "#F5F5F7"},
        },
    }
    if height is not None:
        layout["height"] = height
    if showlegend is not None:
        layout["showlegend"] = showlegend
    fig.update_layout(**layout)
    fig.update_xaxes(
        showgrid=True,
        gridcolor="rgba(255,255,255,0.055)",
        zerolinecolor="rgba(255,255,255,0.12)",
        linecolor="rgba(255,255,255,0.1)",
        tickfont={"color": "#B8B8C2"},
        title_font={"color": "#D1D1D6"},
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(255,255,255,0.055)",
        zerolinecolor="rgba(255,255,255,0.12)",
        linecolor="rgba(255,255,255,0.1)",
        tickfont={"color": "#B8B8C2"},
        title_font={"color": "#D1D1D6"},
    )
    fig.update_traces(
        marker={"line": {"width": 0}, "opacity": 0.94},
        selector={"type": "bar"},
    )
    fig.update_traces(
        line={"width": 3},
        marker={"size": 7, "line": {"width": 1.4, "color": "rgba(255,255,255,0.78)"}},
        selector={"type": "scatter"},
    )
    return fig


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
    fig.update_layout(legend_title_text="", hovermode="x unified")
    polish_figure(fig, height=400)
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
    polish_figure(fig, height=440, showlegend=False)
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
    f"""
    <section class="premium-hero">
        <div class="hero-kicker">
            <span class="hero-chip">NSW Open Data</span>
            <span class="hero-chip">BOCSAR {escape(month_min)} to {escape(month_max)}</span>
            <span class="hero-chip">{latest_year} Intelligence View</span>
        </div>
        <h1>Crime Analysis Dashboard</h1>
        <h2>CrimeStoppers</h2>
        <p>NSW crime intelligence shaped for fast scanning, suburb drill-downs, LGA comparisons, seasonality patterns, and hotspot mapping.</p>
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
    with k1:
        render_metric_card(
            f"NSW Incidents {latest_year}",
            format_number(total_latest),
            format_pct(change_pct),
            tone="blue",
        )
    with k2:
        render_metric_card(
            "Top Category",
            top_category["Offence category"],
            f"{format_number(top_category['incidents'])} incidents",
            tone="green",
            state="neutral",
        )
    with k3:
        render_metric_card(
            "Top Suburb",
            top_suburb["Suburb"],
            f"{format_number(top_suburb[f'incidents_{latest_year}'])} incidents",
            tone="gold",
            state="neutral",
        )
    with k4:
        render_metric_card(
            "Tracked Suburbs",
            format_number(metadata["suburb"]["area_count"]),
            f"{metadata['suburb']['category_count']} offence categories",
            tone="purple",
            state="neutral",
        )

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
    fig.update_layout(hovermode="x unified", legend_title_text="")
    polish_figure(fig, height=430)
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
        polish_figure(heat_fig, height=420)
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
        polish_figure(tree_fig, height=420)
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
    with m1:
        render_metric_card(
            f"{selected_suburb} Incidents {latest_year}",
            format_number(suburb_latest[f"incidents_{latest_year}"]),
            format_pct(suburb_latest["change_pct_vs_prior"]),
            tone="blue",
        )
    with m2:
        render_metric_card(
            "Largest Category",
            suburb_latest[f"top_category_{latest_year}"],
            f"{format_number(suburb_latest[f'top_category_incidents_{latest_year}'])} incidents",
            tone="rose",
            state="neutral",
        )
    rank = int(suburb_index.index[suburb_index["Suburb"] == selected_suburb][0]) + 1
    with m3:
        render_metric_card("NSW Suburb Rank", f"#{rank}", "By total incidents", tone="purple", state="neutral")

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
    polish_figure(suburb_heat, height=470)
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
        detail_fig.update_layout(hovermode="x unified", legend_title_text="")
        polish_figure(detail_fig, height=390)
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
            render_metric_card(
                "Highest LGA",
                top_lga["lga"],
                format_number(top_lga[metric]),
                tone="gold",
                state="neutral",
            )
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
    scatter_fig.update_layout(legend_title_text="2-year trend")
    polish_figure(scatter_fig, height=440)
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
    with m1:
        render_metric_card("Layer", selected_layer_label, selected_layer["period"], tone="blue", state="neutral")
    with m2:
        render_metric_card("Hotspot Polygons", format_number(len(map_rows)), tone="rose")
    with m3:
        render_metric_card("Published Layers", format_number(len(hotspot_layers)), tone="purple")

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
    map_fig.update_traces(marker_line_width=0.8, marker_line_color="rgba(7, 8, 12, 0.54)")
    map_fig.update_layout(
        height=780,
        margin=dict(l=0, r=0, t=58, b=0),
        legend=dict(
            title=dict(text="Density", font=dict(family=SYSTEM_FONT, color="#D1D1D6", size=13)),
            orientation="h",
            x=0.018,
            y=0.026,
            xanchor="left",
            yanchor="bottom",
            bgcolor="rgba(8, 10, 16, 0.78)",
            bordercolor="rgba(255, 255, 255, 0.16)",
            borderwidth=1,
            font=dict(family=SYSTEM_FONT, color="#F5F5F7", size=13),
            itemsizing="constant",
        ),
        mapbox=dict(domain=dict(x=[0, 1], y=[0, 1]), center=center, zoom=5.3),
        paper_bgcolor="rgba(0,0,0,0)",
    )
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
        polish_figure(layer_fig, height=440)
        layer_fig.update_layout(yaxis_title="", xaxis_title="Polygons")
        st.plotly_chart(layer_fig, use_container_width=True)

with catalogue_tab:
    manifest = load_manifest()
    processed_files = sorted(DATA_DIR.glob("*.parquet"))
    f1, f2, f3 = st.columns(3)
    with f1:
        render_metric_card("Raw Files", format_number(len(manifest) if not manifest.empty else 0), tone="blue")
    with f2:
        render_metric_card("Processed Tables", format_number(len(processed_files)), tone="green")
    with f3:
        render_metric_card("Built At", metadata["built_at"].split("T")[0], tone="gold")

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
