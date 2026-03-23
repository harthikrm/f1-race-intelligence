"""
F1 Race Intelligence — Interactive Dashboard
Streamlit app with 6 analytical tabs.
Glassmorphism UI inspired by Apple Design Language.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import sys
import base64

# Add src to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.viz import (
    f1_layout, speed_trace_chart, lap_delta_chart, sector_heatmap,
    stint_timeline_chart, degradation_chart, pit_duration_ranking,
    strategy_iq_radar, affinity_heatmap, top_circuits_bar,
    efficiency_bar_chart, efficiency_rating_chart, circuit_speed_map,
    territorial_speed_map,
    TEAM_COLORS, F1_BG, F1_RED, F1_WHITE, F1_PAPER,
)

# ──────────────────────────────────────────────
# Page Config & Theme
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="F1 Race Intelligence",
    layout="wide",
    page_icon="🏎",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────
# Glassmorphism + Apple-inspired CSS
# ──────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* ─── Base ─── */
    .stApp {
        background: linear-gradient(135deg, #0a0a0a 0%, #111111 40%, #0d0d0d 60%, #0a0a0a 100%);
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* Subtle animated gradient overlay */
    .stApp::before {
        content: '';
        position: fixed;
        top: 0; left: 0; right: 0; bottom: 0;
        background:
            radial-gradient(ellipse 80% 60% at 20% 10%, rgba(232,0,45,0.06) 0%, transparent 60%),
            radial-gradient(ellipse 60% 50% at 80% 90%, rgba(54,113,198,0.04) 0%, transparent 50%);
        pointer-events: none;
        z-index: 0;
    }

    /* ─── Sidebar ─── */
    section[data-testid="stSidebar"] {
        background: rgba(18,18,18,0.85) !important;
        backdrop-filter: blur(24px) saturate(180%);
        -webkit-backdrop-filter: blur(24px) saturate(180%);
        border-right: 1px solid rgba(255,255,255,0.06);
    }

    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stMultiSelect label {
        color: rgba(255,255,255,0.6) !important;
        font-size: 11px !important;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        font-weight: 500;
    }

    /* ─── Typography ─── */
    h1 {
        color: #FFFFFF !important;
        font-weight: 700 !important;
        font-size: 32px !important;
        letter-spacing: -0.5px;
    }
    h2 {
        color: #FFFFFF !important;
        font-weight: 600 !important;
        font-size: 22px !important;
        letter-spacing: -0.3px;
    }
    h3 {
        color: rgba(255,255,255,0.9) !important;
        font-weight: 500 !important;
        font-size: 17px !important;
    }
    div[data-testid="stMarkdownContainer"] p {
        color: rgba(255,255,255,0.7);
        line-height: 1.6;
    }

    /* ─── Glass Card (metric containers) ─── */
    div[data-testid="stMetric"] {
        background: rgba(255,255,255,0.04);
        backdrop-filter: blur(20px) saturate(150%);
        -webkit-backdrop-filter: blur(20px) saturate(150%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 20px 24px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    div[data-testid="stMetric"]:hover {
        background: rgba(255,255,255,0.07);
        border-color: rgba(232,0,45,0.25);
        transform: translateY(-2px);
        box-shadow: 0 8px 32px rgba(232,0,45,0.1);
    }

    div[data-testid="stMetric"] label {
        color: rgba(255,255,255,0.45) !important;
        font-size: 11px !important;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        font-weight: 600;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #FFFFFF !important;
        font-size: 34px !important;
        font-weight: 700;
        letter-spacing: -1px;
    }
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        color: #E8002D !important;
    }

    /* ─── Tabs ─── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: rgba(255,255,255,0.03);
        border-radius: 14px;
        padding: 4px;
        border: 1px solid rgba(255,255,255,0.06);
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        color: rgba(255,255,255,0.5) !important;
        border-radius: 10px !important;
        padding: 10px 20px !important;
        border: none !important;
        font-weight: 500;
        font-size: 13px;
        transition: all 0.2s ease;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: rgba(255,255,255,0.8) !important;
        background: rgba(255,255,255,0.05) !important;
    }
    .stTabs [aria-selected="true"] {
        background: rgba(232,0,45,0.9) !important;
        color: #FFFFFF !important;
        box-shadow: 0 4px 16px rgba(232,0,45,0.3);
    }

    /* Tab highlight bar hidden */
    .stTabs [data-baseweb="tab-highlight"] {
        display: none;
    }

    /* ─── DataFrames ─── */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid rgba(255,255,255,0.06);
    }
    .stDataFrame [data-testid="stDataFrameResizable"] {
        background: rgba(255,255,255,0.02);
    }

    /* ─── Buttons ─── */
    .stButton > button {
        background: linear-gradient(135deg, #E8002D 0%, #c50025 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 12px 32px !important;
        font-weight: 600 !important;
        font-size: 14px !important;
        letter-spacing: 0.3px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        box-shadow: 0 4px 16px rgba(232,0,45,0.25) !important;
    }
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 28px rgba(232,0,45,0.4) !important;
    }

    /* ─── Selectbox / inputs ─── */
    .stSelectbox > div > div,
    .stMultiSelect > div > div {
        background: rgba(255,255,255,0.04) !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
        border-radius: 10px !important;
        color: white !important;
    }

    /* ─── Plotly chart containers ─── */
    .stPlotlyChart {
        background: rgba(255,255,255,0.02);
        border: 1px solid rgba(255,255,255,0.05);
        border-radius: 16px;
        padding: 8px;
        transition: all 0.3s ease;
    }
    .stPlotlyChart:hover {
        border-color: rgba(255,255,255,0.1);
        box-shadow: 0 4px 24px rgba(0,0,0,0.3);
    }

    /* ─── Dividers ─── */
    hr {
        border-color: rgba(255,255,255,0.06) !important;
        margin: 24px 0 !important;
    }

    /* ─── Glass panel utility class ─── */
    .glass-panel {
        background: rgba(255,255,255,0.04);
        backdrop-filter: blur(20px) saturate(150%);
        -webkit-backdrop-filter: blur(20px) saturate(150%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 28px;
        margin: 12px 0;
    }

    .glass-panel-accent {
        background: linear-gradient(135deg, rgba(232,0,45,0.08) 0%, rgba(255,255,255,0.03) 100%);
        backdrop-filter: blur(20px) saturate(150%);
        -webkit-backdrop-filter: blur(20px) saturate(150%);
        border: 1px solid rgba(232,0,45,0.15);
        border-radius: 20px;
        padding: 28px;
        margin: 12px 0;
    }

    /* ─── Hero section ─── */
    .hero-container {
        position: relative;
        border-radius: 24px;
        overflow: hidden;
        margin-bottom: 28px;
        border: 1px solid rgba(255,255,255,0.06);
    }
    .hero-container img {
        width: 100%;
        height: 260px;
        object-fit: cover;
        filter: brightness(0.4) contrast(1.1);
    }
    .hero-overlay {
        position: absolute;
        bottom: 0; left: 0; right: 0;
        padding: 32px;
        background: linear-gradient(transparent, rgba(0,0,0,0.85));
    }
    .hero-overlay h1 {
        margin: 0 !important;
        font-size: 36px !important;
        font-weight: 700 !important;
        letter-spacing: -1px;
    }
    .hero-overlay p {
        color: rgba(255,255,255,0.6) !important;
        margin: 8px 0 0 0;
        font-size: 15px;
    }

    /* ─── Section image cards ─── */
    .section-img {
        border-radius: 16px;
        overflow: hidden;
        border: 1px solid rgba(255,255,255,0.06);
        margin: 8px 0 20px 0;
    }
    .section-img img {
        width: 100%;
        height: 180px;
        object-fit: cover;
        filter: brightness(0.55) contrast(1.05) saturate(1.1);
        transition: all 0.4s ease;
    }
    .section-img:hover img {
        filter: brightness(0.7) contrast(1.1) saturate(1.2);
        transform: scale(1.03);
    }

    /* ─── Stat badge ─── */
    .stat-badge {
        display: inline-block;
        background: rgba(232,0,45,0.12);
        border: 1px solid rgba(232,0,45,0.2);
        color: #E8002D;
        padding: 4px 14px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.5px;
        margin-right: 8px;
    }

    /* ─── Scrollbar ─── */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb {
        background: rgba(255,255,255,0.1);
        border-radius: 3px;
    }
    ::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.2); }

    /* ─── Spinner ─── */
    .stSpinner > div { border-top-color: #E8002D !important; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# F1 Image URLs (Unsplash — free, no API key)
# ──────────────────────────────────────────────
# Using Unsplash source for reliable F1/racing images
IMG_HERO = "https://images.unsplash.com/photo-1724461186447-4b524fd4557e?q=80&w=3132&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D"
IMG_F1_CAR = "https://images.unsplash.com/photo-1773911634357-b88b7b06487a?q=80&w=2073&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D"
IMG_TYRES = "https://images.unsplash.com/photo-1730743676644-df679faa46f6?q=80&w=2070&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D"
IMG_PIT = "https://images.unsplash.com/photo-1681674900318-8ae9d53c5898?q=80&w=3134&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D"
IMG_CIRCUIT = "https://images.unsplash.com/photo-1727854658829-6fbcc176c8b9?q=80&w=987&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D"
IMG_TRACK = "https://images.unsplash.com/photo-1773142181818-5842662aea51?q=80&w=987&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D"
IMG_HELMET = "https://i.pinimg.com/736x/54/f0/14/54f0147d87a5eaf3b26733b0cbe60754.jpg"


def hero_banner(title: str, subtitle: str, img_url: str):
    """Render a hero banner with image background and overlay text."""
    st.markdown(f"""
    <div class="hero-container">
        <img src="{img_url}" alt="F1" onerror="this.style.display='none'">
        <div class="hero-overlay">
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
    </div>
    """, unsafe_allow_html=True)


def section_image(img_url: str, alt: str = ""):
    """Render a section banner image."""
    st.markdown(f"""
    <div class="section-img">
        <img src="{img_url}" alt="{alt}" onerror="this.parentElement.style.display='none'">
    </div>
    """, unsafe_allow_html=True)


def glass_panel(content: str, accent: bool = False):
    """Wrap content in a glass panel."""
    cls = "glass-panel-accent" if accent else "glass-panel"
    st.markdown(f'<div class="{cls}">{content}</div>', unsafe_allow_html=True)


def stat_badges(badges: list):
    """Render a row of stat badges. Each badge is (label, value)."""
    html = " ".join(f'<span class="stat-badge">{label}: {value}</span>' for label, value in badges)
    st.markdown(html, unsafe_allow_html=True)


# ──────────────────────────────────────────────
# Team Identity System
# ──────────────────────────────────────────────
TEAM_META = {
    "red_bull":     {"name": "Red Bull Racing",    "short": "RBR", "color": "#3671C6"},
    "ferrari":      {"name": "Scuderia Ferrari",   "short": "FER", "color": "#E8002D"},
    "mercedes":     {"name": "Mercedes-AMG",       "short": "MER", "color": "#27F4D2"},
    "mclaren":      {"name": "McLaren F1",         "short": "MCL", "color": "#FF8000"},
    "aston_martin": {"name": "Aston Martin",       "short": "AMR", "color": "#229971"},
    "alpine":       {"name": "Alpine F1",          "short": "ALP", "color": "#FF87BC"},
    "williams":     {"name": "Williams Racing",    "short": "WIL", "color": "#64C4FF"},
    "rb":           {"name": "RB F1 Team",         "short": "RB",  "color": "#6692FF"},
    "kick_sauber":  {"name": "Kick Sauber",        "short": "SAU", "color": "#52E252"},
    "sauber":       {"name": "Kick Sauber",        "short": "SAU", "color": "#52E252"},
    "haas":         {"name": "Haas F1",            "short": "HAA", "color": "#B6BABD"},
}


def team_name(cid: str) -> str:
    """Convert constructor_id to display name."""
    return TEAM_META.get(cid, {}).get("name", cid.replace("_", " ").title())


def team_badge_html(cid: str, size: str = "md") -> str:
    """Generate an inline team badge: colored shield + abbreviation."""
    meta = TEAM_META.get(cid, {"name": cid, "short": cid[:3].upper(), "color": "#666"})
    sizes = {
        "sm": ("20px", "20px", "8px", "3px"),
        "md": ("28px", "28px", "10px", "4px"),
        "lg": ("36px", "36px", "13px", "5px"),
    }
    w, h, fs, br = sizes.get(size, sizes["md"])
    return (
        f'<span style="display:inline-flex; align-items:center; gap:8px;">'
        f'<span style="display:inline-flex; align-items:center; justify-content:center; '
        f'width:{w}; height:{h}; background:{meta["color"]}; border-radius:{br}; '
        f'font-size:{fs}; font-weight:700; color:{"#000" if meta["color"] in ("#27F4D2","#FFD700","#52E252","#B6BABD","#64C4FF") else "#FFF"}; '
        f'letter-spacing:0.5px; font-family:Inter,sans-serif;">'
        f'{meta["short"]}</span>'
        f'<span style="color:rgba(255,255,255,0.85); font-size:13px; font-weight:500;">{meta["name"]}</span>'
        f'</span>'
    )


def team_badge_label(cid: str) -> str:
    """For Plotly axis labels: return clean display name with unicode color dot."""
    meta = TEAM_META.get(cid, {"name": cid.replace("_", " ").title(), "color": "#666"})
    return meta["name"]


def render_team_legend():
    """Render a horizontal legend of all teams with color badges."""
    badges = []
    for cid, meta in TEAM_META.items():
        if cid == "sauber":
            continue  # skip duplicate
        badges.append(
            f'<span style="display:inline-flex; align-items:center; gap:6px; margin-right:16px; margin-bottom:8px;">'
            f'<span style="width:12px; height:12px; border-radius:3px; background:{meta["color"]}; display:inline-block;"></span>'
            f'<span style="color:rgba(255,255,255,0.7); font-size:12px; font-weight:500;">{meta["name"]}</span>'
            f'</span>'
        )
    st.markdown(
        f'<div style="display:flex; flex-wrap:wrap; padding:12px 0;">{"".join(badges)}</div>',
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────
# Database Connection
# ──────────────────────────────────────────────
DB_PATH = PROJECT_ROOT / "data" / "processed" / "f1.db"


@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    con = get_connection()
    try:
        return con.execute(query).fetchdf()
    except Exception:
        # Connection may be stale — reconnect once
        st.cache_resource.clear()
        con = get_connection()
        return con.execute(query).fetchdf()


def db_available():
    return DB_PATH.exists()


# ──────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────
st.sidebar.markdown("""
<div style="text-align: center; padding: 16px 0 8px 0;">
    <div style="font-size: 40px; margin-bottom: 4px;">🏎️</div>
    <div style="font-size: 18px; font-weight: 700; color: white; letter-spacing: -0.5px;">F1 Race Intelligence</div>
    <div style="font-size: 11px; color: rgba(255,255,255,0.4); text-transform: uppercase; letter-spacing: 2px; margin-top: 4px;">Analytics Platform</div>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")

if db_available():
    years = run_query("SELECT DISTINCT year FROM races ORDER BY year DESC")["year"].tolist()
    selected_year = st.sidebar.selectbox("Season", years, index=0)

    circuits = run_query(
        f"SELECT DISTINCT ra.circuit_id, c.circuit_name FROM races ra "
        f"JOIN circuits c ON ra.circuit_id = c.circuit_id "
        f"WHERE ra.year = {selected_year} ORDER BY c.circuit_name"
    )
    selected_circuit = st.sidebar.selectbox(
        "Circuit", circuits["circuit_id"].tolist(),
        format_func=lambda x: circuits[circuits["circuit_id"] == x]["circuit_name"].values[0]
    )

    drivers = run_query(
        f"SELECT DISTINCT r.driver_id, d.full_name FROM results r "
        f"JOIN drivers d ON r.driver_id = d.driver_id "
        f"WHERE r.year = {selected_year} ORDER BY d.full_name"
    )
    selected_drivers = st.sidebar.multiselect(
        "Drivers", drivers["driver_id"].tolist(),
        default=drivers["driver_id"].tolist()[:2],
        format_func=lambda x: drivers[drivers["driver_id"] == x]["full_name"].values[0]
    )
else:
    selected_year = 2024
    selected_circuit = None
    selected_drivers = []
    st.sidebar.warning("Database not found. Run the data pipeline first.")

st.sidebar.markdown("---")
st.sidebar.markdown("""
<div style="text-align: center; padding: 8px 0;">
    <div style="font-size: 12px; color: rgba(255,255,255,0.35);">Built by</div>
    <div style="font-size: 13px; color: rgba(255,255,255,0.7); font-weight: 600; margin-top: 2px;">Harthik Royal Mallichetty</div>
    <a href="https://github.com/harthikrm/f1-race-intelligence" target="_blank" style="font-size: 11px; color: rgba(232,0,45,0.8); text-decoration: none;">github.com/harthikrm</a>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# Main Content — 6 Tabs
# ──────────────────────────────────────────────
tab_overview, tab_battle, tab_pit, tab_circuit, tab_constructor, tab_predict, tab_speedmap = st.tabs([
    "Overview", "Driver Battle", "Pit Strategy",
    "Circuit DNA", "Constructor IQ", "Prediction", "Speed Map"
])


# ──────────────────────────────────────────────
# TAB 1: Overview
# ──────────────────────────────────────────────
with tab_overview:
    hero_banner(
        f"Season {selected_year}",
        "15 seasons  ·  305 races  ·  879 drivers  ·  Real-time telemetry at 240 Hz",
        IMG_HERO,
    )

    if not db_available():
        st.info("Run `python src/ingest.py --all` then load into DuckDB to populate this dashboard.")
    else:
        # KPI Row
        col1, col2, col3, col4 = st.columns(4)

        total_races = run_query(f"SELECT COUNT(*) AS n FROM races WHERE year = {selected_year}")["n"][0]
        total_drivers = run_query(
            f"SELECT COUNT(DISTINCT driver_id) AS n FROM results WHERE year = {selected_year}"
        )["n"][0]
        total_constructors = run_query(
            f"SELECT COUNT(DISTINCT constructor_id) AS n FROM results WHERE year = {selected_year}"
        )["n"][0]
        total_pit_stops = run_query(
            f"SELECT COUNT(*) AS n FROM pit_stops WHERE year = {selected_year}"
        )["n"][0]

        col1.metric("Races", total_races)
        col2.metric("Drivers", total_drivers)
        col3.metric("Constructors", total_constructors)
        col4.metric("Pit Stops", total_pit_stops)

        st.markdown("")

        # Championship standings
        left_col, right_col = st.columns([3, 2])

        with left_col:
            st.subheader("Driver Championship")
            standings = run_query(f"""
                SELECT d.full_name, ds.points, ds.wins, ds.position
                FROM driver_standings ds
                JOIN drivers d ON ds.driver_id = d.driver_id
                WHERE ds.year = {selected_year}
                  AND ds.round = (SELECT MAX(round) FROM driver_standings WHERE year = {selected_year})
                ORDER BY ds.position
            """)
            st.dataframe(standings, use_container_width=True, hide_index=True)

        with right_col:
            section_image(IMG_F1_CAR, "F1 Car")
            glass_panel(f"""
                <h3 style="margin: 0 0 12px 0; color: white; font-size: 16px;">Season at a Glance</h3>
                <p style="color: rgba(255,255,255,0.6); font-size: 13px; line-height: 1.8; margin: 0;">
                    Analyzing every qualifying session, race result, pit stop, and
                    telemetry trace from the {selected_year} Formula 1 World Championship.
                    Data sourced from the Jolpica API and FastF1 official telemetry.
                </p>
            """)

        # Race results table
        st.subheader("Race Winners")
        winners = run_query(f"""
            SELECT ra.race_name AS "Race", d.full_name AS "Winner", r.constructor_id AS team_id,
                   r.grid AS "Grid", r.points AS "Points"
            FROM results r
            JOIN races ra ON r.race_id = ra.race_id
            JOIN drivers d ON r.driver_id = d.driver_id
            WHERE r.position = 1 AND r.year = {selected_year}
            ORDER BY ra.round
        """)
        winners["Team"] = winners["team_id"].map(team_name)
        st.dataframe(winners[["Race", "Winner", "Team", "Grid", "Points"]], use_container_width=True, hide_index=True)


# ──────────────────────────────────────────────
# TAB 2: Driver Battle
# ──────────────────────────────────────────────
with tab_battle:
    hero_banner(
        "Driver vs Driver",
        "Head-to-head telemetry analysis — speed traces, braking points, and lap deltas",
        IMG_HELMET,
    )

    if not db_available() or len(selected_drivers) < 2:
        st.info("Select at least 2 drivers in the sidebar to compare.")
    else:
        driver_a, driver_b = selected_drivers[0], selected_drivers[1]
        name_a = drivers[drivers["driver_id"] == driver_a]["full_name"].values[0]
        name_b = drivers[drivers["driver_id"] == driver_b]["full_name"].values[0]

        st.subheader(f"{name_a}  vs  {name_b}")

        # Head-to-head stats
        h2h = run_query(f"""
            SELECT
                d.full_name,
                COUNT(*) AS races,
                SUM(CASE WHEN r.position <= 3 THEN 1 ELSE 0 END) AS podiums,
                SUM(CASE WHEN r.position = 1 THEN 1 ELSE 0 END) AS wins,
                ROUND(AVG(r.position) FILTER (WHERE r.position IS NOT NULL), 1) AS avg_finish,
                ROUND(AVG(r.grid), 1) AS avg_grid,
                SUM(r.points) AS total_points
            FROM results r
            JOIN drivers d ON r.driver_id = d.driver_id
            WHERE r.driver_id IN ('{driver_a}', '{driver_b}')
              AND r.year = {selected_year}
            GROUP BY d.full_name
        """)

        col1, col2 = st.columns(2)
        for i, (_, row) in enumerate(h2h.iterrows()):
            col = col1 if i == 0 else col2
            with col:
                glass_panel(f"""
                    <h3 style="margin: 0; color: white; font-size: 18px;">{row['full_name']}</h3>
                    <div style="display: flex; gap: 24px; margin-top: 16px;">
                        <div>
                            <div style="font-size: 28px; font-weight: 700; color: white;">{int(row['total_points'])}</div>
                            <div style="font-size: 10px; color: rgba(255,255,255,0.4); text-transform: uppercase; letter-spacing: 1px;">Points</div>
                        </div>
                        <div>
                            <div style="font-size: 28px; font-weight: 700; color: #E8002D;">{int(row['wins'])}</div>
                            <div style="font-size: 10px; color: rgba(255,255,255,0.4); text-transform: uppercase; letter-spacing: 1px;">Wins</div>
                        </div>
                        <div>
                            <div style="font-size: 28px; font-weight: 700; color: white;">{int(row['podiums'])}</div>
                            <div style="font-size: 10px; color: rgba(255,255,255,0.4); text-transform: uppercase; letter-spacing: 1px;">Podiums</div>
                        </div>
                        <div>
                            <div style="font-size: 28px; font-weight: 700; color: rgba(255,255,255,0.7);">{row['avg_finish']}</div>
                            <div style="font-size: 10px; color: rgba(255,255,255,0.4); text-transform: uppercase; letter-spacing: 1px;">Avg Finish</div>
                        </div>
                    </div>
                """, accent=(i == 0))

        # Lap time comparison
        st.markdown("")
        st.subheader("Lap Time Comparison")
        lap_comparison = run_query(f"""
            SELECT
                lt.driver_id,
                d.full_name,
                lt.lap_time_ms / 1000.0 AS lap_time_sec,
                lt.lap
            FROM lap_times lt
            JOIN drivers d ON lt.driver_id = d.driver_id
            JOIN races ra ON lt.race_id = ra.race_id
            WHERE lt.driver_id IN ('{driver_a}', '{driver_b}')
              AND ra.circuit_id = '{selected_circuit}'
              AND lt.year = {selected_year}
            ORDER BY lt.lap
        """)

        if not lap_comparison.empty:
            fig = go.Figure()
            colors = [F1_RED, "#3671C6"]
            for idx, did in enumerate([driver_a, driver_b]):
                ddata = lap_comparison[lap_comparison["driver_id"] == did]
                if not ddata.empty:
                    fig.add_trace(go.Scatter(
                        x=ddata["lap"], y=ddata["lap_time_sec"],
                        name=ddata["full_name"].values[0],
                        mode="lines",
                        line=dict(width=2, color=colors[idx]),
                    ))
            fig = f1_layout(fig, "Lap Time Comparison")
            fig.update_xaxes(title_text="Lap")
            fig.update_yaxes(title_text="Lap Time (s)")
            st.plotly_chart(fig, use_container_width=True)


# ──────────────────────────────────────────────
# TAB 3: Pit Strategy
# ──────────────────────────────────────────────
with tab_pit:
    hero_banner(
        "Pit Stop Strategy",
        "Tyre management, pit windows, and crew performance analysis",
        IMG_TYRES,
    )

    if not db_available():
        st.info("Database not loaded.")
    else:
        # Pit stop ranking by team
        st.subheader(f"Pit Stop Duration by Team — {selected_year}")

        pit_data = run_query(f"""
            SELECT
                r.constructor_id,
                COUNT(*) AS total_stops,
                ROUND(AVG(ps.pit_duration_ms) / 1000, 2) AS avg_duration_sec,
                ROUND(MIN(ps.pit_duration_ms) / 1000, 2) AS fastest_stop_sec
            FROM pit_stops ps
            JOIN results r ON ps.race_id = r.race_id AND ps.driver_id = r.driver_id
            WHERE ps.year = {selected_year} AND ps.pit_duration_ms > 0
            GROUP BY r.constructor_id
            ORDER BY avg_duration_sec
        """)

        if not pit_data.empty:
            pit_data["team"] = pit_data["constructor_id"].map(team_name)

            stat_badges([
                ("Teams", len(pit_data)),
                ("Total Stops", int(pit_data["total_stops"].sum())),
                ("Fastest", f"{pit_data['fastest_stop_sec'].min():.2f}s"),
            ])
            st.markdown("")

            # Team badge legend
            render_team_legend()

            fig = go.Figure(go.Bar(
                x=pit_data["avg_duration_sec"],
                y=pit_data["team"],
                orientation="h",
                marker_color=[TEAM_COLORS.get(c, "#666") for c in pit_data["constructor_id"]],
            ))
            fig = f1_layout(fig, "Average Pit Stop Duration (seconds)")
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(26,26,26,0.5)",
            )
            st.plotly_chart(fig, use_container_width=True)

            # Display table with team badges
            pit_display = pit_data[["team", "total_stops", "avg_duration_sec", "fastest_stop_sec"]].rename(
                columns={"team": "Team", "total_stops": "Stops", "avg_duration_sec": "Avg (s)", "fastest_stop_sec": "Fastest (s)"}
            )
            st.dataframe(pit_display, use_container_width=True, hide_index=True)

        # Degradation rates
        st.markdown("---")
        st.subheader("Tyre Degradation Analysis")
        section_image(IMG_PIT, "Pit Lane")

        try:
            deg_data = run_query(f"""
                SELECT * FROM degradation_rates
                WHERE race_id BETWEEN {selected_year}00 AND {selected_year}99
                ORDER BY degradation_rate_ms_per_lap DESC
                LIMIT 50
            """)
            if not deg_data.empty:
                st.dataframe(deg_data, use_container_width=True, hide_index=True)
        except Exception:
            st.caption("Degradation data not yet computed.")


# ──────────────────────────────────────────────
# TAB 4: Circuit DNA
# ──────────────────────────────────────────────
with tab_circuit:
    hero_banner(
        "Circuit DNA",
        "Driver-circuit affinity scores across 78 circuits and 15 seasons",
        IMG_TRACK,
    )

    if not db_available():
        st.info("Database not loaded.")
    else:
        st.subheader(f"Performance at {selected_circuit}")
        circuit_stats = run_query(f"""
            SELECT
                dch.driver_id,
                d.full_name,
                dch.total_appearances,
                ROUND(dch.avg_finish_position, 1) AS avg_finish,
                ROUND(dch.avg_grid_position, 1) AS avg_grid,
                dch.total_wins,
                dch.total_podiums,
                ROUND(dch.positions_gained_avg, 1) AS avg_pos_gained,
                ROUND(dch.dnf_rate * 100, 1) AS dnf_pct
            FROM driver_circuit_history dch
            JOIN drivers d ON dch.driver_id = d.driver_id
            WHERE dch.circuit_id = '{selected_circuit}'
            ORDER BY dch.avg_finish_position
            LIMIT 20
        """)

        if not circuit_stats.empty:
            # Top driver highlight
            top = circuit_stats.iloc[0]
            glass_panel(f"""
                <div style="display: flex; align-items: center; gap: 24px;">
                    <div>
                        <div style="font-size: 10px; color: rgba(255,255,255,0.4); text-transform: uppercase; letter-spacing: 1.5px;">Circuit Specialist</div>
                        <div style="font-size: 24px; font-weight: 700; color: white; margin-top: 4px;">{top['full_name']}</div>
                    </div>
                    <div style="display: flex; gap: 20px; margin-left: auto;">
                        <div style="text-align: center;">
                            <div style="font-size: 22px; font-weight: 700; color: #E8002D;">{int(top['total_wins'])}</div>
                            <div style="font-size: 10px; color: rgba(255,255,255,0.4);">WINS</div>
                        </div>
                        <div style="text-align: center;">
                            <div style="font-size: 22px; font-weight: 700; color: white;">{int(top['total_podiums'])}</div>
                            <div style="font-size: 10px; color: rgba(255,255,255,0.4);">PODIUMS</div>
                        </div>
                        <div style="text-align: center;">
                            <div style="font-size: 22px; font-weight: 700; color: rgba(255,255,255,0.7);">{int(top['total_appearances'])}</div>
                            <div style="font-size: 10px; color: rgba(255,255,255,0.4);">RACES</div>
                        </div>
                    </div>
                </div>
            """, accent=True)

            st.markdown("")
            st.dataframe(circuit_stats, use_container_width=True, hide_index=True)

            # Bar chart of avg finish position
            fig = go.Figure(go.Bar(
                x=circuit_stats["full_name"],
                y=circuit_stats["avg_finish"],
                marker_color=F1_RED,
            ))
            fig = f1_layout(fig, f"Average Finish Position at {selected_circuit}")
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(26,26,26,0.5)",
            )
            fig.update_yaxes(title_text="Avg Finish Position", autorange="reversed")
            st.plotly_chart(fig, use_container_width=True)


# ──────────────────────────────────────────────
# TAB 5: Constructor IQ
# ──────────────────────────────────────────────
with tab_constructor:
    hero_banner(
        "Constructor Intelligence",
        "Team efficiency ratings — actual vs expected performance from grid position",
        IMG_F1_CAR,
    )

    if not db_available():
        st.info("Database not loaded.")
    else:
        st.subheader(f"{selected_year} Points: Actual vs Expected")
        try:
            eff_data = run_query(f"""
                SELECT
                    ce.constructor_id,
                    c.constructor_name,
                    ROUND(ce.actual_points, 0) AS actual_points,
                    ROUND(ce.expected_points, 0) AS expected_points,
                    ROUND(ce.points_vs_expectation, 1) AS over_under,
                    ROUND(ce.efficiency_rating, 1) AS efficiency_pct,
                    ROUND(ce.dnf_rate * 100, 1) AS dnf_pct
                FROM constructor_efficiency ce
                JOIN constructors c ON ce.constructor_id = c.constructor_id
                WHERE ce.year = {selected_year}
                ORDER BY ce.efficiency_rating DESC
            """)

            if not eff_data.empty:
                eff_data["team"] = eff_data["constructor_id"].map(team_name)

                # Efficiency leaders
                best = eff_data.iloc[0]
                worst = eff_data.iloc[-1]
                stat_badges([
                    ("Most Efficient", f"{best['team']} ({best['efficiency_pct']}%)"),
                    ("Least Efficient", f"{worst['team']} ({worst['efficiency_pct']}%)"),
                ])
                st.markdown("")

                render_team_legend()

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=eff_data["team"], y=eff_data["actual_points"],
                    name="Actual",
                    marker_color=[TEAM_COLORS.get(c, "#666") for c in eff_data["constructor_id"]],
                ))
                fig.add_trace(go.Bar(
                    x=eff_data["team"], y=eff_data["expected_points"],
                    name="Expected",
                    marker_color="rgba(255,255,255,0.15)",
                ))
                fig = f1_layout(fig, "Constructor Points: Actual vs Expected")
                fig.update_layout(
                    barmode="group",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(26,26,26,0.5)",
                )
                st.plotly_chart(fig, use_container_width=True)

                eff_display = eff_data[["team", "actual_points", "expected_points", "over_under", "efficiency_pct", "dnf_pct"]].rename(
                    columns={"team": "Team", "actual_points": "Actual Pts", "expected_points": "Expected Pts",
                             "over_under": "+/-", "efficiency_pct": "Efficiency %", "dnf_pct": "DNF %"}
                )
                st.dataframe(eff_display, use_container_width=True, hide_index=True)
        except Exception:
            st.caption("Efficiency data not yet computed. Run feature engineering first.")

        # Teammate gap
        st.markdown("---")
        st.subheader("Teammate Qualifying Gap")
        try:
            tm_data = run_query(f"""
                SELECT
                    tg.constructor_id,
                    d1.full_name AS driver_1,
                    d2.full_name AS driver_2,
                    ROUND(AVG(tg.quali_gap_positions), 2) AS avg_quali_gap,
                    ROUND(AVG(tg.race_gap_positions), 2) AS avg_race_gap
                FROM teammate_gap tg
                JOIN drivers d1 ON tg.driver_1 = d1.driver_id
                JOIN drivers d2 ON tg.driver_2 = d2.driver_id
                WHERE tg.year = {selected_year}
                GROUP BY tg.constructor_id, d1.full_name, d2.full_name
                ORDER BY avg_quali_gap
            """)
            if not tm_data.empty:
                tm_data["Team"] = tm_data["constructor_id"].map(team_name)
                tm_display = tm_data[["Team", "driver_1", "driver_2", "avg_quali_gap", "avg_race_gap"]].rename(
                    columns={"driver_1": "Driver 1", "driver_2": "Driver 2",
                             "avg_quali_gap": "Quali Gap", "avg_race_gap": "Race Gap"}
                )
                st.dataframe(tm_display, use_container_width=True, hide_index=True)
        except Exception:
            st.caption("Teammate gap data not yet computed.")


# ──────────────────────────────────────────────
# TAB 6: Prediction
# ──────────────────────────────────────────────
with tab_predict:
    hero_banner(
        "Podium Predictor",
        "XGBoost classifier with SHAP explainability — trained on 15 seasons, tested on 2024",
        IMG_HELMET,
    )

    if not db_available():
        st.info("Database not loaded.")
    else:
        # Train model on demand
        if st.button("Train XGBoost Model & Predict", type="primary"):
            with st.spinner("Training podium prediction model..."):
                try:
                    from src.models import build_feature_matrix, prepare_features
                    import xgboost as xgb
                    import numpy as np

                    con_pred = get_connection()
                    df_feat = build_feature_matrix(con_pred)

                    train_df = df_feat[df_feat["year"] <= 2022]
                    val_df = df_feat[df_feat["year"] == 2023]
                    test_df = df_feat[df_feat["year"] == 2024]

                    X_train, y_train, feat_names = prepare_features(train_df)
                    X_val, y_val, _ = prepare_features(val_df)
                    X_test, y_test, _ = prepare_features(test_df)

                    min_cols = min(X_train.shape[1], X_val.shape[1], X_test.shape[1])
                    X_train, X_val, X_test = X_train[:, :min_cols], X_val[:, :min_cols], X_test[:, :min_cols]
                    feat_names = feat_names[:min_cols]

                    pos_count = y_train.sum()
                    neg_count = len(y_train) - pos_count
                    scale_pw = neg_count / max(pos_count, 1)

                    model = xgb.XGBClassifier(
                        n_estimators=500, max_depth=6, learning_rate=0.05,
                        subsample=0.8, colsample_bytree=0.8,
                        scale_pos_weight=scale_pw, eval_metric="auc",
                        early_stopping_rounds=50, random_state=42,
                    )
                    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

                    y_pred_proba = model.predict_proba(X_test)[:, 1]
                    test_df = test_df.copy()
                    test_df["podium_probability"] = y_pred_proba

                    # Metrics
                    from sklearn.metrics import roc_auc_score
                    roc_auc = roc_auc_score(y_test, y_pred_proba) if len(np.unique(y_test)) > 1 else 0

                    st.markdown("")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Test ROC-AUC", f"{roc_auc:.3f}")
                    col2.metric("Features", len(feat_names))
                    col3.metric("Test Samples", len(y_test))

                    # Precision@3 per race
                    precision_at_3 = []
                    for rid in test_df["race_id"].unique():
                        race = test_df[test_df["race_id"] == rid]
                        top3 = race.nlargest(3, "podium_probability")
                        precision_at_3.append(top3["podium_flag"].mean())
                    avg_p3 = np.mean(precision_at_3)
                    st.metric("Precision@3 (avg across 2024 races)", f"{avg_p3:.1%}")

                    # Show top predictions per race
                    st.markdown("---")
                    st.subheader("2024 Race Predictions — Top 3 per Race")
                    race_ids_sorted = sorted(test_df["race_id"].unique())
                    race_names = run_query("SELECT race_id, race_name FROM races WHERE year = 2024 ORDER BY round")
                    race_map = dict(zip(race_names["race_id"], race_names["race_name"]))

                    rows = []
                    for rid in race_ids_sorted:
                        race = test_df[test_df["race_id"] == rid]
                        top3 = race.nlargest(3, "podium_probability")
                        rname = race_map.get(rid, str(rid))
                        for rank, (_, r) in enumerate(top3.iterrows(), 1):
                            rows.append({
                                "Race": rname,
                                "Rank": rank,
                                "Driver": r["driver_id"],
                                "Probability": f"{r['podium_probability']:.1%}",
                                "Actual Podium": "Yes" if r["podium_flag"] == 1 else "No",
                            })
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                    # Feature importance
                    st.markdown("---")
                    st.subheader("Feature Importance")
                    importances = model.feature_importances_
                    imp_df = pd.DataFrame({"Feature": feat_names, "Importance": importances})
                    imp_df = imp_df.sort_values("Importance", ascending=True)

                    fig = go.Figure(go.Bar(
                        x=imp_df["Importance"], y=imp_df["Feature"],
                        orientation="h",
                        marker_color=F1_RED,
                            ))
                    fig = f1_layout(fig, "XGBoost Feature Importance", height=max(300, len(feat_names) * 35))
                    fig.update_layout(
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(26,26,26,0.5)",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.error(f"Model training failed: {e}")

        # Historical podium analysis (always shown)
        st.markdown("---")
        st.subheader("Historical Podium Analysis")
        podium_stats = run_query(f"""
            SELECT
                d.full_name,
                COUNT(*) AS races,
                SUM(CASE WHEN r.position <= 3 THEN 1 ELSE 0 END) AS podiums,
                ROUND(SUM(CASE WHEN r.position <= 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS podium_pct,
                SUM(CASE WHEN r.position = 1 THEN 1 ELSE 0 END) AS wins
            FROM results r
            JOIN drivers d ON r.driver_id = d.driver_id
            WHERE r.year = {selected_year} AND r.position IS NOT NULL
            GROUP BY d.full_name
            HAVING COUNT(*) >= 5
            ORDER BY podium_pct DESC
        """)

        if not podium_stats.empty:
            fig = go.Figure(go.Bar(
                x=podium_stats["full_name"],
                y=podium_stats["podium_pct"],
                marker_color=F1_RED,
                text=podium_stats["podium_pct"].apply(lambda x: f"{x}%"),
                textposition="outside",
                textfont=dict(color="rgba(255,255,255,0.7)", size=11),
            ))
            fig = f1_layout(fig, f"{selected_year} Podium Rate by Driver")
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(26,26,26,0.5)",
            )
            fig.update_yaxes(title_text="Podium %")
            st.plotly_chart(fig, use_container_width=True)


# ──────────────────────────────────────────────
# TAB 7: Territorial Speed Map
# ──────────────────────────────────────────────
with tab_speedmap:
    hero_banner(
        "Territorial Speed Map",
        "GPS circuit visualization — see which driver dominates each section of the track",
        IMG_CIRCUIT,
    )

    if not db_available():
        st.info("Database not loaded.")
    else:
        from pathlib import Path as _P
        import numpy as _np

        TEL_DIR = PROJECT_ROOT / "data" / "raw" / "telemetry"

        # Build a lookup of available races from telemetry files
        tel_files = list(TEL_DIR.glob("*_R_*_tel.parquet"))
        races_available = sorted(set(
            "_".join(f.stem.split("_")[1:-3])  # e.g. "bahrain_grand_prix"
            for f in tel_files
        ))

        # Map circuit_id → race slug (best effort)
        race_slug_map = {}
        if selected_circuit:
            circuit_info = run_query(f"""
                SELECT ra.race_name, ra.circuit_id
                FROM races ra
                WHERE ra.year = {selected_year} AND ra.circuit_id = '{selected_circuit}'
                LIMIT 1
            """)
            if not circuit_info.empty:
                rname = circuit_info["race_name"].values[0].lower()
                # Match to available telemetry slugs
                for slug in races_available:
                    slug_clean = slug.replace("_", " ")
                    if slug_clean in rname or rname.replace(" grand prix", "") in slug_clean:
                        race_slug_map[selected_circuit] = slug
                        break

        # Race selector (from available telemetry)
        race_display = {s: s.replace("_", " ").title() for s in races_available}
        default_slug = race_slug_map.get(selected_circuit, races_available[0] if races_available else None)
        default_idx = races_available.index(default_slug) if default_slug in races_available else 0

        col_race, col_info = st.columns([3, 1])
        with col_race:
            chosen_race = st.selectbox(
                "Grand Prix",
                races_available,
                index=default_idx,
                format_func=lambda x: race_display.get(x, x),
            )

        # Find drivers with telemetry for this race
        driver_tel_files = sorted(TEL_DIR.glob(f"*{chosen_race}_R_*_tel.parquet"))
        available_drivers = sorted(set(
            f.stem.split("_")[-2]  # driver code like VER, NOR
            for f in driver_tel_files
        ))

        with col_info:
            st.markdown(f"""
            <div style="padding: 8px 0;">
                <span class="stat-badge">{len(available_drivers)} drivers</span>
                <span class="stat-badge">240 Hz telemetry</span>
            </div>
            """, unsafe_allow_html=True)

        if len(available_drivers) < 2:
            st.warning("Need at least 2 drivers with telemetry for this race.")
        else:
            # Let user pick 2-3 drivers to compare
            default_picks = []
            for code in ["VER", "NOR", "LEC"]:
                if code in available_drivers:
                    default_picks.append(code)
                if len(default_picks) == 3:
                    break
            while len(default_picks) < 2 and len(available_drivers) > len(default_picks):
                for d in available_drivers:
                    if d not in default_picks:
                        default_picks.append(d)
                        break

            map_drivers = st.multiselect(
                "Select 2–3 drivers to compare",
                available_drivers,
                default=default_picks[:3],
                max_selections=3,
            )

            if len(map_drivers) >= 2:
                with st.spinner("Loading telemetry data..."):
                    # Load telemetry for selected drivers
                    telemetry_list = []
                    loaded_codes = []

                    def _extract_fastest_lap(tel_df, circuit_len_m=5400):
                        """Extract the fastest single lap from cumulative-distance telemetry."""
                        dist = tel_df["Distance"].values
                        total = dist[-1] - dist[0]
                        if total < circuit_len_m * 1.5:
                            return tel_df  # already a single lap
                        n_laps = max(1, int(round(total / circuit_len_m)))
                        # Split into laps by distance
                        boundaries = _np.linspace(dist[0], dist[-1], n_laps + 1)
                        best_lap, best_avg_spd = None, 0
                        for i in range(1, min(n_laps, len(boundaries) - 1)):  # skip lap 1 (formation)
                            mask = (dist >= boundaries[i]) & (dist < boundaries[i + 1])
                            lap = tel_df.loc[mask]
                            if len(lap) < 50:
                                continue
                            avg_spd = lap["Speed"].mean()
                            if avg_spd > best_avg_spd:
                                best_avg_spd = avg_spd
                                best_lap = lap.copy()
                        return best_lap if best_lap is not None else tel_df

                    # Known circuit lengths (meters)
                    CIRCUIT_LENGTHS = {
                        "bahrain": 5412, "jeddah": 6174, "albert_park": 5278,
                        "suzuka": 5807, "shanghai": 5451, "miami": 5412,
                        "imola": 4909, "monaco": 3337, "catalunya": 4657,
                        "villeneuve": 4361, "red_bull_ring": 4318, "silverstone": 5891,
                        "hungaroring": 4381, "spa": 7004, "zandvoort": 4259,
                        "monza": 5793, "baku": 6003, "marina_bay": 4940,
                        "americas": 5513, "rodriguez": 4304, "interlagos": 4309,
                        "yas_marina": 5281, "losail": 5419, "vegas": 6201,
                    }
                    CIRCUIT_LEN = CIRCUIT_LENGTHS.get(selected_circuit, 5400)

                    for code in map_drivers:
                        matches = list(TEL_DIR.glob(f"*{chosen_race}_R_{code}_tel.parquet"))
                        if matches:
                            tel_df = pd.read_parquet(matches[0])
                            if "Distance" in tel_df.columns and len(tel_df) > 100:
                                lap_tel = _extract_fastest_lap(tel_df, CIRCUIT_LEN)
                                if lap_tel is not None and len(lap_tel) > 100:
                                    telemetry_list.append(lap_tel)
                                    loaded_codes.append(code)

                    if len(loaded_codes) >= 2:
                        # Driver colors from team colors or defaults
                        driver_team_map = {}
                        try:
                            dtm = run_query(f"""
                                SELECT DISTINCT d.driver_code, r.constructor_id
                                FROM results r
                                JOIN drivers d ON r.driver_id = d.driver_id
                                WHERE r.year = {selected_year} AND d.driver_code IS NOT NULL
                            """)
                            driver_team_map = dict(zip(dtm["driver_code"], dtm["constructor_id"]))
                        except Exception:
                            pass

                        colors = []
                        palette = ["#3671C6", "#E8002D", "#FF8000", "#27F4D2", "#FF87BC"]
                        for i, code in enumerate(loaded_codes):
                            team = driver_team_map.get(code)
                            colors.append(TEAM_COLORS.get(team, palette[i % len(palette)]))

                        fig = territorial_speed_map(telemetry_list, loaded_codes, colors)
                        st.plotly_chart(fig, use_container_width=True)

                        # Stats below the map
                        st.markdown("")
                        stat_cols = st.columns(len(loaded_codes))
                        for i, code in enumerate(loaded_codes):
                            tel = telemetry_list[i]
                            with stat_cols[i]:
                                max_spd = tel["Speed"].max()
                                avg_spd = tel["Speed"].mean()
                                glass_panel(f"""
                                    <div style="text-align: center;">
                                        <div style="font-size: 14px; font-weight: 700; color: {colors[i]};">{code}</div>
                                        <div style="display: flex; justify-content: center; gap: 20px; margin-top: 12px;">
                                            <div>
                                                <div style="font-size: 22px; font-weight: 700; color: white;">{max_spd:.0f}</div>
                                                <div style="font-size: 10px; color: rgba(255,255,255,0.4); text-transform: uppercase;">Top Speed</div>
                                            </div>
                                            <div>
                                                <div style="font-size: 22px; font-weight: 700; color: rgba(255,255,255,0.7);">{avg_spd:.0f}</div>
                                                <div style="font-size: 10px; color: rgba(255,255,255,0.4); text-transform: uppercase;">Avg Speed</div>
                                            </div>
                                        </div>
                                    </div>
                                """)
                    else:
                        st.warning(f"Could only load telemetry for {len(loaded_codes)} driver(s). Need at least 2.")
