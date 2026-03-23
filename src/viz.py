"""
F1 Race Intelligence — Visualization Functions
Plotly chart builders for all 6 modules.
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

# ──────────────────────────────────────────────
# F1 Theme Constants
# ──────────────────────────────────────────────
F1_BG = "rgba(0,0,0,0)"
F1_PAPER = "rgba(26,26,26,0.5)"
F1_RED = "#E8002D"
F1_WHITE = "#FFFFFF"
F1_GRAY = "#666666"
F1_FONT = "Inter, -apple-system, BlinkMacSystemFont, sans-serif"

# Team colors (2024 season)
TEAM_COLORS = {
    "red_bull": "#3671C6", "ferrari": "#E8002D", "mercedes": "#27F4D2",
    "mclaren": "#FF8000", "aston_martin": "#229971", "alpine": "#FF87BC",
    "williams": "#64C4FF", "rb": "#6692FF", "kick_sauber": "#52E252",
    "sauber": "#52E252", "haas": "#B6BABD",
}

# Tyre compound colors
TYRE_COLORS = {
    "SOFT": "#FF3333", "MEDIUM": "#FFD700", "HARD": "#FFFFFF",
    "INTERMEDIATE": "#39B54A", "WET": "#0072CE",
}


def f1_layout(fig: go.Figure, title: str = "", height: int = 500) -> go.Figure:
    """Apply F1 dark theme to any Plotly figure."""
    fig.update_layout(
        title=dict(text=title, font=dict(size=18, color=F1_WHITE, family=F1_FONT)),
        plot_bgcolor=F1_BG,
        paper_bgcolor=F1_PAPER,
        font=dict(color=F1_WHITE, family=F1_FONT, size=12),
        height=height,
        margin=dict(l=60, r=30, t=60, b=50),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=F1_WHITE)),
        xaxis=dict(gridcolor="rgba(255,255,255,0.06)", zerolinecolor="rgba(255,255,255,0.1)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.06)", zerolinecolor="rgba(255,255,255,0.1)"),
    )
    return fig


# ──────────────────────────────────────────────
# MODULE A: Driver Battle Charts
# ──────────────────────────────────────────────

def speed_trace_chart(trace_df: pd.DataFrame, driver_a: str, driver_b: str,
                      color_a: str = F1_RED, color_b: str = "#3671C6") -> go.Figure:
    """Speed trace overlay — x: distance, y: speed."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trace_df["Distance"], y=trace_df["Speed_A"],
        name=driver_a, line=dict(color=color_a, width=2),
    ))
    fig.add_trace(go.Scatter(
        x=trace_df["Distance"], y=trace_df["Speed_B"],
        name=driver_b, line=dict(color=color_b, width=2),
    ))
    fig = f1_layout(fig, f"Speed Trace: {driver_a} vs {driver_b}")
    fig.update_xaxes(title_text="Distance (m)")
    fig.update_yaxes(title_text="Speed (km/h)")
    return fig


def lap_delta_chart(trace_df: pd.DataFrame, driver_a: str, driver_b: str) -> go.Figure:
    """Cumulative time delta across lap distance."""
    # Approximate time delta from speed difference
    speed_a = trace_df["Speed_A"].values
    speed_b = trace_df["Speed_B"].values
    dist_step = 10  # meters per step

    time_a = dist_step / (speed_a / 3.6 + 1e-6)  # seconds per segment
    time_b = dist_step / (speed_b / 3.6 + 1e-6)
    cum_delta = np.cumsum(time_b - time_a)  # positive = A faster

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trace_df["Distance"], y=cum_delta,
        fill="tozeroy", line=dict(color=F1_RED, width=2),
        fillcolor="rgba(232,0,45,0.3)",
    ))
    fig = f1_layout(fig, f"Lap Delta: {driver_a} vs {driver_b}")
    fig.update_xaxes(title_text="Distance (m)")
    fig.update_yaxes(title_text=f"Time Delta (s) — above 0 = {driver_a} faster")
    fig.add_hline(y=0, line_dash="dash", line_color=F1_GRAY)
    return fig


def sector_heatmap(sector_df: pd.DataFrame) -> go.Figure:
    """Sector time heatmap — rows: drivers, cols: S1/S2/S3."""
    drivers = sector_df["Driver"].unique()
    sectors = ["Sector1", "Sector2", "Sector3"]

    z = []
    for driver in drivers:
        row = sector_df[sector_df["Driver"] == driver]
        z.append([row[s].values[0] if s in row.columns else 0 for s in sectors])

    fig = go.Figure(go.Heatmap(
        z=z, x=["S1", "S2", "S3"], y=list(drivers),
        colorscale=[[0, "#00FF00"], [0.5, "#FFFF00"], [1, "#FF0000"]],
        text=[[f"{v:.3f}s" for v in row] for row in z],
        texttemplate="%{text}",
    ))
    fig = f1_layout(fig, "Sector Times Comparison", height=max(300, len(drivers) * 30))
    return fig


def throttle_brake_chart(trace_df: pd.DataFrame, driver: str) -> go.Figure:
    """Dual-axis throttle% and brake flag over distance."""
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                        row_heights=[0.7, 0.3])
    fig.add_trace(go.Scatter(
        x=trace_df["Distance"], y=trace_df[f"Throttle_A"],
        name="Throttle", line=dict(color="#00FF00", width=1.5),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=trace_df["Distance"], y=trace_df[f"Brake_A"],
        name="Brake", fill="tozeroy", line=dict(color=F1_RED, width=1),
        fillcolor="rgba(232,0,45,0.5)",
    ), row=2, col=1)
    fig = f1_layout(fig, f"Throttle & Brake: {driver}")
    fig.update_xaxes(title_text="Distance (m)", row=2)
    fig.update_yaxes(title_text="Throttle %", row=1)
    fig.update_yaxes(title_text="Brake", row=2)
    return fig


# ──────────────────────────────────────────────
# MODULE B: Pit Strategy Charts
# ──────────────────────────────────────────────

def stint_timeline_chart(stints_df: pd.DataFrame) -> go.Figure:
    """Gantt-style tyre stint chart — each driver row, colored by compound."""
    fig = go.Figure()
    for _, stint in stints_df.iterrows():
        compound = stint.get("compound", "MEDIUM")
        color = TYRE_COLORS.get(compound, "#CCCCCC")
        fig.add_trace(go.Bar(
            x=[stint["stint_length"]],
            y=[stint["driver_id"]],
            orientation="h",
            base=stint["start_lap"],
            marker_color=color,
            name=compound,
            showlegend=False,
            hovertemplate=f"{stint['driver_id']}: Lap {stint['start_lap']}-{stint['end_lap']} ({compound})<extra></extra>",
        ))
    fig = f1_layout(fig, "Tyre Strategy Timeline", height=max(400, len(stints_df["driver_id"].unique()) * 25))
    fig.update_xaxes(title_text="Lap")
    return fig


def degradation_chart(deg_df: pd.DataFrame) -> go.Figure:
    """Multi-line degradation curves per stint."""
    fig = go.Figure()
    for _, row in deg_df.iterrows():
        fig.add_trace(go.Scatter(
            x=list(range(row["start_lap"], row["end_lap"] + 1)),
            y=[row["avg_lap_time_ms"] + row["degradation_rate_ms_per_lap"] * i
               for i in range(row["stint_length"])],
            name=f"{row['driver_id']} S{row['stint_number']}",
            mode="lines",
        ))
    fig = f1_layout(fig, "Tyre Degradation Curves")
    fig.update_xaxes(title_text="Lap")
    fig.update_yaxes(title_text="Lap Time (ms)")
    return fig


def pit_duration_ranking(pit_df: pd.DataFrame) -> go.Figure:
    """Horizontal bar — constructors ranked by avg pit time."""
    pit_df = pit_df.sort_values("avg_pit_ms", ascending=True)
    fig = go.Figure(go.Bar(
        x=pit_df["avg_pit_ms"] / 1000,
        y=pit_df["constructor_id"],
        orientation="h",
        marker_color=[TEAM_COLORS.get(c, F1_GRAY) for c in pit_df["constructor_id"]],
    ))
    fig = f1_layout(fig, "Average Pit Stop Duration by Team")
    fig.update_xaxes(title_text="Duration (seconds)")
    return fig


def strategy_iq_radar(iq_df: pd.DataFrame) -> go.Figure:
    """Radar/spider chart — 4 dimensions per team."""
    fig = go.Figure()
    categories = ["Pit Speed", "Undercut Success", "Overcut Success", "Position Gain"]
    for _, row in iq_df.iterrows():
        vals = [row["pit_rank_score"], row["undercut_score"],
                row["overcut_score"], row["pos_gain_score"]]
        fig.add_trace(go.Scatterpolar(
            r=vals + [vals[0]],
            theta=categories + [categories[0]],
            name=row["constructor_id"],
            line_color=TEAM_COLORS.get(row["constructor_id"], F1_GRAY),
        ))
    fig = f1_layout(fig, "Strategy IQ Comparison")
    fig.update_layout(polar=dict(
        bgcolor=F1_BG,
        radialaxis=dict(visible=True, range=[0, 100], gridcolor="#333"),
        angularaxis=dict(gridcolor="#333"),
    ))
    return fig


# ──────────────────────────────────────────────
# MODULE C: Circuit DNA Charts
# ──────────────────────────────────────────────

def affinity_heatmap(affinity_df: pd.DataFrame, drivers: list = None) -> go.Figure:
    """Color-coded tile grid — driver x circuit, score drives intensity."""
    if drivers:
        affinity_df = affinity_df[affinity_df["driver_id"].isin(drivers)]

    pivot = affinity_df.pivot_table(
        index="driver_id", columns="circuit_id", values="affinity_score"
    )
    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=list(pivot.columns),
        y=list(pivot.index),
        colorscale=[[0, "#1a1a2e"], [0.5, "#e94560"], [1, "#00FF00"]],
        text=pivot.values.round(1),
        texttemplate="%{text}",
    ))
    fig = f1_layout(fig, "Driver-Circuit Affinity Map", height=max(400, len(pivot) * 30))
    return fig


def top_circuits_bar(affinity_df: pd.DataFrame, driver_id: str, top_n: int = 10) -> go.Figure:
    """Horizontal ranked bars — top circuits for a driver."""
    df = affinity_df[affinity_df["driver_id"] == driver_id].nlargest(top_n, "affinity_score")
    fig = go.Figure(go.Bar(
        x=df["affinity_score"],
        y=df["circuit_id"],
        orientation="h",
        marker_color=F1_RED,
    ))
    fig = f1_layout(fig, f"Top Circuits for {driver_id}")
    fig.update_xaxes(title_text="Affinity Score (0–100)")
    return fig


# ──────────────────────────────────────────────
# MODULE D: Constructor Efficiency Charts
# ──────────────────────────────────────────────

def efficiency_bar_chart(eff_df: pd.DataFrame, year: int) -> go.Figure:
    """Grouped bars — actual vs expected points per constructor."""
    df = eff_df[eff_df["year"] == year].sort_values("actual_points", ascending=False)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["constructor_id"], y=df["actual_points"],
        name="Actual Points",
        marker_color=[TEAM_COLORS.get(c, F1_GRAY) for c in df["constructor_id"]],
    ))
    fig.add_trace(go.Bar(
        x=df["constructor_id"], y=df["expected_points"],
        name="Expected Points",
        marker_color="rgba(255,255,255,0.3)",
    ))
    fig = f1_layout(fig, f"{year} Constructor Points: Actual vs Expected")
    fig.update_layout(barmode="group")
    fig.update_yaxes(title_text="Points")
    return fig


def efficiency_rating_chart(eff_df: pd.DataFrame, year: int) -> go.Figure:
    """Horizontal bar — sorted by efficiency %."""
    df = eff_df[eff_df["year"] == year].sort_values("efficiency_rating")
    colors = ["#FF4444" if r < 100 else "#44FF44" for r in df["efficiency_rating"]]
    fig = go.Figure(go.Bar(
        x=df["efficiency_rating"] - 100,
        y=df["constructor_id"],
        orientation="h",
        marker_color=colors,
    ))
    fig = f1_layout(fig, f"{year} Constructor Efficiency (100 = expected)")
    fig.update_xaxes(title_text="Over/Under Performance %")
    fig.add_vline(x=0, line_dash="dash", line_color=F1_GRAY)
    return fig


# ──────────────────────────────────────────────
# MODULE F: Circuit Map
# ──────────────────────────────────────────────

def circuit_speed_map(tel_a: pd.DataFrame, tel_b: pd.DataFrame,
                      driver_a: str, driver_b: str) -> go.Figure:
    """GPS scatter — X vs Y colored by speed delta between two drivers."""
    fig = go.Figure()
    speed_delta = tel_a["Speed"].values - tel_b["Speed"].values

    fig.add_trace(go.Scatter(
        x=tel_a["X"], y=tel_a["Y"],
        mode="markers",
        marker=dict(
            size=3,
            color=speed_delta,
            colorscale=[[0, "#3671C6"], [0.5, "#FFFFFF"], [1, "#E8002D"]],
            cmin=-30, cmax=30,
            colorbar=dict(title="Speed Delta (km/h)"),
        ),
        hovertemplate=f"Delta: %{{marker.color:.1f}} km/h<extra></extra>",
    ))
    fig = f1_layout(fig, f"Circuit Speed Map: {driver_a} vs {driver_b}")
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False, scaleanchor="x")
    return fig


def territorial_speed_map(
    telemetry_list: list[pd.DataFrame],
    driver_codes: list[str],
    driver_colors: list[str] | None = None,
) -> go.Figure:
    """
    Territorial speed map — each point colored by the driver who is fastest there.

    Parameters
    ----------
    telemetry_list : list of DataFrames, each with X, Y, Speed, Distance columns
    driver_codes   : list of 3-letter driver codes (same order as telemetry_list)
    driver_colors  : optional list of hex colors per driver
    """
    if driver_colors is None:
        palette = ["#3671C6", "#E8002D", "#FF8000", "#27F4D2", "#FF87BC", "#64C4FF"]
        driver_colors = [palette[i % len(palette)] for i in range(len(driver_codes))]

    # --- Resample all telemetry to a common distance grid ----------------
    ref = telemetry_list[0].copy()
    n_points = len(ref)

    # Build a speed matrix: rows = points, cols = drivers
    speeds = np.column_stack([
        np.interp(
            np.linspace(0, 1, n_points),
            np.linspace(0, 1, len(t)),
            t["Speed"].values,
        )
        for t in telemetry_list
    ])

    # Resample X/Y for each driver too (for matching scatter coords)
    xs = [np.interp(np.linspace(0, 1, n_points), np.linspace(0, 1, len(t)), t["X"].values) for t in telemetry_list]
    ys = [np.interp(np.linspace(0, 1, n_points), np.linspace(0, 1, len(t)), t["Y"].values) for t in telemetry_list]

    # For each point, which driver is fastest?
    fastest_idx = np.argmax(speeds, axis=1)

    # --- Build one trace per driver (only their fastest points) ----------
    fig = go.Figure()
    for i, code in enumerate(driver_codes):
        mask = fastest_idx == i
        if not mask.any():
            continue
        fig.add_trace(go.Scatter(
            x=xs[i][mask],
            y=ys[i][mask],
            mode="markers",
            name=f"{code} Fastest",
            marker=dict(size=5, color=driver_colors[i]),
            hovertemplate=f"{code}: %{{customdata:.0f}} km/h<extra></extra>",
            customdata=speeds[mask, i],
        ))

    # --- Compute live delta annotation (first two drivers) ---------------
    if len(telemetry_list) >= 2:
        ref_dist = ref["Distance"].values
        total_dist = ref_dist[-1] - ref_dist[0] if len(ref_dist) > 1 else 5000
        step = total_dist / n_points

        times = []
        for i in range(min(2, len(telemetry_list))):
            spd = speeds[:, i]
            seg_time = step / (spd / 3.6 + 1e-6)  # seconds per segment
            times.append(seg_time.sum())

        delta = times[0] - times[1]  # negative = first driver faster
        delta_text = f"{delta:+.3f}s"
        delta_color = "#E8002D" if delta < 0 else "#3671C6"
        subtitle = f"{driver_codes[0]} vs {driver_codes[1]}"

        fig.add_annotation(
            x=0.02, y=0.98, xref="paper", yref="paper",
            xanchor="left", yanchor="top",
            text=(
                f'<span style="font-size:11px; color:rgba(255,255,255,0.5);">LIVE DELTA</span><br>'
                f'<span style="font-size:28px; font-weight:700; color:{delta_color};">{delta_text}</span><br>'
                f'<span style="font-size:11px; color:rgba(255,255,255,0.4);">{subtitle}</span>'
            ),
            showarrow=False,
            font=dict(family=F1_FONT),
            bgcolor="rgba(30,30,30,0.75)",
            bordercolor="rgba(255,255,255,0.1)",
            borderwidth=1,
            borderpad=12,
        )

    # --- Layout ----------------------------------------------------------
    fig = f1_layout(fig, "Territorial Speed Map (FastF1 GPS)", height=650)
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False, scaleanchor="x")
    fig.update_layout(
        legend=dict(
            orientation="h", yanchor="top", y=1.02, xanchor="right", x=1,
            font=dict(size=13),
        ),
    )

    fig.add_annotation(
        x=0.5, y=-0.02, xref="paper", yref="paper",
        text="Scatter plot of circuit map (X vs Y coordinates) where each point is colored by the driver with the highest speed.",
        showarrow=False,
        font=dict(size=11, color="rgba(255,255,255,0.35)"),
    )

    return fig
