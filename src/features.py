"""
F1 Race Intelligence — Feature Engineering Functions
Python-side feature computation for modules A–D.
"""

import numpy as np
import pandas as pd
import fastf1


# ──────────────────────────────────────────────
# MODULE A: Driver vs Driver Battle
# ──────────────────────────────────────────────

def compute_speed_trace(session, driver_a: str, driver_b: str, lap_num: int) -> pd.DataFrame:
    """
    Compare speed traces of two drivers on the same lap.
    Returns DataFrame with Distance, Speed_A, Speed_B, Throttle_A, Throttle_B, Brake_A, Brake_B.
    """
    laps_a = session.laps.pick_driver(driver_a).pick_lap(lap_num)
    laps_b = session.laps.pick_driver(driver_b).pick_lap(lap_num)

    tel_a = laps_a.get_telemetry().add_distance()
    tel_b = laps_b.get_telemetry().add_distance()

    # Resample to every 10m for smooth comparison
    max_dist = min(tel_a["Distance"].max(), tel_b["Distance"].max())
    ref_dist = np.arange(0, max_dist, 10)

    result = pd.DataFrame({"Distance": ref_dist})
    for col, tel, suffix in [
        ("Speed", tel_a, "_A"), ("Speed", tel_b, "_B"),
        ("Throttle", tel_a, "_A"), ("Throttle", tel_b, "_B"),
        ("Brake", tel_a, "_A"), ("Brake", tel_b, "_B"),
    ]:
        result[col + suffix] = np.interp(ref_dist, tel["Distance"].values, tel[col].values)

    result["Speed_Delta"] = result["Speed_A"] - result["Speed_B"]

    return result


def compute_braking_points(telemetry_df: pd.DataFrame, speed_col: str = "Speed",
                           brake_col: str = "Brake", dist_col: str = "Distance") -> list:
    """
    Find braking points where Brake transitions to True and Speed > 200 km/h.
    Returns list of (distance, speed) tuples.
    """
    braking = []
    prev_brake = False
    for _, row in telemetry_df.iterrows():
        if row[brake_col] and not prev_brake and row[speed_col] > 200:
            braking.append((row[dist_col], row[speed_col]))
        prev_brake = bool(row[brake_col])
    return braking


def compute_corner_speeds(telemetry_df: pd.DataFrame, circuit_info) -> pd.DataFrame:
    """
    Find minimum speed at each corner using circuit corner data.
    circuit_info from fastf1.get_circuit_info(year, gp).
    """
    corners = circuit_info.corners
    results = []

    for _, corner in corners.iterrows():
        cx, cy = corner["X"], corner["Y"]
        # Find telemetry within ±50m of corner apex
        dists = np.sqrt((telemetry_df["X"] - cx)**2 + (telemetry_df["Y"] - cy)**2)
        near = telemetry_df[dists < 50]
        if not near.empty:
            results.append({
                "Corner": corner.get("Number", corner.name),
                "MinSpeed": near["Speed"].min(),
                "Distance": corner.get("Distance", 0),
            })

    return pd.DataFrame(results)


def get_fastest_laps(session, drivers: list = None) -> pd.DataFrame:
    """Get fastest lap data for specified drivers (or all)."""
    laps = session.laps
    if drivers:
        laps = laps[laps["Driver"].isin(drivers)]

    fastest = laps.loc[laps.groupby("Driver")["LapTime"].idxmin()]
    return fastest


# ──────────────────────────────────────────────
# MODULE B: Pit Stop Strategy IQ
# ──────────────────────────────────────────────

def detect_undercut_windows(lap_times_df: pd.DataFrame, gap_threshold: float = 2.0,
                            closing_rate: float = 0.3, consecutive_laps: int = 3) -> pd.DataFrame:
    """
    Detect undercut opportunities: when gap is closing > 0.3s/lap for 3+ consecutive laps.
    lap_times_df should have: race_id, lap, driver_id, lap_time_ms, position.
    """
    results = []

    for race_id in lap_times_df["race_id"].unique():
        race = lap_times_df[lap_times_df["race_id"] == race_id].sort_values(["lap", "position"])

        for lap in race["lap"].unique():
            lap_data = race[race["lap"] == lap].sort_values("position")
            drivers = lap_data["driver_id"].tolist()

            for i in range(len(drivers) - 1):
                d_ahead = drivers[i]
                d_behind = drivers[i + 1]

                # Check closing rate over last N laps
                closing_laps = 0
                for prev_lap in range(max(1, lap - consecutive_laps), lap):
                    ahead_time = race[(race["driver_id"] == d_ahead) & (race["lap"] == prev_lap)]
                    behind_time = race[(race["driver_id"] == d_behind) & (race["lap"] == prev_lap)]
                    if ahead_time.empty or behind_time.empty:
                        continue
                    delta = ahead_time["lap_time_ms"].values[0] - behind_time["lap_time_ms"].values[0]
                    if delta > closing_rate * 1000:
                        closing_laps += 1

                if closing_laps >= consecutive_laps:
                    results.append({
                        "race_id": race_id,
                        "lap": lap,
                        "driver_ahead": d_ahead,
                        "driver_behind": d_behind,
                        "undercut_window_open": True,
                    })

    return pd.DataFrame(results) if results else pd.DataFrame(
        columns=["race_id", "lap", "driver_ahead", "driver_behind", "undercut_window_open"]
    )


def compute_strategy_iq(constructor_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Composite strategy IQ score per constructor.
    Input needs: constructor_id, pit_speed_rank, undercut_success_pct,
                 overcut_success_pct, position_gain_per_stop
    Each component normalized 0–100, then weighted.
    """
    df = constructor_stats.copy()

    def normalize_0_100(series, invert=False):
        mn, mx = series.min(), series.max()
        if mx == mn:
            return pd.Series(50, index=series.index)
        norm = (series - mn) / (mx - mn) * 100
        return 100 - norm if invert else norm

    df["pit_rank_score"] = normalize_0_100(df["pit_speed_rank"], invert=True)
    df["undercut_score"] = normalize_0_100(df["undercut_success_pct"])
    df["overcut_score"] = normalize_0_100(df["overcut_success_pct"])
    df["pos_gain_score"] = normalize_0_100(df["position_gain_per_stop"])

    df["strategy_iq_score"] = (
        0.3 * df["pit_rank_score"]
        + 0.3 * df["undercut_score"]
        + 0.2 * df["overcut_score"]
        + 0.2 * df["pos_gain_score"]
    )

    return df


# ──────────────────────────────────────────────
# MODULE C: Circuit DNA & Driver Affinity
# ──────────────────────────────────────────────

def compute_affinity_score(driver_circuit_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute composite affinity score per driver-circuit pair.
    Input needs: driver_id, circuit_id, finish_pct_rank, avg_quali_gap_pct,
                 positions_gained_avg, dnf_rate
    """
    df = driver_circuit_df.copy()

    # Component 1 (40%): finish position percentile (already 0-1, higher = better rank)
    c1 = 1 - df["finish_pct_rank"]  # invert: lower rank = better

    # Component 2 (30%): qualifying closeness to pole
    max_gap = df.groupby("circuit_id")["avg_quali_gap_pct"].transform("max")
    c2 = 1 - (df["avg_quali_gap_pct"].fillna(max_gap) / max_gap.replace(0, 1))

    # Component 3 (20%): positions gained (normalized)
    c3 = (df["positions_gained_avg"].clip(-10, 10) + 10) / 20  # normalize to 0-1

    # Component 4 (10%): reliability
    c4 = 1 - df["dnf_rate"]

    df["affinity_score"] = (0.4 * c1 + 0.3 * c2 + 0.2 * c3 + 0.1 * c4) * 100

    return df


# ──────────────────────────────────────────────
# MODULE D: Constructor Efficiency
# ──────────────────────────────────────────────

def compute_rolling_form(results_df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    """
    Compute rolling form metrics for drivers and constructors.
    Returns df with team_form_rolling_5 and driver_form_rolling_5.
    """
    df = results_df.sort_values(["year", "race_id"]).copy()

    # Driver rolling form (avg finish position, lower = better)
    df["driver_form_rolling_5"] = (
        df.groupby("driver_id")["position"]
        .transform(lambda x: x.rolling(window, min_periods=1).mean())
    )

    # Team rolling form (avg points scored)
    team_points = df.groupby(["constructor_id", "race_id"])["points"].sum().reset_index()
    team_points = team_points.sort_values("race_id")
    team_points["team_form_rolling_5"] = (
        team_points.groupby("constructor_id")["points"]
        .transform(lambda x: x.rolling(window, min_periods=1).mean())
    )

    df = df.merge(
        team_points[["constructor_id", "race_id", "team_form_rolling_5"]],
        on=["constructor_id", "race_id"],
        how="left"
    )

    return df
