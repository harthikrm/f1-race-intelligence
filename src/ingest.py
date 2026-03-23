"""
F1 Race Intelligence — Data Ingestion Pipeline
Fetches historical data from Jolpica/Ergast API and telemetry from FastF1.
"""

import json
import os
import time
from pathlib import Path

import fastf1
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
TELEMETRY_DIR = RAW_DIR / "telemetry"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Jolpica API (Ergast drop-in replacement — Ergast deprecated 2024)
BASE_URL = "https://api.jolpi.ca/ergast/f1"

# FastF1 cache
FASTF1_CACHE = PROJECT_ROOT / "data" / "raw" / "fastf1_cache"


def setup_dirs():
    """Create all required data directories."""
    for d in [RAW_DIR, TELEMETRY_DIR, PROCESSED_DIR, FASTF1_CACHE]:
        d.mkdir(parents=True, exist_ok=True)
    fastf1.Cache.enable_cache(str(FASTF1_CACHE))


# ──────────────────────────────────────────────
# Ergast / Jolpica API
# ──────────────────────────────────────────────

def fetch_ergast(endpoint: str, params: dict = None) -> list:
    """
    Generic paginated Ergast API caller.
    Returns all results across all pages for the given endpoint.
    """
    params = params or {}
    all_results = []
    offset = 0
    limit = 100  # Jolpica API caps at 100 per page

    while True:
        url = f"{BASE_URL}/{endpoint}.json"
        req_params = {**params, "limit": limit, "offset": offset}

        # Retry with exponential backoff on rate limit
        for attempt in range(5):
            resp = requests.get(url, params=req_params, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt * 2
                print(f"    Rate limited, waiting {wait}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            print(f"    FAILED after 5 retries: {endpoint} offset={offset}")
            break

        data = resp.json()

        # Navigate into MRData -> *Table -> *
        mrdata = data.get("MRData", {})
        total = int(mrdata.get("total", 0))

        # Find the table key (e.g., RaceTable, DriverTable, etc.)
        table_key = [k for k in mrdata if k.endswith("Table")][0]
        table = mrdata[table_key]

        # Find the data list (e.g., Races, Drivers, etc.)
        list_key = [k for k in table if isinstance(table[k], list)][0]
        results = table[list_key]

        all_results.extend(results)
        offset += limit

        if offset >= total:
            break

        time.sleep(1.5)  # rate limit — Jolpica is strict

    return all_results


def fetch_per_race_data(year: int, num_rounds: int, endpoint_suffix: str) -> list:
    """
    Fetch data that requires per-race requests (lap times, pit stops).
    These endpoints need /{year}/{round}/laps or /{year}/{round}/pitstops.
    """
    all_data = []
    for rnd in range(1, num_rounds + 1):
        endpoint = f"{year}/{rnd}/{endpoint_suffix}"
        try:
            data = fetch_ergast(endpoint)
            # Tag each record with season/round for context
            for item in data:
                item["season"] = str(year)
                item["round"] = str(rnd)
            all_data.extend(data)
        except Exception as e:
            print(f"    Round {rnd}: {e}")
        time.sleep(1.5)
    return all_data


def fetch_season_data(year: int) -> dict:
    """Fetch all endpoints for a single season."""
    # Standard endpoints (season-level)
    endpoints = {
        "races": f"{year}",
        "results": f"{year}/results",
        "qualifying": f"{year}/qualifying",
        "driver_standings": f"{year}/driverStandings",
        "constructor_standings": f"{year}/constructorStandings",
    }

    season_data = {}
    for name, endpoint in endpoints.items():
        print(f"  Fetching {year} {name}...")
        try:
            season_data[name] = fetch_ergast(endpoint)
        except Exception as e:
            print(f"  WARNING: Failed to fetch {name}: {e}")
            season_data[name] = []
        time.sleep(1.5)

    # Get number of rounds this season
    num_rounds = len(season_data.get("races", []))
    print(f"  {year} has {num_rounds} rounds")

    # Per-race endpoints (lap times and pit stops need per-round fetching)
    # These are very slow due to API rate limits — only fetch if requested
    if os.environ.get("FETCH_LAP_TIMES", "0") == "1":
        print(f"  Fetching {year} pit_stops (per-race)...")
        season_data["pit_stops"] = fetch_per_race_data(year, num_rounds, "pitstops")
        print(f"    Got {len(season_data['pit_stops'])} pit stop records")

        print(f"  Fetching {year} lap_times (per-race)...")
        season_data["lap_times"] = fetch_per_race_data(year, num_rounds, "laps")
        print(f"    Got {len(season_data['lap_times'])} lap time records")
    else:
        print(f"  Skipping per-race lap_times/pit_stops (use FETCH_LAP_TIMES=1 to enable)")
        season_data["pit_stops"] = []
        season_data["lap_times"] = []

    return season_data


def fetch_all_seasons(start: int = 2010, end: int = 2024):
    """Fetch all Ergast data for range of seasons and save as JSON."""
    setup_dirs()

    # Static tables (not per-season)
    for name, endpoint in [("circuits", "circuits"), ("drivers", "drivers"),
                           ("constructors", "constructors")]:
        print(f"Fetching {name}...")
        data = fetch_ergast(endpoint)
        path = RAW_DIR / f"{name}.json"
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"  Saved {len(data)} {name} to {path}")

    # Per-season data
    all_seasons = {}
    for year in range(start, end + 1):
        print(f"\n{'='*40}")
        print(f"Season {year}")
        print(f"{'='*40}")
        all_seasons[year] = fetch_season_data(year)

    # Save combined data per endpoint
    for endpoint_name in ["races", "results", "qualifying", "pit_stops",
                          "driver_standings", "constructor_standings", "lap_times"]:
        combined = []
        for year in range(start, end + 1):
            combined.extend(all_seasons[year].get(endpoint_name, []))
        path = RAW_DIR / f"{endpoint_name}.json"
        with open(path, "w") as f:
            json.dump(combined, f, indent=2)
        print(f"Saved {len(combined)} total {endpoint_name} records")

    return all_seasons


# ──────────────────────────────────────────────
# FastF1 Telemetry
# ──────────────────────────────────────────────

def fetch_fastf1_session(year: int, gp_name: str, session_type: str = "R"):
    """
    Fetch telemetry for a single session.
    session_type: "R" (Race), "Q" (Qualifying), "FP1", "FP2", "FP3"
    Returns (laps_df, session) or (None, None) on failure.
    """
    try:
        session = fastf1.get_session(year, gp_name, session_type)
        session.load()

        laps = session.laps
        if laps is None or laps.empty:
            print(f"  No lap data for {year} {gp_name} {session_type}")
            return None, session

        return laps, session

    except Exception as e:
        print(f"  ERROR loading {year} {gp_name} {session_type}: {e}")
        return None, None


def fetch_season_telemetry(year: int, session_types: list = None):
    """
    Fetch telemetry for all races in a season.
    Saves lap data and telemetry as Parquet files.
    """
    setup_dirs()

    if session_types is None:
        session_types = ["R", "Q"]

    # Get race schedule
    schedule = fastf1.get_event_schedule(year)
    # Filter to actual race events (exclude pre-season testing)
    races = schedule[schedule["EventFormat"] != "testing"]

    for _, event in races.iterrows():
        gp_name = event["EventName"]
        round_num = event["RoundNumber"]

        if round_num == 0:
            continue

        for stype in session_types:
            print(f"\n{year} Round {round_num}: {gp_name} — {stype}")

            laps, session = fetch_fastf1_session(year, gp_name, stype)
            if laps is None:
                continue

            # Save lap data
            safe_name = gp_name.replace(" ", "_").lower()
            lap_path = TELEMETRY_DIR / f"{year}_{safe_name}_{stype}_laps.parquet"

            # Convert timedelta columns to seconds for Parquet compatibility
            laps_save = laps.copy()
            td_cols = laps_save.select_dtypes(include=["timedelta64"]).columns
            for col in td_cols:
                laps_save[col] = laps_save[col].dt.total_seconds()

            laps_save.to_parquet(lap_path, index=False)
            print(f"  Saved {len(laps_save)} laps to {lap_path.name}")

            # Save telemetry for each driver (race sessions only)
            if stype == "R":
                for driver in laps["Driver"].unique():
                    try:
                        driver_laps = laps.pick_driver(driver)
                        tel = driver_laps.get_telemetry()
                        if tel is not None and not tel.empty:
                            tel_save = tel.copy()
                            tel_save["Driver"] = driver
                            tel_td_cols = tel_save.select_dtypes(
                                include=["timedelta64"]
                            ).columns
                            for col in tel_td_cols:
                                tel_save[col] = tel_save[col].dt.total_seconds()

                            tel_path = (
                                TELEMETRY_DIR
                                / f"{year}_{safe_name}_{stype}_{driver}_tel.parquet"
                            )
                            tel_save.to_parquet(tel_path, index=False)
                    except Exception as e:
                        print(f"    Telemetry error for {driver}: {e}")

    print(f"\nTelemetry collection complete for {year}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="F1 Race Intelligence — Data Ingestion")
    parser.add_argument("--ergast", action="store_true", help="Fetch Ergast/Jolpica API data")
    parser.add_argument("--telemetry", action="store_true", help="Fetch FastF1 telemetry")
    parser.add_argument("--start", type=int, default=2010, help="Start season (default: 2010)")
    parser.add_argument("--end", type=int, default=2024, help="End season (default: 2024)")
    parser.add_argument("--telemetry-year", type=int, default=2024,
                        help="Year for telemetry (default: 2024)")
    parser.add_argument("--all", action="store_true", help="Fetch everything")

    args = parser.parse_args()

    if args.all or args.ergast:
        print("=" * 60)
        print("FETCHING ERGAST/JOLPICA API DATA")
        print("=" * 60)
        fetch_all_seasons(start=args.start, end=args.end)

    if args.all or args.telemetry:
        print("\n" + "=" * 60)
        print(f"FETCHING FASTF1 TELEMETRY — {args.telemetry_year}")
        print("=" * 60)
        fetch_season_telemetry(args.telemetry_year)

    if not any([args.all, args.ergast, args.telemetry]):
        print("Usage: python src/ingest.py --all")
        print("       python src/ingest.py --ergast --start 2010 --end 2024")
        print("       python src/ingest.py --telemetry --telemetry-year 2024")
