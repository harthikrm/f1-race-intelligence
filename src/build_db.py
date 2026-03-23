"""
F1 Race Intelligence — Build DuckDB Database
Loads all raw JSON + FastF1 Parquet data into a single DuckDB database.
"""

import duckdb
import json
from pathlib import Path
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
TELEMETRY_DIR = RAW_DIR / "telemetry"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DB_PATH = PROCESSED_DIR / "f1.db"
SQL_DIR = PROJECT_ROOT / "sql"


def parse_time_to_ms(time_str):
    """Convert mm:ss.sss or ss.sss to milliseconds."""
    if not time_str or time_str == "":
        return None
    try:
        if ":" in time_str:
            parts = time_str.split(":")
            mins = int(parts[0])
            secs = float(parts[1])
            return int((mins * 60 + secs) * 1000)
        else:
            return int(float(time_str) * 1000)
    except (ValueError, IndexError):
        return None


def load_ergast_tables(con):
    """Load all Ergast JSON data into DuckDB tables via Python DataFrames."""

    # CIRCUITS
    with open(RAW_DIR / "circuits.json") as f:
        circuits_raw = json.load(f)
    circuits_df = pd.DataFrame([{
        "circuit_id": c["circuitId"],
        "circuit_name": c["circuitName"],
        "latitude": float(c["Location"]["lat"]),
        "longitude": float(c["Location"]["long"]),
        "locality": c["Location"]["locality"],
        "country": c["Location"]["country"],
    } for c in circuits_raw])
    con.execute("CREATE OR REPLACE TABLE circuits AS SELECT * FROM circuits_df")
    print(f"  circuits: {len(circuits_df)} rows")

    # DRIVERS
    with open(RAW_DIR / "drivers.json") as f:
        drivers_raw = json.load(f)
    drivers_df = pd.DataFrame([{
        "driver_id": d["driverId"],
        "first_name": d["givenName"],
        "last_name": d["familyName"],
        "full_name": d["givenName"] + " " + d["familyName"],
        "date_of_birth": d.get("dateOfBirth"),
        "nationality": d.get("nationality"),
        "permanent_number": d.get("permanentNumber"),
        "driver_code": d.get("code"),
    } for d in drivers_raw])
    con.execute("CREATE OR REPLACE TABLE drivers AS SELECT * FROM drivers_df")
    print(f"  drivers: {len(drivers_df)} rows")

    # CONSTRUCTORS
    with open(RAW_DIR / "constructors.json") as f:
        constructors_raw = json.load(f)
    constructors_df = pd.DataFrame([{
        "constructor_id": c["constructorId"],
        "constructor_name": c["name"],
        "nationality": c.get("nationality"),
    } for c in constructors_raw])
    con.execute("CREATE OR REPLACE TABLE constructors AS SELECT * FROM constructors_df")
    print(f"  constructors: {len(constructors_df)} rows")

    # RACES
    with open(RAW_DIR / "races.json") as f:
        races_raw = json.load(f)
    races_df = pd.DataFrame([{
        "year": int(r["season"]),
        "round": int(r["round"]),
        "race_id": int(r["season"]) * 100 + int(r["round"]),
        "race_name": r["raceName"],
        "circuit_id": r["Circuit"]["circuitId"],
        "race_date": r.get("date"),
    } for r in races_raw])
    con.execute("CREATE OR REPLACE TABLE races AS SELECT * FROM races_df")
    print(f"  races: {len(races_df)} rows")

    # RESULTS (nested: each race has a Results array)
    with open(RAW_DIR / "results.json") as f:
        results_raw = json.load(f)
    results_rows = []
    for race in results_raw:
        year = int(race["season"])
        rnd = int(race["round"])
        race_id = year * 100 + rnd
        for res in race.get("Results", []):
            pos = res.get("position")
            results_rows.append({
                "year": year,
                "round": rnd,
                "race_id": race_id,
                "driver_id": res["Driver"]["driverId"],
                "constructor_id": res["Constructor"]["constructorId"],
                "car_number": int(res.get("number", 0)),
                "grid": int(res.get("grid", 0)),
                "position": int(pos) if pos and pos != "" else None,
                "position_text": res.get("positionText", ""),
                "points": float(res.get("points", 0)),
                "laps": int(res.get("laps", 0)),
                "status": res.get("status", ""),
                "milliseconds": int(res["Time"]["millis"]) if res.get("Time") and res["Time"].get("millis") else None,
                "fastest_lap_rank": int(res["FastestLap"]["rank"]) if res.get("FastestLap") and res["FastestLap"].get("rank") else None,
                "fastest_lap_time": res["FastestLap"]["Time"]["time"] if res.get("FastestLap") and res["FastestLap"].get("Time") else None,
            })
    results_df = pd.DataFrame(results_rows)
    con.execute("CREATE OR REPLACE TABLE results AS SELECT * FROM results_df")
    print(f"  results: {len(results_df)} rows")

    # QUALIFYING (nested)
    with open(RAW_DIR / "qualifying.json") as f:
        quali_raw = json.load(f)
    quali_rows = []
    for race in quali_raw:
        year = int(race["season"])
        rnd = int(race["round"])
        race_id = year * 100 + rnd
        for q in race.get("QualifyingResults", []):
            quali_rows.append({
                "year": year,
                "round": rnd,
                "race_id": race_id,
                "driver_id": q["Driver"]["driverId"],
                "constructor_id": q["Constructor"]["constructorId"],
                "position": int(q.get("position", 0)),
                "q1": q.get("Q1"),
                "q2": q.get("Q2"),
                "q3": q.get("Q3"),
            })
    quali_df = pd.DataFrame(quali_rows)
    con.execute("CREATE OR REPLACE TABLE qualifying AS SELECT * FROM quali_df")
    print(f"  qualifying: {len(quali_df)} rows")

    # DRIVER STANDINGS (nested)
    with open(RAW_DIR / "driver_standings.json") as f:
        ds_raw = json.load(f)
    ds_rows = []
    for entry in ds_raw:
        year = int(entry["season"])
        rnd = int(entry["round"])
        for s in entry.get("DriverStandings", []):
            ds_rows.append({
                "year": year,
                "round": rnd,
                "driver_id": s["Driver"]["driverId"],
                "position": int(s.get("position", 0)),
                "points": float(s.get("points", 0)),
                "wins": int(s.get("wins", 0)),
            })
    ds_df = pd.DataFrame(ds_rows)
    con.execute("CREATE OR REPLACE TABLE driver_standings AS SELECT * FROM ds_df")
    print(f"  driver_standings: {len(ds_df)} rows")

    # CONSTRUCTOR STANDINGS (nested)
    with open(RAW_DIR / "constructor_standings.json") as f:
        cs_raw = json.load(f)
    cs_rows = []
    for entry in cs_raw:
        year = int(entry["season"])
        rnd = int(entry["round"])
        for s in entry.get("ConstructorStandings", []):
            cs_rows.append({
                "year": year,
                "round": rnd,
                "constructor_id": s["Constructor"]["constructorId"],
                "position": int(s.get("position", 0)),
                "points": float(s.get("points", 0)),
                "wins": int(s.get("wins", 0)),
            })
    cs_df = pd.DataFrame(cs_rows)
    con.execute("CREATE OR REPLACE TABLE constructor_standings AS SELECT * FROM cs_df")
    print(f"  constructor_standings: {len(cs_df)} rows")

    # CIRCUIT TYPES (manual lookup)
    circuit_types = pd.DataFrame([
        ("monaco", "street"), ("baku", "street"), ("marina_bay", "street"),
        ("jeddah", "street"), ("vegas", "street"), ("miami", "street"),
        ("monza", "high_speed"), ("spa", "high_speed"), ("silverstone", "high_speed"),
        ("suzuka", "high_speed"), ("villeneuve", "high_speed"),
        ("hungaroring", "technical"), ("zandvoort", "technical"), ("catalunya", "technical"),
        ("bahrain", "mixed"), ("yas_marina", "mixed"), ("americas", "mixed"),
        ("interlagos", "mixed"), ("albert_park", "mixed"), ("red_bull_ring", "high_speed"),
        ("rodriguez", "mixed"), ("losail", "high_speed"), ("shanghai", "mixed"),
        ("imola", "technical"),
    ], columns=["circuit_id", "circuit_type"])
    con.execute("CREATE OR REPLACE TABLE circuit_types AS SELECT * FROM circuit_types")
    print(f"  circuit_types: {len(circuit_types)} rows")


def build_database():
    """Build the complete DuckDB database from raw data."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Remove old DB if exists
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))

    # Change working directory for relative paths in SQL
    import os
    os.chdir(PROJECT_ROOT)

    print("=" * 60)
    print("BUILDING DUCKDB DATABASE")
    print("=" * 60)

    # ── Load Ergast data via Python (more reliable than SQL read_json_auto) ──
    print("\n1. Loading Ergast data from JSON...")
    load_ergast_tables(con)

    # Verify core tables
    for table in ["circuits", "drivers", "constructors", "races", "results", "qualifying"]:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count} rows")
        except Exception as e:
            print(f"  {table}: FAILED - {e}")

    # ── Build lap_times and pit_stops from FastF1 ──
    print("\n2. Building lap_times from FastF1 Parquet files...")
    build_fastf1_lap_times(con)

    print("\n3. Building pit_stops from FastF1 Parquet files...")
    build_fastf1_pit_stops(con)

    print("\n3b. Building compound-aware stints from FastF1 Parquet files...")
    build_fastf1_stints(con)

    # ── Load feature engineering views ──
    print("\n4. Creating feature engineering views...")
    fe_sql = (SQL_DIR / "02_feature_engineering.sql").read_text()
    fe_lines = [l for l in fe_sql.split("\n") if not l.strip().startswith("--")]
    fe_clean = "\n".join(fe_lines)
    for statement in fe_clean.split(";"):
        stmt = statement.strip()
        if not stmt:
            continue
        try:
            con.execute(stmt)
        except Exception as e:
            print(f"  View error: {str(e)[:120]}")

    # Verify views
    for view in ["lap_time_pct", "stints", "degradation_rates", "pit_position_delta",
                  "constructor_pit_ranking", "driver_circuit_history",
                  "quali_gap_to_pole", "grid_to_points_lookup",
                  "constructor_efficiency", "teammate_gap", "net_positions"]:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
            print(f"  {view}: {count} rows")
        except Exception as e:
            print(f"  {view}: FAILED - {str(e)[:80]}")

    # ── Create affinity scores table ──
    print("\n5. Computing driver-circuit affinity scores...")
    try:
        con.execute("""
            CREATE OR REPLACE TABLE driver_circuit_affinity AS
            SELECT * FROM driver_circuit_history
        """)
        # We'll compute the affinity_score in Python
        affinity_df = con.execute("SELECT * FROM driver_circuit_affinity").fetchdf()
        if not affinity_df.empty:
            from features import compute_affinity_score

            # Need avg_quali_gap_pct - compute from qualifying data
            quali_gap = con.execute("""
                SELECT
                    q.driver_id,
                    ra.circuit_id,
                    AVG(qg.quali_gap_to_pole_pct) AS avg_quali_gap_pct
                FROM qualifying q
                JOIN races ra ON q.race_id = ra.race_id
                LEFT JOIN quali_gap_to_pole qg ON q.race_id = qg.race_id AND q.driver_id = qg.driver_id
                GROUP BY q.driver_id, ra.circuit_id
            """).fetchdf()

            affinity_df = affinity_df.merge(quali_gap, on=["driver_id", "circuit_id"], how="left")
            affinity_df["avg_quali_gap_pct"] = affinity_df["avg_quali_gap_pct"].fillna(5.0)

            affinity_df = compute_affinity_score(affinity_df)
            con.execute("DROP TABLE IF EXISTS driver_circuit_affinity")
            con.execute("CREATE TABLE driver_circuit_affinity AS SELECT * FROM affinity_df")
            count = con.execute("SELECT COUNT(*) FROM driver_circuit_affinity").fetchone()[0]
            print(f"  driver_circuit_affinity: {count} rows")
        else:
            print("  No affinity data to compute")
    except Exception as e:
        print(f"  Affinity computation error: {e}")

    # ── Final verification ──
    print("\n" + "=" * 60)
    print("DATABASE BUILD COMPLETE")
    print("=" * 60)
    tables = con.execute("SHOW TABLES").fetchdf()
    print(f"\nTables: {', '.join(tables['name'].tolist())}")
    for _, row in tables.iterrows():
        try:
            count = con.execute(f"SELECT COUNT(*) FROM \"{row['name']}\"").fetchone()[0]
            print(f"  {row['name']}: {count:,} rows")
        except Exception as e:
            print(f"  {row['name']}: error counting - {str(e)[:60]}")

    con.close()
    print(f"\nDatabase saved to: {DB_PATH}")
    print(f"Size: {DB_PATH.stat().st_size / 1024 / 1024:.1f} MB")


def _build_code_to_id_map(con):
    """Build mapping from FastF1 3-letter codes to Ergast driver_ids."""
    try:
        drivers = con.execute(
            "SELECT driver_id, driver_code FROM drivers WHERE driver_code IS NOT NULL"
        ).fetchdf()
        code_map = dict(zip(drivers["driver_code"], drivers["driver_id"]))
        return code_map
    except Exception:
        return {}


def build_fastf1_lap_times(con):
    """Build lap_times table from FastF1 race lap Parquet files."""
    lap_files = sorted(TELEMETRY_DIR.glob("*_R_laps.parquet"))
    if not lap_files:
        print("  No FastF1 race lap files found")
        con.execute("""
            CREATE OR REPLACE TABLE lap_times (
                year INTEGER, round INTEGER, race_id INTEGER, lap INTEGER,
                driver_id VARCHAR, driver_code VARCHAR, position INTEGER,
                lap_time_str VARCHAR, lap_time_ms INTEGER
            )
        """)
        return

    code_map = _build_code_to_id_map(con)
    all_laps = []
    race_map = {}

    # Build race mapping from races table
    try:
        races = con.execute("SELECT year, round, race_id, race_name FROM races").fetchdf()
        for _, r in races.iterrows():
            key = f"{r['year']}_{r['race_name'].replace(' ', '_').lower()}"
            race_map[key] = (r["year"], r["round"], r["race_id"])
    except Exception:
        pass

    for f in lap_files:
        try:
            df = pd.read_parquet(f)
            # Parse filename: 2024_australian_grand_prix_R_laps.parquet
            parts = f.stem.replace("_R_laps", "").split("_", 1)
            year = int(parts[0])
            gp_name = parts[1] if len(parts) > 1 else ""

            if df.empty:
                continue

            # Map to race_id
            race_key = f"{year}_{gp_name}"
            if race_key in race_map:
                yr, rnd, race_id = race_map[race_key]
            else:
                # Try fuzzy match
                rnd = len(all_laps) + 1  # fallback
                race_id = year * 100 + rnd

            # Extract lap time data
            for _, row in df.iterrows():
                lap_time_s = row.get("LapTime")
                if pd.isna(lap_time_s) or lap_time_s is None:
                    continue

                lap_time_ms = int(lap_time_s * 1000) if isinstance(lap_time_s, (int, float)) else None
                if lap_time_ms is None or lap_time_ms <= 0:
                    continue

                driver_code = str(row.get("Driver", ""))
                all_laps.append({
                    "year": year,
                    "round": rnd if race_key in race_map else 0,
                    "race_id": race_id,
                    "lap": int(row.get("LapNumber", 0)),
                    "driver_id": code_map.get(driver_code, driver_code),
                    "driver_code": driver_code,
                    "position": int(row.get("Position", 0)) if pd.notna(row.get("Position")) else None,
                    "lap_time_str": "",
                    "lap_time_ms": lap_time_ms,
                })
        except Exception as e:
            print(f"    Error processing {f.name}: {e}")

    if all_laps:
        laps_df = pd.DataFrame(all_laps)
        con.execute("CREATE OR REPLACE TABLE lap_times AS SELECT * FROM laps_df")
        print(f"  lap_times: {len(laps_df):,} rows from {len(lap_files)} race files")
    else:
        con.execute("""
            CREATE OR REPLACE TABLE lap_times (
                year INTEGER, round INTEGER, race_id INTEGER, lap INTEGER,
                driver_id VARCHAR, driver_code VARCHAR, position INTEGER,
                lap_time_str VARCHAR, lap_time_ms INTEGER
            )
        """)
        print("  lap_times: 0 rows (no valid data)")


def build_fastf1_pit_stops(con):
    """Build pit_stops table from FastF1 race lap data (PitInTime/PitOutTime)."""
    lap_files = sorted(TELEMETRY_DIR.glob("*_R_laps.parquet"))
    if not lap_files:
        print("  No FastF1 race lap files found")
        con.execute("""
            CREATE OR REPLACE TABLE pit_stops (
                year INTEGER, round INTEGER, race_id INTEGER, driver_id VARCHAR,
                driver_code VARCHAR, stop INTEGER, lap INTEGER, time_of_day VARCHAR,
                duration_str VARCHAR, pit_duration_ms INTEGER
            )
        """)
        return

    code_map = _build_code_to_id_map(con)
    all_pits = []
    race_map = {}
    try:
        races = con.execute("SELECT year, round, race_id, race_name FROM races").fetchdf()
        for _, r in races.iterrows():
            key = f"{r['year']}_{r['race_name'].replace(' ', '_').lower()}"
            race_map[key] = (r["year"], r["round"], r["race_id"])
    except Exception:
        pass

    for f in lap_files:
        try:
            df = pd.read_parquet(f)
            parts = f.stem.replace("_R_laps", "").split("_", 1)
            year = int(parts[0])
            gp_name = parts[1] if len(parts) > 1 else ""
            race_key = f"{year}_{gp_name}"

            if race_key in race_map:
                yr, rnd, race_id = race_map[race_key]
            else:
                rnd = 0
                race_id = year * 100

            if df.empty:
                continue

            # Detect pit stops from PitInTime/PitOutTime columns
            if "PitInTime" not in df.columns or "PitOutTime" not in df.columns:
                continue

            for driver in df["Driver"].unique():
                driver_laps = df[df["Driver"] == driver].sort_values("LapNumber")
                pit_in_laps = driver_laps[driver_laps["PitInTime"].notna()]
                stop_num = 0

                for _, row in pit_in_laps.iterrows():
                    stop_num += 1
                    pit_in = row["PitInTime"]
                    lap_num = int(row["LapNumber"])

                    # PitOutTime is on the NEXT lap (the out-lap)
                    next_lap = driver_laps[driver_laps["LapNumber"] == lap_num + 1]
                    pit_out = next_lap["PitOutTime"].values[0] if not next_lap.empty and pd.notna(next_lap["PitOutTime"].values[0]) else None

                    duration_s = None
                    if pit_out is not None and pd.notna(pit_in):
                        duration_s = float(pit_out) - float(pit_in)
                        if duration_s < 0 or duration_s > 120:
                            duration_s = None

                    all_pits.append({
                        "year": year,
                        "round": rnd,
                        "race_id": race_id,
                        "driver_id": code_map.get(driver, driver),
                        "driver_code": driver,
                        "stop": stop_num,
                        "lap": lap_num,
                        "time_of_day": "",
                        "duration_str": f"{duration_s:.3f}" if duration_s else "",
                        "pit_duration_ms": int(duration_s * 1000) if duration_s else None,
                    })
        except Exception as e:
            print(f"    Error processing {f.name}: {e}")

    if all_pits:
        pits_df = pd.DataFrame(all_pits)
        con.execute("CREATE OR REPLACE TABLE pit_stops AS SELECT * FROM pits_df")
        print(f"  pit_stops: {len(pits_df):,} rows from FastF1 data")
    else:
        con.execute("""
            CREATE OR REPLACE TABLE pit_stops (
                year INTEGER, round INTEGER, race_id INTEGER, driver_id VARCHAR,
                driver_code VARCHAR, stop INTEGER, lap INTEGER, time_of_day VARCHAR,
                duration_str VARCHAR, pit_duration_ms INTEGER
            )
        """)
        print("  pit_stops: 0 rows")


def build_fastf1_stints(con):
    """Build a compound-aware stints table from FastF1 lap data."""
    lap_files = sorted(TELEMETRY_DIR.glob("*_R_laps.parquet"))
    if not lap_files:
        print("  No FastF1 race lap files found")
        return

    code_map = _build_code_to_id_map(con)
    race_map = {}
    try:
        races = con.execute("SELECT year, round, race_id, race_name FROM races").fetchdf()
        for _, r in races.iterrows():
            key = f"{r['year']}_{r['race_name'].replace(' ', '_').lower()}"
            race_map[key] = (r["year"], r["round"], r["race_id"])
    except Exception:
        pass

    all_stints = []
    for f in lap_files:
        try:
            df = pd.read_parquet(f)
            if df.empty or "Stint" not in df.columns:
                continue

            parts = f.stem.replace("_R_laps", "").split("_", 1)
            year = int(parts[0])
            gp_name = parts[1] if len(parts) > 1 else ""
            race_key = f"{year}_{gp_name}"

            if race_key in race_map:
                yr, rnd, race_id = race_map[race_key]
            else:
                rnd = 0
                race_id = year * 100

            for driver in df["Driver"].unique():
                dlaps = df[df["Driver"] == driver].sort_values("LapNumber")
                for stint_num in dlaps["Stint"].dropna().unique():
                    stint_laps = dlaps[dlaps["Stint"] == stint_num]
                    if stint_laps.empty:
                        continue
                    compound = stint_laps["Compound"].dropna().mode()
                    compound = compound.iloc[0] if not compound.empty else "UNKNOWN"
                    fresh = stint_laps["FreshTyre"].dropna().mode()
                    fresh = bool(fresh.iloc[0]) if not fresh.empty else True

                    all_stints.append({
                        "year": year,
                        "race_id": race_id,
                        "driver_id": code_map.get(driver, driver),
                        "driver_code": driver,
                        "stint_number": int(stint_num),
                        "start_lap": int(stint_laps["LapNumber"].min()),
                        "end_lap": int(stint_laps["LapNumber"].max()),
                        "stint_length": int(stint_laps["LapNumber"].max() - stint_laps["LapNumber"].min() + 1),
                        "compound": compound,
                        "fresh_tyre": fresh,
                        "tyre_life_end": int(stint_laps["TyreLife"].max()) if stint_laps["TyreLife"].notna().any() else None,
                    })
        except Exception as e:
            print(f"    Error processing {f.name}: {e}")

    if all_stints:
        stints_df = pd.DataFrame(all_stints)
        con.execute("CREATE OR REPLACE TABLE fastf1_stints AS SELECT * FROM stints_df")
        print(f"  fastf1_stints: {len(stints_df):,} rows from {len(lap_files)} race files")
    else:
        print("  fastf1_stints: 0 rows")


if __name__ == "__main__":
    build_database()
