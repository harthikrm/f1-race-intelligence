-- ============================================
-- F1 Race Intelligence — DuckDB Schema
-- Creates all core tables from raw JSON/Parquet
-- ============================================

-- ──────────────────────────────────────────────
-- CIRCUITS
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE circuits AS
SELECT
    circuitId AS circuit_id,
    circuitName AS circuit_name,
    CAST(Location.lat AS DOUBLE) AS latitude,
    CAST(Location.long AS DOUBLE) AS longitude,
    Location.locality AS locality,
    Location.country AS country
FROM read_json_auto('data/raw/circuits.json');

-- ──────────────────────────────────────────────
-- DRIVERS
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE drivers AS
SELECT
    driverId AS driver_id,
    givenName AS first_name,
    familyName AS last_name,
    givenName || ' ' || familyName AS full_name,
    dateOfBirth AS date_of_birth,
    nationality,
    COALESCE(permanentNumber, NULL) AS permanent_number,
    COALESCE(code, NULL) AS driver_code
FROM read_json_auto('data/raw/drivers.json');

-- ──────────────────────────────────────────────
-- CONSTRUCTORS
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE constructors AS
SELECT
    constructorId AS constructor_id,
    name AS constructor_name,
    nationality
FROM read_json_auto('data/raw/constructors.json');

-- ──────────────────────────────────────────────
-- RACES
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE races AS
SELECT
    CAST(season AS INTEGER) AS year,
    CAST(round AS INTEGER) AS round,
    raceName AS race_name,
    Circuit.circuitId AS circuit_id,
    CAST(date AS DATE) AS race_date
FROM read_json_auto('data/raw/races.json');

-- Add a synthetic race_id
ALTER TABLE races ADD COLUMN race_id INTEGER;
UPDATE races SET race_id = (year * 100) + round;

-- ──────────────────────────────────────────────
-- RESULTS
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE results AS
SELECT
    CAST(r.season AS INTEGER) AS year,
    CAST(r.round AS INTEGER) AS round,
    (CAST(r.season AS INTEGER) * 100 + CAST(r.round AS INTEGER)) AS race_id,
    res.Driver.driverId AS driver_id,
    res.Constructor.constructorId AS constructor_id,
    CAST(res.number AS INTEGER) AS car_number,
    CAST(res.grid AS INTEGER) AS grid,
    CASE WHEN res.position IS NOT NULL AND res.position != ''
         THEN CAST(res.position AS INTEGER) ELSE NULL END AS position,
    res.positionText AS position_text,
    CAST(res.points AS DOUBLE) AS points,
    CAST(res.laps AS INTEGER) AS laps,
    res.status,
    CASE WHEN res.Time IS NOT NULL AND res.Time.millis IS NOT NULL
         THEN CAST(res.Time.millis AS INTEGER) ELSE NULL END AS milliseconds,
    CASE WHEN res.FastestLap IS NOT NULL AND res.FastestLap.rank IS NOT NULL
         THEN CAST(res.FastestLap.rank AS INTEGER) ELSE NULL END AS fastest_lap_rank,
    CASE WHEN res.FastestLap IS NOT NULL AND res.FastestLap.Time IS NOT NULL
         THEN res.FastestLap.Time.time ELSE NULL END AS fastest_lap_time
FROM read_json_auto('data/raw/results.json') r,
     UNNEST(r.Results) AS res;

-- ──────────────────────────────────────────────
-- QUALIFYING
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE qualifying AS
SELECT
    CAST(q.season AS INTEGER) AS year,
    CAST(q.round AS INTEGER) AS round,
    (CAST(q.season AS INTEGER) * 100 + CAST(q.round AS INTEGER)) AS race_id,
    qual.Driver.driverId AS driver_id,
    qual.Constructor.constructorId AS constructor_id,
    CAST(qual.position AS INTEGER) AS position,
    COALESCE(qual.Q1, NULL) AS q1,
    COALESCE(qual.Q2, NULL) AS q2,
    COALESCE(qual.Q3, NULL) AS q3
FROM read_json_auto('data/raw/qualifying.json') q,
     UNNEST(q.QualifyingResults) AS qual;

-- ──────────────────────────────────────────────
-- PIT STOPS (per-race flat records with season/round tags)
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE pit_stops AS
SELECT
    CAST(p.season AS INTEGER) AS year,
    CAST(p.round AS INTEGER) AS round,
    (CAST(p.season AS INTEGER) * 100 + CAST(p.round AS INTEGER)) AS race_id,
    p.driverId AS driver_id,
    CAST(p.stop AS INTEGER) AS stop,
    CAST(p.lap AS INTEGER) AS lap,
    p.time AS time_of_day,
    p.duration AS duration_str,
    CASE WHEN p.duration IS NOT NULL
         THEN TRY_CAST(REPLACE(p.duration, ':', '') AS DOUBLE) * 1000
         ELSE NULL END AS pit_duration_ms
FROM read_json_auto('data/raw/pit_stops.json') p;

-- ──────────────────────────────────────────────
-- LAP TIMES (per-race records with Timings nested)
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE lap_times AS
SELECT
    CAST(lt.season AS INTEGER) AS year,
    CAST(lt.round AS INTEGER) AS round,
    (CAST(lt.season AS INTEGER) * 100 + CAST(lt.round AS INTEGER)) AS race_id,
    CAST(lt.number AS INTEGER) AS lap,
    timing.driverId AS driver_id,
    CAST(timing.position AS INTEGER) AS position,
    timing.time AS lap_time_str,
    -- Convert mm:ss.sss to milliseconds
    (CAST(SPLIT_PART(timing.time, ':', 1) AS INTEGER) * 60000 +
     CAST(REPLACE(SPLIT_PART(timing.time, ':', 2), '.', '') AS INTEGER)) AS lap_time_ms
FROM read_json_auto('data/raw/lap_times.json') lt,
     UNNEST(lt.Timings) AS timing;

-- ──────────────────────────────────────────────
-- DRIVER STANDINGS
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE driver_standings AS
SELECT
    CAST(ds.season AS INTEGER) AS year,
    CAST(ds.round AS INTEGER) AS round,
    standing.Driver.driverId AS driver_id,
    CAST(standing.position AS INTEGER) AS position,
    CAST(standing.points AS DOUBLE) AS points,
    CAST(standing.wins AS INTEGER) AS wins
FROM read_json_auto('data/raw/driver_standings.json') ds,
     UNNEST(ds.DriverStandings) AS standing;

-- ──────────────────────────────────────────────
-- CONSTRUCTOR STANDINGS
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE constructor_standings AS
SELECT
    CAST(cs.season AS INTEGER) AS year,
    CAST(cs.round AS INTEGER) AS round,
    standing.Constructor.constructorId AS constructor_id,
    CAST(standing.position AS INTEGER) AS position,
    CAST(standing.points AS DOUBLE) AS points,
    CAST(standing.wins AS INTEGER) AS wins
FROM read_json_auto('data/raw/constructor_standings.json') cs,
     UNNEST(cs.ConstructorStandings) AS standing;

-- ──────────────────────────────────────────────
-- CIRCUIT TYPE CLASSIFICATION (manual lookup)
-- ──────────────────────────────────────────────
CREATE OR REPLACE TABLE circuit_types (
    circuit_id VARCHAR PRIMARY KEY,
    circuit_type VARCHAR
);

INSERT INTO circuit_types VALUES
    ('monaco', 'street'),
    ('baku', 'street'),
    ('marina_bay', 'street'),
    ('jeddah', 'street'),
    ('vegas', 'street'),
    ('miami', 'street'),
    ('monza', 'high_speed'),
    ('spa', 'high_speed'),
    ('silverstone', 'high_speed'),
    ('suzuka', 'high_speed'),
    ('villeneuve', 'high_speed'),
    ('hungaroring', 'technical'),
    ('zandvoort', 'technical'),
    ('catalunya', 'technical'),
    ('bahrain', 'mixed'),
    ('yas_marina', 'mixed'),
    ('americas', 'mixed'),
    ('interlagos', 'mixed'),
    ('albert_park', 'mixed'),
    ('red_bull_ring', 'high_speed'),
    ('rodriguez', 'mixed'),
    ('losail', 'high_speed'),
    ('shanghai', 'mixed'),
    ('imola', 'technical');

-- ──────────────────────────────────────────────
-- VERIFICATION QUERIES
-- ──────────────────────────────────────────────
-- Run these after loading to verify data integrity:
-- SELECT COUNT(*) AS total_results FROM results;
-- SELECT COUNT(DISTINCT year) AS seasons FROM results;
-- SELECT year, COUNT(*) AS races FROM races GROUP BY year ORDER BY year;
-- SELECT COUNT(*) AS total_laps FROM lap_times;
-- SELECT COUNT(*) AS total_pit_stops FROM pit_stops;
