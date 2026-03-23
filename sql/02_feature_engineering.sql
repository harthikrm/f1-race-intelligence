-- ============================================
-- F1 Race Intelligence — Feature Engineering
-- Creates all derived tables/views used by modules A–D
-- ============================================

-- ──────────────────────────────────────────────
-- MODULE A: Driver vs Driver Battle
-- ──────────────────────────────────────────────

-- Lap time percentage vs session fastest
CREATE OR REPLACE VIEW lap_time_pct AS
SELECT
    lt.*,
    ((lt.lap_time_ms - MIN(lt.lap_time_ms) OVER (PARTITION BY lt.race_id))
     * 100.0 / MIN(lt.lap_time_ms) OVER (PARTITION BY lt.race_id)) AS pct_off_fastest
FROM lap_times lt
WHERE lt.lap_time_ms > 0;

-- Sector field rank (requires FastF1 lap data loaded separately)
-- This will be computed in Python from Parquet telemetry files


-- ──────────────────────────────────────────────
-- MODULE B: Pit Stop Strategy IQ
-- ──────────────────────────────────────────────

-- Stint reconstruction from pit stops
CREATE OR REPLACE VIEW stints AS
WITH pit_laps AS (
    SELECT
        race_id, driver_id, stop, lap AS pit_lap,
        pit_duration_ms,
        LEAD(lap) OVER (PARTITION BY race_id, driver_id ORDER BY stop) AS next_pit_lap
    FROM pit_stops
),
driver_race_laps AS (
    SELECT race_id, driver_id, MAX(laps) AS total_laps
    FROM results
    GROUP BY race_id, driver_id
)
SELECT
    p.race_id,
    p.driver_id,
    p.stop AS stint_number,
    CASE WHEN p.stop = 1 THEN 1
         ELSE LAG(p.pit_lap) OVER (PARTITION BY p.race_id, p.driver_id ORDER BY p.stop) + 1
    END AS start_lap,
    p.pit_lap AS end_lap,
    p.pit_lap - CASE WHEN p.stop = 1 THEN 1
                     ELSE LAG(p.pit_lap) OVER (PARTITION BY p.race_id, p.driver_id ORDER BY p.stop) + 1
                END + 1 AS stint_length,
    p.pit_duration_ms
FROM pit_laps p
JOIN driver_race_laps d ON p.race_id = d.race_id AND p.driver_id = d.driver_id;

-- Degradation rate per stint (lap time slope over stint laps)
CREATE OR REPLACE VIEW degradation_rates AS
SELECT
    s.race_id,
    s.driver_id,
    s.stint_number,
    s.start_lap,
    s.end_lap,
    s.stint_length,
    REGR_SLOPE(lt.lap_time_ms, lt.lap) AS degradation_rate_ms_per_lap,
    AVG(lt.lap_time_ms) AS avg_lap_time_ms,
    MIN(lt.lap_time_ms) AS best_lap_time_ms
FROM stints s
JOIN lap_times lt
    ON s.race_id = lt.race_id
    AND s.driver_id = lt.driver_id
    AND lt.lap BETWEEN s.start_lap AND s.end_lap
WHERE s.stint_length >= 5  -- need enough laps for meaningful regression
GROUP BY s.race_id, s.driver_id, s.stint_number, s.start_lap, s.end_lap, s.stint_length;

-- Position delta around pit stops
CREATE OR REPLACE VIEW pit_position_delta AS
WITH before_pit AS (
    SELECT
        ps.race_id, ps.driver_id, ps.stop, ps.lap AS pit_lap,
        AVG(lt.position) AS avg_pos_before
    FROM pit_stops ps
    JOIN lap_times lt
        ON ps.race_id = lt.race_id
        AND ps.driver_id = lt.driver_id
        AND lt.lap BETWEEN GREATEST(ps.lap - 3, 1) AND ps.lap - 1
    GROUP BY ps.race_id, ps.driver_id, ps.stop, ps.lap
),
after_pit AS (
    SELECT
        ps.race_id, ps.driver_id, ps.stop, ps.lap AS pit_lap,
        AVG(lt.position) AS avg_pos_after
    FROM pit_stops ps
    JOIN lap_times lt
        ON ps.race_id = lt.race_id
        AND ps.driver_id = lt.driver_id
        AND lt.lap BETWEEN ps.lap + 1 AND ps.lap + 3
    GROUP BY ps.race_id, ps.driver_id, ps.stop, ps.lap
)
SELECT
    b.race_id, b.driver_id, b.stop, b.pit_lap,
    b.avg_pos_before,
    a.avg_pos_after,
    b.avg_pos_before - a.avg_pos_after AS position_delta  -- positive = gained positions
FROM before_pit b
JOIN after_pit a ON b.race_id = a.race_id AND b.driver_id = a.driver_id AND b.stop = a.stop;

-- Pit stop speed ranking per race
CREATE OR REPLACE VIEW pit_speed_ranking AS
SELECT
    race_id,
    driver_id,
    AVG(pit_duration_ms) AS avg_pit_ms,
    RANK() OVER (PARTITION BY race_id ORDER BY AVG(pit_duration_ms) ASC) AS pit_speed_rank
FROM pit_stops
WHERE pit_duration_ms IS NOT NULL AND pit_duration_ms > 0
GROUP BY race_id, driver_id;

-- Constructor pit speed ranking per season
CREATE OR REPLACE VIEW constructor_pit_ranking AS
SELECT
    ps.year,
    r.constructor_id,
    AVG(ps.pit_duration_ms) AS avg_pit_ms,
    RANK() OVER (PARTITION BY ps.year ORDER BY AVG(ps.pit_duration_ms) ASC) AS pit_speed_rank
FROM pit_stops ps
JOIN results r ON ps.race_id = r.race_id AND ps.driver_id = r.driver_id
WHERE ps.pit_duration_ms IS NOT NULL AND ps.pit_duration_ms > 0
GROUP BY ps.year, r.constructor_id;


-- ──────────────────────────────────────────────
-- MODULE C: Circuit DNA & Driver Affinity
-- ──────────────────────────────────────────────

-- Driver-circuit history aggregation
CREATE OR REPLACE VIEW driver_circuit_history AS
SELECT
    r.driver_id,
    ra.circuit_id,
    COUNT(*) AS total_appearances,
    AVG(r.position) FILTER (WHERE r.position IS NOT NULL) AS avg_finish_position,
    AVG(r.grid) AS avg_grid_position,
    AVG(r.points) AS avg_points,
    SUM(CASE WHEN r.position = 1 THEN 1 ELSE 0 END) AS total_wins,
    SUM(CASE WHEN r.position <= 3 THEN 1 ELSE 0 END) AS total_podiums,
    SUM(CASE WHEN r.position IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS dnf_rate,
    AVG(r.grid - COALESCE(r.position, r.grid)) AS positions_gained_avg,
    PERCENT_RANK() OVER (
        PARTITION BY ra.circuit_id
        ORDER BY AVG(r.position) FILTER (WHERE r.position IS NOT NULL) ASC
    ) AS finish_pct_rank
FROM results r
JOIN races ra ON r.race_id = ra.race_id
GROUP BY r.driver_id, ra.circuit_id
HAVING COUNT(*) >= 3;

-- Qualifying gap to pole
CREATE OR REPLACE VIEW quali_gap_to_pole AS
WITH q_times AS (
    SELECT
        q.race_id,
        q.year,
        q.driver_id,
        q.q3,
        -- Parse mm:ss.sss to milliseconds
        CASE WHEN q.q3 IS NOT NULL AND q.q3 != '' AND q.q3 LIKE '%:%'
            THEN (TRY_CAST(SPLIT_PART(q.q3, ':', 1) AS INTEGER) * 60000 +
                  TRY_CAST(REPLACE(SPLIT_PART(q.q3, ':', 2), '.', '') AS INTEGER))
            ELSE NULL END AS q3_ms
    FROM qualifying q
),
pole_times AS (
    SELECT
        race_id,
        MIN(q3_ms) AS pole_time_ms
    FROM q_times
    WHERE q3_ms IS NOT NULL
    GROUP BY race_id
)
SELECT
    qt.race_id,
    qt.year,
    qt.driver_id,
    qt.q3_ms,
    pt.pole_time_ms,
    (qt.q3_ms - pt.pole_time_ms) * 100.0 / pt.pole_time_ms AS quali_gap_to_pole_pct
FROM q_times qt
JOIN pole_times pt ON qt.race_id = pt.race_id
WHERE qt.q3_ms IS NOT NULL;


-- ──────────────────────────────────────────────
-- MODULE D: Constructor Efficiency
-- ──────────────────────────────────────────────

-- Expected points from grid position (historical lookup)
CREATE OR REPLACE VIEW grid_to_points_lookup AS
SELECT
    grid,
    AVG(points) AS expected_points,
    COUNT(*) AS sample_size
FROM results
WHERE grid > 0
GROUP BY grid;

-- Constructor efficiency per season
CREATE OR REPLACE VIEW constructor_efficiency AS
SELECT
    r.year,
    r.constructor_id,
    SUM(r.points) AS actual_points,
    SUM(gp.expected_points) AS expected_points,
    SUM(r.points) - SUM(gp.expected_points) AS points_vs_expectation,
    CASE WHEN SUM(gp.expected_points) > 0
         THEN SUM(r.points) * 100.0 / SUM(gp.expected_points)
         ELSE NULL END AS efficiency_rating,
    COUNT(*) AS total_entries,
    SUM(CASE WHEN r.position IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS dnf_rate
FROM results r
JOIN grid_to_points_lookup gp ON r.grid = gp.grid
GROUP BY r.year, r.constructor_id;

-- Teammate gap per constructor per season
CREATE OR REPLACE VIEW teammate_gap AS
WITH ranked AS (
    SELECT
        r.race_id, r.year, r.constructor_id, r.driver_id,
        r.grid, r.position, r.points,
        ROW_NUMBER() OVER (PARTITION BY r.race_id, r.constructor_id ORDER BY r.driver_id) AS driver_num
    FROM results r
)
SELECT
    d1.year,
    d1.constructor_id,
    d1.race_id,
    d1.driver_id AS driver_1,
    d2.driver_id AS driver_2,
    ABS(d1.grid - d2.grid) AS quali_gap_positions,
    ABS(COALESCE(d1.position, 25) - COALESCE(d2.position, 25)) AS race_gap_positions
FROM ranked d1
JOIN ranked d2
    ON d1.race_id = d2.race_id
    AND d1.constructor_id = d2.constructor_id
    AND d1.driver_num = 1 AND d2.driver_num = 2;

-- Net positions gained per driver per race (grid - finish)
CREATE OR REPLACE VIEW net_positions AS
SELECT
    race_id, year, driver_id, constructor_id,
    grid,
    position,
    CASE WHEN position IS NOT NULL THEN grid - position ELSE NULL END AS net_positions_gained
FROM results
WHERE grid > 0;
