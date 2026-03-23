-- ============================================
-- F1 Race Intelligence — Analysis Queries
-- Ready-to-run queries for insights and dashboards
-- ============================================

-- ──────────────────────────────────────────────
-- Overview Queries
-- ──────────────────────────────────────────────

-- Championship standings for a given year
-- SELECT d.full_name, ds.points, ds.wins, ds.position
-- FROM driver_standings ds
-- JOIN drivers d ON ds.driver_id = d.driver_id
-- WHERE ds.year = 2024 AND ds.round = (SELECT MAX(round) FROM driver_standings WHERE year = 2024)
-- ORDER BY ds.position;

-- Race winners by season
-- SELECT ra.race_name, d.full_name, r.constructor_id, r.grid, r.points
-- FROM results r
-- JOIN races ra ON r.race_id = ra.race_id
-- JOIN drivers d ON r.driver_id = d.driver_id
-- WHERE r.position = 1 AND r.year = 2024
-- ORDER BY ra.round;


-- ──────────────────────────────────────────────
-- Module A: Driver Battle Queries
-- ──────────────────────────────────────────────

-- Head-to-head record between two drivers
-- SELECT
--     d.full_name,
--     COUNT(*) AS races,
--     SUM(CASE WHEN r.position <= 3 THEN 1 ELSE 0 END) AS podiums,
--     AVG(r.position) FILTER (WHERE r.position IS NOT NULL) AS avg_finish,
--     AVG(r.grid) AS avg_grid
-- FROM results r
-- JOIN drivers d ON r.driver_id = d.driver_id
-- WHERE r.driver_id IN ('max_verstappen', 'norris')
--   AND r.year = 2024
-- GROUP BY d.full_name;


-- ──────────────────────────────────────────────
-- Module B: Pit Strategy Queries
-- ──────────────────────────────────────────────

-- Top pit stop teams by average duration
-- SELECT
--     r.constructor_id,
--     COUNT(*) AS total_stops,
--     AVG(ps.pit_duration_ms) / 1000 AS avg_duration_sec,
--     MIN(ps.pit_duration_ms) / 1000 AS fastest_stop_sec
-- FROM pit_stops ps
-- JOIN results r ON ps.race_id = r.race_id AND ps.driver_id = r.driver_id
-- WHERE ps.year = 2024 AND ps.pit_duration_ms > 0
-- GROUP BY r.constructor_id
-- ORDER BY avg_duration_sec;


-- ──────────────────────────────────────────────
-- Module C: Circuit DNA Queries
-- ──────────────────────────────────────────────

-- Driver affinity at specific circuit
-- SELECT
--     dch.driver_id,
--     d.full_name,
--     dch.total_appearances,
--     dch.avg_finish_position,
--     dch.total_wins,
--     dch.total_podiums,
--     dch.positions_gained_avg
-- FROM driver_circuit_history dch
-- JOIN drivers d ON dch.driver_id = d.driver_id
-- WHERE dch.circuit_id = 'silverstone'
-- ORDER BY dch.avg_finish_position;


-- ──────────────────────────────────────────────
-- Module D: Constructor Efficiency Queries
-- ──────────────────────────────────────────────

-- Constructor efficiency ranking
-- SELECT
--     ce.constructor_id,
--     c.constructor_name,
--     ce.actual_points,
--     ROUND(ce.expected_points, 1) AS expected_points,
--     ROUND(ce.points_vs_expectation, 1) AS over_under,
--     ROUND(ce.efficiency_rating, 1) AS efficiency_pct,
--     ROUND(ce.dnf_rate * 100, 1) AS dnf_pct
-- FROM constructor_efficiency ce
-- JOIN constructors c ON ce.constructor_id = c.constructor_id
-- WHERE ce.year = 2024
-- ORDER BY ce.efficiency_rating DESC;
