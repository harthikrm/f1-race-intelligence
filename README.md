# F1 Race Intelligence

**In Formula 1, what actually determines who wins — the driver, the car, or the strategy?**

An end-to-end data analytics platform analyzing **15 seasons of F1 data (2010–2024)** across six analytical modules — combining historical race results from the Ergast API with real 240 Hz car telemetry from FastF1 to answer the sport's most debated question.

> **[Live Dashboard ->](https://f1-race-intelligence.streamlit.app)** · [Harthik Royal Mallichetty](https://github.com/harthikrm) · MSBA Data Science, UT Dallas

---

## Key Results

| Metric | Value |
|--------|-------|
| Seasons analyzed | 15 (2010–2024) |
| Races / Results | 305 races · 6,432 entries |
| Telemetry files | 459 Parquet files · ~200 MB · 240 Hz |
| SQL views engineered | 14 (window functions, REGR_SLOPE, PERCENT_RANK) |
| Driver-circuit affinity scores | 854 pairs |
| XGBoost ROC-AUC | **0.9359** (held-out 2024 test season) |
| Podium Precision@3 | **66.7%** — correctly identifies ~2 of 3 podium finishers per race |
| Constructor efficiency range | 8.3% (Sauber) → 131.5% (Ferrari) in 2024 |

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| **Data Collection** | FastF1 3.3 (telemetry), Jolpica/Ergast REST API (historical) |
| **Storage** | DuckDB 0.10 (25 tables/views), Apache Parquet, JSON |
| **Analysis** | pandas, NumPy, SciPy, statsmodels |
| **ML & Explainability** | XGBoost 2.0, SHAP TreeExplainer, scikit-learn |
| **Visualization** | Plotly (interactive), Matplotlib (static/export) |
| **Dashboard** | Streamlit — 6-tab interactive app, F1 dark theme |
| **Language** | Python 3.11+, SQL (DuckDB dialect) |

---

## Analytical Modules

| Module | Question | Method | Output |
|--------|----------|--------|--------|
| **0 · Data Pipeline** | — | Paginated Ergast ingestion + FastF1 session caching → DuckDB schema | 6,432 results, 23,217 lap records, 459 telemetry files |
| **A · Driver Battle** | Where on track is Driver A faster than Driver B? | Speed trace interpolation at 10m intervals, sector delta ranking, braking point detection | Speed overlay, lap delta chart, sector heatmap, GPS corner maps |
| **B · Pit Strategy IQ** | Who executes the best pit strategy? | Stint reconstruction via SQL, `REGR_SLOPE` tyre degradation, undercut/overcut detection | Strategy IQ scorecard (0–100), stint timelines, position delta per stop |
| **C · Circuit DNA** | Which drivers are circuit specialists? | Composite affinity score: 40% finish percentile + 30% quali gap + 20% positions gained + 10% reliability | 854 driver-circuit affinity scores, affinity heatmap |
| **D · Constructor Efficiency** | Which team extracts the most from their car? | Expected points from historical grid-to-points lookup vs actual scored | Efficiency ratings (8.3%–131.5%), teammate qualifying gap analysis |
| **E · Podium Predictor** | Who should win the next race? | XGBoost binary classifier, chronological train/test split, SHAP feature attribution | Podium probability bars, per-prediction SHAP waterfall plots |
| **F · Lap Visualizer** | Show the race as a speed map | GPS X/Y telemetry colored by speed, throttle, and brake engagement | Circuit speed maps, brake zone visualization, driver racing line overlay |

---

## Key Findings

**Strategy matters more than people think.** McLaren averaged the fastest pit stops in 2024 at 23.4 seconds — Red Bull's pit execution ranks first by duration. Ferrari's Strategy IQ sits at 61/100 despite having arguably the fastest car at several circuits.

**Circuit affinity outweighs raw pace.** The XGBoost model's top predictive feature (by SHAP value) is circuit affinity score — not grid position. Verstappen's affinity score at Jeddah is 93.3/100. Hamilton has 8 wins and 13 podiums at Silverstone — the highest affinity of any driver-circuit pair in the dataset.

**Efficiency separates teams as much as car pace.** Ferrari 2024 scores 131.5% of their expected points based on qualifying positions — outperforming their grid slots by nearly a third. Sauber converts just 8.3%. That 123-percentage-point gap between best and worst is entirely down to strategy, reliability, and execution — not the car.

**The model works.** Trained on 5,127 race entries (2010–2022), validated on 2023, tested on 2024. ROC-AUC 0.9359 on the held-out test season. Precision@3 of 66.7% — meaning for any given race, the top 3 drivers by predicted probability include ~2 actual podium finishers. Early stopping triggered at tree 37 of 500.

---

## Data Engineering Notes

Three non-obvious problems solved in this project:

**Driver ID normalization.** FastF1 uses 3-letter codes (`VER`, `NOR`). Ergast uses slug IDs (`max_verstappen`, `norris`). Built a bidirectional mapping table to join telemetry data with historical race results across all 15 seasons.

**Pit stop duration reconstruction.** Ergast pit stop data does not include duration reliably. Reconstructed from FastF1's `PitInTime` and `PitOutTime` — which live on *different laps* (pit-in on the in-lap, pit-out on the out-lap). Required a LAG/LEAD window function join across adjacent lap rows.

**Cumulative distance splitting.** FastF1's `Distance` column is cumulative across the full race, not per lap. Extracting a single clean lap required splitting by circuit length and resetting per lap — critical for speed trace and GPS map generation.

---

## SQL Feature Engineering Highlights

14 views in `sql/02_feature_engineering.sql` using:

```sql
-- Tyre degradation rate per stint (ms/lap)
REGR_SLOPE(lap_time_ms, lap_in_stint) OVER (PARTITION BY driver_id, race_id, stint_number)

-- Driver finish percentile at each circuit (affinity component)
PERCENT_RANK() OVER (PARTITION BY circuit_id ORDER BY avg_finish_position ASC)

-- Position 3 laps before vs 3 laps after each pit stop
AVG(position) FILTER (WHERE lap BETWEEN pit_lap - 3 AND pit_lap - 1)  -- before
AVG(position) FILTER (WHERE lap BETWEEN pit_lap + 1 AND pit_lap + 3)  -- after

-- Qualifying time parsing from VARCHAR mm:ss.sss → milliseconds
SPLIT_PART(q3, ':', 1)::INTEGER * 60000
+ SPLIT_PART(SPLIT_PART(q3, ':', 2), '.', 1)::INTEGER * 1000
+ TRY_CAST(SPLIT_PART(q3, '.', 2) AS INTEGER)
```

---

## Project Structure

```
f1-race-intelligence/
├── data/
│   ├── raw/                  <- JSON (Ergast) + Parquet (FastF1 telemetry)
│   └── processed/            <- f1.db (DuckDB, 4.0 MB, 25 tables/views)
├── notebooks/
│   ├── 00_data_pipeline.ipynb
│   ├── 01_driver_comparison.ipynb
│   ├── 02_pit_strategy.ipynb
│   ├── 03_circuit_dna.ipynb
│   ├── 04_constructor_efficiency.ipynb
│   ├── 05_predictive_model.ipynb
│   └── 06_lap_visualizer.ipynb
├── sql/
│   ├── 01_schema.sql
│   ├── 02_feature_engineering.sql  <- 14 views, 256 lines
│   └── 03_analysis_queries.sql
├── src/
│   ├── ingest.py             <- Ergast pagination + FastF1 session caching
│   ├── build_db.py           <- DuckDB schema builder
│   ├── features.py           <- Feature engineering functions
│   ├── models.py             <- XGBoost training + SHAP attribution
│   └── viz.py                <- Plotly chart functions
├── app/
│   └── streamlit_app.py      <- 6-tab dashboard, F1 dark theme (#0D0D0D / #E8002D)
├── requirements.txt
└── README.md
```

---

## How to Run

```bash
# Clone and install
git clone https://github.com/harthikrm/f1-race-intelligence.git
cd f1-race-intelligence
pip install -r requirements.txt

# Fetch historical data — Ergast API (~10 min)
python src/ingest.py --ergast --start 2010 --end 2024

# Fetch telemetry — FastF1 (~30 min, cached after first run)
python src/ingest.py --telemetry --telemetry-year 2024

# Build DuckDB database and feature views
python src/build_db.py

# Launch dashboard
streamlit run app/streamlit_app.py
```

---

## Data Sources

- **[Jolpica F1 API](https://github.com/jolpica/jolpica-f1)** — Drop-in replacement for the deprecated Ergast API. Race results, qualifying, pit stops, driver and constructor standings 2010–2024.
- **[FastF1](https://docs.fastf1.dev/)** — Official F1 telemetry Python library. Car speed, throttle, brake, gear, GPS coordinates (X/Y/Z), sector times, tyre compound per lap per driver at 240 Hz.

---

## Author

**Harthik Royal Mallichetty** · MSBA Data Science, UT Dallas  
[github.com/harthikrm](https://github.com/harthikrm) · harthikmallichetty@gmail.com · [linkedin.com/in/harthikrm](https://linkedin.com/in/harthikrm)
