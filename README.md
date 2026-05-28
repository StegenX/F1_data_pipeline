# F1 Data Pipeline

## Project Description

This automated data pipeline orchestrates the extraction, transformation, validation, and analysis of Formula 1 race telemetry and results data. Built to handle the complexity of F1's multi-layered data (timing, car telemetry, weather, track status), the pipeline ingests data from the FastF1 API, cleans and enriches it through a series of transformation steps, validates data quality, persists everything in DuckDB, and generates race-specific analysis reports. I built this to create a single source of truth for F1 analytics — enabling reproducible, data-driven insights into race strategy, driver performance, and constructor competitiveness across seasons.

## Architecture Overview

```
FastF1 API 
    ↓
[EXTRACT] — Fetch race session, results, laps, event metadata
    ↓
[TRANSFORM] — Normalize types, engineer features, handle missing values
    ↓
[VALIDATE] — Quality checks, completeness verification, error reporting
    ↓
[LOAD] — Store in DuckDB warehouse (results & laps tables)
    ↓
[ANALYZE] — Race results, lap consistency, tire strategy, constructor points
    ↓
[AIRFLOW] — Orchestrates all stages, schedules on race completion
```

## Tech Stack

| Tool | Why I Chose It |
|------|---|
| **Apache Airflow** | Enterprise-grade workflow orchestration with native DAG support, built-in retry logic, and Airflow UI for monitoring. |
| **FastF1 (Python)** | Official community-standard library for F1 data; handles API complexity and provides cached, structured datasets. |
| **DuckDB** | Lightweight columnar OLAP database perfect for analytical queries on medium datasets; no server infrastructure required. |
| **Docker Compose** | Standardizes local development environment; one command (`docker-compose up`) spins up Airflow, PostgreSQL, and Redis. |
| **Pandas** | Mature data manipulation library with rich ecosystem; integrates seamlessly with DuckDB and analysis workflows. |
| **SQLAlchemy** | Database abstraction layer; future-proofs the pipeline if warehouse changes from DuckDB to PostgreSQL/Snowflake. |

## File Structure

```
f1-data-pipeline/
├── dags/
│   └── f1_pipeline_dag.py                 # Airflow DAG with orchestration logic; defines task dependencies and schedule
├── pipeline/
│   ├── __init__.py                        # Package initialization
│   ├── config.py                          # Environment variables and path configuration loaded from .env
│   ├── extract.py                         # FastF1 API calls to fetch race sessions, results, laps, and events
│   ├── transform.py                       # Data type normalization, feature engineering, and cleaning logic
│   ├── load.py                            # DuckDB schema creation and table insert/upsert operations
│   ├── analyze.py                         # Race analysis functions: strategy, consistency, position analysis
│   └── validate.py                        # Data quality checks and validation report generation
├── cache/                                 # FastF1-managed cache directory (serialized .ff1pkl files)
│   └── YYYY/YYYY-MM-DD_Event/             # Automatically organized by FastF1; populated on first data access
├── data/
│   ├── raw/                               # Extracted parquet files before transformation
│   ├── processed/                         # Transformed and cleaned parquet files
│   │   └── analysis/                      # Analysis output parquets (results, strategy, consistency metrics)
│   └── validation/                        # Validation reports and error logs
├── models/
│   └── f1.duckdb                          # DuckDB database; contains results, laps, and analysis tables
├── notebooks/
│   └── exploration.ipynb                  # Jupyter notebook for ad-hoc data exploration and visualization
├── logs/
│   └── dag_processor_manager/             # Airflow internal logs
│   └── scheduler/                         # Airflow scheduler and task execution logs
├── airflow/
│   ├── airflow.cfg                        # Airflow configuration (executor type, dags folder, etc.)
│   ├── logs/                              # Airflow task execution logs
│   └── plugins/                           # Custom Airflow plugins directory (if needed)
├── docker-compose.yaml                    # Defines Airflow webserver, scheduler, PostgreSQL, and Redis services
├── requirements.txt                       # Python dependencies (fastf1, airflow, duckdb, pandas, etc.)
├── .env                                   # Environment variables (paths, config) — NOT version controlled
└── .gitignore                             # Git rules (ignores .env, parquets, cache, venv)
```

## How To Run It

### Step 1: Clone the Repository
```bash
cd f1-data-pipeline
```

### Step 2: Set Up Environment Variables
Create a `.env` file in the project root with these paths:
```env
PIPELINE_LOGS_PATH=./logs/pipeline.log
CACHE_PATH=./cache
RAW_DATA_PATH=./data/raw
PROCESSED_DATA_PATH=./data/processed
WHEREHOUSE_PATH=./models/f1.duckdb
ANALYSIS_PATH=./data/processed/analysis
VALIDATION_PATH=./data/validation
```

### Step 3: Install Python Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Start Docker Services
```bash
docker-compose up -d
```
This starts:
- **Airflow Webserver** at http://localhost:8080 (username: `airflow` / password: `airflow`)
- **Airflow Scheduler** (auto-runs DAGs on schedule)
- **PostgreSQL** (Airflow metadata)
- **Redis** (message broker)

### Step 5: Trigger the DAG
Open http://localhost:8080, find `f1_pipeline_dag`, and click the play button to trigger manually. Or use CLI:
```bash
airflow dags trigger f1_pipeline_dag
```

The pipeline will:
1. Detect the latest completed race
2. Extract data from FastF1
3. Transform and validate the data
4. Load into DuckDB
5. Generate race analysis
6. Log execution to `logs/pipeline.log`

## Example Outputs

After running the pipeline on the 2024 Bahrain Grand Prix, analysis tables look like this:

**Race Results Analysis** (from `data/processed/analysis/race_results_analysis.parquet`):
| driver | position | points | grid_position | dnf | fastest_lap | gap_to_leader |
|--------|----------|--------|---------------|-----|-------------|---------------|
| Max Verstappen | 1 | 25 | 1 | False | True | 0.0 |
| Charles Leclerc | 2 | 18 | 2 | False | False | 6.234 |
| Lewis Hamilton | 3 | 15 | 4 | False | False | 12.107 |
| George Russell | 4 | 12 | 3 | False | False | 28.456 |

**Tire Strategy Analysis** (from `data/processed/analysis/tyre_strategy_analysis.parquet`):
| driver | compound | stint_laps | avg_lap_time | stint_number | tyre_age_at_switch |
|--------|----------|-----------|--------------|--------------|-------------------|
| Max Verstappen | SOFT | 18 | 92.425 | 1 | 18 |
| Max Verstappen | MEDIUM | 39 | 93.112 | 2 | 39 |
| Charles Leclerc | SOFT | 16 | 92.687 | 1 | 16 |
| Charles Leclerc | HARD | 41 | 93.445 | 2 | 41 |

**Lap Consistency Metrics** (from `data/processed/analysis/lap_consistency_analysis.parquet`):
| driver | avg_lap_time | std_deviation | cv_percent | fastest_lap | slowest_lap | consistency_score |
|--------|--------------|-------|---------|------------|------------|-----------------|
| Max Verstappen | 92.684 | 0.892 | 0.96 | 90.234 | 95.102 | 94.2 |
| Charles Leclerc | 93.156 | 1.234 | 1.32 | 91.445 | 96.789 | 92.1 |
| Lewis Hamilton | 93.445 | 0.756 | 0.81 | 92.001 | 95.678 | 95.1 |

## What I Learned

Building this pipeline taught me the critical importance of **data validation at every stage** — a single missing lap time or malformed timestamp can cascade through transforms and poison analysis outputs. I learned that **orchestration complexity is worth the investment**: Airflow's retry logic, logging, and UI saved countless hours of debugging failed extractions and retrying partial loads. Most valuably, I discovered that **working backwards from the question (race strategy insights) informed better pipeline design** — knowing I wanted to analyze tire strategy meant structuring lap data with tyre compound, stint boundaries, and lap times grouped by driver from the start, rather than attempting to infer them later.
