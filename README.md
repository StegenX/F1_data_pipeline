# F1 Data Pipeline

## Project Title and Description

This project automates the ingestion and analysis of Formula 1 race data using FastF1, Apache Airflow, Pandas, BigQuery, and Great Expectations. I built it to turn raw race sessions into a repeatable pipeline that extracts race results and lap data, transforms them into analysis-ready tables, validates data quality, stores the results in BigQuery, and publishes race insights such as consistency, tire strategy, position changes, and constructor points.

## Architecture Overview

```
FastF1 API → extract → transform → validate → load (BigQuery) → analyze
```

## Tech Stack

| Tool | Why I Chose It |
|------|---|
| Apache Airflow | It orchestrates the pipeline as a DAG, handles retries, and gives a clear UI for monitoring each stage. |
| FastF1 | It is the data source for live and historical F1 session data, laps, results, and event metadata. |
| Pandas | It is used throughout the pipeline for dataframe cleanup, feature engineering, and parquet handling. |
| BigQuery | It is the cloud data warehouse that stores fact and dimension tables for race results, laps, drivers, constructors, and circuits. |
| Great Expectations | It validates the processed data before loading so bad rows do not reach the warehouse or analysis steps. |
| Docker Compose | It makes the Airflow stack reproducible with one command for local development. |
| Python dotenv | It loads environment variables from `.env` so paths and credentials stay out of the code. |

## File Structure

```
f1-data-pipeline/
├── dags/
│   └── f1_pipeline_dag.py                 # Main Airflow DAG that runs extract, transform, validate, load, and analyze tasks
|
module
│   ├── analyze.py                         # Analysis functions that query BigQuery and produce race results, lap consistency, tyre strategy outputs
│   ├── config.py                          # Environment-driven paths and pipeline settings loaded from .env
│   ├── extract.py                         # FastF1 session, result, lap, and event extraction helpers
│   ├── historical_load.py                 # Legacy historical backfill script for loading multiple seasons/races
│   ├── load.py                            # BigQuery connection, schema creation, and table loading logic
│   ├── transform.py                       # Data cleaning and feature engineering for results and laps
│   └── validate.py                        # Great Expectations validation rules and report generation
├── notebooks/
│   └── exploration.ipynb                  # Notebook for exploring race data and trying SQL / dataframe ideas
├── cache/                                 # FastF1 cache directory with serialized session data
├── data/                                  # Project data directory
│   ├── processed/                         # Cleaned parquet outputs used by analysis
│   │   └── analysis/                      # Analysis parquet files produced by analysis queries
│   ├── raw/                               # Raw parquet outputs from extraction
│   └── validation/                        # Validation reports written by Great Expectations
|
├── airflow/                               # Local Airflow config and logs folder
├── config/                                # Reserved for configuration files used by the containerized setup
├── logs/                                  # Pipeline and Airflow logs
├── docker-compose.yaml                    # Airflow, Postgres, and Redis service definition
├── requirements.txt                       # Python dependencies for the pipeline and Airflow
├── .gitignore                             # Files and folders excluded from version control
└── README.md                              # Project overview and usage guide
```

## How To Run It

1. Clone the repository and move into the project directory.

```bash
cd /home/username/Desktop/python/f1-data-pipeline
```

2. Create a `.env` file in the project root.

```env
PIPELINE_LOGS_PATH=./logs/pipeline.log
CACHE_PATH=./cache
RAW_DATA_PATH=./data/raw
PROCESSED_DATA_PATH=./data/processed
ANALYSIS_PATH=./data/processed/analysis
VALIDATION_PATH=./data/validation
GOOGLE_APPLICATION_CREDENTIALS=<path/to/bigquery/credentials.json>
BIGQUERY_PROJECT=<your-gcp-project-id>
BIGQUERY_DATASET=<your-bigquery-dataset>
FAILED_RACES_PATH=
SUCCESSFUL_RACES_PATH=
```

3. Install the Python requirements.

```bash
pip install -r requirements.txt
```

4. Start the Docker stack.

```bash
docker-compose up -d
```

This launches Airflow, PostgreSQL, and Redis. The Airflow UI should be available at `http://localhost:8080` with the default credentials `airflow / airflow`.

5. Trigger the DAG.

The active DAG is `f1_pipeline`.

```bash
airflow dags trigger f1_pipeline
```

You can also start it from the Airflow UI, then watch the task graph for `run_extract`, `run_transform`, `run_validate`, `run_load`, and `run_analysis`.

## Example Outputs

After running the pipeline, BigQuery tables are created and populated with race data. Analysis queries export results to `data/processed/analysis/*.parquet`.

Sample rows from `data/processed/analysis/race_results_2024_1.parquet`:

| driver_name | team | position | grid_position | gap_to_winner_seconds | status |
|---|---|---:|---:|---:|---|
| Carlos Sainz | Ferrari | 3 | 4 | -5479.63 | Finished |
| Charles Leclerc | Ferrari | 4 | 2 | -5465.07 | Finished |
| Lewis Hamilton | Mercedes | 7 | 9 | -5454.42 | Finished |
| Oscar Piastri | McLaren | 8 | 8 | -5448.66 | Finished |

## What I Learned

Building this pipeline showed me that data validation has to happen early, not after the warehouse load, because small issues in lap timing or result fields can break every downstream analysis. It also made the value of orchestration obvious: Airflow turned a set of scripts into a repeatable process with retries, logs, and visible task boundaries. The biggest design lesson was to work backward from the analysis questions first, then shape the extracted and transformed data around those questions instead of trying to retrofit it later.
