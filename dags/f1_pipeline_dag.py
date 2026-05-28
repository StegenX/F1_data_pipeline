from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
import datetime
import sys
sys.path.append('/opt/airflow/f1-data-pipeline')
import logging
import pipeline.config as config
import fastf1
import pandas as pd

from pipeline.extract import extract_race_session, extract_results, extract_laps, extract_race_event
from pipeline.transform import get_dataframe, transform_results, transform_laps
from pipeline.load import create_connection, create_schema, load_results, load_laps
from pipeline.analyze import analyze_race_results, analyze_lap_consistency, analyze_tyre_strategy, analyze_position_vs_grid, analyze_constructor_points
from pipeline.validate import validate_laps, validate_results, generate_validation_report



logging.basicConfig(
    filename=config.PIPELINE_LOGS_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



default_args = {
    'owner': 'aymane agharbi',
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5)
}



def get_latest_race_round() -> int:
    try:
        logger.info("Extracting latest race round...")

        today_date = datetime.datetime.now().date()
        event_schedule = fastf1.get_event_schedule(today_date.year)
        event_schedule = event_schedule[event_schedule['EventFormat'] != 'testing']
        first_race = event_schedule['EventDate'].min().date()
        last_race = event_schedule['EventDate'].max().date()

        if today_date < first_race or today_date > last_race:
            logger.warning("Warning: Season hasn't started yet...")
            raise AirflowSkipException("Season hasn't started yet")

        past_events = event_schedule[event_schedule['EventDate'].dt.date < today_date]
        race_date = past_events.iloc[-1]['EventDate']
        event = event_schedule[event_schedule['EventDate'] == race_date]
        round_number = event['RoundNumber'].iloc[0]

        logger.info("Extracting latest race round completed successfully...")

        return round_number
    except Exception as e:
        logger.error(f"Error while extracting race round: {e}")
        raise


    

def run_extract():
    try:
        logger.info("DAG automation started...")
        YEAR = datetime.datetime.now().year
        ROUND = get_latest_race_round()
        logger.info("Extraction phase started...")

        
        session = extract_race_session(YEAR, ROUND, config.SESSION)
        results = extract_results(session, YEAR, ROUND, config.SESSION)
        laps = extract_laps(session, YEAR, ROUND, config.SESSION)
        event_info = extract_race_event(YEAR, ROUND)

        logger.info("Extraction phase completed successfully...")
    except Exception as e:
        logger.error("Extraction phase failed, Check logs")
        raise

def run_transform():
    try:
        logger.info("Transformation phase started ...")

        YEAR = datetime.datetime.now().year
        ROUND = get_latest_race_round()
        results_df = get_dataframe(f'{config.RAW_DATA_PATH}/results_{YEAR}_{config.SESSION}{ROUND}.parquet')
        laps_df = get_dataframe(f'{config.RAW_DATA_PATH}/laps_{YEAR}_{config.SESSION}{ROUND}.parquet')

        transform_results(results_df, YEAR, ROUND, config.SESSION)
        transform_laps(laps_df, YEAR, ROUND, config.SESSION)

        logger.info("Transformation phase completed successfully...")
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

def run_validate():
    try:
        logger.info("Validation phase started...")

        YEAR = datetime.datetime.now().year
        ROUND = get_latest_race_round()
        
        results_df = pd.read_parquet(f'{config.PROCESSED_DATA_PATH}/results_{YEAR}_R{ROUND}.parquet')
        laps_df = pd.read_parquet(f'{config.PROCESSED_DATA_PATH}/laps_{YEAR}_R{ROUND}.parquet')
        res_validation_obj = validate_results(results_df, YEAR, ROUND)
        laps_validation_obj = validate_laps(laps_df, YEAR, ROUND)
        generate_validation_report(res_validation_obj, laps_validation_obj)


        logger.info("Validation phase completed successfully...")
    except Exception as e:
        logger.error(f"Validation phase failed: {e}")
        raise

def run_load():
    try:
        logger.info("Load phase started...")

        YEAR = datetime.datetime.now().year
        ROUND = get_latest_race_round()

        results_df = pd.read_parquet(f"{config.PROCESSED_DATA_PATH}/results_{YEAR}_{config.SESSION}{ROUND}.parquet")
        laps_df = pd.read_parquet(f"{config.PROCESSED_DATA_PATH}/laps_{YEAR}_{config.SESSION}{ROUND}.parquet")
        event = extract_race_event(year=YEAR, round_number=ROUND)

        conn = create_connection(config.WHEREHOUSE_PATH)

        create_schema(conn)


        load_results(conn, results_df, event)
        load_laps(conn, laps_df, results_df, event)

        logger.info("Loading phase completed successfully...")
    except Exception as e:
        logger.error(f'Load phase failed: {e}')
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def run_analysis():
    try:
        logger.info("Analysis Phase started...")

        YEAR = datetime.datetime.now().year
        ROUND = get_latest_race_round()

        conn = create_connection(config.WHEREHOUSE_PATH)
        race_results = analyze_race_results(conn, YEAR, ROUND)
        lap_consistency = analyze_lap_consistency(conn, YEAR, ROUND)
        tyre_strategy = analyze_tyre_strategy(conn, YEAR, ROUND)
        position_vs_grid = analyze_position_vs_grid(conn, YEAR, ROUND)
        constructor_points = analyze_constructor_points(conn, YEAR, ROUND)

        logger.info("Analysis Phase completed successfully...")
        logger.info("DAG automation ended...")
    except Exception as e:
        logger.error(f"Analysis phase failed: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
        

with DAG(
    dag_id='f1_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * 1',
    start_date=datetime.datetime(2026, 5, 1),
    max_active_runs=1,
    catchup=False,
) as dag:
    task_run_extract = PythonOperator(task_id='run_extract', python_callable=run_extract)
    task_run_transform = PythonOperator(task_id='run_transform', python_callable=run_transform)
    task_run_load = PythonOperator(task_id='run_load', python_callable=run_load)
    task_run_analysis = PythonOperator(task_id='run_analysis', python_callable=run_analysis)
    task_run_validate = PythonOperator(task_id='run_validate', python_callable=run_validate)

    task_run_extract >> task_run_transform >> task_run_validate >> task_run_load >> task_run_analysis

