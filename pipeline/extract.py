import pandas as pd
import logging
import fastf1
import config


fastf1.Cache.enable_cache(config.CACHE_PATH)

logging.basicConfig(
    filename=config.PIPELINE_LOGS_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



def extract_race_session(year: int, round_number: int, session_type: str) -> fastf1.core.Session:
    try:
        logger.info(f'Race session extraction started, year: {year}, round: {round_number}, session type {session_type}')

        session = fastf1.get_session(year, round_number, session_type)
        session.load()

        return session
    except Exception as e:
        logger.error(f'Extraction failed: {e}')
        raise

def extract_results(session: object, year: int, round_number: int, session_type: str) -> pd.DataFrame:
    try:
        logger.info("Results dataframe extraction started...")
        results_df =  pd.DataFrame(session.results)
        results_df.to_parquet(f'{config.RAW_DATA_PATH}/results_{year}_{session_type}{round_number}.parquet', index=False)
        logger.info(f'Results dataframe extracted successfully,{results_df.shape[0]} row/s saved in /raw/results_{year}_{session_type}{round_number}.parquet')

        return results_df
    except Exception as e:
        logger.error(f'Result extraction failed: {e}')
        raise

def extract_laps(session: object, year: int, round_number: int, session_type: str) -> pd.DataFrame:
    try:
        logger.info("laps dataframe extraction started...")
        laps_df =  pd.DataFrame(session.laps)
        laps_df.to_parquet(f'{config.RAW_DATA_PATH}/laps_{year}_{session_type}{round_number}.parquet', index=False)
        logger.info(f'Laps dataframe extracted successfully,{laps_df.shape[0]} row/s saved in /raw/laps_{year}_{session_type}{round_number}.parquet')

        return laps_df
    except Exception as e:
        logger.error(f'Result extraction failed: {e}')
        raise



def extract_race_event(year: int, round_number: int) -> fastf1.events.Event:
    try:
        logger.info("Race event extraction started...")
        event = fastf1.get_event(year, round_number)
        logger.info("Race event extraction completed successfully")
        return event
    except Exception as e:
        logger.error(f'Race event extraction failed: {e}')
        raise

if __name__ == "__main__":
    try:
        print("Extraction started...")
        logger.info("Extraction phase started...")

        session = extract_race_session(config.YEAR, config.ROUND, config.SESSION)
        results = extract_results(session, config.YEAR, config.ROUND, config.SESSION)
        laps = extract_laps(session, config.YEAR, config.ROUND, config.SESSION)
        event_info = extract_race_event(config.YEAR, config.ROUND)
        print("Extraction phase completed successfully...")
        logger.info("Extraction phase completed successfully...")
    except:
        print("Extraction Phase failed, Check logs")
        logger.error("Extraction phase failed, Check logs")


