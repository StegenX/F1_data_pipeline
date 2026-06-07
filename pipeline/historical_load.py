import fastf1
import config
import logging
import time
import json

from extract import extract_laps, extract_race_event, extract_race_session, extract_results
from transform import transform_laps, transform_results
from validate import validate_results, validate_laps, generate_validation_report
from load import load_results, load_laps, create_connection, create_schemas

logging.basicConfig(
    filename=config.PIPELINE_LOGS_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def get_season_rounds(year) -> list:
    try:
        logger.info(f"Extracting {year} race rounds...")

        event_schedule = fastf1.get_event_schedule(year)
        event_schedule = event_schedule[event_schedule['EventFormat'] != 'testing']



        logger.info(f"Extracting {year} race rounds completed successfully...")

        return event_schedule['RoundNumber'].tolist()
    except Exception as e:
        logger.error(f"Error while extracting {year} race rounds: {e}")
        raise

def load_single_race(client ,year, round_number):
    try:
        logger.info(f"Extraction {year} race started...")

        session = extract_race_session(year, round_number, config.SESSION)
        results_df = extract_results(session, year, round_number, config.SESSION)
        laps_df = extract_laps(session, year, round_number, config.SESSION)
        event_info = extract_race_event(year, round_number)

        logger.info(f"Extraction {year} race completed successfully...")



        logger.info(f"Transformation {year} race started ...")

        results_df = transform_results(results_df, year, round_number, config.SESSION)
        laps_df = transform_laps(laps_df, year, round_number, config.SESSION)

        logger.info(f"Transformation {year} race completed successfully...")



        logger.info(f"validation {year} race started ...")

        res_validation_obj = validate_results(results_df, year, round_number)
        laps_validation_obj = validate_laps(laps_df, year, round_number)
        generate_validation_report(res_validation_obj, laps_validation_obj)

        logger.info(f"validation {year} race completed successfully ...")



        logger.info(f"Loading {year} race started...")

        
        load_results(client, results_df, event_info, year, round_number)
        load_laps(client, laps_df, results_df, event_info, year, round_number)

        logger.info(f"Loading {year} race completed successfully...")
    except Exception as e:
        logger.error(f"Historical single load in {year} race failed: {e}")
        raise

def load_historical(start_year, end_year):
    try:
        logger.info(f"Loading historical data into Bigquery started...")
        successful = []
        failed = []

        client = create_connection()
        create_schemas(client)

        for year in range(start_year, end_year + 1):
        
            try:
                rounds = get_season_rounds(year)
                logger.info(f"📅 Found {len(rounds)} rounds for the {year} season.")
            except Exception as e:
                logger.error(f"❌ Failed to fetch schedule for {year}. Skipping year. Error: {e}")
                continue

            for round_number in rounds:
                race_tag = f"{year} Round {round_number}"
                
                try:
                    logger.info(f"⏳ Processing {race_tag}...")
                    
                    load_single_race(client, year, round_number)
                    
                    successful.append(race_tag)
                    logger.info(f"✅ Successfully loaded {race_tag}")
                    
                    time.sleep(30) 
                    
                except Exception as e:
                    error_msg = f"{race_tag}, Reason: {e}"
                    failed.append(error_msg)
                    logger.error(f"❌ Failed {error_msg}")
                    if "500 calls/h" in str(e) or "rate" in str(e).lower():
                        logger.warning("Rate limit hit — waiting 10 minutes before continuing...")
                        time.sleep(600)
        
        with open(f"{config.FAILED_RACES_PATH}/failed_races.json", "w") as f:
            json.dump(failed, f, indent=2)
        with open(f"{config.SUCCESSFUL_RACES_PATH}/sucessful_races.json", "w") as f:
            json.dump(successful, f, indent=2)
        
        logger.info(f"Historical data loading completed, {len(successful)} succeded, {len(failed)} failed, Check data/failed_races/failed_races.json")

    except Exception as e:
        logger.error(f"Error While loading historical data: {e}")
        raise

if __name__ == "__main__":
    try:
        print("Historical data loading started...")
        load_historical(2018, 2025)
        print("Historical data loading completed...")
    except Exception as e:
        print("Historical data loading failed, check logs")