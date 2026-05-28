import pandas as pd
import duckdb
import logging
import config
import fastf1
from extract import extract_race_event

logging.basicConfig(
    filename=config.PIPELINE_LOGS_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    try:
        logger.info("Creating connection with duckdb")
        conn = duckdb.connect(database=db_path, read_only=False)
        logger.info("Connection with duckdb succeded")

        return conn
    except Exception as e:
        logger.error(f'Error while trying to connect to duckdb: {e}')
        raise

def get_event_df(event: fastf1.events.Event) -> pd.DataFrame:
    try:
        logger.info('Extracting data from event object...')
        event_df = pd.DataFrame([{"circuit_id": event["EventName"],
                                "circuit_name": event["EventName"],
                                "country": event["Country"],
                                "location": event["Location"]}])
        event_df["circuit_id"] = event_df["circuit_id"].str.lower().str.replace(' ', '_')
        logger.info('Data extracted successfully...')
        return event_df
    except Exception as e:
        logger.error(f'Error while extracting data from event object: {e}')
        raise

def create_schema(conn: duckdb.DuckDBPyConnection):
    try:
        creating_tables_query = """ CREATE TABLE IF NOT EXISTS dim_drivers (driver_id VARCHAR PRIMARY KEY,
                                    abbreviation VARCHAR, first_name VARCHAR, last_name VARCHAR, full_name VARCHAR,
                                    country_code VARCHAR, driver_number int);
                                    
                                    CREATE TABLE IF NOT EXISTS dim_constructors (team_id VARCHAR PRIMARY KEY,
                                    team_name VARCHAR);

                                    CREATE TABLE IF NOT EXISTS dim_circuits (circuit_id VARCHAR PRIMARY KEY,
                                    circuit_name VARCHAR, country VARCHAR, location VARCHAR);

                                    CREATE TABLE IF NOT EXISTS fact_race_results(result_id VARCHAR PRIMARY KEY,
                                    driver_id VARCHAR REFERENCES dim_drivers(driver_id), team_id VARCHAR REFERENCES dim_constructors(team_id),
                                    circuit_id VARCHAR REFERENCES dim_circuits(circuit_id), year INT, round_number INT, position INT,
                                    grid_position INT, points INT, laps INT, race_time_seconds DOUBLE, gap_to_winner_seconds DOUBLE,
                                    status VARCHAR, ClassifiedPosition VARCHAR, is_classified BOOLEAN, session_type VARCHAR);

                                    CREATE TABLE IF NOT EXISTS fact_lap_times (lap_id VARCHAR PRIMARY KEY,
                                    driver_id VARCHAR REFERENCES dim_drivers(driver_id), circuit_id VARCHAR REFERENCES dim_circuits(circuit_id),
                                    year INT, round_number INT, lap_number INT, stint INT, lap_time_seconds DOUBLE,
                                    sector1_seconds DOUBLE, sector2_seconds DOUBLE, sector3_seconds DOUBLE,
                                    speedI1 DOUBLE, speedI2 DOUBLE, speedFl DOUBLE, speedSt DOUBLE,
                                    compound VARCHAR, tyre_life INT, fresh_tyre BOOLEAN, is_pit_lap BOOLEAN,
                                    is_valid_lap BOOLEAN, is_personal_best BOOLEAN, position INT, track_status VARCHAR, session_type VARCHAR)
                                    """
        logger.info("Creating tables schemas...")

        conn.execute(creating_tables_query)

        logger.info("Tables schemas created successfully...")
    except Exception as e:
        logger.error(f'Error while creating schemas: {e}')
        raise

def load_results(conn, results_df, event):
    try:
        logger.info("loading results schema...")
        dim_driver_tb_columns = ['driver_id', 'abbreviation' , 'first_name' , 'last_name' , 'full_name' , 'country_code' , 'driver_number']
        dim_constructors = ['team_id', 'team_name']
        dim_circuits_tb_columns = ['circuit_id', 'circuit_name', 'country', 'location']
        fact_race_results_tb_columns = ['result_id', 'driver_id', 'team_id', 'circuit_id', 'year', 'round_number', 'position', 'grid_position', 'points', 'laps', 'race_time_seconds', 'gap_to_winner_seconds', 'status', 'ClassifiedPosition', 'is_classified', 'session_type']

        dim_drivers_columns = ['DriverId', 'Abbreviation', 'FirstName', 'LastName', 'FullName', 'CountryCode', 'DriverNumber']
        dim_constructors_columns = ['TeamId', 'TeamName']
        dim_circuits_columns = ['circuit_id', 'circuit_name', 'country', 'location']
        fact_results_columns = ['result_id', 'DriverId', 'TeamId', 'circuit_id', 'year', 'round_number', 'Position', 'GridPosition', 'Points',
                                'Laps', 'race_time_seconds', 'gap_to_winner_seconds', 'Status', 'ClassifiedPosition', 'is_classified',
                                'session_type']

        

        event_df = get_event_df(event)
        merged_fact_table = results_df.merge(event_df, how='cross')


        conn.register("event_df", event_df)
        conn.register("results_df", results_df)
        conn.register("merged_fact_table", merged_fact_table)
        conn.execute(f"""INSERT INTO dim_drivers ({', '.join(dim_driver_tb_columns)}) SELECT {', '.join(dim_drivers_columns)} FROM results_df ON CONFLICT DO NOTHING;""")
        conn.execute(f"""INSERT INTO dim_constructors ({', '.join(dim_constructors)}) SELECT {', '.join(dim_constructors_columns)} FROM results_df ON CONFLICT DO NOTHING;""")
        conn.execute(f"""INSERT INTO dim_circuits ({', '.join(dim_circuits_tb_columns)}) SELECT {', '.join(dim_circuits_columns)} FROM event_df ON CONFLICT DO NOTHING;""")
        conn.execute(f""" INSERT INTO fact_race_results ({', '.join(fact_race_results_tb_columns)}) SELECT {', '.join(fact_results_columns)} FROM merged_fact_table ON CONFLICT DO NOTHING;""")

        logger.info("Results schema loaded successfully...")
    except Exception as e:
        logger.error(f'Error while loading results schema: {e}')
        raise

def load_laps(conn, laps_df, results_df, event):
    try:
        logger.info("loading laps schema...")

        laps_columns = ['lap_id', 'DriverId', 'circuit_id', 'year','round_number', 'LapNumber', 'Stint', 'LapTime_seconds',
                        'Sector1Time_seconds', 'Sector2Time_seconds', 'Sector3Time_seconds',
                        'SpeedI1', 'SpeedI2', 'SpeedFL', 'SpeedST', 'Compound', 'TyreLife',
                        'FreshTyre', 'is_pit_lap','is_valid_lap', 'IsPersonalBest', 'Position', 'TrackStatus', 'session_type']
        fact_lap_times_tb_columns = ['lap_id', 'driver_id', 'circuit_id', 'year', 'round_number', 'lap_number', 'stint', 'lap_time_seconds', 'sector1_seconds', 'sector2_seconds', 'sector3_seconds', 'speedI1', 'speedI2', 'speedFl', 'speedSt', 'compound', 'tyre_life', 'fresh_tyre', 'is_pit_lap', 'is_valid_lap', 'is_personal_best', 'position', 'track_status', 'session_type']
    
        
        event_df = get_event_df(event)
        circuit_id = event_df["circuit_id"].iloc[0]
        laps_df["circuit_id"] = circuit_id
        laps_df = laps_df.merge(results_df[["DriverNumber", "DriverId"]], how='left')
        
        conn.register("laps_df", laps_df)
        conn.execute(f""" INSERT INTO fact_lap_times ({', '.join(fact_lap_times_tb_columns)}) SELECT {', '.join(laps_columns)} FROM laps_df ON CONFLICT DO NOTHING""")

        logger.info("Laps schema loaded successfully...")
    except Exception as e:
        logger.error(f"Error while loading laps: {e}")
        raise


if __name__ == "__main__":
    try:
        print("Load phase started...")
        logger.info("Load phase started...")

        results_df = pd.read_parquet(f"{config.PROCESSED_DATA_PATH}/results_{config.YEAR}_{config.SESSION}{config.ROUND}.parquet")
        laps_df = pd.read_parquet(f"{config.PROCESSED_DATA_PATH}/laps_{config.YEAR}_{config.SESSION}{config.ROUND}.parquet")
        event = extract_race_event(year=config.YEAR, round_number=config.ROUND)

        conn = create_connection(config.WHEREHOUSE_PATH)

        create_schema(conn)


        load_results(conn, results_df, event)
        load_laps(conn, laps_df, results_df, event)

        print("Loading phase completed successfully...")
        logger.info("Loading phase completed successfully...")
    except Exception as e:
        print("Load phase failed, check logs...")
        logger.error(f'Load phase failed: {e}')
    finally:
        if 'conn' in locals():
            conn.close()
