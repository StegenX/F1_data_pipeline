import pandas as pd
import duckdb
import logging
import config

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

def save_df_in_files(df: pd.DataFrame, df_name: str):
    try:
        logger.info("Saving files into analysis folder...")

        df.to_parquet(f"{config.ANALYSIS_PATH}/{df_name}.parquet")

        logger.info("files saved into analysis successfully...")
    except Exception as e:
        logger.error(f"Error while saving analysis files: {e}")
        raise

def analyze_race_results(conn, year, round_number) -> pd.DataFrame:
    try:
        logger.info("Analyzing race results...")

        race_results = conn.execute(f""" SELECT full_name as driver_name, team_name as team, position, grid_position, gap_to_winner_seconds, status 
                            FROM fact_race_results 
                            JOIN dim_drivers USING(driver_id) 
                            JOIN dim_constructors USING(team_id)
                            WHERE year = {year} AND round_number = {round_number}""").df()
        save_df_in_files(race_results, f"race_results_{year}_{round_number}")

        logger.info(f"Analyzing race results completed successfully, {race_results.shape[0]} row/s loaded...")
        return race_results
    except Exception as e:
        logger.error(f"Error while analyzing results: {e}")
        raise

def analyze_lap_consistency(conn, year, round_number) -> pd.DataFrame:

    try:
        logger.info("Analyzing lap consistency...")

        lap_consistency = conn.execute(f""" SELECT full_name as driver_name, ROUND(AVG(lap_time_seconds), 2) as avg_lap_time, 
                                        ROUND(MIN(lap_time_seconds), 2) as min_lap_time, 
                                        ROUND(MAX(lap_time_seconds), 2) as max_lap_time, 
                                        ROUND(STDDEV(lap_time_seconds), 2) as consistency FROM fact_lap_times 
                                        JOIN dim_drivers USING(driver_id)
                                        WHERE is_valid_lap = True AND year = {year} AND round_number = {round_number}
                                        GROUP BY full_name
                                        ORDER BY consistency""").df()
        save_df_in_files(lap_consistency, f"lap_consistency_{year}_{round_number}")
        
        logger.info(f"Analyzing lap consistency completed successfully, {lap_consistency.shape[0]} row/s loaded...")
        return lap_consistency
    except Exception as e:
        logger.error(f"Error while analyzing laps: {e}")
        raise


def analyze_tyre_strategy(conn, year, round_number) -> pd.DataFrame:
    try:
        logger.info("Analyzing tyre strategy...")

        tyre_strategy = conn.execute(f""" SELECT full_name as driver, compound,
                                         COUNT(lap_number) as laps_on_compound, 
                                         ROUND(AVG(lap_time_seconds), 2) as avg_lapTime
                                         FROM fact_lap_times
                                         JOIN dim_drivers USING(driver_id)
                                         WHERE year = {year} AND round_number = {round_number}
                                         GROUP BY full_name, compound""").df()
        save_df_in_files(tyre_strategy, f"tyre_strategy_{year}_{round_number}")

        logger.info(f"Analyzing tyre strategy completed successfully..., {tyre_strategy.shape[0]} row/s loaded")
        return tyre_strategy
    except Exception as e:
        logger.error(f"Error while analyzing tyre strategy: {e}")
        raise

def analyze_position_vs_grid(conn, year, round_number) -> pd.DataFrame:
    try:
        logger.info("Analyzing position_vs_grid...")
        position_vs_grid = conn.execute(f""" SELECT full_name as driver,
                                            (grid_position - position) as position_gained
                                            FROM fact_race_results
                                            JOIN dim_drivers USING(driver_id)
                                            WHERE year = {year} AND round_number = {round_number}
                                            ORDER BY position_gained DESC """).df()
        save_df_in_files(position_vs_grid, f"position_vs_grid_{year}_{round_number}")

        logger.info(f"Analyzing position_vs_grid completed successfully, {position_vs_grid.shape[0]} row/s loaded")
        return position_vs_grid
    except Exception as e:
        logger.error(f"Error while analyzing position_vs_grid: {e}")
        raise

def analyze_constructor_points(conn, year, round_number) -> pd.DataFrame:

    try:
        logger.info("Analyzing constructor points...")

        constructor_points = conn.execute(f""" SELECT team_name, SUM(points) as total_points
                                              FROM fact_race_results
                                              JOIN dim_constructors USING(team_id)
                                              WHERE year = {year} AND round_number = {round_number}
                                              GROUP BY team_name
                                              ORDER BY total_points DESC""").df()
        save_df_in_files(constructor_points, f"constructor_points_{year}_{round_number}")
        
        logger.info(f"Analyzing constructor points completed successfully, {constructor_points.shape[0]} row/s loaded")
        return constructor_points
    except Exception as e:
        logger.error(f"Error while analyzing constructor points: {e}")
        raise

if __name__ == "__main__":
    try:
        print("Analysis Phase started...")
        logger.info("Analysis Phase started...")
        year = config.YEAR
        round_number = config.ROUND

        conn = create_connection(config.WHEREHOUSE_PATH)
        race_results = analyze_race_results(conn, year, round_number)
        lap_consistency = analyze_lap_consistency(conn, year, round_number)
        tyre_strategy = analyze_tyre_strategy(conn, year, round_number)
        position_vs_grid = analyze_position_vs_grid(conn, year, round_number)
        constructor_points = analyze_constructor_points(conn, year, round_number)

        print("Analysis Phase completed successfully...")
        logger.info("Analysis Phase completed successfully...")
    except Exception as e:
        logger.error(f"Analysis phase failed: {e}")
        print("Analysis phase failed, Check logs")
    finally:
        if 'conn' in locals():
            conn.close()