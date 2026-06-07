import pandas as pd
import os
import logging
import config

logging.basicConfig(
    filename=config.PIPELINE_LOGS_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_dataframe(path: str) -> pd.DataFrame:
    try:
        if not os.path.exists(path):
            logger.error("Error: File path doesn't exist")
            raise FileNotFoundError
        logger.info("Getting dataframe...")

        df = pd.read_parquet(path)

        logger.info("Dataframe extracted successfully...")
        return df
    except Exception as e:
        logger.error(f'Error accured while getting dataframe: {e}')
        raise

def save_dataframe(dataframe: pd.DataFrame,file_name: str):
    try:
        logger.info("Saving dataframe as a parquet...")
        dataframe.to_parquet(config.PROCESSED_DATA_PATH + f'/{file_name}.parquet', index=False)
        logger.info("Dataframe saved as a parquet successfully...")
    except Exception as e:
        logger.error(f"Error while trying to save dataframe: {e}")
        raise 

def transform_results(results_df: pd.DataFrame, year, round_number, session_type) -> pd.DataFrame:
    try:
        logger.info("Results transfomation started...")
        results_df = results_df.drop(columns=["HeadshotUrl", "BroadcastName", "TeamColor", "Q1", "Q2", "Q3"])
        group_id = results_df["Position"].notna().cumsum()
        increment = results_df.groupby(group_id).cumcount()
        results_df["Position"] = results_df["Position"].ffill() + increment
        int_cols = ["DriverNumber", "Position", "GridPosition", "Points", "Laps"]
        for col in int_cols:
            results_df[col] = pd.to_numeric(results_df[col], errors="coerce")

        if "Points" in results_df.columns:
            results_df["Points"] = results_df["Points"] // 1
        results_df[int_cols] = results_df[int_cols].astype("Int64")
    
        results_df["Time"] = results_df["Time"].dt.total_seconds()
        results_df = results_df.rename(columns={"Time": "race_time_seconds"})
        numeric = pd.to_numeric(results_df["ClassifiedPosition"], errors='coerce')
        results_df["is_classified"] = numeric.notna()

        winner_time = results_df.loc[results_df["Position"] == 1, "race_time_seconds"].iloc[0]
        results_df = results_df.assign(
            gap_to_winner_seconds=lambda x: x["race_time_seconds"] - winner_time,
            year=year,
            round_number=round_number,
            session_type=session_type
        )
        results_df["result_id"] = results_df["year"].astype(str) + "_" + results_df["round_number"].astype(str) + "_" + results_df["DriverNumber"].astype(str)

        # save_dataframe(results_df, f'results_{year}_{session_type}{round_number}')

        logger.info(f"Results transformation completed successfully, {results_df.shape[0]} row/s and {results_df.shape[1]} column/s left")
        # print(list(results_df.columns))

        return results_df
    except Exception as e:
        logger.error(f'Results transformation failed: {e}')
        raise


def transform_laps(laps_df, year, round_number, session_type):
    try:
        logger.info("Laps transformation started...")
        #lap_int_cols = ["year", "round_number", "LapNumber", "Stint", "TyreLife", "Position"]

        laps_df = laps_df.drop(columns=["Sector1SessionTime", "Sector2SessionTime", "Sector3SessionTime", "FastF1Generated", "LapStartTime"])
        laps_df[["DriverNumber", "LapNumber", "Stint", "TyreLife", "Position"]] = laps_df[["DriverNumber", "LapNumber", "Stint", "TyreLife", "Position"]].astype("Int64")
        time_columns = ["LapTime", "Sector1Time", "Sector2Time", "Sector3Time"]
        laps_df[time_columns] = laps_df[time_columns].apply(lambda x: x.dt.total_seconds())
        laps_df = laps_df.rename(columns=lambda x: f'{x}_seconds' if x in time_columns else x)
        laps_df["is_pit_lap"] = laps_df["PitInTime"].notna() | laps_df["PitOutTime"].notna()
        laps_df["is_valid_lap"] = (laps_df["IsAccurate"] == True) & (laps_df["Deleted"] == False)
        laps_df = laps_df.drop(columns=["PitInTime", "PitOutTime"])

        laps_df =laps_df.assign(
            year=year,
            round_number=round_number,
            session_type=session_type,
        )

        laps_df["lap_id"] = laps_df["year"].astype(str) + "_" + laps_df["round_number"].astype(str) + "_" + laps_df["Driver"] + "_" + laps_df["LapNumber"].astype(str)
        # save_dataframe(laps_df, f'laps_{year}_{session_type}{round_number}')

        logger.info(f"Laps transformation completed successfully, {laps_df.shape[0]} row/s and {laps_df.shape[1]} column/s left")
        # print(list(laps_df.columns))

        return laps_df
    except Exception as e:
        logger.error(f"Laps transformation failed: {e}")
        raise

if __name__ == "__main__":
    try:
        print("Transformation phase started...")
        logger.info("Transformation phase started ...")

        results_df = get_dataframe(f'{config.RAW_DATA_PATH}/results_{config.YEAR}_{config.SESSION}{config.ROUND}.parquet')
        laps_df = get_dataframe(f'{config.RAW_DATA_PATH}/laps_{config.YEAR}_{config.SESSION}{config.ROUND}.parquet')

        transform_results(results_df, config.YEAR, config.ROUND, config.SESSION)
        transform_laps(laps_df, config.YEAR, config.ROUND, config.SESSION)

        print("Transformation phase completed successfully...")
        logger.info("Transformation phase completed successfully...")
    except Exception as e:
        print("Trasformation failed, Check the logs")
        logger.error(f"Transformation failed: {e}")
