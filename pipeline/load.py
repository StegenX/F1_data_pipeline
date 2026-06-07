import pandas as pd
from google.cloud import bigquery
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

def create_connection() -> bigquery.Client:
    try:
        logger.info("Creating connection with BigQuery")
        client = bigquery.Client(project=config.BIGQUERY_PROJECT)
        logger.info("Connection with BigQuery succeeded")
        return client
    except Exception as e:
        logger.error(f'Error while trying to connect to BigQuery: {e}')
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

def define_schemas() -> dict:
    DIM_DRIVERS_SCHEMA = [
        bigquery.SchemaField("DriverId", "STRING", mode="REQUIRED", description="Primary Key for the driver"),
        bigquery.SchemaField("Abbreviation", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FirstName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("LastName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FullName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CountryCode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("DriverNumber", "INT64", mode="NULLABLE"),
    ]

    DIM_CONSTRUCTORS_SCHEMA = [
        bigquery.SchemaField("TeamId", "STRING", mode="REQUIRED", description="Primary Key for the constructor/team"),
        bigquery.SchemaField("TeamName", "STRING", mode="NULLABLE"),
    ]

    DIM_CIRCUITS_SCHEMA = [
        bigquery.SchemaField("circuit_id", "STRING", mode="REQUIRED", description="Primary Key for the circuit"),
        bigquery.SchemaField("circuit_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
    ]

    FACT_RACE_RESULTS_SCHEMA = [
        bigquery.SchemaField("result_id", "STRING", mode="REQUIRED", description="Primary Key for race results"),
        bigquery.SchemaField("DriverId", "STRING", mode="REQUIRED", description="FK to dim_drivers"),
        bigquery.SchemaField("TeamId", "STRING", mode="REQUIRED", description="FK to dim_constructors"),
        bigquery.SchemaField("circuit_id", "STRING", mode="REQUIRED", description="FK to dim_circuits"),
        bigquery.SchemaField("year", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("round_number", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("Position", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("GridPosition", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("Points", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("Laps", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("race_time_seconds", "FLOAT64", mode="NULLABLE"), 
        bigquery.SchemaField("gap_to_winner_seconds", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("Status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ClassifiedPosition", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_classified", "BOOLEAN", mode="NULLABLE"), 
        bigquery.SchemaField("session_type", "STRING", mode="NULLABLE"),
    ]

    FACT_LAP_TIMES_SCHEMA = [
        bigquery.SchemaField("lap_id", "STRING", mode="REQUIRED", description="Primary Key for lap times"),
        bigquery.SchemaField("DriverId", "STRING", mode="REQUIRED", description="FK to dim_drivers"),
        bigquery.SchemaField("circuit_id", "STRING", mode="REQUIRED", description="FK to dim_circuits"),
        bigquery.SchemaField("year", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("round_number", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("lap_number", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stint", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("lap_time_seconds", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("sector1_seconds", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("sector2_seconds", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("sector3_seconds", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("speedI1", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("speedI2", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("speedFl", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("speedSt", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("compound", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tyre_life", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("fresh_tyre", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_pit_lap", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_valid_lap", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_personal_best", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("Position", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("track_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("session_type", "STRING", mode="NULLABLE"),
    ]

    return {
        "dim_drivers": DIM_DRIVERS_SCHEMA,
        "dim_constructors": DIM_CONSTRUCTORS_SCHEMA,
        "dim_circuits": DIM_CIRCUITS_SCHEMA,
        "fact_race_results": FACT_RACE_RESULTS_SCHEMA,
        "fact_lap_times": FACT_LAP_TIMES_SCHEMA
    }

def create_schemas(client: bigquery.Client):
    try:
        logger.info("Creating tables schemas...")
        F1_TABLES_BLUEPRINT = define_schemas()
 
        for table_name, schema in F1_TABLES_BLUEPRINT.items():
            table_id = f"{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}.{table_name}"
            table_blueprint = bigquery.Table(table_id, schema=schema)

            if table_name == 'fact_lap_times':
                table_blueprint.clustering_fields = ['DriverId', 'year', 'round_number']
            elif table_name == 'fact_race_results':
                table_blueprint.clustering_fields = ['year', 'round_number']
            
            try:
                client.create_table(table_blueprint, exists_ok=True)
                logger.info(f"{table_name} table verified/created successfully...")
            except Exception as e:
                logger.error(f"{table_name} table creation failed: {e}")
                raise

        logger.info("Tables schemas processing routine closed...")
    except Exception as e:
        logger.error(f'Error while creating schemas: {e}')
        raise

def load_results(client: bigquery.Client, results_df, event, year: int, round_number: int):
    try:
        logger.info(f"Loading results data for Year: {year}, Round: {round_number}...")
       
        F1_SCHEMAS = define_schemas()
        event_df = get_event_df(event)

        logger.info("Evicting any existing race result rows for idempotency...")
        query = f"""
            SELECT COUNT(*) as count 
            FROM `{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}.fact_race_results`
            WHERE year = {year} AND round_number = {round_number}
        """
        result = client.query(query).result()
        count = list(result)[0].count
        if count > 0:
            logger.warning(f"Data for year {year} round {round_number} already exists — skipping load")
            return

        merged_fact_table = results_df.merge(event_df, how='cross')

        for table_name, schema in F1_SCHEMAS.items():
            if table_name == "fact_lap_times":
                continue

            table_destination = f"{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}.{table_name}"
            columns = [field.name for field in schema]
            write_disposition = "WRITE_TRUNCATE" if table_name.startswith("dim_") else "WRITE_APPEND"
            if table_name in ["dim_drivers", "dim_constructors"]:
                dataframe_source = results_df[columns].drop_duplicates()
            elif table_name == "dim_circuits":
                dataframe_source = event_df[columns].drop_duplicates()
            else:
                dataframe_source = merged_fact_table[columns]

            job = client.load_table_from_dataframe(
                dataframe=dataframe_source,
                destination=table_destination,
                job_config=bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition=write_disposition
                )
            )
            job.result()
            logger.info(f"{table_name} data loaded successfully")

        logger.info("Results elements batch processed safely...")
    except Exception as e:
        logger.error(f'Error while loading results data: {e}')
        raise

def load_laps(client: bigquery.Client, laps_df, results_df, event, year: int, round_number: int):
    try:
        logger.info(f"Loading laps data for Year: {year}, Round: {round_number}...")

        F1_SCHEMAS = define_schemas()
        schema = F1_SCHEMAS["fact_lap_times"]
        columns_source = [
            'lap_id', 'DriverId', 'circuit_id', 'year', 'round_number', 
            'LapNumber', 'Stint', 'LapTime_seconds', 'Sector1Time_seconds', 
            'Sector2Time_seconds', 'Sector3Time_seconds', 'SpeedI1', 'SpeedI2', 
            'SpeedFL', 'SpeedST', 'Compound', 'TyreLife', 'FreshTyre', 
            'is_pit_lap', 'is_valid_lap', 'IsPersonalBest', 'Position', 
            'TrackStatus', 'session_type'
        ]
        
        table_destination = f"{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}.fact_lap_times"
            
        event_df = get_event_df(event)
        circuit_id = event_df["circuit_id"].iloc[0]
        laps_df["circuit_id"] = circuit_id
        
        laps_df = laps_df.merge(results_df[["DriverNumber", "DriverId"]], how='left')

        logger.info("Evicting any existing lap records for idempotency...")
        query = f"""
            SELECT COUNT(*) as count 
            FROM `{config.BIGQUERY_PROJECT}.{config.BIGQUERY_DATASET}.fact_lap_times`
            WHERE year = {year} AND round_number = {round_number}
        """
        result = client.query(query).result()
        count = list(result)[0].count
        if count > 0:
            logger.warning(f"Data for year {year} round {round_number} already exists — skipping load")
            return
        
        for col in columns_source:
            if col not in laps_df.columns:
                laps_df[col] = None


        sliced_laps_df = laps_df[columns_source].copy()
        sliced_laps_df.columns = [field.name for field in schema]

        fact_job = client.load_table_from_dataframe(
            dataframe=sliced_laps_df,
            destination=table_destination,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_APPEND"
            )
        )
        fact_job.result()

        logger.info("Laps data loaded successfully...")
    except Exception as e:
        logger.error(f"Error while loading laps data: {e}")
        raise

if __name__ == "__main__":
    try:
        print("Load phase started...")
        logger.info("Load phase started...")

        results_df = pd.read_parquet(f"{config.PROCESSED_DATA_PATH}/results_{config.YEAR}_{config.SESSION}{config.ROUND}.parquet")
        laps_df = pd.read_parquet(f"{config.PROCESSED_DATA_PATH}/laps_{config.YEAR}_{config.SESSION}{config.ROUND}.parquet")
        event = extract_race_event(year=config.YEAR, round_number=config.ROUND)

        client = create_connection()
        create_schemas(client)

        load_results(client, results_df, event, year=int(config.YEAR), round_number=int(config.ROUND))
        load_laps(client, laps_df, results_df, event, year=int(config.YEAR), round_number=int(config.ROUND))

        print("Loading phase completed successfully...")
        logger.info("Loading phase completed successfully...")
    except Exception as e:
        print("Load phase failed, check logs...")
        logger.error(f'Load phase failed: {e}')
        raise