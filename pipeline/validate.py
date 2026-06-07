import great_expectations as gx
import pandas as pd
import logging
import datetime

import config

logging.basicConfig(
    filename=config.PIPELINE_LOGS_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def build_results_suite(context):
    try:
        logger.info("Building results suite...")

        results_suits = context.suites.add(gx.ExpectationSuite(name='f1_results_suits'))
        
        results_expectation_rules = [
            gx.expectations.ExpectColumnValuesToNotBeNull(column='result_id'),
            gx.expectations.ExpectColumnValuesToNotBeNull(column='DriverId'),
            gx.expectations.ExpectColumnValuesToNotBeNull(column='TeamId'),
            gx.expectations.ExpectColumnValuesToNotBeNull(column='points'),
            gx.expectations.ExpectColumnValuesToNotBeNull(column='position'),
            
            gx.expectations.ExpectColumnValuesToBeBetween(column='position', min_value=1, max_value=20),
            gx.expectations.ExpectColumnValuesToBeBetween(column='points', min_value=0, max_value=26),
            gx.expectations.ExpectColumnValuesToBeBetween(column='GridPosition', min_value=0, max_value=22),
            
            gx.expectations.ExpectColumnValuesToBeUnique(column='result_id'),
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=15, max_value=20),
            
            gx.expectations.ExpectColumnValuesToBeBetween(column='race_time_seconds', min_value=0.00, max_value=None)
        ]
    
        for exp in results_expectation_rules:
            results_suits.add_expectation(exp)
            
        logger.info("Building results suite completed successfully...")
        return results_suits
    except Exception as e:
        logger.error(f"Error while building results suite: {e}")
        raise

def validate_results(results_df, year, round):
    try:
        logger.info(f"Gx validation for results {year} season round {round} dataframe started...")
        context = gx.get_context()

        result_suits = build_results_suite(context)

        data_source = context.data_sources.add_pandas(f"validate_results_{year}_R{round}")
        data_assets = data_source.add_dataframe_asset("validate_asset")

        batch_definition = data_assets.add_batch_definition_whole_dataframe("validate_batch")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": results_df})

        results = batch.validate(result_suits)

        if not results.success:
            logger.error(f"Error: Data quality checks failed: {results.statistics}")

            for res in results.results:
                if not res.success:
                    expectation_type = res.expectation_config.type
                    kwargs = res.expectation_config.kwargs

                    logger.warning(f"🔴 FAILED: {expectation_type}")
                    logger.info(f"   Target configuration: {kwargs}")

                    res_detail = res.result
                    if "unexpected_values" in res_detail and res_detail["unexpected_values"]:
                        logger.info(f"   Unexpected values caught: {res_detail['unexpected_values']}")
                    if "unexpected_index_list" in res_detail and res_detail["unexpected_index_list"]:
                        logger.warning(f"   DataFrame index row positions: {res_detail['unexpected_index_list']}")
                        
            
            logger.warning(f"Data validation failed! Metrics summary: {results.statistics}")
            raise ValueError("Validation for results failed, check logs for details")
        logger.info("Validation for results dataframe completed successfully, ")

        return results
        
    except Exception as e:
        logger.error(f"Validation for results failed: {e}")
        raise


def build_laps_suite(context):
    try:
        logger.info("Building laps suits...")
        laps_suits = context.suites.add(gx.ExpectationSuite(name="laps_suits"))

        laps_suits_rules = [
            gx.expectations.ExpectColumnValuesToNotBeNull(column='lap_id'),
            gx.expectations.ExpectColumnValuesToNotBeNull(column='LapNumber'),
            gx.expectations.ExpectColumnValuesToBeUnique(column='lap_id'),
            gx.expectations.ExpectColumnValuesToBeBetween(column='LapNumber', min_value=0.00, max_value=None),
            gx.expectations.ExpectColumnValuesToBeBetween(column='TyreLife', min_value=0.00, max_value=None)]

        for exp in laps_suits_rules:
            laps_suits.add_expectation(exp)
        
        

        logger.info("Building laps suites completed successfully...")
        return laps_suits
    except Exception as e:
        logger.error(f"Error while building laps suits: {e}")
        raise


def validate_laps(laps_df, year, round):
    try:
        logger.info(f"Gx validation for laps {year} season round {round} dataframe started...")
        context = gx.get_context()

        laps_suits = build_laps_suite(context)

        data_sources = context.data_sources.add_pandas(f"validate_laps_{year}_R{round}")
        data_assets = data_sources.add_dataframe_asset("Validate_assets")

        batch_definition = data_assets.add_batch_definition_whole_dataframe("Validate_batch")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": laps_df})

        results = batch.validate(laps_suits)

        if not results.success:
            logger.error(f"Error: Data quality checks failed: {results.statistics}")

            for res in results.results:
                if not res.success:
                    expectation_type = res.expectation_config.type
                    kwargs = res.expectation_config.kwargs

                    logger.warning(f"🔴 FAILED: {expectation_type}")
                    logger.info(f"   Target configuration: {kwargs}")

                    res_detail = res.result
                    if "unexpected_values" in res_detail and res_detail["unexpected_values"]:
                        logger.info(f"   Unexpected values caught: {res_detail['unexpected_values']}")
                    if "unexpected_index_list" in res_detail and res_detail["unexpected_index_list"]:
                        logger.warning(f"   DataFrame index row positions: {res_detail['unexpected_index_list']}")
                        
            
            logger.warning(f"Data validation failed! Metrics summary: {results.statistics}")
            raise ValueError("Validation for laps failed, check logs for details")
        
        logger.info("Validation for laps completed successfully...")
        return results
    except Exception as e:
        logger.error(f"Validation for laps failed: {e}")
        raise

def generate_validation_report(results_validation, laps_validation):
    try:
        logger.info("Creating validation report...")

        report = []

        resTimestamp = results_validation.meta.get("run_id", {}).get("run_time")
        for val in results_validation.results:
            check_name = val.expectation_config.type
            status = "PASSED" if val.success else "FAILED"
            details = val.result
            report.append({
                    'source': "results",
                    'check_name': check_name,
                    'status': status, 
                    'details': details,
                    'timestamp': resTimestamp
                })
        
        lapsTimestamp = laps_validation.meta.get("run_id", {}).get("run_time")
        for val in laps_validation.results:
            check_name = val.expectation_config.type
            status = "PASSED" if val.success else "FAILED"
            details = val.result
            report.append({
                    'source': "laps",
                    'check_name': check_name,
                    'status': status, 
                    'details': details, 
                    'timestamp': lapsTimestamp
                    })

        pd.DataFrame(report).to_parquet(f"{config.VALIDATION_PATH}/report.parquet")
        logger.info("report created successfully...")
    except Exception as e:
        logger.error(f"Error while generating report: {e}")
        raise


if __name__ == "__main__":
    try:
        logger.info("Validation phase started...")
        year = config.YEAR
        round = config.ROUND
        results_df = pd.read_parquet(f'{config.PROCESSED_DATA_PATH}/results_{year}_R{round}.parquet')
        laps_df = pd.read_parquet(f'{config.PROCESSED_DATA_PATH}/laps_{year}_R{round}.parquet')
        res_validation_obj = validate_results(results_df, year, round)
        laps_validation_obj = validate_laps(laps_df, year, round)
        generate_validation_report(res_validation_obj, laps_validation_obj)
        logger.info("Validation phase completed successfully...")
    except Exception as e:
        logger.error(f"Validation phase failed: {e}")