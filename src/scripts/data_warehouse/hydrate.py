import sqlite3

import pandas as pd
from tqdm import tqdm

import src.scripts.data_warehouse.etl as etl

# from src.scripts.data_warehouse.etl import get_total_sales_revenue_from_parquet_new
from src.scripts.data_warehouse.utils import (
    aggregate_metric_by_group_hierachy,
    aggregate_metric_by_time_period,
    insert_facts_from_df,
)
from src.utils.logging import LOGGER


def insert_sales_data(parquet_file_path: str):
    if parquet_file_path:
        LOGGER.info(f"Loading sales data from {parquet_file_path}")
    else:
        raise ValueError("parquet_file_path must be provided")
    if not parquet_file_path.endswith(".parquet"):
        raise ValueError("File must be a .parquet file")
    LOGGER.info("Loading sales data...")
    df = pd.read_parquet(parquet_file_path)
    LOGGER.info(f"Data shape: {df.shape}")
    LOGGER.info(f"Data types: \n{df.dtypes}")
    conn = sqlite3.connect("./db/database.sqlite3")
    df.to_sql(name="sales", con=conn, if_exists="replace", index=False)
    LOGGER.info(f"Inserted {df.shape[0]} rows into 'sales' table")


if __name__ == "__main__":
    # parquet_file_list = [
    #     "/Users/bz/Developer/MCCS Dataset/RetailData(Apr-Jun-24).parquet",
    #     "/Users/bz/Developer/MCCS Dataset/RetailData(Dec-Jan-24-25).parquet",
    #     "/Users/bz/Developer/MCCS Dataset/RetailData(Jan-Mar-24).parquet",
    #     "/Users/bz/Developer/MCCS Dataset/RetailData(Jul-Sep-24).parquet",
    # ]

    # retail_data_etl_methods_list = [
    #     "get_total_sales_revenue_from_parquet",
    #     "get_total_units_sold_from_parquet",
    #     "get_number_of_transactions_from_parquet",
    #     "get_average_order_value_from_parquet",
    #     "get_number_of_returned_items_from_parquet",
    #     "get_number_of_return_transactions_from_parquet",
    # ]

    # Example usage
    # parquet_file_path = "/Users/bz/Developer/MCCS Dataset/RetailData(Oct-Nov-24).parquet"
    # insert_sales_data(parquet_file_path)
    # for parquet_file_path in tqdm(parquet_file_list):
    #     for etl_method_name in tqdm(retail_data_etl_methods_list):
    #         LOGGER.info(f"Processing method: {etl_method_name}")
    #         metric_etl_method = getattr(etl, etl_method_name)
    #         lowest_level_df = metric_etl_method(parquet_file_path)
    #         LOGGER.info(f"Lowest level data shape: {lowest_level_df.shape}")

    #         # Aggregate the data by time period
    #         aggregated_df = aggregate_metric_by_time_period(
    #             lowest_level=lowest_level_df, _method="sum") # bug: what if aggregate is average?
    #         LOGGER.info(f"Aggregated data shape: {aggregated_df.shape}")
    #         # Insert the aggregated data into the facts table
    #         insert_facts_from_df(aggregated_df)
    for metric_id in range(1, 7):
        df = aggregate_metric_by_group_hierachy(metric_id, "sum")
        insert_facts_from_df(df)
        LOGGER.info(
            "Group hierarchy aggregation completed successfully for metric_id: %s", metric_id)

    # survey_file_path = "/Users/bz/Developer/marines-data-analytics/src/scripts/data_warehouse/customer_survey_responses_updated.json"
    # lowest_level_df = etl.get_positive_feedback_from_json(survey_file_path)
    # aggregated_df = aggregate_metric_by_time_period(
    #     lowest_level=lowest_level_df, _method="sum")
    # LOGGER.info(f"Aggregated data shape: {aggregated_df.shape}")
    # insert_facts_from_df(aggregated_df)
    # LOGGER.info("ETL process completed successfully.")

    # lowest_level_df = etl.get_average_satisfaction_score_from_json(survey_file_path)
    # aggregated_df = aggregate_metric_by_time_period(
    #     lowest_level=lowest_level_df, _method="mean")
    # LOGGER.info(f"Aggregated data shape: {aggregated_df.shape}")
    # insert_facts_from_df(aggregated_df)
    # LOGGER.info("ETL process completed successfully.")
