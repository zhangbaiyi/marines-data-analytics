

import sqlite3
import pandas as pd
from src.scripts.data_warehouse.utils import aggregate_metric_by_group_hierachy, aggregate_metric_by_time_period, get_metric_1_lowest_level, insert_facts_from_df

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
    conn = sqlite3.connect("./python-prediction-model/src/db/database.sqlite3")
    df.to_sql(name='sales', con=conn, if_exists='replace', index=False)
    LOGGER.info(f"Inserted {df.shape[0]} rows into 'sales' table")


if __name__ == "__main__":
    # Example usage
    parquet_file_path = "/Users/bz/Developer/MCCS Dataset/RetailData(Oct-Nov-24).parquet"
    insert_sales_data(parquet_file_path)

    # Load the lowest level data for testing
    lowest_level_df = get_metric_1_lowest_level()
    LOGGER.info(f"Lowest level data shape: {lowest_level_df.shape}")

    # Aggregate the data by time period
    aggregated_df = aggregate_metric_by_time_period(_metric_id=1, lowest_level=lowest_level_df, _method='sum')
    LOGGER.info(f"Aggregated data shape: {aggregated_df.shape}")

    # Insert the aggregated data into the facts table
    insert_facts_from_df(aggregated_df)
