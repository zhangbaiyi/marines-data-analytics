import sqlite3
from typing import List

import pandas as pd
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType, StringType
import logging

from src.scripts.data_warehouse.models.warehouse import Facts, Metrics, Session
from src.utils.logging import LOGGER


# --- Constants ---
COL_SALE_DATE = "SALE_DATE"
COL_SITE_ID = "SITE_ID"
COL_EXTENSION_AMOUNT = "EXTENSION_AMOUNT"
COL_QTY = "QTY" # Assuming positive for sales, potentially negative for returns
COL_SLIP_NO = "SLIP_NO" # Unique identifier for each transaction
COL_RETURN_IND = "RETURN_IND" 

# Define date format used in the source data
ASSUMED_DATE_FORMAT = "MM/dd/yy"

def _initialize_spark_and_read(file_name: str, required_cols: list) -> SparkSession | None:
    """Initializes Spark session and reads parquet file."""
    spart = SparkSession.builder \
        .appName(f"MetricExtraction_{file_name.split('/')[-1]}") \
        .master("local[*]") \
        .getOrCreate()
    try:
        df_spark = spart.read.parquet(file_name)
        LOGGER.info(f"Successfully read parquet file: '{file_name}'")

        # --- Debugging: Inspect Schema and Sample Data ---
        LOGGER.info("Schema of raw data:")
        df_spark.printSchema()
        LOGGER.info("Sample raw data (first 5 rows):")
        df_spark.show(5, truncate=False) # Show data without truncation

        if not all(col in df_spark.columns for col in required_cols):
            LOGGER.error(f"Missing one or more required columns from {required_cols}.")
            missing = [col for col in required_cols if col not in df_spark.columns]
            LOGGER.error(f"Columns missing: {missing}")
            spart.stop()
            return None, None # Return None for both spark session and dataframe

        # --- Data Type Standardization ---
        # Convert SALE_DATE to DateType
        df_spark = df_spark.withColumn(COL_SALE_DATE, F.to_date(F.col(COL_SALE_DATE), ASSUMED_DATE_FORMAT))
        if df_spark.filter(F.col(COL_SALE_DATE).isNull()).count() > 0:
             LOGGER.warning(f"Some '{COL_SALE_DATE}' values might not have been parsed correctly using format '{ASSUMED_DATE_FORMAT}'. Check sample data and format.")

        # Cast other potential columns (will be done in specific functions as needed)

        return spart, df_spark

    except Exception as e:
        LOGGER.error(f"Error during Spark initialization or file reading for '{file_name}': {e}")
        if 'spart' in locals() and spart:
            spart.stop()
        return None, None
    


def get_total_sales_revenue_from_parquet(_file_name: str) -> pd.DataFrame:
    spart = SparkSession.builder \
        .appName("DataWarehouse") \
        .master("local[*]") \
        .getOrCreate()
    try:
        df_spark = spart.read.parquet(_file_name)
        LOGGER.info(f"Successfully read parquet file: '{_file_name}'")

        # --- Debugging Step 1: Inspect Schema and Sample Data ---
        LOGGER.info("Schema of raw data:")
        df_spark.printSchema()
        LOGGER.info("Sample raw data (first 5 rows):")
        # Show data without truncation to fully see date strings
        df_spark.show(5, truncate=False)
        required_cols = ["SALE_DATE", "SITE_ID", "EXTENSION_AMOUNT"]
        if not all(col in df_spark.columns for col in required_cols):
            LOGGER.error(f"Missing one or more required columns: {required_cols}")
            missing = [col for col in required_cols if col not in df_spark.columns]
            LOGGER.error(f"Columns missing: {missing}")
            spart.stop()
            return pd.DataFrame()
        assumed_date_format = "MM/dd/yy"
        LOGGER.info(f"Attempting date conversion using format: {assumed_date_format}")
    except Exception as e:
        LOGGER.error(f"Error reading parquet file: {e}")
        return pd.DataFrame()
    # TODO: calculate total sales revenue, group by site_id and sale_date
    df_spark = df_spark.withColumn("SALE_DATE", F.to_date(F.col("SALE_DATE"), assumed_date_format))
    df_spark = df_spark.withColumn("EXTENSION_AMOUNT", F.col("EXTENSION_AMOUNT").cast("double"))
    grouped_df = df_spark.groupBy("SALE_DATE", "SITE_ID").agg(
        F.sum("EXTENSION_AMOUNT").alias("value")
    )
    result_df_spark = grouped_df.select(
        F.lit(1).alias("metric_id"),
        F.col("SITE_ID").alias("group_name"),
        F.col("value"),
        F.col("SALE_DATE").alias("date"),
        F.lit(1).alias("period_level")
    )
    result_df = result_df_spark.toPandas()
    return result_df

def get_total_sales_revenue_from_parquet_new(_file_name: str) -> pd.DataFrame:
    """
    Calculates *Net* Sales Revenue per site per day (sum of EXTENSION_AMOUNT).
    Metric ID: 1

    Args:
        _file_name: Path to the Parquet file.

    Returns:
        Pandas DataFrame with columns: metric_id, group_name, value, date, period_level.
        Returns empty DataFrame on error.
    """
    METRIC_ID = 1
    required_cols = [COL_SALE_DATE, COL_SITE_ID, COL_EXTENSION_AMOUNT]
    spart, df_spark = _initialize_spark_and_read(_file_name, required_cols)

    if not spart or df_spark is None:
        return pd.DataFrame()

    try:
        # Cast amount to double for aggregation
        df_spark = df_spark.withColumn(COL_EXTENSION_AMOUNT, F.col(COL_EXTENSION_AMOUNT).cast(DoubleType()))

        # Group by date and site, then sum the extension amount (net revenue)
        grouped_df = df_spark.groupBy(COL_SALE_DATE, COL_SITE_ID).agg(
            F.sum(COL_EXTENSION_AMOUNT).alias("value")
        )

        # Ensure all site/date combos have a value (0 if no sales/returns)
        all_site_dates = df_spark.select(COL_SALE_DATE, COL_SITE_ID).distinct()
        grouped_df = all_site_dates.join(
            grouped_df,
            [COL_SALE_DATE, COL_SITE_ID],
            "left"
        ).fillna(0.0, subset=["value"]) # Use 0.0 for double type


        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if spart:
            spart.stop()
            LOGGER.info(f"Spark session stopped for metric {METRIC_ID}.")

def _format_output(df_spark: pyspark.sql.DataFrame, metric_id: int, value_col: str = "value") -> pd.DataFrame:
    """Formats the aggregated Spark DataFrame into the standard Pandas output."""
    if df_spark is None:
        LOGGER.error(f"Cannot format output for metric_id {metric_id} because input DataFrame is None.")
        return pd.DataFrame()

    # Ensure required columns for formatting exist
    required_format_cols = [COL_SALE_DATE, COL_SITE_ID, value_col]
    if not all(col in df_spark.columns for col in required_format_cols):
         LOGGER.error(f"Cannot format output for metric_id {metric_id}. Missing columns in aggregated DataFrame. Expected: {required_format_cols}, Got: {df_spark.columns}")
         return pd.DataFrame()

    result_df_spark = df_spark.select(
        F.lit(metric_id).alias("metric_id"),
        F.col(COL_SITE_ID).alias("group_name"), 
        F.col(value_col).alias("value"),
        F.col(COL_SALE_DATE).alias("date"),
        F.lit(1).alias("period_level") 
    )
    # --- Debugging: Inspect final Spark DF before converting to Pandas ---
    LOGGER.info(f"Final Spark DataFrame schema for metric_id {metric_id}:")
    result_df_spark.printSchema()
    LOGGER.info(f"Sample final Spark DataFrame data (first 5 rows) for metric_id {metric_id}:")
    result_df_spark.show(5, truncate=False)

    try:
        result_df = result_df_spark.toPandas()
        LOGGER.info(f"Successfully created Pandas DataFrame for metric_id {metric_id}. Shape: {result_df.shape}")
        return result_df
    except Exception as e:
        LOGGER.error(f"Error converting Spark DataFrame to Pandas for metric_id {metric_id}: {e}", exc_info=True)
        return pd.DataFrame()


def get_total_units_sold_from_parquet(_file_name: str) -> pd.DataFrame:
    """
    Calculates Total Units Sold (excluding returns) per site per day.
    Filters based on RETURN_IND = 'N'.
    Metric ID: 2

    Args:
        _file_name: Path to the Parquet file.

    Returns:
        Pandas DataFrame with columns: metric_id, group_name, value, date, period_level.
        Returns empty DataFrame on error.
    """
    METRIC_ID = 2
    required_cols = [COL_SALE_DATE, COL_SITE_ID, COL_QTY, COL_RETURN_IND]
    spart, df_spark = _initialize_spark_and_read(_file_name, required_cols)

    if not spart or df_spark is None:
        return pd.DataFrame()

    try:
        # Cast quantity to integer for aggregation
        df_spark = df_spark.withColumn(COL_QTY, F.col(COL_QTY).cast(IntegerType()))

        # Filter for sales (RETURN_IND = 'N') and group/aggregate
        grouped_df = df_spark.filter(F.col(COL_RETURN_IND) == 'N') \
                             .groupBy(COL_SALE_DATE, COL_SITE_ID) \
                             .agg(F.sum(COL_QTY).alias("value"))

        # Fill sites/dates with no sales with 0
        all_site_dates = df_spark.select(COL_SALE_DATE, COL_SITE_ID).distinct()
        grouped_df = all_site_dates.join(
            grouped_df,
            [COL_SALE_DATE, COL_SITE_ID],
            "left"
        ).fillna(0, subset=["value"]) # Use 0 for integer type

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if spart:
            spart.stop()
            LOGGER.info(f"Spark session stopped for metric {METRIC_ID}.")


def get_number_of_transactions_from_parquet(_file_name: str) -> pd.DataFrame:
    """
    Calculates the total Number of unique Transactions (Slips) per site per day.
    Uses count distinct on SLIP_NO within each SITE_ID and SALE_DATE group.
    Metric ID: 3

    Args:
        _file_name: Path to the Parquet file.

    Returns:
        Pandas DataFrame with columns: metric_id, group_name, value, date, period_level.
        Returns empty DataFrame on error.
    """
    METRIC_ID = 3
    required_cols = [COL_SALE_DATE, COL_SITE_ID, COL_SLIP_NO]
    spart, df_spark = _initialize_spark_and_read(_file_name, required_cols)

    if not spart or df_spark is None:
        return pd.DataFrame()

    try:
        # Group by date and site, then count distinct slip numbers
        grouped_df = df_spark.groupBy(COL_SALE_DATE, COL_SITE_ID).agg(
            F.countDistinct(COL_SLIP_NO).alias("value")
        )
        # No need for fillna here, countDistinct naturally handles groups with no slips (though they shouldn't exist if grouping by site/date from original data)

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if spart:
            spart.stop()
            LOGGER.info(f"Spark session stopped for metric {METRIC_ID}.")


def get_average_order_value_from_parquet(_file_name: str) -> pd.DataFrame:
    """
    Calculates the Average Order Value (AOV) per site per day.
    AOV = Net Sales Revenue / Number of Transactions (Slips). Handles division by zero.
    Metric ID: 4

    Args:
        _file_name: Path to the Parquet file.

    Returns:
        Pandas DataFrame with columns: metric_id, group_name, value, date, period_level.
        Returns empty DataFrame on error.
    """
    METRIC_ID = 4
    # Requires amount for sum, slip_no for count distinct
    required_cols = [COL_SALE_DATE, COL_SITE_ID, COL_EXTENSION_AMOUNT, COL_SLIP_NO]
    spart, df_spark = _initialize_spark_and_read(_file_name, required_cols)

    if not spart or df_spark is None:
        return pd.DataFrame()

    try:
        # Cast amount to double for calculation
        df_spark = df_spark.withColumn(COL_EXTENSION_AMOUNT, F.col(COL_EXTENSION_AMOUNT).cast(DoubleType()))

        # Group by date and site, calculate net revenue and number of unique slips
        grouped_df = df_spark.groupBy(COL_SALE_DATE, COL_SITE_ID).agg(
            F.sum(COL_EXTENSION_AMOUNT).alias("total_net_revenue"),
            F.countDistinct(COL_SLIP_NO).alias("num_transactions")
        )

        # Calculate AOV, handling division by zero
        calculated_aov_df = grouped_df.withColumn(
            "value",
            F.when(F.col("num_transactions") == 0, 0.0) # Avoid division by zero, result is 0.0
             .otherwise(F.col("total_net_revenue") / F.col("num_transactions"))
        )

        result_df = _format_output(calculated_aov_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if spart:
            spart.stop()
            LOGGER.info(f"Spark session stopped for metric {METRIC_ID}.")


def get_number_of_returned_items_from_parquet(_file_name: str) -> pd.DataFrame:
    """
    Calculates the Total Number of Returned Items (Units) per site per day.
    Filters using RETURN_IND = 'Y' and sums the *absolute value* of QTY.
    Metric ID: 5

    Args:
        _file_name: Path to the Parquet file.

    Returns:
        Pandas DataFrame with columns: metric_id, group_name, value, date, period_level.
        Returns empty DataFrame on error.
    """
    METRIC_ID = 5
    required_cols = [COL_SALE_DATE, COL_SITE_ID, COL_QTY, COL_RETURN_IND]
    # Ensure these helpers are defined as in the previous version
    global _initialize_spark_and_read, _format_output, LOGGER # Make sure helpers are accessible

    spart, df_spark = _initialize_spark_and_read(_file_name, required_cols)

    if not spart or df_spark is None:
        return pd.DataFrame()

    try:
        # Cast quantity to integer (absolute value works on integers)
        df_spark = df_spark.withColumn(COL_QTY, F.col(COL_QTY).cast(IntegerType()))

        # Filter for returns (RETURN_IND = 'Y')
        returns_df = df_spark.filter(F.col(COL_RETURN_IND) == 'Y')

        # Group by date and site, then sum the *absolute value* of the quantity
        grouped_df = returns_df.groupBy(COL_SALE_DATE, COL_SITE_ID) \
                               .agg(F.sum(F.abs(F.col(COL_QTY))).alias("value")) # Use F.abs() here

        # Fill sites/dates with no returns with 0
        # Get all distinct site/date combinations from the original dataframe
        # to ensure sites/dates without returns are included.
        all_site_dates = df_spark.select(COL_SALE_DATE, COL_SITE_ID).distinct()

        # Left join the aggregated returns data onto the complete list of site/dates
        grouped_df = all_site_dates.join(
            grouped_df,
            [COL_SALE_DATE, COL_SITE_ID],
            "left"
        ).fillna(0, subset=["value"]) # Fill nulls (no returns) with 0 for integer type

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if spart:
            spart.stop()
            LOGGER.info(f"Spark session stopped for metric {METRIC_ID}.")

def get_number_of_return_transactions_from_parquet(_file_name: str) -> pd.DataFrame:
    """
    Calculates the Total Number of Transactions (Slips) that included returns per site per day.
    Filters based on RETURN_IND = 'Y' and counts distinct SLIP_NO.
    Metric ID: 6

    Args:
        _file_name: Path to the Parquet file.

    Returns:
        Pandas DataFrame with columns: metric_id, group_name, value, date, period_level.
        Returns empty DataFrame on error.
    """
    METRIC_ID = 6
    required_cols = [COL_SALE_DATE, COL_SITE_ID, COL_SLIP_NO, COL_RETURN_IND]
    spart, df_spark = _initialize_spark_and_read(_file_name, required_cols)

    if not spart or df_spark is None:
        return pd.DataFrame()

    try:
        # Filter rows corresponding to returned items
        return_rows_df = df_spark.filter(F.col(COL_RETURN_IND) == 'Y')

        # Group by date and site, then count distinct slip numbers from the return rows
        grouped_df = return_rows_df.groupBy(COL_SALE_DATE, COL_SITE_ID).agg(
            F.countDistinct(COL_SLIP_NO).alias("value")
        )

        # Fill sites/dates with no return transactions with 0
        all_site_dates = df_spark.select(COL_SALE_DATE, COL_SITE_ID).distinct()
        grouped_df = all_site_dates.join(
            grouped_df,
            [COL_SALE_DATE, COL_SITE_ID],
            "left"
        ).fillna(0, subset=["value"]) # Use 0 for integer type

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if spart:
            spart.stop()
            LOGGER.info(f"Spark session stopped for metric {METRIC_ID}.")

