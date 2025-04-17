import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import List

import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType

from src.utils.logging import LOGGER

COL_SALE_DATE = "SALE_DATE"
COL_SITE_ID = "SITE_ID"
COL_EXTENSION_AMOUNT = "EXTENSION_AMOUNT"
COL_QTY = "QTY"
COL_SLIP_NO = "SLIP_NO"
COL_RETURN_IND = "RETURN_IND"


def _initialize_spark_and_read(
    file_name: str, required_cols: list
) -> tuple[SparkSession | None, pyspark.sql.DataFrame | None]:
    """
    Initializes Spark session (with legacy time parser policy recommended),
    reads parquet file, validates required columns, and parses date based on string length.

    Args:
        file_name: Path to the Parquet file.
        required_cols: List of column names required for the specific metric.

    Returns:
        A tuple containing the SparkSession and Spark DataFrame, or (None, None) on error.
    """
    spart = None
    # Define the expected formats based on length
    date_format_yyyy = "MM/dd/yyyy"  # For length 10
    date_format_yy = "MM/dd/yy"  # For length 8

    try:
        # *** Recommend adding LEGACY policy to prevent to_date exceptions on invalid values ***
        spart = (
            SparkSession.builder.appName(
                f"MetricExtraction_{os.path.basename(file_name)}_When")
            .master("local[*]")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )

        LOGGER.info(
            "Spark session created (using When/Otherwise for date parsing).")
        # Optional: Log if legacy policy is active
        # current_policy = spart.conf.get("spark.sql.legacy.timeParserPolicy", "Not Set")
        # LOGGER.info(f"Current spark.sql.legacy.timeParserPolicy: {current_policy}")

        df_spark = spart.read.parquet(file_name)
        LOGGER.info(f"Successfully read parquet file: '{file_name}'")

        # --- Debugging: Inspect Schema and Sample Data ---
        LOGGER.info("Schema of raw data:")
        df_spark.printSchema()
        LOGGER.info("Sample raw data (first 5 rows):")
        df_spark.show(5, truncate=False)

        actual_columns = df_spark.columns
        if not all(col in actual_columns for col in required_cols):
            LOGGER.error(
                f"Missing one or more required columns. Expected: {required_cols}.")
            missing = [
                col for col in required_cols if col not in actual_columns]
            LOGGER.error(f"Columns missing: {missing}")
            if spart:
                spart.stop()
            return None, None

        # --- Data Type Standardization & Validation ---

        # *** Conditional Date Parsing based on String Length ***
        LOGGER.info(
            f"Applying conditional logic (based on length) to parse '{COL_SALE_DATE}' column...")
        df_spark = df_spark.withColumn(
            COL_SALE_DATE + "_parsed",
            F.when(F.length(F.col(COL_SALE_DATE)) == 10, F.to_date(F.col(COL_SALE_DATE), date_format_yyyy)).when(
                F.length(F.col(COL_SALE_DATE)) == 8, F.to_date(
                    F.col(COL_SALE_DATE), date_format_yy)
            )
            # Set to null if length is not 8 or 10
            .otherwise(F.lit(None).cast(DateType())),
        )
        LOGGER.info(f"Finished applying conditional date parsing.")

        # Check for parsing errors
        # This check now catches:
        # 1. Rows where length was not 8 or 10.
        # 2. Rows where length was correct, but to_date failed (e.g., "99/99/99") - requires LEGACY policy to show as null here.
        null_date_count = df_spark.filter(
            F.col(COL_SALE_DATE +
                  "_parsed").isNull() & F.col(COL_SALE_DATE).isNotNull()
        ).count()
        if null_date_count > 0:
            LOGGER.warning(
                f"{null_date_count} non-null '{COL_SALE_DATE}' values resulted in NULL after conditional parsing (length not 8/10 or invalid date value for detected format)."
            )

        # Rename the successfully parsed column back to the original name
        df_spark = df_spark.drop(COL_SALE_DATE).withColumnRenamed(
            COL_SALE_DATE + "_parsed", COL_SALE_DATE)

        # --- Continue with other type casting as before ---
        if COL_SITE_ID in required_cols:
            df_spark = df_spark.withColumn(
                COL_SITE_ID, F.col(COL_SITE_ID).cast(StringType()))
        # ... other casting ...

        # Log schema *after* type conversions and parsing
        LOGGER.info(
            "Schema after type conversions and conditional date parsing:")
        df_spark.printSchema()

        return spart, df_spark

    except Exception as e:
        # Catch potential errors during session creation, read, or the transformations
        LOGGER.error(
            f"Error during Spark processing for '{file_name}': {e}", exc_info=True)
        if spart:
            spart.stop()
        return None, None


def _format_output(df_spark: pyspark.sql.DataFrame, metric_id: int, value_col: str = "value") -> pd.DataFrame:
    """Formats the aggregated Spark DataFrame into the standard Pandas output."""
    if df_spark is None:
        LOGGER.error(
            f"Cannot format output for metric_id {metric_id} because input DataFrame is None.")
        return pd.DataFrame()

    # Ensure required columns for formatting exist
    required_format_cols = [COL_SALE_DATE, COL_SITE_ID, value_col]
    if not all(col in df_spark.columns for col in required_format_cols):
        LOGGER.error(
            f"Cannot format output for metric_id {metric_id}. Missing columns in aggregated DataFrame. Expected: {required_format_cols}, Got: {df_spark.columns}"
        )
        return pd.DataFrame()

    result_df_spark = df_spark.select(
        F.lit(metric_id).alias("metric_id"),
        F.col(COL_SITE_ID).alias("group_name"),
        F.col(value_col).alias("value"),
        F.col(COL_SALE_DATE).alias("date"),
        F.lit(1).alias("period_level"),
    )
    # --- Debugging: Inspect final Spark DF before converting to Pandas ---
    LOGGER.info(f"Final Spark DataFrame schema for metric_id {metric_id}:")
    result_df_spark.printSchema()
    LOGGER.info(
        f"Sample final Spark DataFrame data (first 5 rows) for metric_id {metric_id}:")
    result_df_spark.show(5, truncate=False)

    try:
        result_df = result_df_spark.toPandas()
        LOGGER.info(
            f"Successfully created Pandas DataFrame for metric_id {metric_id}. Shape: {result_df.shape}")
        return result_df
    except Exception as e:
        LOGGER.error(
            f"Error converting Spark DataFrame to Pandas for metric_id {metric_id}: {e}", exc_info=True)
        return pd.DataFrame()


def get_total_sales_revenue_from_parquet(_file_name: str) -> pd.DataFrame:
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
        df_spark = df_spark.withColumn(COL_EXTENSION_AMOUNT, F.col(
            COL_EXTENSION_AMOUNT).cast(DoubleType()))

        # Group by date and site, then sum the extension amount (net revenue)
        grouped_df = df_spark.groupBy(COL_SALE_DATE, COL_SITE_ID).agg(
            F.sum(COL_EXTENSION_AMOUNT).alias("value"))

        # Ensure all site/date combos have a value (0 if no sales/returns)
        all_site_dates = df_spark.select(COL_SALE_DATE, COL_SITE_ID).distinct()
        grouped_df = all_site_dates.join(grouped_df, [COL_SALE_DATE, COL_SITE_ID], "left").fillna(
            0.0, subset=["value"]
        )  # Use 0.0 for double type

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if spart:
            spart.stop()
            LOGGER.info(f"Spark session stopped for metric {METRIC_ID}.")


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
        df_spark = df_spark.withColumn(
            COL_QTY, F.col(COL_QTY).cast(IntegerType()))

        # Filter for sales (RETURN_IND = 'N') and group/aggregate
        grouped_df = (
            df_spark.filter(F.col(COL_RETURN_IND) == "N")
            .groupBy(COL_SALE_DATE, COL_SITE_ID)
            .agg(F.sum(COL_QTY).alias("value"))
        )

        # Fill sites/dates with no sales with 0
        all_site_dates = df_spark.select(COL_SALE_DATE, COL_SITE_ID).distinct()
        grouped_df = all_site_dates.join(grouped_df, [COL_SALE_DATE, COL_SITE_ID], "left").fillna(
            0, subset=["value"]
        )  # Use 0 for integer type

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
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
            F.countDistinct(COL_SLIP_NO).alias("value"))
        # No need for fillna here, countDistinct naturally handles groups with no slips (though they shouldn't exist if grouping by site/date from original data)

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
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
    required_cols = [COL_SALE_DATE, COL_SITE_ID,
                     COL_EXTENSION_AMOUNT, COL_SLIP_NO]
    spart, df_spark = _initialize_spark_and_read(_file_name, required_cols)

    if not spart or df_spark is None:
        return pd.DataFrame()

    try:
        # Cast amount to double for calculation
        df_spark = df_spark.withColumn(COL_EXTENSION_AMOUNT, F.col(
            COL_EXTENSION_AMOUNT).cast(DoubleType()))

        # Group by date and site, calculate net revenue and number of unique slips
        grouped_df = df_spark.groupBy(COL_SALE_DATE, COL_SITE_ID).agg(
            F.sum(COL_EXTENSION_AMOUNT).alias("total_net_revenue"),
            F.countDistinct(COL_SLIP_NO).alias("num_transactions"),
        )

        # Calculate AOV, handling division by zero
        calculated_aov_df = grouped_df.withColumn(
            "value",
            F.when(F.col("num_transactions") == 0, 0.0).otherwise(  # Avoid division by zero, result is 0.0
                F.col("total_net_revenue") / F.col("num_transactions")
            ),
        )

        result_df = _format_output(calculated_aov_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
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
    # Make sure helpers are accessible
    global _initialize_spark_and_read, _format_output, LOGGER

    spart, df_spark = _initialize_spark_and_read(_file_name, required_cols)

    if not spart or df_spark is None:
        return pd.DataFrame()

    try:
        # Cast quantity to integer (absolute value works on integers)
        df_spark = df_spark.withColumn(
            COL_QTY, F.col(COL_QTY).cast(IntegerType()))

        # Filter for returns (RETURN_IND = 'Y')
        returns_df = df_spark.filter(F.col(COL_RETURN_IND) == "Y")

        # Group by date and site, then sum the *absolute value* of the quantity
        grouped_df = returns_df.groupBy(COL_SALE_DATE, COL_SITE_ID).agg(
            F.sum(F.abs(F.col(COL_QTY))).alias("value")
        )  # Use F.abs() here

        # Fill sites/dates with no returns with 0
        # Get all distinct site/date combinations from the original dataframe
        # to ensure sites/dates without returns are included.
        all_site_dates = df_spark.select(COL_SALE_DATE, COL_SITE_ID).distinct()

        # Left join the aggregated returns data onto the complete list of site/dates
        grouped_df = all_site_dates.join(grouped_df, [COL_SALE_DATE, COL_SITE_ID], "left").fillna(
            0, subset=["value"]
        )  # Fill nulls (no returns) with 0 for integer type

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
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
        return_rows_df = df_spark.filter(F.col(COL_RETURN_IND) == "Y")

        # Group by date and site, then count distinct slip numbers from the return rows
        grouped_df = return_rows_df.groupBy(COL_SALE_DATE, COL_SITE_ID).agg(
            F.countDistinct(COL_SLIP_NO).alias("value"))

        # Fill sites/dates with no return transactions with 0
        all_site_dates = df_spark.select(COL_SALE_DATE, COL_SITE_ID).distinct()
        grouped_df = all_site_dates.join(grouped_df, [COL_SALE_DATE, COL_SITE_ID], "left").fillna(
            0, subset=["value"]
        )  # Use 0 for integer type

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        if spart:
            spart.stop()
            LOGGER.info(f"Spark session stopped for metric {METRIC_ID}.")


def get_positive_feedback_from_json(_file_name: str) -> pd.DataFrame:
    """
    Calculates the Total Number of Positive Feedbacks per site per day.
    Metric ID: 7

    Args:
        _file_name: Path to the JSON file.

    Returns:
        Pandas DataFrame with columns: metric_id, group_name, value, date, period_level.
        Returns empty DataFrame on error.
    """
    METRIC_ID = 7
    data_list = []
    with open(_file_name, "r") as f:
        data = json.load(f)
    for top_level_key, responses in data.items():
        # Ensure the value associated with the top-level key is a dictionary of responses
        if not isinstance(responses, dict):
            logging.warning(
                f"Skipping top-level key '{top_level_key}': Value is not a dictionary.")
            continue

        if str(top_level_key) == "FoodBeverage" or str(top_level_key) == "HospitalityServices":
            continue

        logging.info(
            f"Processing responses under top-level key: {top_level_key}")

        # Iterate through responses within this top-level key
        for response_id, response_data in responses.items():

            LOGGER.info(
                f"Processing response ID {response_id} under {top_level_key}: {response_data}")

            try:
                response_time_str = response_data.get("responseTime")
                store_id = response_data.get("storeid")
                response_sentiment = response_data.get("sentiment")
                if not all([response_time_str, store_id is not None, response_sentiment is not None]):
                    LOGGER.warning(
                        f"Skipping response {response_id} under {top_level_key}: Missing required data (time, storeid, or sentiment)."
                    )
                    continue

                LOGGER.debug(
                    f"Extracted response_time: {response_time_str}, store_id: {store_id}, sentiment: {response_sentiment}"
                )

                if str(response_sentiment).upper() == "POSITIVE":
                    data_list.append(
                        {
                            "date": datetime.strptime(response_time_str, "%Y-%m-%d %H:%M:%S").date(),
                            "storeid": store_id,
                            "value": 1,
                        }
                    )
                    LOGGER.debug(
                        f"Added positive sentiment record for store {store_id} on {response_time_str}")
            except (ValueError, TypeError) as e:
                logging.warning(
                    f"Skipping response {response_id} under {top_level_key} due to data conversion error: {e}. Data: {response_data}"
                )
                continue  # Skip record if conversion fails
            except Exception as e:
                logging.error(
                    f"Unexpected error processing response {response_id} under {top_level_key}: {e}")
                continue
    LOGGER.info(
        f"Finished processing all keys. Found {len(data_list)} positive sentiment records.")

    df = pd.DataFrame(data_list)

    # Optional: Add metric_id and period_level if needed for consistency later
    if not df.empty:
        df["metric_id"] = METRIC_ID
        df["period_level"] = 1

    df_agg = df.groupby(["date", "storeid"], as_index=False).agg(
        metric_id=("metric_id", "first"),
        group_name=("storeid", "first"),
        value=("value", "sum"),
        period_level=("period_level", "first"),
    )
    return df_agg


def get_average_satisfaction_score_from_json(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 8
    SATISFACTION_KEYS = ["Satisfaction - Overall",
                         "Satisfaction - Overall 5pt"]
    data_list = []
    with open(_file_name, "r") as f:
        data = json.load(f)
    for top_level_key, responses in data.items():
        # Ensure the value associated with the top-level key is a dictionary of responses
        if not isinstance(responses, dict):
            logging.warning(
                f"Skipping top-level key '{top_level_key}': Value is not a dictionary.")
            continue

        if str(top_level_key) == "FoodBeverage" or str(top_level_key) == "HospitalityServices":
            continue

        logging.info(
            f"Processing responses under top-level key: {top_level_key}")

        # Iterate through responses within this top-level key
        for response_id, response_data in responses.items():

            LOGGER.info(
                f"Processing response ID {response_id} under {top_level_key}: {response_data}")

            try:
                response_time_str = response_data.get("responseTime")
                store_id = response_data.get("storeid")
                response_satisfaction = response_data.get(SATISFACTION_KEYS[0])
                if response_satisfaction is None:
                    response_satisfaction = response_data.get(
                        SATISFACTION_KEYS[1])
                if not all([response_time_str, store_id is not None, response_satisfaction is not None]):
                    LOGGER.warning(
                        f"Skipping response {response_id} under {top_level_key}: Missing required data (time, storeid, or satisfaction)."
                    )
                    continue
                LOGGER.debug(
                    f"Extracted response_time: {response_time_str}, store_id: {store_id}, satisfaction: {response_satisfaction}"
                )
                # Convert satisfaction to float
                response_satisfaction = float(response_satisfaction)
                # Append to data list
                data_list.append(
                    {
                        "date": datetime.strptime(response_time_str, "%Y-%m-%d %H:%M:%S").date(),
                        "storeid": store_id,
                        "value": response_satisfaction,
                    }
                )
                LOGGER.debug(
                    f"Added satisfaction record for store {store_id} on {response_time_str}")
            except (ValueError, TypeError) as e:
                logging.warning(
                    f"Skipping response {response_id} under {top_level_key} due to data conversion error: {e}. Data: {response_data}"
                )
                continue
            except Exception as e:
                logging.error(
                    f"Unexpected error processing response {response_id} under {top_level_key}: {e}")
                continue
    LOGGER.info(
        f"Finished processing all keys. Found {len(data_list)} satisfaction records.")
    df = pd.DataFrame(data_list)
    if not df.empty:
        df["metric_id"] = METRIC_ID
        df["period_level"] = 1
    df_agg = df.groupby(["date", "storeid"], as_index=False).agg(
        metric_id=("metric_id", "first"),
        group_name=("storeid", "first"),
        value=("value", "mean"),
        period_level=("period_level", "first"),
    )
    return df_agg


def get_social_media_engagement_from_xlsx(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 9
    data_list = []
    try:
        # Read the Excel file
        df = pd.read_excel(_file_name, sheet_name="Brand Post vs Total Engageme", header=2)
        df.rename(columns={"Volume of Published Messages (SUM)": "volumn_published_msg", 
                           "Total Engagements (SUM)": "total_engagement",
                           "Date":"date"}, inplace=True)
        df['date'] = pd.to_datetime(df['date'], format="%m-%d-%Y", errors='coerce')
        for index, row in df.iterrows():
            try:
                date = row["date"]
                volumn_published_msg = row["volumn_published_msg"]
                total_engagement = row["total_engagement"]
                data_list.append(
                    {
                        "date": date,
                        "group_name": "all",
                        "value": total_engagement 
                    }
                )
            except Exception as e:
                LOGGER.error(
                    f"Error processing row {index} in Excel file: {e}", exc_info=True)
                continue
        LOGGER.info(
            f"Finished processing all rows. Found {len(data_list)} engagement records.")
    
    except Exception as e:
        LOGGER.error(
            f"Error processing Excel file '{_file_name}': {e}", exc_info=True)
        return pd.DataFrame()

    res = pd.DataFrame(data_list)

    if not res.empty:
        res["metric_id"] = METRIC_ID
        res["period_level"] = 1
    return res


if __name__ == "__main__":
    # Example usage
    file_name = (
        "/Users/bz/Developer/MCCS Dataset/Social_Media_Performance_2024.xlsx"
    )
    result_df = get_social_media_engagement_from_xlsx(file_name)
    # Print the result DataFrame
    LOGGER.info(result_df)
    result_df.to_csv("metric_9.csv", index=False)
