import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List

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
    Calculates **the percentage of positive feedback responses** received
    for each site on each day (positive / total).

    Metric ID: 7
    Returned columns: metric_id, group_name (store ID), value (decimal
    fraction), date, period_level.

    If no valid responses are found the function returns an empty
    DataFrame.
    """
    METRIC_ID = 7
    records: list[dict] = []

    with open(_file_name, "r") as f:
        data = json.load(f)

    for top_level_key, responses in data.items():
        # Skip non-dict branches or unwanted sections
        if not isinstance(responses, dict):
            LOGGER.warning(
                f"Skipping top level key '{top_level_key}': value is not a dict.")
            continue
        if top_level_key in {"FoodBeverage", "HospitalityServices"}:
            continue

        LOGGER.info(f"Processing {top_level_key}")

        for response_id, response in responses.items():
            try:
                ts = response.get("responseTime")
                store = response.get("storeid")
                sent = response.get("sentiment")

                if not (ts and store is not None and sent is not None):
                    LOGGER.warning(
                        f"Missing data in response {response_id} under {top_level_key}")
                    continue

                is_pos = int(str(sent).upper() == "POSITIVE")
                records.append(
                    {
                        "date": datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").date(),
                        "storeid": store,
                        "is_pos": is_pos,
                    }
                )

            except Exception as exc:
                LOGGER.error(
                    f"Error processing response {response_id} under {top_level_key}: {exc}")

    if not records:
        return pd.DataFrame()  # nothing to aggregate

    df = pd.DataFrame(records)

    # ── Aggregate: % positive = positive / total ────────────────────────────────
    agg = df.groupby(["date", "storeid"], as_index=False).agg(
        positive_cnt=("is_pos", "sum"),
        total_cnt=("is_pos", "count"),
    )
    agg = agg[agg["total_cnt"] > 0]  # safety
    agg["value"] = agg["positive_cnt"] / agg["total_cnt"]

    # ── Final shape expected by the warehouse ──────────────────────────────────
    agg = agg.assign(
        metric_id=METRIC_ID,
        group_name=agg["storeid"],
        period_level=1,
    )[["metric_id", "group_name", "value", "date", "period_level"]]

    # filter out zeros
    agg = agg[agg["value"] > 0]
    agg = agg[agg["value"] < 1]  # safety

    return agg


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


def get_store_atmosphere_score_from_json(_file_name: str) -> pd.DataFrame:
    """
    Parse a JSON survey-response file and compute a daily, per-store
    “store-atmosphere score”.

    • For every response, take the mean of the numeric values found under
      the three atmosphere keys:
          1. "Store Atmosphere - Space 5pt"
          2. "Store Atmosphere - Layout 5pt"
          3. "Store Atmosphere - Finding 5pt"
      If only one or two keys are present (or convertible to float), take
      the mean of the available keys. Skip the response entirely if **no**
      keys yield numeric data.

    • Aggregate to one record per (date, storeid) with the mean value.
    """
    METRIC_ID = 20
    ATMOSPHERE_KEYS: List[str] = [
        "Store Atmosphere - Space 5pt",
        "Store Atmosphere - Layout 5pt",
        "Store Atmosphere - Finding 5pt",
    ]

    data_list: List[Dict[str, Any]] = []

    # ---------- load file ----------
    with open(_file_name, "r", encoding="utf-8") as f:
        data = json.load(f)

    # ---------- iterate -----------
    for top_level_key, responses in data.items():
        if str(top_level_key) != "MainStores":
            continue
        if not isinstance(responses, dict):
            logging.warning(
                "Top-level key '%s' is not a dict – skipped.", top_level_key)
            continue

        for response_id, response_data in responses.items():
            try:
                # ── 1. pull date & store ────────────────────────────────────────
                response_time_str = response_data.get("responseTime")
                if not response_time_str:
                    raise ValueError("Missing responseTime")
                response_date = datetime.strptime(
                    response_time_str, "%Y-%m-%d %H:%M:%S").date()

                store_id = response_data.get("storeid")
                if store_id is None:
                    raise ValueError("Missing storeid")

                # ── 2. collect numeric keys & average ──────────────────────────
                values: List[float] = []
                for key in ATMOSPHERE_KEYS:
                    raw_val = response_data.get(key)
                    if raw_val is None or raw_val == "":
                        continue
                    try:
                        values.append(float(raw_val))
                    except (ValueError, TypeError):
                        LOGGER.debug(
                            "[M%02d] Non-numeric value '%s' for key '%s' in response %s",
                            METRIC_ID,
                            raw_val,
                            key,
                            response_id,
                        )

                if not values:
                    # nothing usable in this response
                    continue

                avg_score = sum(values) / len(values)

                if avg_score > 5 or avg_score < 0:
                    continue

                data_list.append(
                    {
                        "date": response_date,
                        "storeid": store_id,
                        "value": avg_score,
                    }
                )
            except Exception as e:
                logging.warning("Skipping response %s in %s: %s",
                                response_id, top_level_key, e)
                continue

    LOGGER.info("Collected %d atmosphere records", len(data_list))

    # ---------- build DataFrame ----------
    df = pd.DataFrame(data_list)
    if df.empty:
        return df

    df["metric_id"] = METRIC_ID
    df["period_level"] = 1  # daily

    # ---------- aggregate to one row per store-day ----------
    df_agg = (
        df.groupby(["date", "storeid"], as_index=False)
        .agg(
            metric_id=("metric_id", "first"),
            group_name=("storeid", "first"),
            value=("value", "mean"),
            period_level=("period_level", "first"),
        )
        .rename(columns={"storeid": "store_id"})  # optional: tidy up naming
    )

    return df_agg


def get_store_price_satisfaction_score_from_json(_file_name: str) -> pd.DataFrame:
    """
    Parse a JSON survey-response file and compute a daily, per-store
    “store-atmosphere score”.

    • For every response, take the mean of the numeric values found under
      the three atmosphere keys:
          1. "Store Atmosphere - Space 5pt"
          2. "Store Atmosphere - Layout 5pt"
          3. "Store Atmosphere - Finding 5pt"
      If only one or two keys are present (or convertible to float), take
      the mean of the available keys. Skip the response entirely if **no**
      keys yield numeric data.

    • Aggregate to one record per (date, storeid) with the mean value.
    """
    METRIC_ID = 21
    ATMOSPHERE_KEYS: List[str] = [
        "Price - Clarity 5pt",
        "Price - Value 5pt",
        "Price - Competitiveness 5pt",
    ]

    data_list: List[Dict[str, Any]] = []

    # ---------- load file ----------
    with open(_file_name, "r", encoding="utf-8") as f:
        data = json.load(f)

    # ---------- iterate -----------
    for top_level_key, responses in data.items():
        if str(top_level_key) != "MainStores":
            continue
        if not isinstance(responses, dict):
            logging.warning(
                "Top-level key '%s' is not a dict – skipped.", top_level_key)
            continue

        for response_id, response_data in responses.items():
            try:
                # ── 1. pull date & store ────────────────────────────────────────
                response_time_str = response_data.get("responseTime")
                if not response_time_str:
                    raise ValueError("Missing responseTime")
                response_date = datetime.strptime(
                    response_time_str, "%Y-%m-%d %H:%M:%S").date()

                store_id = response_data.get("storeid")
                if store_id is None:
                    raise ValueError("Missing storeid")

                # ── 2. collect numeric keys & average ──────────────────────────
                values: List[float] = []
                for key in ATMOSPHERE_KEYS:
                    raw_val = response_data.get(key)
                    if raw_val is None or raw_val == "":
                        continue
                    try:
                        values.append(float(raw_val))
                    except (ValueError, TypeError):
                        LOGGER.debug(
                            "[M%02d] Non-numeric value '%s' for key '%s' in response %s",
                            METRIC_ID,
                            raw_val,
                            key,
                            response_id,
                        )

                if not values:
                    # nothing usable in this response
                    continue

                avg_score = sum(values) / len(values)

                if avg_score > 5 or avg_score < 0:
                    continue

                data_list.append(
                    {
                        "date": response_date,
                        "storeid": store_id,
                        "value": avg_score,
                    }
                )
            except Exception as e:
                logging.warning("Skipping response %s in %s: %s",
                                response_id, top_level_key, e)
                continue

    LOGGER.info("Collected %d atmosphere records", len(data_list))

    # ---------- build DataFrame ----------
    df = pd.DataFrame(data_list)
    if df.empty:
        return df

    df["metric_id"] = METRIC_ID
    df["period_level"] = 1  # daily

    # ---------- aggregate to one row per store-day ----------
    df_agg = (
        df.groupby(["date", "storeid"], as_index=False)
        .agg(
            metric_id=("metric_id", "first"),
            group_name=("storeid", "first"),
            value=("value", "mean"),
            period_level=("period_level", "first"),
        )
        .rename(columns={"storeid": "store_id"})  # optional: tidy up naming
    )

    return df_agg


def get_store_service_satisfaction_score_from_json(_file_name: str) -> pd.DataFrame:
    """
    Parse a JSON survey-response file and compute a daily, per-store
    “store-atmosphere score”.

    • For every response, take the mean of the numeric values found under
      the three atmosphere keys:
          1. "Store Atmosphere - Space 5pt"
          2. "Store Atmosphere - Layout 5pt"
          3. "Store Atmosphere - Finding 5pt"
      If only one or two keys are present (or convertible to float), take
      the mean of the available keys. Skip the response entirely if **no**
      keys yield numeric data.

    • Aggregate to one record per (date, storeid) with the mean value.
    """
    METRIC_ID = 22
    ATMOSPHERE_KEYS: List[str] = [
        "Service - Knowledge 5pt",
        "Service - Responsiveness 5pt",
        "Service - Availability 5pt",
    ]

    data_list: List[Dict[str, Any]] = []

    # ---------- load file ----------
    with open(_file_name, "r", encoding="utf-8") as f:
        data = json.load(f)

    # ---------- iterate -----------
    for top_level_key, responses in data.items():
        if str(top_level_key) != "MainStores":
            continue
        if not isinstance(responses, dict):
            logging.warning(
                "Top-level key '%s' is not a dict – skipped.", top_level_key)
            continue

        for response_id, response_data in responses.items():
            try:
                # ── 1. pull date & store ────────────────────────────────────────
                response_time_str = response_data.get("responseTime")
                if not response_time_str:
                    raise ValueError("Missing responseTime")
                response_date = datetime.strptime(
                    response_time_str, "%Y-%m-%d %H:%M:%S").date()

                store_id = response_data.get("storeid")
                if store_id is None:
                    raise ValueError("Missing storeid")

                # ── 2. collect numeric keys & average ──────────────────────────
                values: List[float] = []
                for key in ATMOSPHERE_KEYS:
                    raw_val = response_data.get(key)
                    if raw_val is None or raw_val == "":
                        continue
                    try:
                        values.append(float(raw_val))
                    except (ValueError, TypeError):
                        LOGGER.debug(
                            "[M%02d] Non-numeric value '%s' for key '%s' in response %s",
                            METRIC_ID,
                            raw_val,
                            key,
                            response_id,
                        )

                if not values:
                    # nothing usable in this response
                    continue

                avg_score = sum(values) / len(values)

                if avg_score > 5 or avg_score < 0:
                    continue

                data_list.append(
                    {
                        "date": response_date,
                        "storeid": store_id,
                        "value": avg_score,
                    }
                )
            except Exception as e:
                logging.warning("Skipping response %s in %s: %s",
                                response_id, top_level_key, e)
                continue

    LOGGER.info("Collected %d atmosphere records", len(data_list))

    # ---------- build DataFrame ----------
    df = pd.DataFrame(data_list)
    if df.empty:
        return df

    df["metric_id"] = METRIC_ID
    df["period_level"] = 1  # daily

    # ---------- aggregate to one row per store-day ----------
    df_agg = (
        df.groupby(["date", "storeid"], as_index=False)
        .agg(
            metric_id=("metric_id", "first"),
            group_name=("storeid", "first"),
            value=("value", "mean"),
            period_level=("period_level", "first"),
        )
        .rename(columns={"storeid": "store_id"})  # optional: tidy up naming
    )

    return df_agg


def _read_social_sheet(_file_name: str, sheet: str) -> pd.DataFrame:
    """Internal helper – returns the requested sheet with date already parsed."""
    df = pd.read_excel(_file_name, sheet_name=sheet, header=2)
    df.rename(columns={"Date": "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"], format="%m-%d-%Y", errors="coerce")
    return df


def get_total_engagement_from_xlsx(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 9
    try:
        df = _read_social_sheet(_file_name, "Brand Post vs Total Engageme")
        df.rename(
            columns={"Total Engagements (SUM)": "total_engagement"},
            inplace=True,
        )

        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["total_engagement"].astype(float),
                "date": df["date"],
                "period_level": 1,
            }
        )
        return res

    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


def get_followers_change_from_xlsx(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 10
    try:
        df = _read_social_sheet(_file_name, "Overall Follower vs Change")
        df.rename(
            columns={
                "Followers (SUM)": "followers",
                "Change in Followers": "followers_change",
            },
            inplace=True,
        )

        # ensure numeric
        df["followers"] = pd.to_numeric(df["followers"], errors="coerce")
        df["followers_change"] = pd.to_numeric(
            df["followers_change"], errors="coerce")

        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["followers_change"],
                "date": df["date"],
                "period_level": 1,
            }
        )
        return res

    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


# 11.  Daily # of brand posts published
def get_posts_published_from_xlsx(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 11
    try:
        df = _read_social_sheet(_file_name, "Brand Post vs Total Engageme")
        df.rename(
            columns={"Volume of Published Messages (SUM)": "posts_published"},
            inplace=True,
        )

        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["posts_published"].astype(float),
                "date": df["date"],
                "period_level": 1,
            }
        )
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


# 12.  Likes / Reactions
def get_post_likes_from_xlsx(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 12
    try:
        df = _read_social_sheet(_file_name, "Brand Post Engagement Breakd")
        df.rename(
            columns={"Post Likes And Reactions (SUM)": "likes_reactions"},
            inplace=True,
        )

        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["likes_reactions"].astype(float),
                "date": df["date"],
                "period_level": 1,
            }
        )
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


# 13.  Comments
def get_post_comments_from_xlsx(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 13
    try:
        df = _read_social_sheet(_file_name, "Brand Post Engagement Breakd")
        df.rename(columns={"Post Comments (SUM)": "comments"}, inplace=True)

        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["comments"].astype(float),
                "date": df["date"],
                "period_level": 1,
            }
        )
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


# 14.  Shares
def get_post_shares_from_xlsx(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 14
    try:
        df = _read_social_sheet(_file_name, "Brand Post Engagement Breakd")
        df.rename(columns={"Post Shares (SUM)": "shares"}, inplace=True)

        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["shares"].astype(float),
                "date": df["date"],
                "period_level": 1,
            }
        )
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


# 15.  Estimated Clicks
def get_estimated_clicks_from_xlsx(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 15
    try:
        df = _read_social_sheet(_file_name, "Engagement Behaviour across ")
        df.rename(columns={"Estimated Clicks": "clicks"}, inplace=True)

        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["clicks"].astype(float),
                "date": df["date"],
                "period_level": 1,
            }
        )
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


# 16.  Post Reach
def get_post_reach_from_xlsx(_file_name: str) -> pd.DataFrame:
    METRIC_ID = 16
    try:
        df = _read_social_sheet(_file_name, "Engagement Behaviour across ")
        df.rename(columns={"Post Reach": "reach"}, inplace=True)

        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["reach"].astype(float),
                "date": df["date"],
                "period_level": 1,
            }
        )
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


# 17.  % Δ Engagement Rate
def get_engagement_rate_change_from_xlsx(_file_name: str) -> pd.DataFrame:
    """
    Converts the '% change in Engagement Rate' column (string w/ '%')
    to a decimal (0.3373981) and stores as value.
    """
    METRIC_ID = 17
    try:
        df = _read_social_sheet(_file_name, "Engagement Rate Changes over")
        df.rename(
            columns={
                "% change in Engagement Rate": "eng_rate_pct_change",
            },
            inplace=True,
        )

        # strip % and convert to decimal
        df["eng_rate_pct_change"] = df["eng_rate_pct_change"].str.replace(
            "%", "").astype(float) / 100.0

        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["eng_rate_pct_change"],
                "date": df["date"],
                "period_level": 1,
            }
        )
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


def get_email_deliveries_from_xlsx(_file_name: str) -> pd.DataFrame:
    """
    Converts the '% change in Engagement Rate' column (string w/ '%')
    to a decimal (0.3373981) and stores as value.
    """
    METRIC_ID = 18

    try:
        df = pd.read_excel(
            _file_name, sheet_name="Email Deliveries Delivery Timel", header=4)
        LOGGER.info(f"Email Deliveries DataFrame shape: {df.head(5)}")
        df.rename(columns={"Daily": "date"}, inplace=True)
        df["date"] = pd.to_datetime(
            df["date"], format="%d-%b-%Y", errors="coerce")
        df.rename(
            columns={
                "Delivery Rate": "delivery_rate",
            },
            inplace=True,
        )
        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["delivery_rate"],
                "date": df["date"],
                "period_level": 1,
            }
        )
        res.dropna(axis=0, inplace=True)
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


def get_email_engagement_from_xlsx(_file_name: str) -> pd.DataFrame:
    """
    Converts the '% change in Engagement Rate' column (string w/ '%')
    to a decimal (0.3373981) and stores as value.
    """
    METRIC_ID = 19

    try:
        df = pd.read_excel(
            _file_name, sheet_name="Email Engagement Engagement Tim", header=4)
        LOGGER.info(f"Email Open Date DataFrame shape: {df.head(5)}")
        df.rename(columns={"Daily": "date"}, inplace=True)
        df["date"] = pd.to_datetime(
            df["date"], format="%d-%b-%Y", errors="coerce")
        df.rename(
            columns={
                "Open Rate": "open_rate",
            },
            inplace=True,
        )
        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["open_rate"],
                "date": df["date"],
                "period_level": 1,
            }
        )
        res.dropna(axis=0, inplace=True)
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


def get_email_engagement_from_xlsx(_file_name: str) -> pd.DataFrame:
    """
    Converts the '% change in Engagement Rate' column (string w/ '%')
    to a decimal (0.3373981) and stores as value.
    """
    METRIC_ID = 19

    try:
        df = pd.read_excel(
            _file_name, sheet_name="Email Engagement Engagement Tim", header=4)
        LOGGER.info(f"Email Open Date DataFrame shape: {df.head(5)}")
        df.rename(columns={"Daily": "date"}, inplace=True)
        df["date"] = pd.to_datetime(
            df["date"], format="%d-%b-%Y", errors="coerce")
        df.rename(
            columns={
                "Open Rate": "open_rate",
            },
            inplace=True,
        )
        res = pd.DataFrame(
            {
                "metric_id": METRIC_ID,
                "group_name": "all",
                "value": df["open_rate"],
                "date": df["date"],
                "period_level": 1,
            }
        )
        res.dropna(axis=0, inplace=True)
        return res
    except Exception as e:
        LOGGER.error(f"[M{METRIC_ID}] {e}", exc_info=True)
        return pd.DataFrame()


if __name__ == "__main__":
    # xl_path = "/Users/bz/Developer/MCCS Dataset/Advertising_Email_Engagement_2024.xlsx"
    json_path = (
        "/Users/bz/Developer/marines-data-analytics/src/scripts/data_warehouse/customer_survey_responses_updated.json"
    )
    funcs = [
        get_positive_feedback_from_json,
    ]

    for f in funcs:
        try:
            res = f(json_path)
            LOGGER.info(res.head(10))
            LOGGER.info(f"Function {f.__name__} returned {res.shape[0]} rows.")
            res.to_csv("metric 7.csv", index=False)
        except Exception as e:
            LOGGER.error(f"Error in function {f.__name__}: {e}")
