import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import List

import pandas as pd

from src.utils.logging import LOGGER

COL_SALE_DATE = "SALE_DATE"
COL_SITE_ID = "SITE_ID"
COL_EXTENSION_AMOUNT = "EXTENSION_AMOUNT"
COL_QTY = "QTY"
COL_SLIP_NO = "SLIP_NO"
COL_RETURN_IND = "RETURN_IND"


def _read_parquet_to_pandas(
    file_name: str, required_cols: list
) -> pd.DataFrame | None:
    """
    Reads parquet file into a Pandas DataFrame and validates required columns.
    Parses date based on string length.

    Args:
        file_name: Path to the Parquet file.
        required_cols: List of column names required for the specific metric.

    Returns:
        A Pandas DataFrame, or None on error.
    """
    try:
        df_pandas = pd.read_parquet(file_name)
        LOGGER.info(f"Successfully read parquet file: '{file_name}'")

        # --- Debugging: Inspect Schema and Sample Data ---
        LOGGER.info("Schema of raw data:")
        LOGGER.info(df_pandas.dtypes)
        LOGGER.info("Sample raw data (first 5 rows):")
        LOGGER.info(df_pandas.head())

        actual_columns = df_pandas.columns
        if not all(col in actual_columns for col in required_cols):
            LOGGER.error(
                f"Missing one or more required columns. Expected: {required_cols}.")
            missing = [
                col for col in required_cols if col not in actual_columns]
            LOGGER.error(f"Columns missing: {missing}")
            return None

        # --- Data Type Standardization & Validation ---

        # *** Conditional Date Parsing based on String Length ***
        LOGGER.info(
            f"Applying conditional logic (based on length) to parse '{COL_SALE_DATE}' column...")

        def parse_date(date_str):
            if isinstance(date_str, str):
                if len(date_str) == 10:
                    try:
                        return pd.to_datetime(date_str, format="%m/%d/%Y").date()
                    except ValueError:
                        return None
                elif len(date_str) == 8:
                    try:
                        return pd.to_datetime(date_str, format="%m/%d/%y").date()
                    except ValueError:
                        return None
            return None

        df_pandas[COL_SALE_DATE] = df_pandas[COL_SALE_DATE].apply(parse_date)
        LOGGER.info(f"Finished applying conditional date parsing.")

        # Check for parsing errors
        null_date_count = df_pandas[COL_SALE_DATE].isnull().sum()
        original_non_null_count = df_pandas[df_pandas[COL_SALE_DATE].notna()].shape[0]
        if null_date_count > 0 and original_non_null_count > null_date_count:
            LOGGER.warning(
                f"{null_date_count} non-null '{COL_SALE_DATE}' values resulted in NaT after conditional parsing (length not 8/10 or invalid date value for detected format)."
            )

        # --- Continue with other type casting as before ---
        if COL_SITE_ID in required_cols:
            df_pandas[COL_SITE_ID] = df_pandas[COL_SITE_ID].astype(str)
        if COL_EXTENSION_AMOUNT in required_cols:
            df_pandas[COL_EXTENSION_AMOUNT] = pd.to_numeric(
                df_pandas[COL_EXTENSION_AMOUNT], errors='raise')
        if COL_QTY in required_cols:
            df_pandas[COL_QTY] = pd.to_numeric(
                df_pandas[COL_QTY], errors='raise').astype('Int64')  # Use Int64 for nullable integers
        if COL_SLIP_NO in required_cols:
            df_pandas[COL_SLIP_NO] = df_pandas[COL_SLIP_NO].astype(str)
        if COL_RETURN_IND in required_cols:
            df_pandas[COL_RETURN_IND] = df_pandas[COL_RETURN_IND].astype(str)

        # Log schema *after* type conversions and parsing
        LOGGER.info(
            "Schema after type conversions and conditional date parsing:")
        LOGGER.info(df_pandas.dtypes)

        return df_pandas

    except Exception as e:
        # Catch potential errors during read or the transformations
        LOGGER.error(
            f"Error during Pandas processing for '{file_name}': {e}", exc_info=True)
        return None


def _format_output(df_pandas: pd.DataFrame, metric_id: int, value_col: str = "value") -> pd.DataFrame:
    """Formats the aggregated Pandas DataFrame into the standard Pandas output."""
    if df_pandas is None:
        LOGGER.error(
            f"Cannot format output for metric_id {metric_id} because input DataFrame is None.")
        return pd.DataFrame()

    # Ensure required columns for formatting exist
    required_format_cols = [COL_SALE_DATE, COL_SITE_ID, value_col]
    if not all(col in df_pandas.columns for col in required_format_cols):
        LOGGER.error(
            f"Cannot format output for metric_id {metric_id}. Missing columns in aggregated DataFrame. Expected: {required_format_cols}, Got: {df_pandas.columns}"
        )
        return pd.DataFrame()

    result_df = pd.DataFrame({
        "metric_id": metric_id,
        "group_name": df_pandas[COL_SITE_ID],
        "value": df_pandas[value_col],
        "date": df_pandas[COL_SALE_DATE],
        "period_level": 1,
    })
    # --- Debugging: Inspect final Pandas DF ---
    LOGGER.info(f"Final Pandas DataFrame schema for metric_id {metric_id}:")
    LOGGER.info(result_df.dtypes)
    LOGGER.info(
        f"Sample final Pandas DataFrame data (first 5 rows) for metric_id {metric_id}:")
    LOGGER.info(result_df.head())

    return result_df


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
    df_pandas = _read_parquet_to_pandas(_file_name, required_cols)

    if df_pandas is None:
        return pd.DataFrame()

    try:
        # Group by date and site, then sum the extension amount (net revenue)
        grouped_df = df_pandas.groupby([COL_SALE_DATE, COL_SITE_ID])[
            COL_EXTENSION_AMOUNT].sum().reset_index(name="value")

        # Ensure all site/date combos have a value (0 if no sales/returns)
        all_site_dates = df_pandas[[COL_SALE_DATE, COL_SITE_ID]].drop_duplicates()
        grouped_df = pd.merge(
            all_site_dates, grouped_df, on=[COL_SALE_DATE, COL_SITE_ID], how="left").fillna(0.0)

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
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
    df_pandas = _read_parquet_to_pandas(_file_name, required_cols)

    if df_pandas is None:
        return pd.DataFrame()

    try:
        # Filter for sales (RETURN_IND = 'N')
        sales_df = df_pandas[df_pandas[COL_RETURN_IND] == "N"]

        # Group by date and site, then sum the quantity
        grouped_df = sales_df.groupby([COL_SALE_DATE, COL_SITE_ID])[
            COL_QTY].sum().reset_index(name="value")

        # Fill sites/dates with no sales with 0
        all_site_dates = df_pandas[[COL_SALE_DATE, COL_SITE_ID]].drop_duplicates()
        grouped_df = pd.merge(
            all_site_dates, grouped_df, on=[COL_SALE_DATE, COL_SITE_ID], how="left").fillna(0)

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()


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
    df_pandas = _read_parquet_to_pandas(_file_name, required_cols)

    if df_pandas is None:
        return pd.DataFrame()

    try:
        # Group by date and site, then count distinct slip numbers
        grouped_df = df_pandas.groupby([COL_SALE_DATE, COL_SITE_ID])[
            COL_SLIP_NO].nunique().reset_index(name="value")

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()


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
    df_pandas = _read_parquet_to_pandas(_file_name, required_cols)

    if df_pandas is None:
        return pd.DataFrame()

    try:
        # Group by date and site, calculate net revenue and number of unique slips
        grouped_df = df_pandas.groupby([COL_SALE_DATE, COL_SITE_ID]).agg(
            total_net_revenue=(COL_EXTENSION_AMOUNT, 'sum'),
            num_transactions=(COL_SLIP_NO, 'nunique')
        ).reset_index()

        # Calculate AOV, handling division by zero
        grouped_df['value'] = grouped_df.apply(
            lambda row: row['total_net_revenue'] / row['num_transactions']
            if row['num_transactions'] != 0 else 0.0,
            axis=1
        )

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()


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
    df_pandas = _read_parquet_to_pandas(_file_name, required_cols)

    if df_pandas is None:
        return pd.DataFrame()

    try:
        # Filter for returns (RETURN_IND = 'Y')
        returns_df = df_pandas[df_pandas[COL_RETURN_IND] == "Y"]

        # Group by date and site, then sum the *absolute value* of the quantity
        grouped_df = returns_df.groupby([COL_SALE_DATE, COL_SITE_ID])[
            COL_QTY].apply(lambda x: x.abs().sum()).reset_index(name="value")

        # Fill sites/dates with no returns with 0
        all_site_dates = df_pandas[[COL_SALE_DATE, COL_SITE_ID]].drop_duplicates()
        grouped_df = pd.merge(
            all_site_dates, grouped_df, on=[COL_SALE_DATE, COL_SITE_ID], how="left").fillna(0)

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()


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
    df_pandas = _read_parquet_to_pandas(_file_name, required_cols)

    if df_pandas is None:
        return pd.DataFrame()

    try:
        # Filter rows corresponding to returned items
        return_rows_df = df_pandas[df_pandas[COL_RETURN_IND] == "Y"]

        # Group by date and site, then count distinct slip numbers from the return rows
        grouped_df = return_rows_df.groupby([COL_SALE_DATE, COL_SITE_ID])[
            COL_SLIP_NO].nunique().reset_index(name="value")

        # Fill sites/dates with no return transactions with 0
        all_site_dates = df_pandas[[COL_SALE_DATE, COL_SITE_ID]].drop_duplicates()
        grouped_df = pd.merge(
            all_site_dates, grouped_df, on=[COL_SALE_DATE, COL_SITE_ID], how="left").fillna(0)

        result_df = _format_output(grouped_df, METRIC_ID)
        return result_df

    except Exception as e:
        LOGGER.error(
            f"Error calculating metric_id {METRIC_ID}: {e}", exc_info=True)
        return pd.DataFrame()