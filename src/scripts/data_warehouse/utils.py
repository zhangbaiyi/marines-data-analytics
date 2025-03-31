import pandas as pd
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.scripts.data_warehouse.models.warehouse import Facts, Metrics, Session
from src.utils.logging import LOGGER


def get_metric_md(metric_id: int):
    """
    Retrieves a single Metrics object from the database
    by its 'metric_id'. Returns None if not found.
    """
    with Session() as session:
        metric = session.query(Metrics).filter_by(id=metric_id).one_or_none()
        if metric is None:
            LOGGER.error(f"Metric with ID {metric_id} not found.")
            raise ValueError(f"Metric with ID {metric_id} not found.")
        LOGGER.info(f"Metric found: {metric.metric_name}")
        return metric


def aggregate_metric_by_time_period(lowest_level: pd.DataFrame, _method: str) -> pd.DataFrame:
    """
    Aggregates 'lowest_level' (daily) data into monthly, quarterly, or yearly
    totals, depending on the flags in your metric. Appends the aggregated rows
    to the original DataFrame ('res') and returns the combined result.

    :param _metric_id: The ID of the metric (retrieved from your 'metrics' table).
    :param lowest_level: DataFrame with at least:
        - 'date': The daily date (can be string/int in YYYYMMDD, or datetime).
        - 'value': The numeric column we want to aggregate.
    :param _method: The name of a pandas aggregation method (e.g. 'sum', 'mean').
    :return: A DataFrame with daily rows plus aggregated rows, each flagged
        by 'period_level' (1=daily, 2=monthly, 3=quarterly, 4=yearly).
    """
    # LOGGER.info(f"Metric Name: {metric.metric_name}")
    metric_ids = lowest_level["metric_id"].unique()
    if len(metric_ids) > 1:
        LOGGER.error(
            f"Input DataFrame contains multiple metric_ids: {metric_ids}. Aggregation requires a single metric_id."
        )
        raise ValueError(
            "Input DataFrame must contain only one unique metric_id.")
    if len(metric_ids) == 0:
        LOGGER.error("Input DataFrame has no values in 'metric_id' column.")
        raise ValueError(
            "Input DataFrame has no values in 'metric_id' column.")
    metric_id = int(metric_ids[0])
    LOGGER.info(f"Processing metric_id: {metric_id}")

    metric = get_metric_md(metric_id)

    if not pd.api.types.is_datetime64_any_dtype(lowest_level["date"]):
        lowest_level["date"] = pd.to_datetime(lowest_level["date"])

    start_date: pd.Timestamp = lowest_level["date"].min()
    end_date: pd.Timestamp = lowest_level["date"].max()

    res = lowest_level.copy()

    if "period_level" not in res.columns:
        res["period_level"] = 1

    if metric.is_daily:
        # Not sure what to put inside here
        assert start_date.day == 1, "Start date should be the 1st of the month for daily data."
        assert end_date.is_month_end, "End date should be the last day of the month for daily data."

    if metric.is_monthly:
        # e.g., 2025-03-15 -> 2025-03-01 00:00:00
        lowest_level["month_start"] = lowest_level["date"].dt.to_period(
            "M").dt.start_time

        monthly_agg = (
            lowest_level.groupby(["group_name", "month_start"], dropna=False).agg(
                {"value": _method}).reset_index()
        )
        monthly_agg["date"] = monthly_agg["month_start"].dt.strftime("%Y%m01")
        monthly_agg.drop(columns=["month_start"], inplace=True)
        monthly_agg["period_level"] = 2
        res = pd.concat([res, monthly_agg], ignore_index=True)

    if metric.is_quarterly:
        # e.g., 2025-04-15 -> 2025-04-01, 2025-02-01 -> 2025-01-01
        lowest_level["quarter_start"] = lowest_level["date"].dt.to_period(
            "Q").dt.start_time
        quarterly_agg = (
            lowest_level.groupby(["group_name", "quarter_start"], dropna=False).agg(
                {"value": _method}).reset_index()
        )
        quarterly_agg["date"] = quarterly_agg["quarter_start"].dt.strftime(
            "%Y%m01")
        quarterly_agg.drop(columns=["quarter_start"], inplace=True)
        quarterly_agg["period_level"] = 3
        res = pd.concat([res, quarterly_agg], ignore_index=True)

    if metric.is_yearly:
        # e.g., 2025-03-15 -> 2025-01-01
        lowest_level["year_start"] = lowest_level["date"].dt.to_period(
            "Y").dt.start_time

        yearly_agg = (
            lowest_level.groupby(["group_name", "year_start"], dropna=False).agg(
                {"value": _method}).reset_index()
        )
        yearly_agg["date"] = yearly_agg["year_start"].dt.strftime("%Y0101")
        yearly_agg.drop(columns=["year_start"], inplace=True)
        yearly_agg["period_level"] = 4
        res = pd.concat([res, yearly_agg], ignore_index=True)

    res["metric_id"] = metric_id
    return res


def insert_facts_from_df(df_facts: pd.DataFrame) -> int:
    df_facts["date"] = pd.to_datetime(
        df_facts["date"], errors="coerce").dt.date
    records = df_facts.to_dict(orient="records")

    with Session() as session:
        num_processed = 0

        for row in records:
            # 1) Build a base insert statement:
            base_stmt = sqlite_insert(Facts).values(
                metric_id=row["metric_id"],
                group_name=row["group_name"],
                date=row["date"],
                period_level=row["period_level"],
                value=row["value"],
            )
            # 2) Add the on_conflict_do_update part:
            stmt = base_stmt.on_conflict_do_update(
                index_elements=["metric_id",
                                "group_name", "date", "period_level"],
                set_={
                    # Refer to base_stmt.excluded instead of stmt.excluded
                    "value": base_stmt.excluded.value
                },
            )

            session.execute(stmt)
            num_processed += 1

        session.commit()

    return num_processed


def aggregate_metric_by_group_hierachy(_metric_id: int, _method: str) -> pd.DataFrame:
    """
    Query all data for the given metric ID from the 'facts' table
    and compute a single 'ALL' group that aggregates the 'value'
    across all group_name entries for each time dimension (date, period_level).
    """

    # 1. Query the existing facts records for our given metric_id:
    with Session() as session:
        results = (
            session.query(Facts.metric_id, Facts.group_name,
                          Facts.value, Facts.date, Facts.period_level)
            .filter(Facts.metric_id == _metric_id)
            .all()
        )

    # 2. Load query results into a DataFrame:
    df = pd.DataFrame(results, columns=[
                      "metric_id", "group_name", "value", "date", "period_level"])

    if df.empty:
        # If there's no data for this metric, return an empty DataFrame
        LOGGER.warning(f"No facts found for metric_id = {_metric_id}")
        return pd.DataFrame(columns=["metric_id", "group_name", "value", "date", "period_level"])

    # Ensure that 'date' is a datetime type
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # 3. Group by (metric_id, date, period_level) and aggregate 'value' with the given _method:
    grouped = df.groupby(["metric_id", "date", "period_level"],
                         dropna=False, as_index=False).agg({"value": _method})

    # 4. Label this new aggregated row with group_name = 'ALL':
    grouped["group_name"] = "all"

    # 5. Rearrange columns to match the Facts schema order:
    grouped = grouped[["metric_id", "group_name",
                       "value", "date", "period_level"]]

    return grouped
