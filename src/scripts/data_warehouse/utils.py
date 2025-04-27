import pandas as pd
from sqlalchemy import Integer, cast
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.scripts.data_warehouse.access import getSites, query_facts
from src.scripts.data_warehouse.models.warehouse import Facts, Metrics, SessionLocal
from src.utils.logging import LOGGER


def get_metric_md(metric_id: int):
    """
    Retrieves a single Metrics object from the database
    by its 'metric_id'. Returns None if not found.
    """
    with SessionLocal() as session:
        metric = session.query(Metrics).filter_by(id=metric_id).one_or_none()
        if metric is None:
            LOGGER.error(f"Metric with ID {metric_id} not found.")
            raise ValueError(f"Metric with ID {metric_id} not found.")
        LOGGER.info(f"Metric found: {metric.metric_name}")
        return metric


def aggregate_metric_by_time_period_legacy(lowest_level: pd.DataFrame, _method: str) -> pd.DataFrame:
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

    # ── NEW: determine the metric’s *lowest* granularity and purge higher levels

    if not pd.api.types.is_datetime64_any_dtype(lowest_level["date"]):
        lowest_level["date"] = pd.to_datetime(lowest_level["date"])

    start_date: pd.Timestamp = lowest_level["date"].min()
    end_date: pd.Timestamp = lowest_level["date"].max()

    res = lowest_level.copy()

    if "period_level" not in res.columns:
        res["period_level"] = 1

    # if metric.is_daily:
    #     # Not sure what to put inside here
    #     assert start_date.day == 1, "Start date should be the 1st of the month for daily data."
    #     assert end_date.is_month_end, "End date should be the last day of the month for daily data."

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

    with SessionLocal() as session:
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


def aggregate_metric_by_time_period(_metric_id: int, _method: str) -> pd.DataFrame:
    """
    Aggregates daily data into monthly, quarterly, or yearly totals,
    depending on the flags in the metric associated with the given metric_id.
    Appends the aggregated rows to the original DataFrame ('res') and returns
    the combined result.

    :param metric_id: The ID of the metric to process.
    :param _method: The name of a pandas aggregation method (e.g. 'sum', 'mean').
    :param session: An active SQLAlchemy Session for database interaction.
    :return: A DataFrame with daily rows plus aggregated rows, each flagged
        by 'period_level' (1=daily, 2=monthly, 3=quarterly, 4=yearly).
    """
    LOGGER.info(f"Fetching daily data for metric_id: {_metric_id}")

    metric_id = int(_metric_id)
    LOGGER.info(f"Processing metric_id: {metric_id}")

    metric = get_metric_md(metric_id)

    if metric is None:
        LOGGER.error(f"Metric with id {metric_id} not found.")
        raise ValueError(f"Metric with id {metric_id} not found.")

    if metric.is_daily:
        min_level = 1
    elif metric.is_monthly:
        min_level = 2
    elif metric.is_quarterly:
        min_level = 3
    elif metric.is_yearly:
        min_level = 4
    else:
        raise ValueError(f"Metric {metric_id} has no granularity flags set.")

    with SessionLocal() as session:
        deleted = (
            session.query(Facts)
            # anything above the base level
            .filter(Facts.metric_id == metric_id, Facts.period_level > min_level)
            .delete(synchronize_session=False)
        )
        session.commit()
    LOGGER.info(
        f"Deleted {deleted} stale rows (period_level >{min_level}) for metric_id={metric_id}")

    with SessionLocal() as session:
        lowest_level = query_facts(
            session=session, metric_id=metric_id, period_level=min_level)

    if lowest_level.empty:
        LOGGER.warning(f"No daily data found for metric_id: {metric_id}")
        return pd.DataFrame(columns=["date", "value", "metric_id", "group_name", "period_level"])

    if not pd.api.types.is_datetime64_any_dtype(lowest_level["date"]):
        # Assuming YYYYMMDD format if not datetime
        try:
            lowest_level["date"] = pd.to_datetime(
                lowest_level["date"], format="%Y%m%d", errors="raise")
        except ValueError:
            try:
                lowest_level["date"] = pd.to_datetime(
                    lowest_level["date"], errors="raise")
            except ValueError as e:
                LOGGER.error(
                    f"Failed to convert 'date' column to datetime: {e}")
                raise

    LOGGER.info(f"Processing lowest level {lowest_level.shape}")

    res = lowest_level.copy()

    if "period_level" not in res.columns:
        res["period_level"] = 1

    if metric.is_monthly:
        LOGGER.info(f"Aggregating metric_id {metric_id} at monthly level.")

        lowest_level["month_start"] = lowest_level["date"].dt.to_period(
            "M").dt.start_time

        monthly_agg = (
            lowest_level.groupby(["group_name", "month_start"], dropna=False).agg(
                {"value": _method}).reset_index()
        )
        monthly_agg["date"] = monthly_agg["month_start"].dt.strftime("%Y%m%d")
        monthly_agg.drop(columns=["month_start"], inplace=True)
        monthly_agg["period_level"] = 2
        res = pd.concat([res, monthly_agg], ignore_index=True)
        LOGGER.info(
            f"Aggregated {monthly_agg.shape[0]} rows at monthly level.")

    if metric.is_quarterly:
        LOGGER.info(f"Aggregating metric_id {metric_id} at quarterly level.")
        lowest_level["quarter_start"] = lowest_level["date"].dt.to_period(
            "Q").dt.start_time
        quarterly_agg = (
            lowest_level.groupby(["group_name", "quarter_start"], dropna=False).agg(
                {"value": _method}).reset_index()
        )
        quarterly_agg["date"] = quarterly_agg["quarter_start"].dt.strftime(
            "%Y%m%d")
        quarterly_agg.drop(columns=["quarter_start"], inplace=True)
        quarterly_agg["period_level"] = 3
        res = pd.concat([res, quarterly_agg], ignore_index=True)
        LOGGER.info(
            f"Aggregated {quarterly_agg.shape[0]} rows at quarterly level.")

    if metric.is_yearly:
        LOGGER.info(f"Aggregating metric_id {metric_id} at yearly level.")
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
        LOGGER.info(f"Aggregated {yearly_agg.shape[0]} rows at yearly level.")

    res["metric_id"] = metric_id
    return res


def aggregate_metric_by_group_hierachy(_metric_id: int, _method: str) -> pd.DataFrame:
    """
    Query all data for the given metric ID from the 'facts' table
    and compute aggregated values for 'ALL', camp level, and store format level
    for each time dimension (date, period_level).
    """

    LOGGER.info(
        f"Aggregating metric_id {_metric_id} by group hierarchy with method '{_method}'")
    _metric_id = int(_metric_id)

    with SessionLocal() as session:
        deleted = (
            session.query(Facts)
            # non‑numeric ⇒ NULL
            .filter(Facts.metric_id == _metric_id, cast(Facts.group_name, Integer).is_(None))
            .delete(synchronize_session=False)
        )
        session.commit()

    LOGGER.info(
        f"Deleted {deleted} facts with non‑numeric group_name for metric_id={_metric_id}")
    # 1. Query the existing facts records for our given metric_id:
    with SessionLocal() as session:
        df_facts = query_facts(session=session, metric_id=_metric_id)

    if df_facts.empty:
        # If there's no data for this metric, return an empty DataFrame
        LOGGER.warning(f"No facts found for metric_id = {_metric_id}")
        return pd.DataFrame(columns=["metric_id", "group_name", "value", "date", "period_level"])

    # Ensure that 'date' is a datetime type
    df_facts["date"] = pd.to_datetime(df_facts["date"], errors="coerce")

    # Initialize a list to store the aggregated DataFrames
    aggregated_dfs = []

    # --- Aggregate at the 'all' level ---
    LOGGER.info(f"Aggregating metric_id {_metric_id} at 'all' level.")
    grouped_all = df_facts.groupby(["metric_id", "date", "period_level"], dropna=False, as_index=False).agg(
        {"value": _method}
    )
    grouped_all["group_name"] = "all"
    aggregated_dfs.append(grouped_all)

    # --- Aggregate at the camp level ---
    LOGGER.info(f"Aggregating metric_id {_metric_id} at camp level.")
    with SessionLocal() as session:
        # Fetch site to camp mapping
        sites = getSites(session=session)
        site_camp_map = {
            site.site_id: site.command_name for site in sites if site.command_name}

        # Filter for rows where group_name can be converted to integer (site_id)
        df_camp_eligible = df_facts[pd.to_numeric(
            df_facts["group_name"], errors="coerce").notna()].copy()
        df_camp_eligible["site_id"] = df_camp_eligible["group_name"].astype(
            int)

        # Map camp names to the filtered DataFrame
        df_camp_eligible["camp_name"] = df_camp_eligible["site_id"].map(
            site_camp_map)
        df_camp = df_camp_eligible.dropna(subset=["camp_name"])

        if not df_camp.empty:
            grouped_camp = df_camp.groupby(
                ["metric_id", "camp_name", "date", "period_level"], dropna=False, as_index=False
            ).agg({"value": _method})
            grouped_camp.rename(
                columns={"camp_name": "group_name"}, inplace=True)
            aggregated_dfs.append(grouped_camp)
        else:
            LOGGER.info(
                f"No camp information found for metric_id {_metric_id}.")

    # --- Aggregate at the store format level ---
    LOGGER.info(f"Aggregating metric_id {_metric_id} at store format level.")
    with SessionLocal() as session:
        # Fetch site to store format mapping
        sites = getSites(session=session)
        site_store_format_map = {
            site.site_id: site.store_format for site in sites if site.store_format}

        # Filter for rows where group_name can be converted to integer (site_id)
        df_store_format_eligible = df_facts[pd.to_numeric(
            df_facts["group_name"], errors="coerce").notna()].copy()
        df_store_format_eligible["site_id"] = df_store_format_eligible["group_name"].astype(
            int)

        # Map store formats to the filtered DataFrame
        df_store_format_eligible["store_format"] = df_store_format_eligible["site_id"].map(
            site_store_format_map)
        df_store_format = df_store_format_eligible.dropna(
            subset=["store_format"])

        if not df_store_format.empty:
            grouped_store_format = df_store_format.groupby(
                ["metric_id", "store_format", "date", "period_level"], dropna=False, as_index=False
            ).agg({"value": _method})
            grouped_store_format.rename(
                columns={"store_format": "group_name"}, inplace=True)
            aggregated_dfs.append(grouped_store_format)
        else:
            LOGGER.info(
                f"No store format information found for metric_id {_metric_id}.")

    # 5. Concatenate all aggregated DataFrames:
    if aggregated_dfs:
        final_df = pd.concat(aggregated_dfs, ignore_index=True)

        # 6. Rearrange columns to match the Facts schema order:
        final_df = final_df[["metric_id", "group_name",
                             "value", "date", "period_level"]]
        return final_df
    else:
        return pd.DataFrame(columns=["metric_id", "group_name", "value", "date", "period_level"])
