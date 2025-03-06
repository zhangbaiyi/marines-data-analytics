

from datetime import date, datetime
import sqlite3
import pandas as pd
from src.scripts.data_warehouse.models.warehouse import Session, Metrics, Facts
from src.utils.logging import LOGGER
from pandas.tseries.offsets import MonthEnd

def get_metric_md(metric_id: int):
    """
    Retrieves a single Metrics object from the database
    by its 'metric_id'. Returns None if not found.
    """
    with Session() as session:
        metric = session.query(Metrics).filter_by(id=metric_id).one_or_none()
        return metric

def get_metric_1_lowest_level() -> pd.DataFrame:
    """
    Testing purpose
    """
    conn = sqlite3.connect('./python-prediction-model/src/db/database.sqlite3')
    sql = """
    SELECT
        SITE_ID,
        EXTENSION_AMOUNT,
        SALE_DATE
    FROM sales
    """
    raw_df = pd.read_sql_query(sql, conn)
    conn.close()
    raw_df['SALE_DATE'] = pd.to_datetime(raw_df['SALE_DATE'], errors='coerce')
    df = (
        raw_df
        .groupby(['SALE_DATE', 'SITE_ID'], as_index=False)
        .agg({'EXTENSION_AMOUNT': 'sum'})
    )
    df.rename(columns={
        'SITE_ID': 'group_name',
        'EXTENSION_AMOUNT': 'value',
        'SALE_DATE': 'date'
    }, inplace=True)
    df['metric_id'] = 1
    df['period_level'] = 1
    
    df = df[['metric_id', 'group_name', 'value', 'date', 'period_level']]
    
    return df



def aggregate_metric(_metric_id: int, lowest_level: pd.DataFrame, _method: str) -> pd.DataFrame:
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
    metric = get_metric_md(metric_id=_metric_id)
    LOGGER.info(f"Metric Name: {metric.metric_name}")

    if not pd.api.types.is_datetime64_any_dtype(lowest_level['date']):
        lowest_level['date'] = pd.to_datetime(lowest_level['date'])

    start_date: pd.Timestamp = lowest_level['date'].min()
    end_date: pd.Timestamp = lowest_level['date'].max()

    res = lowest_level.copy()

    if 'period_level' not in res.columns:
        res['period_level'] = 1

    if metric.is_daily:
        # Not sure what to put inside here
        assert start_date.day == 1, "Start date should be the 1st of the month for daily data."
        assert end_date.is_month_end, "End date should be the last day of the month for daily data."

    if metric.is_monthly:
        # e.g., 2025-03-15 -> 2025-03-01 00:00:00
        lowest_level['month_start'] = lowest_level['date'].dt.to_period('M').dt.start_time

        monthly_agg = (
            lowest_level
            .groupby(['group_name', 'month_start'], dropna=False)
            .agg({'value': _method})
            .reset_index()
        )
        monthly_agg['date'] = monthly_agg['month_start'].dt.strftime('%Y%m01')
        monthly_agg.drop(columns=['month_start'], inplace=True)
        monthly_agg['period_level'] = 2
        res = pd.concat([res, monthly_agg], ignore_index=True)

    if metric.is_quarterly:
        # e.g., 2025-04-15 -> 2025-04-01, 2025-02-01 -> 2025-01-01
        lowest_level['quarter_start'] = lowest_level['date'].dt.to_period('Q').dt.start_time
        quarterly_agg = (
            lowest_level
            .groupby(['group_name', 'quarter_start'], dropna=False)
            .agg({'value': _method})
            .reset_index()
        )
        quarterly_agg['date'] = quarterly_agg['quarter_start'].dt.strftime('%Y%m01')
        quarterly_agg.drop(columns=['quarter_start'], inplace=True)
        quarterly_agg['period_level'] = 3
        res = pd.concat([res, quarterly_agg], ignore_index=True)

    if metric.is_yearly:
        # e.g., 2025-03-15 -> 2025-01-01
        lowest_level['year_start'] = lowest_level['date'].dt.to_period('Y').dt.start_time

        yearly_agg = (
            lowest_level
            .groupby(['group_name', 'year_start'], dropna=False)
            .agg({'value': _method})
            .reset_index()
        )
        yearly_agg['date'] = yearly_agg['year_start'].dt.strftime('%Y0101')
        yearly_agg.drop(columns=['year_start'], inplace=True)
        yearly_agg['period_level'] = 4
        res = pd.concat([res, yearly_agg], ignore_index=True)
    
    res['metric_id'] = _metric_id
    return res
        
def insert_facts_from_df(df_facts: pd.DataFrame):

    df_facts['date'] = pd.to_datetime(df_facts['date'], errors='coerce').dt.date
    records = df_facts.to_dict(orient="records")

    facts_objects = []
    for row in records:
        fact = Facts(
            metric_id=row["metric_id"],
            group_name=row["group_name"],
            value=row["value"],
            date=row["date"],
            period_level=row["period_level"],
        )
        facts_objects.append(fact)

    with Session() as session:
        session.add_all(facts_objects)
        session.commit()
    return len(facts_objects)


if __name__ == "__main__":
    df = get_metric_1_lowest_level()
    LOGGER.info(df.dtypes)
    LOGGER.info(df.shape)
    LOGGER.info(df.head(10))
    df = aggregate_metric(_metric_id=1, lowest_level=df, _method='sum')
    LOGGER.info(df.dtypes)
    LOGGER.info(df.shape)
    LOGGER.info(df.tail(10))

    num_inserted = insert_facts_from_df(df)
    LOGGER.info(f"Inserted {num_inserted} rows into 'facts' table")