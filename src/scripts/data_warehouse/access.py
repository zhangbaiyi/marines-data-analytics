from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import and_, union
from sqlalchemy.orm import Session as SessionClass

from src.scripts.data_warehouse.models.warehouse import Camps, Facts, Metrics, Sites
from src.utils.logging import LOGGER


def query_facts(
    session: SessionClass,
    metric_id: Optional[int] = None,
    metric_ids: Optional[List[int]] = None,
    group_name: Optional[str] = None,
    group_names: Optional[List[str]] = None,
    period_level: Optional[int] = None,
    period_levels: Optional[List[int]] = None,
    exact_date: Optional[date] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> pd.DataFrame:

    # Ensure at least one metric ID is provided
    if metric_id is None and (not metric_ids or len(metric_ids) == 0):
        raise ValueError(
            "At least one metric ID is required (metric_id or metric_ids).")

    query = session.query(Facts)
    conditions = []

    # Single metric_id
    if metric_id is not None:
        conditions.append(Facts.metric_id == metric_id)

    # Multiple metric_ids
    if metric_ids:
        conditions.append(Facts.metric_id.in_(metric_ids))

    # Single group_name
    if group_name is not None:
        conditions.append(Facts.group_name == group_name)

    # Multiple group_names
    if group_names:
        conditions.append(Facts.group_name.in_(group_names))

    # Single period_level
    if period_level is not None:
        conditions.append(Facts.period_level == period_level)

    # Multiple period_levels
    if period_levels:
        conditions.append(Facts.period_level.in_(period_levels))

    # Exact date
    if exact_date is not None:
        conditions.append(Facts.date == exact_date)

    # Date range
    if date_from is not None:
        conditions.append(Facts.date >= date_from)
    if date_to is not None:
        conditions.append(Facts.date <= date_to)

    LOGGER.info(
        f"Querying Facts with conditions: metric_id = [ {metric_id},  {metric_ids} ] \n group name = [ {group_name},  {group_names} ] \n period level = [ {period_level},  {period_levels} ] \n exact date = [ {exact_date} ] \n date from = [ {date_from} ] \n date to = [ {date_to} ] "
    )

    # Combine all conditions using AND logic
    if conditions:
        query = query.filter(and_(*conditions))

    rows = query.all()

    columns = [column.name for column in Facts.__table__.columns]
    data = []
    for row in rows:
        data.append([getattr(row, col) for col in columns])

    df = pd.DataFrame(data, columns=columns)
    LOGGER.info(f"Query returned {len(df)} rows.")
    return df


def get_date_range_by_datekey(period_level: int, datekey: date) -> str:
    """Given:
    # Period_id = 1 (daily level), datekey = 2024-12-31 -> return 20241231 to 20241231
    # Period_id = 2 (monthly level), datekey = 2024-12-01 (always will be the first day of the period) -> return 20241201 to 20241231
    # Period_id = 3 (quarterly level), dateley = 2024-10-01 (same as above, always first day) -> return 20241001 to 20241231
    """
    # Parse the incoming datekey string into a date object
    # dt = datetime.datetime.strptime(datekey, "%Y-%m-%d").date()
    dt = datekey
    # Helpers

    def end_of_month(any_date: date) -> date:
        # Move to the 28th (safe), then add 4 days (guaranteed next month),
        # then backtrack to get the last day of the original month
        next_month = any_date.replace(day=28) + timedelta(days=4)
        return next_month - timedelta(days=next_month.day)

    def end_of_quarter(any_date: date) -> date:
        # Identify which quarter we’re in: 1: Jan-Mar, 2: Apr-Jun, 3: Jul-Sep, 4: Oct-Dec
        # Quarter = (month-1)//3 + 1
        quarter = (any_date.month - 1) // 3 + 1

        # The next quarter starts after 3 months from the current quarter's start
        next_q_start_month = (quarter * 3) + 1
        next_q_start_year = any_date.year

        # If it spills over into the next year
        if next_q_start_month > 12:
            next_q_start_month -= 12
            next_q_start_year += 1

        # The last day of the current quarter is the day before the next quarter's start
        next_quarter_start = date(next_q_start_year, next_q_start_month, 1)
        return next_quarter_start - timedelta(days=1)

    def end_of_year(any_date: date) -> date:
        return date(any_date.year, 12, 31)

    # Compute start_dt and end_dt based on period_level
    if period_level == 1:
        # Daily => same day
        start_dt = dt
        end_dt = dt
    elif period_level == 2:
        # Monthly => first day to last day of the same month
        start_dt = dt
        end_dt = end_of_month(dt)
    elif period_level == 3:
        # Quarterly => from the first day of that quarter to the last day of that quarter
        start_dt = dt
        end_dt = end_of_quarter(dt)
    elif period_level == 4:
        # Quarterly => from the first day of that quarter to the last day of that quarter
        start_dt = dt
        end_dt = end_of_year(dt)
    else:
        # If you wanted to handle other levels, you could extend here.
        raise ValueError(f"Unsupported period level: {period_level}")

    # Format YYYYMMDD
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")
    return f"{start_str} to {end_str}"


def convert_jargons(df: pd.DataFrame, session: SessionClass):
    df = df.drop(axis=1, columns=["record_inserted_date", "id"])
    LOGGER.info(df)
    nested_result = {"result": {}}
    if len(df) == 0:
        LOGGER.error("Empty Dataframe")
        return nested_result
    df["date_range"] = df.apply(lambda x: get_date_range_by_datekey(
        x["period_level"], x["date"]), axis=1)
    df = df.drop(axis=1, columns=["date", "period_level"])
    df.groupby(by=["metric_id", "group_name", "date_range"])

    nested_result = {"result": {}}

    for row in df.itertuples(index=False):
        metric_id = row.metric_id
        group_name = row.group_name
        date_range = row.date_range
        value = row.value

        # If this metric_id is not in our nested dict yet, initialize it,
        # including "metadata" from get_metric_by_id
        if metric_id not in nested_result["result"]:
            nested_result["result"][metric_id] = {
                "metadata": getMetricByID(session=session, metric_id=metric_id)}

        # If this group_name is not in the metric’s dict yet, initialize it,
        # including "metadata" from get_site_by_id
        if group_name not in nested_result["result"][metric_id]:
            nested_result["result"][metric_id][group_name] = {
                "metadata": getSiteByID(session=session, site_id=group_name)
            }

        nested_result["result"][metric_id][group_name][date_range] = value

    return nested_result


def getMetricFromCategory(session: SessionClass, category: List[str]) -> List[int]:
    # If no category is specified, return all metrics
    if not category or "*" in category:
        all_ids = session.query(Metrics.id).all()
        return [metric_id for (metric_id,) in all_ids]

    # Build individual queries based on the category
    query_parts = []
    if "Retail" in category:
        query_parts.append(session.query(
            Metrics.id).filter(Metrics.is_retail == True))
    if "Email & Social Media" in category:
        query_parts.append(session.query(Metrics.id).filter(
            Metrics.is_marketing == True))
    if "Customer Survey" in category:
        query_parts.append(session.query(
            Metrics.id).filter(Metrics.is_survey == True))

    if not query_parts:
        return []

    # Union all queries into one
    combined_query = query_parts[0]
    for q in query_parts[1:]:
        combined_query = combined_query.union(q)

    # Execute and retrieve a unique list of IDs
    # returns list of tuples, e.g. [(1,), (2,), ...]
    results = combined_query.all()
    distinct_ids = [r[0] for r in results]
    return distinct_ids


def getSiteByID(session: SessionClass, site_id: int) -> Optional[Sites]:
    """
    Retrieve a Sites record by its site_id, using an existing session.

    :param session: An existing SQLAlchemy Session.
    :param site_id: ID of the site to retrieve.
    :return: A Sites object if found, otherwise None.
    """
    try:
        site = session.query(Sites).filter_by(site_id=site_id).first()
        return site
    except Exception as e:
        LOGGER.error(f"Error fetching site_id={site_id}: {e}")
        return None


def getMetricByID(session: SessionClass, metric_id: int) -> Optional[Dict[str, str]]:
    """
    Retrieve a Metrics record by its primary key ID, using an existing session.
    Return a dictionary with only 'metric_name' and 'metric_desc'.

    :param session: An existing SQLAlchemy Session.
    :param metric_id: ID of the metric to retrieve.
    :return: A dict with {'metric_name': ..., 'metric_desc': ...} if found, otherwise None.
    """
    try:
        metric = session.query(Metrics).filter_by(id=metric_id).first()
        if metric is None:
            return None

        # Return only the desired fields
        return {"metric_name": metric.metric_name, "metric_desc": metric.metric_desc, "id": metric.id}
    except Exception as e:
        LOGGER.error(f"Error fetching metric_id={metric_id}: {e}")
        return None


def getSites(session: SessionClass) -> List[Sites]:
    """
    Retrieve all Sites records from the database.

    :param session: An existing SQLAlchemy Session.
    :return: A list of Sites objects.
    """
    try:
        sites = session.query(Sites).all()
        return sites
    except Exception as e:
        LOGGER.error(f"Error fetching all sites: {e}")
        return []


def getCamps(session: SessionClass) -> List[Camps]:
    """
    Retrieve all Camps records from the database.

    :param session: An existing SQLAlchemy Session.
    :return: A list of Camps objects.
    """
    try:
        camps = session.query(Camps).all()
        return camps
    except Exception as e:
        LOGGER.error(f"Error fetching all camps: {e}")
        return []
