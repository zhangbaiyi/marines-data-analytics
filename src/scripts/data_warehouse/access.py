from datetime import date, timedelta
from typing import Dict, List, Optional


import pandas as pd
from sqlalchemy import and_, union
from sqlalchemy.orm import Session as SessionClass

from src.scripts.data_warehouse.models.warehouse import Camps, Facts, Metrics, SessionLocal, Sites
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
    """
    Transform a fact table into the nested dict structure:
    result → {retail | survey | marketing} → metric_id → site/group → date_range → value

    Notes
    -----
    * A metric can live in only one category; precedence is retail > survey > marketing.
      (Adjust the precedence list if your schema allows overlaps.)
    * Unknown / uncategorised metrics go into an "other" bucket so nothing is lost.
    * Site and metric metadata are cached to minimise round-trips.
    """
    # ── Basic guards & setup ───────────────────────────────────────────────────────────
    df = df.drop(columns=["record_inserted_date", "id"], errors="ignore")
    LOGGER.info(df)

    nested_result = {"result": {"retail": {}, "survey": {}, "marketing": {}, "other": {}}}
    if df.empty:
        LOGGER.error("Empty DataFrame")
        return nested_result

    # ── Pre-compute the human-readable date_range ──────────────────────────────────────
    df["date_range"] = df.apply(
        lambda x: get_date_range_by_datekey(x["period_level"], x["date"]), axis=1
    )
    df = df.drop(columns=["date", "period_level"])

    # ── Tiny caches to avoid repeated look-ups ─────────────────────────────────────────
    metric_cache: dict[int, Any] = {}
    site_cache: dict[str, Any] = {}

    # ── Row-wise build of the nested structure ────────────────────────────────────────
    for row in df.itertuples(index=False):
        metric_id   = row.metric_id
        group_name  = row.group_name
        date_range  = row.date_range
        value       = row.value

        # ── Metric metadata & category -------------------------------------------------
        if metric_id not in metric_cache:
            metric_meta = getMetricByID(session=session, metric_id=metric_id)
            metric_cache[metric_id] = metric_meta
        else:
            metric_meta = metric_cache[metric_id]

        # Decide which top-level bucket the metric belongs to
        if getattr(metric_meta, "is_retail", False):
            category = "retail"
        elif getattr(metric_meta, "is_survey", False):
            category = "survey"
        elif getattr(metric_meta, "is_marketing", False):
            category = "marketing"
        else:
            category = "other"

        cat_dict = nested_result["result"][category]

        # ── Initialise metric node if needed ------------------------------------------
        if metric_id not in cat_dict:
            cat_dict[metric_id] = {"metadata": metric_meta}

        metric_dict = cat_dict[metric_id]

        # ── Site metadata --------------------------------------------------------------
        if group_name not in metric_dict:
            if group_name not in site_cache:
                site_cache[group_name] = getSiteByID(session=session, site_id=group_name)
            metric_dict[group_name] = {"metadata": site_cache[group_name]}

        # ── Assign the actual datapoint -----------------------------------------------
        metric_dict[group_name][date_range] = value

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
    
# def build_markdown(session, year: int, month: int) -> str:
#     """Return a Markdown string for the **Retail** category.

#     Parameters
#     ----------
#     session : sqlalchemy.orm.Session
#         Active DB session.
#     year : int
#         Calendar year (e.g. ``2024``)
#     month : int
#         Calendar month 1–12
#     """

#     # ── Month helpers ──────────────────────────────────────────────────────
#     month_start = date(year, month, 1)
#     month_end   = date(year, month, calendar.monthrange(year, month)[1])
#     month_name  = month_start.strftime("%B %Y")  # e.g. "May 2025"

#     prev_year   = year if month > 1 else year - 1
#     prev_month  = month - 1 if month > 1 else 12
#     prev_start  = date(prev_year, prev_month, 1)

#     # ── 1. Total revenue & MoM change ─────────────────────────────────────
#     revenue_df = query_facts(
#         session=session,
#         metric_id=1,
#         group_name="all",
#         period_level=2,
#         exact_date=month_start,
#     )
#     total_revenue = revenue_df["value"].sum() if not revenue_df.empty else 0.0

#     prev_rev_df = query_facts(
#         session=session,
#         metric_id=1,
#         group_name="all",
#         period_level=2,
#         exact_date=prev_start,
#     )
#     prev_revenue = prev_rev_df["value"].sum() if not prev_rev_df.empty else 0.0

#     pct_change = (100 * (total_revenue - prev_revenue) / prev_revenue) if prev_revenue else 0.0

#     # ── 2. Top‑5 Marine Marts by units sold ───────────────────────────────
#     sites         = getSites(session)
#     mart_site_ids = [s.site_id for s in sites if getattr(s, "store_format", "").upper() == "MARINE MART"]

#     # Map *stringified* site_id → site.name to avoid dtype mismatches
#     id_to_name = {str(s.site_id): s.site_name.replace("MARINE MART", "") for s in sites}

#     units_df = query_facts(
#         session=session,
#         metric_id=2,
#         group_names=mart_site_ids,
#         period_level=2,
#         exact_date=month_start,
#     )
#     if not units_df.empty:
#         # Ensure group_name is str before mapping
#         units_df["site_name"] = (
#             units_df["group_name"].astype(str).map(id_to_name).fillna(units_df["group_name"].astype(str))
#         )
#         top5_df = units_df.sort_values("value", ascending=False).head(5)
#         top5_md = "\n".join(
#             [f"   * {row.site_name} — {int(row.value):,} units" for row in top5_df.itertuples(index=False)]
#         )
#     else:
#         top5_md = "   * _No data available_"

#     # ── 3. Day with highest Average Order Value ───────────────────────────
#     aov_df = query_facts(
#         session=session,
#         metric_id=4,
#         group_name="all",
#         period_level=1,
#         date_from=month_start,
#         date_to=month_end,
#     )
#     if not aov_df.empty:
#         best_row   = aov_df.loc[aov_df["value"].idxmax()]
#         best_day   = best_row["date"].strftime("%B %d %Y")
#         best_aov   = best_row["value"]
#     else:
#         best_day, best_aov = "-", 0.0

#     # ── 4. Camp with least returned transactions (metric_id=2 per spec) ───
#     camps           = getCamps(session)
#     camp_names      = [c.name for c in camps]
#     returns_df      = query_facts(
#         session=session,
#         metric_id=2,  # Note: adjust to 6 if "return transactions" metric available
#         group_names=camp_names,
#         period_level=2,
#         exact_date=month_start,
#     )
#     if not returns_df.empty:
#         min_row     = returns_df.loc[returns_df["value"].idxmin()]
#         worst_camp  = min_row["group_name"]
#         return_cnt  = int(min_row["value"])
#     else:
#         worst_camp, return_cnt = "-", 0

#     # ── Compose markdown ──────────────────────────────────────────────────
#     change_word = "increase" if pct_change > 0 else "decrease" if pct_change < 0 else "change"
#     markdown = (
#         f"# MCCS Data Analytics – {month_name}\n\n"
#         f"## Retail\n\n"
#         f"1. In this month, the total revenue is **${total_revenue:,.2f}**, which is **{pct_change:+.1f}%** {change_word} from last month.\n\n"
#         f"2. These 5 Marine Marts sold the most items:\n{top5_md}\n\n"
#         f"3. Of all days in this month, **{best_day}** had the largest average order value at **${best_aov:,.2f}**.\n\n"
#         f"4. Among all camps, **{worst_camp}** recorded the fewest returned transactions with **{return_cnt:,}**.\n"
#     )

#     LOGGER.info("Markdown built successfully")
#     return markdown

# if __name__ == "__main__":
#     # Example usage
#     with SessionLocal() as session:
#         md = build_markdown(session=session, year = 2024, month=5)
#         print(md)
    
