

from typing import Optional, List
from datetime import date
from src.utils.logging import LOGGER

from sqlalchemy.orm import Session as SessionClass
from sqlalchemy import and_
from src.scripts.data_warehouse.models.warehouse import Facts, Session

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
) -> List[Facts]:

    # Ensure at least one metric ID is provided
    if metric_id is None and (not metric_ids or len(metric_ids) == 0):
        raise ValueError("At least one metric ID is required (metric_id or metric_ids).")

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

    # Combine all conditions using AND logic
    if conditions:
        query = query.filter(and_(*conditions))

    return query.all()


if __name__ == "__main__":
    session = Session()
    LOGGER.info("""What is the total sales of 
                    HHM MCX MAIN STORE (Henderson Hall Main Store) in 
                        December 2024? """)
    LOGGER.info(query_facts(session=session, metric_id=1, group_names=['1100'], period_levels=[2], exact_date=date(2024,12,1)))
    LOGGER.info("""What is the total sales of 
                    HHM MCX MAIN STORE (Henderson Hall Main Store) 
                        in the first three days of 2025? """)
    LOGGER.info(query_facts(session=session, metric_id=1, group_names=['1100'], period_levels=[2], date_from=date(2025,1,1), date_to=date(2025,1,3)))
    LOGGER.info("""What is the total sales of 
                                        HHM MCX MAIN STORE (Henderson Hall Main Store) 
                          compared with CLM MCX MAIN STORE (Camp Lejelle Main Store)
                                                 On January 1st 2025? """)
    LOGGER.info(query_facts(session=session, metric_id=1, group_names=['1100','5100'], period_levels=[2], exact_date=date(2025,1,1)))
    LOGGER.info("""What is the total sales of 
                        HHM MCX MAIN STORE (Henderson Hall Main Store) 
                                    On 4Q24 and 1Q25? """)
    LOGGER.info(query_facts(session=session, metric_id=1, group_names=['1100'], period_levels=[3], date_from=date(2024,10,1), date_to=date(2025,1,1)))
    LOGGER.info("""What is the total sales 
                           and [Another metric] of 
                                    HHM MCX MAIN STORE (Henderson Hall Main Store) 
                                        On Jan 1st 2025? """)
    LOGGER.info(query_facts(session=session, metric_ids=[1,2], group_names=['1100'], period_levels=[1], exact_date=date(2025,1,1)))
    query_facts(session=session)