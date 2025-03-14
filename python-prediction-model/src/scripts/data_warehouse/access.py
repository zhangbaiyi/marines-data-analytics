

from typing import Optional, List
from datetime import date

import pandas as pd
from sqlalchemy.orm import Session as SessionClass
from sqlalchemy import and_
from src.scripts.data_warehouse.models.warehouse import Facts

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

    rows = query.all()

    columns = [column.name for column in Facts.__table__.columns]
    data = []
    for row in rows:
        data.append([getattr(row, col) for col in columns])

    df = pd.DataFrame(data, columns=columns)

    return df


