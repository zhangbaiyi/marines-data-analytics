import sqlite3

import pandas as pd

from src.scripts.data_warehouse.utils import (
    aggregate_metric_by_group_hierachy,
    aggregate_metric_by_time_period,
    get_metric_1_lowest_level,
    insert_facts_from_df,
)
from src.utils.logging import LOGGER

df = pd.read_parquet("./data-lake/MCCS_RetailData.parquet")
site = df[["SITE_ID", "SITE_NAME", "COMMAND_NAME", "STORE_FORMAT"]].copy()
site.drop_duplicates(inplace=True)
site.reset_index(drop=True, inplace=True)
site.to_csv("site.csv")

conn = sqlite3.connect("./python-prediction-model/src/db/database.sqlite3")
site.to_sql(name="sites", con=conn, if_exists="append", index=False)

df_metrics = pd.read_csv(
    "./python-prediction-model/src/scripts/data_warehouse/metrics.csv")
df_metrics.to_sql(name="metrics", con=conn, if_exists="append", index=False)


df = get_metric_1_lowest_level()
LOGGER.info(df.dtypes)
LOGGER.info(df.shape)
LOGGER.info(df.head(10))
df = aggregate_metric_by_time_period(
    _metric_id=1, lowest_level=df, _method="sum")
LOGGER.info(df.dtypes)
LOGGER.info(df.shape)
LOGGER.info(df.tail(10))
num_inserted = insert_facts_from_df(df)
LOGGER.info(f"Inserted {num_inserted} rows into 'facts' table")
LOGGER.info(aggregate_metric_by_group_hierachy(_metric_id=1, _method="sum"))
LOGGER.info(insert_facts_from_df(
    df_facts=aggregate_metric_by_group_hierachy(_metric_id=1, _method="sum")))
