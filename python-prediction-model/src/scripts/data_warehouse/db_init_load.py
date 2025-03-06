import sqlite3
import pandas as pd

df = pd.read_parquet('./data-lake/MCCS_RetailData.parquet')
site = df[['SITE_ID', 'SITE_NAME', 'COMMAND_NAME', 'STORE_FORMAT']].copy()
site.drop_duplicates(inplace=True)
site.reset_index(drop=True, inplace=True)
site.to_csv('site.csv')

conn = sqlite3.connect('./python-prediction-model/src/db/database.sqlite3')
site.to_sql(name='sites', con=conn, if_exists='replace', index=False)

df_metrics = pd.read_csv('./python-prediction-model/src/scripts/data_warehouse/metrics.csv')
df_metrics.to_sql(name='metrics', con=conn, if_exists='append', index=False)




