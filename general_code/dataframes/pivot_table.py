import os
import json
from pathlib import Path
from google.cloud import bigquery
from sqlalchemy import desc
from tabulate import tabulate

# CONFIG FILES
base_path = Path(__file__).resolve().parent
gcp_json = os.path.join(base_path, 'config/gcp.json')
gcp_file = json.load(open(gcp_json))

# GCP CLIENT
BQ_client = bigquery.Client.from_service_account_json(gcp_json)

# QUERY
SQL_table = """
    SELECT *
    FROM `big-query-project.big_query_dataset.big_query_table`
    WHERE Country IN ('Argentina','Chile')
"""

# DATAFRAME
table_dataframe_raw = BQ_client.query(SQL_table).to_dataframe()
table_dataframe_raw.to_csv('./general_code/temporal_folder/original_csv.csv')

cols_order = table_dataframe_raw['Measure_Names'].unique().tolist()

cols = [c for c in table_dataframe_raw.columns.values if c not in (
    'Measure_Values', 'Measure_Names')]

table_dataframe = table_dataframe_raw.pivot(
    index=cols, columns=['Measure_Names'], values='Measure_Values')[cols_order]

table_dataframe.sort_values(cols)

table_dataframe.to_csv(
    './general_code/dataframes/temporal_folder/pivot_csv.csv')
print('OK CSV')


#print(tabulate(table_dataframe, headers='keys', tablefmt='psql'))
