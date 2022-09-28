import os
import json
from pathlib import Path
from google.cloud import bigquery
from tabulate import tabulate

# CONFIG FILES
base_path = Path(__file__).resolve().parent.parent
gcp_json = os.path.join(base_path, 'config/gcp_pro.json')
gcp_file = json.load(open(gcp_json))

# GCP CLIENT
BQ_client = bigquery.Client.from_service_account_json(gcp_json)

# QUERY
SQL_table = """
    SELECT * FROM `peya-data-dev-tools-pro.external_process.test_table`
"""
# DATAFRAME
table_dataframe_raw = BQ_client.query(SQL_table).to_dataframe()
print(tabulate(table_dataframe_raw, headers='keys', tablefmt='psql'))

table_dataframe = table_dataframe_raw.loc[:, ~table_dataframe_raw.columns.isin(
    ['columna_2', 'columna_4'])]
print(tabulate(table_dataframe, headers='keys', tablefmt='psql'))
