import os
import json
from pathlib import Path
from google.cloud import bigquery
import numpy as np

# BigQuery Client
base_path = Path(__file__).resolve().parent.parent
gcp_json = os.path.join(base_path, 'config/gcp_pro.json')
bq_client = bigquery.Client.from_service_account_json(gcp_json)

# Read Bigquery Table
sql = f"""
    SELECT 
        *
    FROM `peya-data-dev-tools-pro.external_process.braze_test_vendor_portal`
"""
table_dataframe = bq_client.query(sql).to_dataframe()

table_dataframe_chuncks_1 = np.array_split(table_dataframe, 2)
print(table_dataframe_chuncks_1[0])
print(table_dataframe_chuncks_1[1])

table_dataframe_chuncks_2 = np.array_split(table_dataframe, 2)
print(table_dataframe_chuncks_2[0])
print(table_dataframe_chuncks_2[1])
