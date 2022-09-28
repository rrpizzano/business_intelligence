import os
import json

from google.cloud import bigquery
from braze.client import BrazeClient
from pathlib import Path
from datetime import datetime

base_path = Path(__file__).resolve().parent
config_json = os.path.join(base_path, 'config/config.json')
config_file = json.load(open(config_json))
gcp_json = os.path.join(base_path, 'config/gcp.json')
gcp_file = json.load(open(gcp_json))

# BigQuery Client
bq_client = bigquery.Client.from_service_account_json(gcp_json)

# Braze Client
braze_client = BrazeClient(
    api_key=config_file['braze_api_key'], api_url=config_file['braze_api_url'], use_auth_header=True)

# Read Bigquery Table
sql = f"""
    SELECT * FROM `peya-data-dev-tools-stg.external_process.braze_daily_data_test`
    ORDER BY row_num
"""
table_dataframe = bq_client.query(sql).to_dataframe()
table_json = json.loads(table_dataframe.to_json(orient='records'))

# Send POST request to Braze API
for row in table_json:

    request = braze_client.user_track(
        attributes=[row],
        events=None,
        purchases=None
    )
    if request['success']:
        print('OK >> {}/{}/row_{} - external_id {} loaded in Braze'.format(
            datetime.now().strftime("%H:%M:%S"), row['table_partition'], row['row_num'], row['external_id']))
    else:
        print('FAIL >> {}/{}/row_{} - external_id {} NOT loaded in Braze'.format(
            datetime.now().strftime("%H:%M:%S"), row['table_partition'], row['row_num'], row['external_id']))
