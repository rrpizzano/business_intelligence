import os
import json
import requests
from pathlib import Path
from google.cloud import bigquery
from datetime import datetime

API_ENDPOINT = 'https://rest...' + '/users/track'
API_KEY = 'f305...'

base_path = Path(__file__).resolve().parent
gcp_json = os.path.join(base_path, 'config/gcp.json')
gcp_file = json.load(open(gcp_json))

BQ_CLIENT = bigquery.Client.from_service_account_json(gcp_json)
SQL = f"""
    SELECT * FROM `peya-data-dev-tools-stg.external_process.braze_daily_data_test`
    WHERE external_id IN ('21102000000002','21102000000003')
    ORDER BY row_num
"""
SQL_dataframe = BQ_CLIENT.query(SQL).to_dataframe()
SQL_json = json.loads(SQL_dataframe.to_json(orient='records'))

for row in SQL_json:

    payload = json.dumps({
        'attributes': [row],
        'events': [{}],
        'purchases': [{}]
    })

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(API_KEY)
    }

    response = requests.request(
        'POST', API_ENDPOINT, headers=headers, data=payload)

    if json.loads(response.text)['message'] == 'success':
        print('OK >> {}/{}/row_{} - external_id {} loaded in Braze'.format(
            datetime.now().strftime("%H:%M:%S"), row['table_partition'], row['row_num'], row['external_id']))
    else:
        print('FAIL >> {}/{}/row_{} - external_id {} NOT loaded in Braze'.format(
            datetime.now().strftime("%H:%M:%S"), row['table_partition'], row['row_num'], row['external_id']))
