def send_table_data_to_braze(
    # api_credentials_in_airflow,
    project,
    dataset,
    table,
    splits,
    split_selected
):

    import json
    from datetime import datetime
    from google.cloud import bigquery
    from braze.client import BrazeClient
    import numpy as np
    import os  # local
    from pathlib import Path  # local
    # from airflow.models import Variable

    # Creacion cliente BigQuery
    try:
        # BQ_CLIENT = bigquery.Client() # Airflow
        base_path = Path(__file__).resolve().parent  # local
        gcp_json = os.path.join(base_path, 'config/gcp_pro.json')  # local
        BQ_CLIENT = bigquery.Client.from_service_account_json(
            gcp_json)  # local
    except Exception as err:
        raise Exception(f'FAIL: BQ client error, exception was {str(err)}')

    # Creacion cliente Braze
    try:
        # BRAZE_CONFIG = Variable.get(f'{api_credentials_in_airflow}', deserialize_json=True) # Airflow
        base_path = Path(__file__).resolve().parent  # local
        braze_json = os.path.join(base_path, 'config/braze.json')  # local
        BRAZE_CONFIG = json.load(open(braze_json))  # local
        BRAZE_API_URL = BRAZE_CONFIG['braze_api_url']
        BRAZE_API_KEY = BRAZE_CONFIG['braze_api_key']
        BRAZE_CLIENT = BrazeClient(
            api_key=BRAZE_API_KEY, api_url=BRAZE_API_URL, use_auth_header=True)
    except Exception as err:
        raise Exception(f'FAIL: Braze client error, exception was {str(err)}')

    # Leemos tabla de BigQuery, la dividimos y elegimos un fragmento
    SQL = f"""
        SELECT * FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER () row_num
            FROM `{project}.{dataset}.{table}`
        ) ORDER BY row_num
    """
    SQL_dataframe_raw = BQ_CLIENT.query(SQL).to_dataframe()
    SQL_dataframe_chunks = np.array_split(SQL_dataframe_raw, splits)
    SQL_dataframe = SQL_dataframe_chunks[split_selected]

    # Enviamos la data del fragmento elegido a traves de la API de Braze
    SQL_dataframe_json = json.loads(SQL_dataframe.to_json(orient='records'))
    for row in SQL_dataframe_json:
        try:
            request = BRAZE_CLIENT.user_track(
                attributes=[row],
                events=None,
                purchases=None
            )
            if request['success']:
                print(
                    f"OK: {datetime.now().strftime('%H:%M:%S')} | 'split: '{split_selected} | row: {row['row_num']} | external_id: {row['external_id']} loaded in Braze")
        except Exception as err:
            print(
                f"FAIL: {datetime.now().strftime('%H:%M:%S')} | 'split: '{split_selected} | row: {row['row_num']} | external_id: {row['external_id']} NOT loaded in Braze")


send_table_data_to_braze(
    project='big-query-project',
    dataset='big_query_dataset',
    table='big_query_table',
    splits=2,
    split_selected=0
)

send_table_data_to_braze(
    project='big-query-project',
    dataset='big_query_dataset',
    table='big_query_table',
    splits=2,
    split_selected=1
)
