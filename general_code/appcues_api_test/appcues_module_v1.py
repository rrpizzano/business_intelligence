def send_table_data_to_appcues(project, dataset, table, splits, split_selected):
    """
    Sends via API the information of a BigQuery table to the wizard Appcues in Vendor Portal (used by the Comms team)

    Attributes
    ----------
    project : string
        BigQuery datasource project name
    dataset : string
        BigQuery datasource dataset name
    table : string 
        BigQuery datasource table name
    splits : integer
        Indicates the total of splits of the table, more splits less time running
    split_selected: integer
        Indicates the split part that we want to select to send the data
    """
    import json
    import requests
    import numpy as np
    import os
    from pathlib import Path
    from datetime import datetime
    from google.cloud import bigquery
    from google.oauth2 import service_account
    # from airflow.models import Variable
    # from airflow.hooks.base_hook import BaseHook

    # Creacion cliente BigQuery (Airflow)
    """
    connection = BaseHook.get_connection('gcp-connection-in-aiflow')
    connection_extra = json.loads(connection.extra)
    gcp_json = json.loads(
        connection_extra['extra__google_cloud_platform__keyfile_dict'])
    gcp_project_id = gcp_json['project_id']
    credentials = service_account.Credentials.from_service_account_info(
        gcp_json)
    try:
        BQ_CLIENT = bigquery.Client(
            credentials=credentials, project=gcp_project_id)
    except Exception as err:
        raise Exception(f'FAIL: BQ client error, exception was {str(err)}')
    """
    # Creacion cliente Bigquery (Local)
    base_path = Path(__file__).resolve().parent
    gcp_json = os.path.join(base_path, 'config/gcp_pro.json')
    try:
        BQ_CLIENT = bigquery.Client.from_service_account_json(gcp_json)
    except Exception as err:
        raise Exception(f'FAIL: BQ client error, exception was {str(err)}')

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
    SQL_json = json.loads(SQL_dataframe.to_json(orient='records'))

    # Enviamos la data del fragmento elegido a traves de la API de Appcues
    # appcues_keys = Variable.get('appcues_credentials', deserialize_json=True) # Airflow
    appcues_keys = json.load(
        open('general_code/appcues_api_test/config/appcues_keys.json'))

    for row in SQL_json:
        try:
            response = requests.patch(
                f"{appcues_keys['endpoint']}/{row['user_id']}/profile",
                headers={'Content-type': 'application/json'},
                data=str(json.dumps(row)),
                auth=(appcues_keys['key'], appcues_keys['secret'])
            )
            response_json = json.loads(response.content)
            if response_json['status'] == 200:
                print(
                    f"OK: {datetime.now().strftime('%H:%M:%S')} | 'split: '{split_selected} | row: {row['row_num']} | user_id: {row['user_id']} loaded in Appcues")
            elif response_json['status'] != 200:
                print(
                    f"FAIL: {datetime.now().strftime('%H:%M:%S')} | 'split: '{split_selected} | row: {row['row_num']} | user_id: {row['user_id']} NOT loaded in Appcues")
        except Exception as err:
            raise Exception(f"FAIL - exception is {str(err)}")


send_table_data_to_appcues(
    project='peya-data-dev-tools-pro',
    dataset='external_process',
    table='appcues_data_test',
    splits=3,
    split_selected=0
)

send_table_data_to_appcues(
    project='peya-data-dev-tools-pro',
    dataset='external_process',
    table='appcues_data_test',
    splits=3,
    split_selected=1
)
