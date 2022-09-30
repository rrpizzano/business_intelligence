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
    import shutil
    from pathlib import Path
    from datetime import datetime
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from os.path import exists
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

    # Guardamos el fragmento de tabla en un archivo CSV
    if not exists(f'{base_path}/temp'):
        os.makedirs(f'{base_path}/temp')
    SQL_dataframe.to_csv(
        f'{base_path}/temp/data_to_send_plit_{split_selected}.csv', index=False)

    # Enviamos la data del CSV guardado a traves de la API de Appcues
    # appcues_keys = Variable.get('appcues_credentials', deserialize_json=True) # Airflow
    try:
        appcues_keys = json.load(
            open('general_code/appcues_api_test/config/appcues_keys.json'))
        files = {'file': (f'{base_path}/temp/data_to_send_plit_{split_selected}.csv',
                          open(f'{base_path}/temp/data_to_send_plit_{split_selected}.csv', 'rb')), }
        response = requests.post(
            'https://api.appcues.com/v2/accounts/ACCOUNT_ID/import/profiles',
            files=files,
            auth=(appcues_keys['key'], appcues_keys['secret'])
        )
        response_json = json.loads(response.content)
        if response_json['status'] == 202:
            print(
                f"OK CSV : 'data_to_send_plit_{split_selected}.csv' | API_status: {response_json['status']} | API_response: {response_json['title']}")
        elif response_json['status'] != 202:
            print(
                f"FAIL CSV : 'data_to_send_plit_{split_selected}.csv' | API_status: {response_json['status']} | API_response: {response_json['title']}")
    except Exception as err:
        raise Exception(f"FAIL - exception is {str(err)}")

    # Borramos la carpeta local temp y su contenido
    if exists(f'{base_path}/temp'):
        try:
            shutil.rmtree(f'{base_path}/temp')
            print(f'Temporal folder emptied and deleted: {base_path}/temp')
        except Exception as err:
            raise Exception(
                f'FAIL: Error while deleting the local folder, exception is: {str(err)}')


"""
send_table_data_to_appcues(
    project='peya-data-dev-tools-pro',
    dataset='external_process',
    table='appcues_data_test',
    splits=4,
    split_selected=0
)

send_table_data_to_appcues(
    project='peya-data-dev-tools-pro',
    dataset='external_process',
    table='appcues_data_test',
    splits=4,
    split_selected=1
)

send_table_data_to_appcues(
    project='peya-data-dev-tools-pro',
    dataset='external_process',
    table='appcues_data_test',
    splits=4,
    split_selected=2
)

send_table_data_to_appcues(
    project='peya-data-dev-tools-pro',
    dataset='external_process',
    table='appcues_data_test',
    splits=4,
    split_selected=3
)
"""

send_table_data_to_appcues(
    project='peya-delivery-and-support',
    dataset='automated_tables_reports',
    table='road_to_graduation_incubation',
    splits=10,
    split_selected=0
)
