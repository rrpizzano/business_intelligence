def send_table_data_to_braze(api_credentials_in_airflow, project, dataset, table, splits, split_selected):
    """
    Sends via API the information of a BigQuery table to the website Braze (used by the Comms team)
    Attributes
    ----------
    api_credentials_in_airflow : string
        Indicates de name of the Airflow variable that has the credentials for the API
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
    from datetime import datetime
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from braze.client import BrazeClient
    import numpy as np
    from airflow.models import Variable
    from airflow.hooks.base_hook import BaseHook

    # Creacion cliente BigQuery
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

    # Creacion cliente Braze
    try:
        BRAZE_CONFIG = Variable.get(
            f'{api_credentials_in_airflow}', deserialize_json=True)
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


def send_table_data_to_braze_operator(task_id, api_credentials_in_airflow, project, dataset, table, splits, split_selected):
    """
    Returns an Airflow PythonVirtualenvOperator using the function 'send_table_data_to_braze'
    Requires the install of the braze-client v2.3.1 package
    Attributes
    ----------
    task_id : string
        Name of the Airflow task
    api_credentials_in_airflow : string
        Indicates de name of the Airflow variable that has the credentials for the API
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
    from airflow.operators.python_operator import PythonVirtualenvOperator
    return PythonVirtualenvOperator(
        task_id=task_id,
        python_callable=send_table_data_to_braze,
        op_kwargs=dict(
            api_credentials_in_airflow=api_credentials_in_airflow,
            project=project,
            dataset=dataset,
            table=table,
            splits=splits,
            split_selected=split_selected
        ),
        requirements=[
            "braze-client==2.3.1"
        ]
    )
