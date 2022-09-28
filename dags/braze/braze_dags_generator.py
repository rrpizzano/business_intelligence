from airflow import DAG
from datetime import datetime, timedelta
import os
import json


def create_dag(dag_id, schedule_interval, api_credentials_in_airflow, project, dataset, table, splits):
    """
    Creates a DAG for the given attributes
    The attributes are loaded in a json file in the same folder as this script
    Attributes
    ----------
    dag_id: string
        Name of the Airflow dag
    schedule_interval: Cron schedule expressions (Ex. 30 12 * * *)
        Schedule interval for the dag execution
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
    import importlib

    braze_module = importlib.import_module(
        "dags.utils.braze_module")
    slack_module = importlib.import_module(
        "dags.utils.slack_module")
    sensor_module = importlib.import_module(
        "dags.utils.sensor_module")

    # Configuracion DAG
    config = dict(
        dag_id=dag_id,
        default_args=dict(
            owner='rodrigo_pizzano',
            depends_on_past=False,
            start_date=datetime(2022, 9, 1),
            execution_timeout=timedelta(minutes=60),
            email='rodrigo.pizzano@outlook.com',
            email_on_failure=True,
            email_on_retry=False,
            retries=3,
            retry_delay=timedelta(minutes=10),
        ),
        catchup=False,
        schedule_interval=schedule_interval,
    )

    with DAG(**config) as dag:

        # Operadores
        start = sensor_module.dummy('start')

        table_sensor = sensor_module.get_table_etl_status_operator(
            project=project,
            dataset=dataset,
            table=table
        )

        braze_send_tasks = []
        for i in range(splits):
            braze_task = braze_module.send_table_data_to_braze_operator(
                task_id=f'split_{i}',
                api_credentials_in_airflow=api_credentials_in_airflow,
                project=project,
                dataset=dataset,
                table=table,
                splits=splits,
                split_selected=i
            )
            braze_send_tasks.append(braze_task)

        end = sensor_module.dummy('end')

        slack_ok = slack_module.slack_ok(dag=dag)

        slack_fail = slack_module.slack_fail(dag=dag)

        # Orden de operadores
        start >> table_sensor >> braze_send_tasks >> end >> [
            slack_ok, slack_fail]

    return dag


for file in os.listdir("/dags/braze"):
    if os.path.splitext(file)[1] == '.json':
        with open(f"/dags/braze/{file}", 'r') as f:
            file_json = json.loads(f.read())

            dag_id = file_json['dagId']
            schedule_interval = file_json['scheduleInterval']
            api_credentials_in_airflow = file_json['apiCredentialsInAirflow']
            project = file_json['project']
            dataset = file_json['dataset']
            table = file_json['table']
            splits = file_json['splits']

        globals()[dag_id] = create_dag(
            dag_id,
            schedule_interval,
            api_credentials_in_airflow,
            project,
            dataset,
            table,
            splits
        )
