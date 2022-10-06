from airflow import DAG
from datetime import datetime, timedelta
import os
import json


def create_dag(dag_id, schedule_interval, project, dataset, table, splits):
    """
    Creates a DAG for the given attributes
    The attributes are loaded in a json file in the same folder as this script

    Attributes
    ----------
    dag_id: string
        Name of the Airflow dag
    schedule_interval: Cron schedule expressions (Ex. 30 12 * * *)
        Schedule interval for the dag execution
    project : string
        BigQuery datasource project name
    dataset : string
        BigQuery datasource dataset name
    table : string 
        BigQuery datasource table name
    splits : integer
        Indicates the total of splits of the table, more splits less time running
    """
    import importlib

    appcues_module = importlib.import_module(
        "dags.utils.appcues_module")
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

        appcues_send_tasks = []
        for i in range(splits):
            appcues_task = appcues_module.send_table_data_to_appcues_operator(
                task_id=f'split_{i}',
                project=project,
                dataset=dataset,
                table=table,
                splits=splits,
                split_selected=i
            )
            appcues_send_tasks.append(appcues_task)

        end = sensor_module.dummy('end')

        slack_ok = slack_module.slack_ok(dag=dag)

        slack_fail = slack_module.slack_fail(dag=dag)

        # Orden de operadores (en serie)
        start >> table_sensor >> appcues_send_tasks[0]
        for i in range(splits-1):
            appcues_send_tasks[i] >> appcues_send_tasks[i+1]
        appcues_send_tasks[-1] >> end >> [slack_ok, slack_fail]

    return dag


for file in os.listdir("/dags/appcues"):
    if os.path.splitext(file)[1] == '.json':
        with open(f"/dags/appcues/{file}", 'r') as f:
            file_json = json.loads(f.read())

            dag_id = file_json['dagId']
            schedule_interval = file_json['scheduleInterval']
            project = file_json['project']
            dataset = file_json['dataset']
            table = file_json['table']
            splits = file_json['splits']

        globals()[dag_id] = create_dag(
            dag_id,
            schedule_interval,
            project,
            dataset,
            table,
            splits
        )
