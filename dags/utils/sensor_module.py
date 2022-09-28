def sensor_dag(task_id, external_dag_id, external_task_id):
    """
    Returns an Airflow ExternalTaskSensor for the execution of a DAG
    This ExternalTaskSensor will return true only if the 'external_dag_id' was executed successfully

    Attributes
    ----------
        task_id : string
            Name of the Airflow task
        external_dag_id : string
            Name of the sensed DAG (external_dag_id must have the same schedule as the DAG that uses this operator)
        external_taks_id : string 
            Name of the last task in the sensed DAG
    """
    from airflow.sensors.external_task_sensor import ExternalTaskSensor
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )


def dummy(task_id):
    """
    Returns an Airflow DummyOperator
    This DummyOperator will not return anything, this are used to organize a DAG workflow

    Attributes
    ----------
        task_id: string
            Name of the Airflow task
    """
    from airflow.operators.dummy_operator import DummyOperator
    return DummyOperator(task_id=task_id)


def get_table_etl_status(project, dataset, table):
    """
    Returns True if the ETL status of a table is 'FINALIZED', false otherwise.
    This uses the Data Quality API.

    Attributes
    ----------
    project: string
        Table BigQuery project name
    dataset: string
        Table BigQuery dataset name
    table: string
        Table BigQuery name
    """
    import requests
    import json
    from airflow.models import Variable

    headers = Variable.get('dq_api_credentials', deserialize_json=True)
    base_url = Variable.get('dq_api_base_url')
    endpoint = base_url + f'?projectID={project}' + \
        f'&datasetID={dataset}' + f'&tableID={table}'
    response = requests.get(endpoint, headers=headers)
    data = json.loads(response.content)

    if 'status' in data and data['status'].upper() == 'FINALIZED':
        print(f"ETL OK: status {data['status'].upper()}")
    elif 'status' in data and data['status'].upper() != 'FINALIZED':
        raise Exception(f"ETL FAIL: {data['status']}")
    elif 'error' in data:
        raise Exception(f"ETL FAIL: {data['error']}")
    else:
        raise Exception(f"ETL FAIL: Unknown status or error")


def get_table_etl_status_operator(project, dataset, table):
    """
    Returns an Airflow PythonOperator using the function 'get_table_etl_status'

    Attributes
    ----------
    task_id : string
        Name of the Airflow task
    project: string
        Table BigQuery project name (Ex. peya-bi-tools-pro)
    dataset: string
        Table BigQuery dataset name (Ex. il_core)
    table: string
        Table BigQuery name (Ex. dim_partner)
    """
    from airflow.operators.python_operator import PythonOperator
    return PythonOperator(
        task_id=f'sensor_{table}',
        python_callable=get_table_etl_status,
        op_kwargs=dict(
            project=project,
            dataset=dataset,
            table=table
        )
    )
