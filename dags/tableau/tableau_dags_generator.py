from airflow import DAG
from datetime import datetime, timedelta
import os
import json


def create_dag(dag_id, schedule_interval, workbook_name, view_names, bucket_name, bucket_folder, emails, email_subject):
    """
    Creates a DAG for the given attributes
    The attributes are loaded in a json file in the same folder as this script

    Attributes
    ----------
    dag_id: string
        Name of the Airflow dag
    schedule_interval: Cron schedule expressions (Ex. 30 12 * * *)
        Schedule interval for the dag execution
    workbook_name : string
        Workbook name to be searched in the Tableau server.
    view_name : string
        View name used to generate the files.
    pdf_csv : string 
        Created files format, possible entries: PDF, CSV, PDF_and_CSV
    bucket_name : string
        Bucket GCP name to store the files
    bucket_folder : string
        Bucket GCP folder to store the files
    emails : comma separated email addresses (Ex: rodrigo.pizzano@outlook.com, rrpizzano@gmail.com)
        List of email recipients
    email_subject : string
        Email subject details
    """
    import importlib

    # Import de modulos
    tableau_module = importlib.import_module(
        "dags.utils.tableau_module")
    email_module = importlib.import_module(
        "dags.utils.email_module")
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
            start_date=datetime(2022, 8, 1),
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

        tableau_files_tasks = []
        for view in view_names:
            file_task = tableau_module.tableau_csv_pdf_to_gcp_operator(
                task_id=f"tableau_files_view_{view['view_name'].replace(' ', '_')}",
                workbook_name=workbook_name,
                view_name=view['view_name'],
                pdf_csv=view['view_format'],
                bucket_name=bucket_name,
                bucket_folder=bucket_folder
            )
            tableau_files_tasks.append(file_task)

        gcp_bucket_files_to_email = email_module.gcp_files_to_email_operator(
            task_id='gcp_bucket_files_to_email',
            bucket_name=bucket_name,
            bucket_folder=bucket_folder,
            emails=emails,
            email_subject=email_subject
        )

        end = sensor_module.dummy('end')

        slack_ok = slack_module.slack_ok(dag=dag)

        slack_fail = slack_module.slack_fail(dag=dag)

        # Orden de operadores
        start >> tableau_files_tasks >> gcp_bucket_files_to_email >> end
        end >> [slack_ok, slack_fail]

    return dag


for file in os.listdir("dags/tableau"):
    if os.path.splitext(file)[1] == '.json':
        with open(f"dags/tableau/{file}", 'r') as f:
            file_json = json.loads(f.read())

            dag_id = file_json["dagId"]
            schedule_interval = file_json["scheduleInterval"]
            workbook_name = file_json["workbookName"]
            view_names = file_json["viewNames"]
            bucket_name = file_json["bucketName"]
            bucket_folder = file_json["bucketFolder"]
            emails = file_json["emails"]
            email_subject = file_json["emailSubject"]

            globals()[dag_id] = create_dag(
                dag_id,
                schedule_interval,
                workbook_name,
                view_names,
                bucket_name,
                bucket_folder,
                emails,
                email_subject)
