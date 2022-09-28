from airflow import DAG
from datetime import datetime, timedelta
import os
import json


def create_dag(dag_id, schedule_interval, looker_name, csv_filter, pdf_filter, files_format, bucket_name, bucket_folder, emails, email_subject):
    """
    Creates a DAG for the given attributes
    The attributes are loaded in a json file in the same folder as this script

    Attributes
    ----------
    dag_id: string
        Name of the Airflow dag
    schedule_interval: Cron schedule expressions (Ex. 30 12 * * *)
        Schedule interval for the dag execution
    looker_name : string
        Looker dashboard name to be searched in the Looker server.
    csv_filter : dictionary
        Filter for all the queries of the tiles in the Looker dashboard (this is used for CSV files)
        Value for no filters is {'none': {}}
    pdf_filter : dictionary
        Filter for all the views of the tiles in the Looker dashboard (this is used for PDF files)
        Value for no filters is {'none': {}}
    files_format : string
        Created files format, possible entries: PDF, CSV, PDF_and_CSV
    bucket_name : string
        Bucket GCP name to store the files
    bucket_folder : string
        Bucket GCP folder to store the files 
    emails : comma separated email addresses (Ex: rodrigo.pizzano@pedidosya.com, alan.torre@pedidosya.com, andres.sorza@pedidosya.com)
        List of email recipients
    email_subject : string
        Email subject details
    """
    import importlib

    # Import de modulos
    looker_module = importlib.import_module(
        "dags.utils.looker_module")
    email_module = importlib.import_module(
        "dags.utils.email_module")
    slack_module = importlib.import_module(
        "dags.utils.slack_module")
    sensor_module = importlib.import_module(
        "dags.utils.sensor_module")

    # Configuracion DAG #
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

        looker_files_to_gcp_bucket = looker_module.looker_csv_pdf_to_gcp_operator(
            task_id='looker_files_to_gcp_bucket',
            looker_name=looker_name,
            csv_filter=csv_filter,
            pdf_filter=pdf_filter,
            files_format=files_format,
            bucket_name=bucket_name,
            bucket_folder=bucket_folder
        )

        looker_gcp_bucket_files_to_email = email_module.gcp_files_to_email_operator(
            task_id='looker_gcp_bucket_files_to_email',
            bucket_name=bucket_name,
            bucket_folder=bucket_folder,
            emails=emails,
            email_subject=email_subject
        )

        end = sensor_module.dummy('end')

        slack_ok = slack_module.slack_ok(dag=dag)

        slack_fail = slack_module.slack_fail(dag=dag)

        # Orden de operadores
        start >> looker_files_to_gcp_bucket >> looker_gcp_bucket_files_to_email >> end
        end >> [slack_ok, slack_fail]

    return dag


for file in os.listdir("/dags/looker"):
    if os.path.splitext(file)[1] == '.json':

        with open(f"/dags/looker/{file}", 'r') as f:
            file_json = json.loads(f.read())

            dag_id = file_json["dagId"]
            schedule_interval = file_json["scheduleInterval"]
            looker_name = file_json["lookerName"]
            csv_filter = file_json["csvFilter"]
            pdf_filter = file_json["pdfFilter"]
            files_format = file_json["filesFormat"]
            bucket_name = file_json["bucketName"]
            bucket_folder = file_json["bucketFolder"]
            emails = file_json["emails"]
            email_subject = file_json["emailSubject"]

            globals()[dag_id] = create_dag(
                dag_id,
                schedule_interval,
                looker_name,
                csv_filter,
                pdf_filter,
                files_format,
                bucket_name,
                bucket_folder,
                emails,
                email_subject)
