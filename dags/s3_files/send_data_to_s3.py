from airflow import DAG
from datetime import datetime, timedelta
import importlib


# Import de modulos #
s3_module = importlib.import_module(
    "dags.utils.s3_module")
slack_module = importlib.import_module(
    "dags.utils.slack_module")
sensor_module = importlib.import_module(
    "dags.utils.sensor_module")


# Variables globales S3
AWS_S3_BUCKET = 'aws.bucket.name'
AWS_S3_FOLDER = 'aws_bucket'
AWS_S3_SUBFOLDER = 'folder/sub_folder'

PROJECT = 'big-query-project'
DATASET = 'big-query-dataset'
TABLE = 'big_query_table'
EXCEPT_COLUMNS = "exclude_1, exclude_2, exclude_3"

CSV_FRAG_FIELD = 'field_for_fragment'
CSV_WEEK_FIELD = 'field_with_week_name'
CSV_NAME_FIELD = 'field_with_csv_name'


# Configuracion DAG #
config = dict(
    dag_id='send_data_to_s3',
    default_args=dict(
        owner='rodrigo_pizzano',
        depends_on_past=False,
        start_date=datetime(2022, 6, 1),
        execution_timeout=timedelta(minutes=45),
        email='rodrigo.pizzano@outlook.com',
        email_on_failure=True,
        email_on_retry=False,
        retries=3,
        retry_delay=timedelta(minutes=5),
    ),
    catchup=False,
    schedule_interval="0 12 * * 2",
)

with DAG(**config) as dag:

    # Operadores
    start = sensor_module.dummy('start')

    sensor_table_1 = sensor_module.sensor_dag(
        task_id='sensor_table_1',
        external_dag_id='table_1',
        external_task_id='end-audit-table_1'
    )

    csv_table_1 = s3_module.send_table_data_in_fragments_to_S3_operator(
        task_id='csv_table_1',
        aws_s3_bucket=AWS_S3_BUCKET,
        aws_s3_folder=AWS_S3_FOLDER,
        aws_s3_subfolder=AWS_S3_SUBFOLDER,
        project=PROJECT,
        dataset=DATASET,
        table=TABLE,
        except_columns=EXCEPT_COLUMNS,
        csv_fragment_field=CSV_FRAG_FIELD,
        csv_week_field=CSV_WEEK_FIELD,
        csv_name_field=CSV_NAME_FIELD
    )

    end = sensor_module.dummy('end')

    slack_ok = slack_module.slack_ok(dag=dag)

    slack_fail = slack_module.slack_fail(dag=dag)

    # Orden de operadores
    start >> [
        sensor_table_1
    ] >> csv_table_1 >> end >> [
        slack_ok,
        slack_fail
    ]
