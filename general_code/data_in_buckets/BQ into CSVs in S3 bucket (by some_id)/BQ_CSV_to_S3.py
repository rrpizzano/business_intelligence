#########################
#### Python Operator ####
#########################

def sendTableDataInFragmentsToS3(
        aws_s3_bucket,
        aws_s3_folder,
        aws_s3_subfolder,
        project,
        dataset,
        table,
        except_columns,
        csv_fragment_field,
        csv_week_field,
        csv_name_field):

    import boto3  # 1.21.25
    import io
    from google.cloud import bigquery
    # from airflow.models import Variable

    # Creacion cliente S3
    # AWS_S3_CONFIG = Variable.get('aws_s3_credentials', deserialize_json=True)
    import json
    AWS_S3_CONFIG = json.load(
        open("./BQ into CSVs in S3 bucket (by some_id)/config/aws_s3.json"))
    AWS_ACCESS_KEY_ID = AWS_S3_CONFIG['aws_access_key_id']
    AWS_SECRET_ACCESS_KEY = AWS_S3_CONFIG['aws_secret_access_key']
    try:
        S3_CLIENT = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    except Exception as err:
        raise Exception(
            'FAIL: S3 client error, exception was {}'.format(err))

    # Creacion cliente BigQuery
    try:
        # BQ_CLIENT = bigquery.Client()
        BQ_CLIENT = bigquery.Client.from_service_account_json(
            "./BQ into CSVs in S3 bucket (by some_id)/config/gcp_pro.json")
    except Exception as err:
        raise Exception(
            'FAIL: BQ client error, exception was {}'.format(err))

    # Lectura del campo utilizado para el folder de la semana (ya viene en la query)
    SQL_csv_week = """
        SELECT DISTINCT {} FROM `{}.{}.{}`
    """.format(csv_week_field, project, dataset, table)
    csv_week_rows = BQ_CLIENT.query(SQL_csv_week).result()
    for csv_week_row in csv_week_rows:
        csv_week = csv_week_row[0]

    # Lectura de los valores distintos de la columna "fragment_column" (tomados para la iteracion)
    SQL_fragments = """
        SELECT DISTINCT {} FROM `{}.{}.{}`
    """.format(csv_fragment_field, project, dataset, table)
    fragments_rows = BQ_CLIENT.query(SQL_fragments).result()

    # Comienzo de iteracion sobre los valores distintos de "fragment_column" y envio a S3
    print('---------- SENDING DATA TO S3 ----------')
    print('S3 location: {}/{}/{}/{}'.format(aws_s3_bucket,
          aws_s3_folder, aws_s3_subfolder, csv_week))

    for fragments_row in fragments_rows:

        # Lectura del campo utilizado para el nombre del CSV (ya viene en la query y esta hasheado)
        SQL_csv_name = """
            SELECT DISTINCT {} FROM `{}.{}.{}` WHERE {} = '{}'
        """.format(csv_name_field, project, dataset, table, csv_fragment_field, fragments_row[0])
        csv_name_rows = BQ_CLIENT.query(SQL_csv_name).result()
        for csv_name_row in csv_name_rows:
            csv_name = csv_name_row[0]

        # Lectura de fragmento de la tabla
        SQL_table = """
            SELECT * EXCEPT({}) FROM `{}.{}.{}` WHERE {} = '{}'
        """.format(except_columns, project, dataset, table, csv_fragment_field, fragments_row[0])
        SQL_table_dataframe = BQ_CLIENT.query(SQL_table).to_dataframe()

        # Envio de la data en formato CSV a un bucket en S3
        with io.StringIO() as csv_buffer:
            SQL_table_dataframe.to_csv(csv_buffer, index=False)
            response = S3_CLIENT.put_object(
                Bucket=aws_s3_bucket,
                Key='{}/{}/{}/{}'.format(
                    aws_s3_folder,
                    aws_s3_subfolder,
                    csv_week,
                    csv_name
                ),
                Body=csv_buffer.getvalue()
            )
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print("OK IN S3: CSV '{}'".format(csv_name))
            else:
                raise Exception(
                    "FAIL IN S3: CSV '{}'".format(csv_name))


##############################
###### AIRFLOW DAG PART ######
##############################


# Variables globales S3

AWS_S3_BUCKET = 'bucket.name'
AWS_S3_FOLDER = 'folder'
AWS_S3_SUBFOLDER = 'subfolder'

# Variables globales BigQuery

PROJECT = 'big-query-project'
DATASET = 'bigquery_dataset'
TABLE = 'bigquery_table'
EXCEPT_COLUMNS = "column_1, column_2, column_3"

# Variables globales auxiliares

CSV_FRAG_FIELD = 'field_for_fragments'
CSV_WEEK_FIELD = 'field_with_week_name'
CSV_NAME_FIELD = 'filed_with_csv_name'

sendTableDataInFragmentsToS3(
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
