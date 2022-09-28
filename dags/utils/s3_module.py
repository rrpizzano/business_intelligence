def send_table_data_in_fragments_to_S3(aws_s3_bucket, aws_s3_folder, aws_s3_subfolder, project, dataset, table, except_columns, csv_fragment_field, csv_week_field, csv_name_field):
    """
    Sends to an Amazon Web Service S3 bucket the information of a BigQuery table in CSV format. 
    The CSV files are partitioned by a chosen column of the BigQuery table specified in 'csv_fragment_field'

    Attributes
    ----------
    aws_s3_bucket : string
        Amazon Web Service S3 bucket name to store the CSV files
    aws_s3_folder : string
        Amazon Web Service S3 bucket folder name to store the CSV files
    aws_s3_subfolder : string 
        Amazon Web Service S3 bucket subfolder name to store the CSV files
    project : string
        BigQuery datasource project name
    dataset : string
        BigQuery datasource dataset name
    table : string
        BigQuery datasource table name
    except_columns : comma separated strings (Ex: week_file, key, send_file_name)
        BigQuery table columns to exclude in the CSV files
    csv_fragment_field : string
        BigQuery table column chose to partition the CSV files
    csv_week_field : string
        BigQuery table column chose to generate a subfolder with the week name
    csv_name_field : string
        BigQuery table column chose to name the CSV files
    """
    import boto3  # 1.21.25
    import io
    from google.cloud import bigquery
    from airflow.models import Variable

    # Creacion cliente S3
    AWS_S3_CONFIG = Variable.get('aws_s3_credentials', deserialize_json=True)
    AWS_ACCESS_KEY_ID = AWS_S3_CONFIG['aws_access_key_id']
    AWS_SECRET_ACCESS_KEY = AWS_S3_CONFIG['aws_secret_access_key']
    try:
        S3_CLIENT = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    except Exception as err:
        raise Exception(f'FAIL: S3 client error, exception was {str(err)}')

    # Creacion cliente BigQuery
    try:
        BQ_CLIENT = bigquery.Client()
    except Exception as err:
        raise Exception(f'FAIL: BQ client error, exception was {str(err)}')

    # Lectura del campo utilizado para el folder de la semana (ya viene en la query)
    SQL_csv_week = f"""
        SELECT DISTINCT 
            {csv_week_field} 
        FROM `{project}.{dataset}.{table}`
    """
    csv_week_rows = BQ_CLIENT.query(SQL_csv_week).result()
    for csv_week_row in csv_week_rows:
        csv_week = csv_week_row[0]

    # Lectura de los valores distintos de la columna "fragment_column" (tomados para la iteracion)
    SQL_fragments = f"""
        SELECT DISTINCT 
            {csv_fragment_field} 
        FROM `{project}.{dataset}.{table}`
    """
    fragments_rows = BQ_CLIENT.query(SQL_fragments).result()

    # Comienzo de iteracion sobre los valores distintos de "fragment_column" y envio a S3
    print('---------- SENDING DATA TO S3 ----------')
    print(
        f'S3 location: {aws_s3_bucket}/{aws_s3_folder}/{aws_s3_subfolder}/{csv_week}')

    for fragments_row in fragments_rows:

        # Lectura del campo utilizado para el nombre del CSV (ya viene en la query y esta hasheado)
        SQL_csv_name = f"""
            SELECT DISTINCT 
                {csv_name_field} 
            FROM `{project}.{dataset}.{table}` 
            WHERE {csv_fragment_field} = '{fragments_row[0]}'
        """
        csv_name_rows = BQ_CLIENT.query(SQL_csv_name).result()
        for csv_name_row in csv_name_rows:
            csv_name = csv_name_row[0]

        # Lectura de fragmento de la tabla
        SQL_table = f"""
            SELECT 
                * EXCEPT({except_columns}) 
            FROM `{project}.{dataset}.{table}` 
            WHERE {csv_fragment_field} = '{fragments_row[0]}'
        """
        SQL_table_dataframe = BQ_CLIENT.query(SQL_table).to_dataframe()

        # Envio de la data en formato CSV a un bucket en S3
        with io.StringIO() as csv_buffer:
            SQL_table_dataframe.to_csv(csv_buffer, index=False)
            response = S3_CLIENT.put_object(
                Bucket=aws_s3_bucket,
                Key=f'{aws_s3_folder}/{aws_s3_subfolder}/{csv_week}/{csv_name}',
                Body=csv_buffer.getvalue()
            )
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"OK IN S3: CSV '{csv_name}'")
            else:
                raise Exception(f"FAIL IN S3: CSV '{csv_name}'")


def send_table_data_in_fragments_to_S3_operator(task_id, aws_s3_bucket, aws_s3_folder, aws_s3_subfolder, project, dataset, table, except_columns, csv_fragment_field, csv_week_field, csv_name_field):
    """
    Returns an Airflow PythonVirtualenvOperator using the function 'send_table_data_in_fragments_to_S3'
    Requires the install of the boto3 v1.21.25 package

    Attributes
    ----------
    task_id : string
        Name of the Airflow task
    aws_s3_bucket : string
        Amazon Web Service S3 bucket name to store the CSV files
    aws_s3_folder : string
        Amazon Web Service S3 bucket folder name to store the CSV files
    aws_s3_subfolder : string 
        Amazon Web Service S3 bucket subfolder name to store the CSV files
    project : string
        BigQuery datasource project name
    dataset : string
        BigQuery datasource dataset name
    table : string
        BigQuery datasource table name
    except_columns : comma separated strings (Ex: week_file, key, send_file_name)
        BigQuery table columns to exclude in the CSV files
    csv_fragment_field : string
        BigQuery table column chose to partition the CSV files
    csv_week_field : string
        BigQuery table column chose to generate a subfolder with the week name
    csv_name_field : string
        BigQuery table column chose to name the CSV files
    """
    from airflow.operators.python_operator import PythonVirtualenvOperator
    return PythonVirtualenvOperator(
        task_id=task_id,
        python_callable=send_table_data_in_fragments_to_S3,
        op_kwargs=dict(
            aws_s3_bucket=aws_s3_bucket,
            aws_s3_folder=aws_s3_folder,
            aws_s3_subfolder=aws_s3_subfolder,
            project=project,
            dataset=dataset,
            table=table,
            except_columns=except_columns,
            csv_fragment_field=csv_fragment_field,
            csv_week_field=csv_week_field,
            csv_name_field=csv_name_field
        ),
        requirements=[
            "boto3==1.21.25"
        ]
    )
