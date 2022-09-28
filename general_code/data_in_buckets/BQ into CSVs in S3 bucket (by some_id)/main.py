# Writing pandas data frame to a CSV file on S3 using the boto3 library ##
import io
import os
import json
import boto3
import pandas
from pathlib import Path
from google.cloud import bigquery
from datetime import datetime

# CONFIG FILES
base_path = Path(__file__).resolve().parent

config_json = os.path.join(base_path, 'config/config.json')
aws_s3_json = os.path.join(base_path, 'config/aws_s3.json')
gcp_json = os.path.join(base_path, 'config/gcp.json')

config_file = json.load(open(config_json))
aws_s3_file = json.load(open(aws_s3_json))
gcp_file = json.load(open(gcp_json))

# S3 CLIENT
AWS_S3_BUCKET = aws_s3_file['AWS_S3_BUCKET']
AWS_S3_FOLDER = aws_s3_file['AWS_S3_FOLDER']
AWS_S3_SUBFOLDER = aws_s3_file['AWS_S3_SUBFOLDER']
AWS_ACCESS_KEY_ID = aws_s3_file['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = aws_s3_file['AWS_SECRET_ACCESS_KEY']

S3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# GCP CLIENT, READ TABLES AND SEND TO S3
GCP_project = config_file['GCP_project']
GCP_dataset = config_file['GCP_dataset']
GCP_table = config_file['GCP_table']

BQ_client = bigquery.Client.from_service_account_json(gcp_json)

SQL_table = f"""
    SELECT 
        *
    FROM `{GCP_project}.{GCP_dataset}.{GCP_table}`
    """

table_dataframe_raw = BQ_client.query(SQL_table).to_dataframe()
#billing_ids = table_dataframe_raw['billing_info_id'].unique()
billing_ids = [65069, 146897]

for billing_id in billing_ids:

    # TABLE DATA FOR SELECTED BILLING ID
    table_dataframe = table_dataframe_raw[table_dataframe_raw.billing_info_id == billing_id]
    folder_name = table_dataframe['folder_name'].unique()[0]
    anonymized_file_name = table_dataframe['anonymized_file_name'].unique()[0]

    # SEND TABLE DATA TO S3 BUCKET
    with io.StringIO() as csv_buffer:
        table_dataframe.loc[:, ~table_dataframe.columns.isin(
            ['folder_name', 'anonymized_file_name'])].to_csv(csv_buffer, index=False)

        response = S3_client.put_object(
            Bucket=AWS_S3_BUCKET,
            Key="{}/{}/{}/{}.csv".format(
                AWS_S3_FOLDER,
                AWS_S3_SUBFOLDER,
                str(folder_name),
                str(anonymized_file_name)
            ),
            Body=csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print("OK --> File: '{}' successfully saved in S3/{}".format(
                str(anonymized_file_name),
                AWS_S3_BUCKET + '/' + AWS_S3_FOLDER + '/' + AWS_S3_SUBFOLDER))
        else:
            print("FAIL --> File: '{}' not saved in S3".format(
                str(anonymized_file_name)))
