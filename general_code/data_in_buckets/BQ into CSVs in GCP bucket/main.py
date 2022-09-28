import os
import json

from google.cloud import bigquery
from pathlib import Path
from datetime import datetime

base_path = Path(__file__).resolve().parent
config_json = os.path.join(base_path, 'config/config.json')
config_file = json.load(open(config_json))
gcp_json = os.path.join(base_path, 'config/gcp.json')
gcp_file = json.load(open(gcp_json))

client = bigquery.Client.from_service_account_json(gcp_json)
project = config_file['gcp_project']
bucket_name = config_file['bucket_name']
tables = config_file['tables']

for table in tables:
    destination_uri = "gs://{}/{}".format(bucket_name, str(table) + '.csv.gz')
    dataset_ref = bigquery.DatasetReference(project, "external_process")
    table_ref = dataset_ref.table(table)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.compression = bigquery.Compression.GZIP

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="US",
        job_config=job_config
    )
    extract_job.result()
    print("Date: {} --> Exported table: `{}` to Bucket: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                                   str(table_ref), str(bucket_name)))
