import os
import json
from pathlib import Path
from google.cloud import bigquery
from tabulate import tabulate

# CONFIG FILES
base_path = Path(__file__).resolve().parent.parent
gcp_json = os.path.join(base_path, 'config/gcp_pro.json')
gcp_file = json.load(open(gcp_json))

# GCP CLIENT
BQ_client = bigquery.Client.from_service_account_json(gcp_json)

# QUERY
SQL_table = """
    SELECT
        Business_partner,
        Description
    FROM `peya-data-dev-tools-pro.external_process.finance_mercados_norte_sap_sv_partial_devs`
    WHERE Business_partner = 213914
"""
# DATAFRAME
table_dataframe_raw = BQ_client.query(SQL_table).to_dataframe()
table_dataframe = table_dataframe_raw.replace(
    to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True)

print(tabulate(table_dataframe, headers='keys', tablefmt='psql'))
