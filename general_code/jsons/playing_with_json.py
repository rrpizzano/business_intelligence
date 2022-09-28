import os
import json

LOOKER_NAME = 'Looker Dashboard name (must be unique)'
BUCKET_NAME = 'gcp-bucket-name'

SEND_GROUPS = {
    'no_filter': {
        'bucket_folder': f'raw/looker_files/{LOOKER_NAME}/no_filter',
        'emails': ['rrpizzano@gmail.com', 'rodrigo.pizzano@outlook.com'],
        'csv_filter': {'none': {}},
        'pdf_filter': {'none': {}}
    }
}

NO_FILTER = {
    'bucket_folder': f'raw/looker_files/{LOOKER_NAME}/no_filter',
    'emails': ['rrpizzano@gmail.com', 'rodrigo.pizzano@outlook.com'],
    'email_subject': f'(NO REPLY) - PedidosYa attached data: {LOOKER_NAME}',
    'csv_filter': {'none': {}},
    'pdf_filter': {'none': {}}
}

emails = ['rrpizzano@gmail.com', 'rodrigo.pizzano@outlook.com']

jsons = [
    {
        "dagId": "looker_send_id_20357",
        "scheduleInterval": "30 12 * * *",
        "lookerName": "Looker Dashboard name (must be unique)",
        "csvFilter": "{'none': {}}",
        "pdfFilter": "{'none': {}}",
        "filesFormat": "PDF",
        "bucketName": "gcp-bucket-name",
        "bucketFolder": "folder/sub_folder/Looker Dashboard name (must be unique)/no_filter",
        "emails": ["rrpizzano@gmail.com", "rrpizzano@gmail.com"],
        "emailSubject": "[Attached data]: Looker Dashboard name (must be unique)"
    },
    {
        "dagId": "looker_send_id_20576",
        "scheduleInterval": "30 12 * * *",
        "lookerName": "Rating y reviews RBI",
        "csvFilter": "{'table_1.field_1': 'value_1, value_2'}",
        "pdfFilter": "{'Field_1 Name': 'value_1, value_2'}",
        "filesFormat": "PDF",
        "bucketName": "gcp-bucket-name",
        "bucketFolder": "folder/sub_folder/Looker Dashboard name (must be unique)/value_1",
        "emails": ["rrpizzano@gmail.com", "rrpizzano@gmail.com"],
        "emailSubject": "[Attached data]: Looker Dashboard name (must be unique)"
    }
]

# print(", ".join(emails))
# print(SEND_GROUPS['group_0']['emails'])
# print(SEND_GROUPS['group_0'])
# print(SEND_GROUPS['no_filter']['bucket_folder'])
# print(NO_FILTER['emails'])
# for file in jsons:
#    print(file["dagId"])

"""
for file in os.listdir("./general_code/jsons"):
    file_json = json.load(open(f"./general_code/jsons/{file}"))
    print(file_json['dagId'])
"""

views = [
    {
        "view_name": "Daily Performance & Operations Times",
        "view_format": 'PDF_and_CSV'
    },
    {
        "view_name": "Daily Performance & Operations Times by Branch",
        "view_format": 'CSV'
    }
]

for view in views:
    print(view['view_name'])
    print(view['view_name'].replace(' ', '_'))
