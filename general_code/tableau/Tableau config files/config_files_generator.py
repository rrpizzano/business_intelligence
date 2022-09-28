# CREO LISTA DE DASHBOARDS
import os
import pandas as pd
from datetime import datetime, timedelta
import ruamel.yaml
import shutil
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Basepath
base_path = Path(__file__).resolve().parent

# Archivo google sheet
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/....'
RANGE_NAME = 'Sheet1!A:H'

# Conexion google sheet API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
credentials = service_account.Credentials.from_service_account_file(
    '{}/gcp.json'.format(base_path), scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)

# Lectura de datos en google sheet
SPREADSHEET_ID = SPREADSHEET_URL.split('/')[5]
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
values = result.get('values', [])

# Google sheet a dataframe
df = pd.DataFrame(values)
df.columns = df.iloc[0]
df = df[1:]

list_names = list(set((df['Name'])))

for i in list_names:

    print(i)
    file_name = 'tableau_' + i.lower().replace("/", "_and_").replace("'",
                                                                     "").replace("(", "").replace(")", "").replace(" ", "_")
    dash_cond_filter = df['Name'] == i
    df_filter = df[dash_cond_filter]
    # Create file
    output = os.path.join(base_path, '{}'.format(file_name))

    if not os.path.exists(output):
        os.makedirs(output)
    else:
        shutil.rmtree(output)
        os.makedirs(output)

    # PARSE EXCEL
    # DAG NAME
    dag_name = 'TABLEAU_' + file_name + '-refresh_extract'

    schedule_interval = df_filter['schedule_interval'].values[0]
    schedule_interval = '"'+schedule_interval+'"'

    # START DATE
    start_date = datetime.now() - timedelta(days=1)

    start_date = "datetime("+str(start_date.year)+', ' + \
        str(start_date.month)+', '+str(start_date.day)+')'
    # RETRAY_DELAY
    retry_delay = 'timedelta(seconds=600)'

    # RETRIES
    retries = 18

    # OWNER and NOTIFICACIONS
    owner = 'rodrigo_pizzano'
    notificationChannelSuccess = 'slack_channel_success'
    notificationChannelError = 'slack_channel_error'

    # TARGET
    # TYPE
    type = df_filter["Type"].values[0]

    # PROJECT
    project = df_filter["Project"].values[0]

    # Create general variables
    dagConfig = dict(schedule_interval=schedule_interval)

    defaultArgs = dict(
        start_date=start_date,
        retry_delay=retry_delay,
        retries=retries,
        notificationChannelSuccess=notificationChannelSuccess,
        notificationChannelError=notificationChannelError,
        owner=owner
    )

    # STAGES
    # target
    target = dict(type=type, name=i, project=project)

    # steps
    # name
    name = 'refresh_' + file_name + '_extract'

    type_steps = {
        'steps': [{'type': 'refresh_tableau', 'name': name, 'target': target}]}

    stages_name = [{'name': 'refresh_extract', 'steps': [
        {'type': 'refresh_tableau', 'name': name, 'checkStatus': True, 'target': target}]}]

    # CREATE PRECONDITIONS
    preconditions = df_filter[['Schema', 'DataSet', "Table Name"]]
    type_prec = 'dq-api-ws'
    pre_dict = []

    for index, row in preconditions.iterrows():
        if row['Schema'] == 'big-query-project':
            table_uri = "{{params.datasets." + \
                row['DataSet']+"_do}}."+row["Table Name"]
        elif row['Schema'] == 'big-query-project':
            table_uri = "{{params.datasets." + \
                row['DataSet']+"}}."+row["Table Name"]
        else:
            print("Esquema erróneo= {}".format(row['Schema']))
        pre_dict.append(
            {'type': type_prec, 'name': row["Table Name"]+"_loaded", 'table_uri': table_uri})

    dag_dict = dict(name=dag_name,
                    dagConfig=dagConfig,
                    defaultArgs=defaultArgs,
                    preconditions=pre_dict,
                    stages=stages_name)

    # Add new blanks lines between keys
    dag_dict = ruamel.yaml.comments.CommentedMap(dag_dict)
    dag_dict.yaml_set_comment_before_after_key('dagConfig', before='\n')
    dag_dict.yaml_set_comment_before_after_key('defaultArgs', before='\n')
    dag_dict.yaml_set_comment_before_after_key('preconditions', before='\n')
    dag_dict.yaml_set_comment_before_after_key('stages', before='\n')

    with open(os.path.join(output, 'config.yml'), 'w') as stream:
        yaml = ruamel.yaml.YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml.dump(dag_dict, stream)
