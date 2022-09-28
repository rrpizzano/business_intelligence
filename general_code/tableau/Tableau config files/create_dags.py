# CREO LISTA DE DASHBOARDS
import os
import json
import pandas as pd
from datetime import datetime, timedelta
import ruamel.yaml
import shutil

df = pd.read_excel(
    "./Tableau config files/Food - Tableau extract dependencies.xlsx", header=0)

list_names = list(set((df['Name'])))

for i in list_names:
    print(i)
    file_name = 'tableau_'+i.lower().replace(" ", "_")
    dash_cond_filter = df['Name'] == i
    df_filter = df[dash_cond_filter]
    # Create file
    path = os.getcwd()
    output = os.path.join(path, '{}'.format(file_name))
    #os.makedirs (output)

    if not os.path.exists(output):
        os.makedirs(output)
    else:
        shutil.rmtree(output)    # Removes all the subdirectories
        os.makedirs(output)

    # PARSE EXCEL
    # DAG NAME
    dag_name = 'TABLEAU_'+i.replace(" ", "_")+'-refresh_extract'

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

    # TARGET
    # TYPE
    type = df_filter["Type"].values[0]

    # PROJECT
    project = df_filter["Project"].values[0]

    # Create general variables
    dagConfig = dict(schedule_interval=schedule_interval)

    defaultArgs = dict(start_date=start_date,
                       retry_delay=retry_delay, retries=retries)

    # STAGES
    # target
    target = dict(type=type, name=i, project=project)

    # steps
    # name
    name = 'refresh_'+i.lower().replace(" ", "_")+'_extract'

    type_steps = {
        'steps': [{'type': 'refresh_tableau', 'name': name, 'target': target}]}

    stages_name = [{'name': 'refresh_extract', 'steps': [
        {'type': 'refresh_tableau', 'name': name, 'target': target}]}]

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

    dag_dict = dict(name=dag_name, dagConfig=dagConfig,
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
