"""
NOTAS:

- tableauhyperapi funciona solo para chips Intel por lo que hay que obligar al programa a que use Rosetta2 para simular esto, ejecutar desde la terminal: 'arch -x86_64 python3 nombre_script.py' (Ubicarse (cd) sobre el folder del script)
  Docu: https://tableau.github.io/hyper-db/docs/installation

- Instalar nuevamente los paquetes necesarios de Python como si tuvieramos un chip Intel:
    - Terminal: arch -x86_64 pip3 install tableauserverclient
    - Terminal: arch -x86_64 pip3 install tableauhyperapi
      Docu: https://tableau.github.io/hyper-db/docs/
    - Terminal: arch -x86_64 pip3 install pantab 
      Docu: https://pantab.readthedocs.io/en/latest/caveats.html 
    - Terminal: arch -x86_64 pip3 install chardet 
    - Terminal: arch -x86_64 pip3 install --upgrade --force-reinstall numpy
    - Terminal: arch -x86_64 pip3 install --upgrade --force-reinstall pandas

- Hay que editar a mano el schema en la funcion 'create_hyper_file_from_csv' agregándo la nueva columna y su type
  Si el type big_int() da error es porque en el CSV quedaron con un decimal en cero, pasarlo a type double()

"""
import os
import tableauserverclient as TSC
import pandas
import numpy
import pantab
import zipfile
from tableauhyperapi import TableName, TableDefinition, SqlType, NULLABLE, HyperProcess, Telemetry, Connection, CreateMode, escape_string_literal

tableau_server_url = 'https://tableau.xxxxx.com/'
tableau_site = 'xxxxx'
tableau_project = 'xxxxx'
tableau_user = 'xxxxx'
tableau_password = 'xxxxx'

# Tableau connection (if local use VPN)
try:
    tableau_auth = TSC.TableauAuth(
        tableau_user, tableau_password, tableau_site)
    server = TSC.Server(tableau_server_url, use_server_version=True)
    server.auth.sign_in(tableau_auth)
except Exception as err:
    raise Exception(
        f'Error while connecting to Tableau server, exception is: {str(err)}')

def print_line():
    print('-------------------------')

def process_started():
    print_line()
    print('PROCESS STARTED')

def process_ended():
    print_line()
    print('PROCESS ENDED')
    print_line()

def get_datasource_id_by_name(datasource_name):

    datasource_id = None
    try:
        with server.auth.sign_in(tableau_auth):
            print_line()
            print('Searching datasource...')
            req_options = TSC.RequestOptions().page_size(500)
            all_datasources, pagination_item = server.datasources.get(req_options)
            for datasource in [datasource for datasource in all_datasources]:
                if datasource.name == datasource_name:
                    datasource_id = datasource.id
        print('OK -> Datasource found!') 
        print(f'      Datasource name: {datasource_name}')
        print(f'      Datasource ID: {datasource_id}')
    except Exception as err:
        raise Exception(
            'Error while getting datasource_id, exception is: {}'.format(str(err)))
    
    return datasource_id

def download_datasource_by_id(datasource_id, datasource_name):

    try:
        if not os.path.exists('datasources'):
            os.makedirs('datasources')
        with server.auth.sign_in(tableau_auth):
            print_line()
            print('Downloading datasource...')
            server.datasources.download(datasource_id, filepath=f'datasources/{datasource_name}', include_extract=True, no_extract=None)
        print('OK -> Datasource downloaded from server')
    except Exception as err:
        raise Exception(
            f'Error while downloading datasource, exception is: {str(err)}')

def convert_datasource_to_hyper(datasource_name):    
    
    try:
        with zipfile.ZipFile(f'datasources/{datasource_name}.tdsx', 'r') as zip_ref:
            print_line()
            print('Converting datasource to hyper...')
            zip_ref.extractall('datasources')
        print('OK -> Datasource converted to hyper')
    except Exception as err:
        raise Exception(
            f'Error while converting datasource to hyper, exception is: {str(err)}')

def hyper_to_csv(datasource_name):

    table_name = TableName('Extract','Extract')
    try:
        df = pantab.frame_from_hyper(f'datasources/Data/Extracts/{datasource_name}.hyper', table=table_name)
        print_line()
        print('Converting hyper to CSV...')
        df.to_csv('datasources/{}.csv'.format(datasource_name), sep=',', index=False)
        print('OK -> Datasource converted to CSV')
    except Exception as err:
        raise Exception(
            f'Error while converting datasource to csv, exception is: {str(err)}')

def add_column_to_csv(datasource_name, new_column_name, next_to):

    try:
        df = pandas.read_csv(f'datasources/{datasource_name}.csv')
        print_line()
        print('Adding new column to CSV...')
        next_to_index = df.columns.get_loc(next_to)
        df.insert(next_to_index + 1, new_column_name, '')
        df.to_csv(f'datasources/{datasource_name}.csv', sep=',', index=False)
        print(f'OK -> Column {new_column_name} added to CSV')
    except Exception as err:
        raise Exception(
            f'Error while adding new column to csv, exception is: {str(err)}')

def create_hyper_file_from_csv(datasource_name, datasource_name_new):

    hyper_table = TableDefinition(
        table_name='hyper_new',
        # WARNING: ADD NEW COLUMN TO THIS SCHEMA
        columns=[
            TableDefinition.Column(name='date_day', type=SqlType.date(), nullability=NULLABLE),
            TableDefinition.Column(name='site', type=SqlType.text(), nullability=NULLABLE),
            TableDefinition.Column(name='content_source', type=SqlType.text(), nullability=NULLABLE),
            TableDefinition.Column(name='product_type', type=SqlType.text(), nullability=NULLABLE),
            TableDefinition.Column(name='priority', type=SqlType.text(), nullability=NULLABLE),
            TableDefinition.Column(name='page', type=SqlType.text(), nullability=NULLABLE),
            TableDefinition.Column(name='placement', type=SqlType.text(), nullability=NULLABLE),
            TableDefinition.Column(name='advertiser_category', type=SqlType.text(), nullability=NULLABLE),
            TableDefinition.Column(name='campaign_target', type=SqlType.text(), nullability=NULLABLE),
            TableDefinition.Column(name='new_column_test', type=SqlType.text(), nullability=NULLABLE),
            TableDefinition.Column(name='total_impressions', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='impression_cost_lc', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='impression_cost_usd', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_impressions_view', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_empty_impressions', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='clicks', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_att', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_print_att', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_views_att', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_click_att', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_total_amount_lc_att', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_total_amount_usd_att', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_cre', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_print_cre', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_views_cre', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_click_cre', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_total_amount_lc_cre', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='orders_total_amount_usd_cre', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_advertisers_day', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_advertisers_day_all_contents', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_advertisers_week', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_advertisers_week_all_contents', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_advertisers_month', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_advertisers_month_all_contents', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_campaigns_day', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_campaigns_week', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='total_campaigns_month', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='campaign_budget_real_lc', type=SqlType.double(), nullability=NULLABLE),
            TableDefinition.Column(name='campaign_budget_real_usd', type=SqlType.double(), nullability=NULLABLE) 
        ]
    )
    new_hyper_path = f'datasources/{datasource_name_new}.hyper'
    process_parameters = {
        "log_file_max_count": "2",
        "log_file_size_limit": "100M"
    }
    try:
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:
            print_line()
            print('Creating new hyper...')
            #connection_parameters = {"lc_time": "en_US"}
            with Connection(endpoint=hyper.endpoint,
                            database=new_hyper_path,
                            create_mode=CreateMode.CREATE_AND_REPLACE,
                            #parameters=connection_parameters
                            ) as connection:
                connection.catalog.create_table(table_definition=hyper_table)
                path_to_csv = f'datasources/{datasource_name}.csv'
                count_in_hyper_table = connection.execute_command(
                    command=f"COPY {hyper_table.table_name} from {escape_string_literal(path_to_csv)} with "
                    f"(format csv, delimiter ',', header)")
                print(f'OK -> New hyper {datasource_name_new}.hyper created, total rows: {count_in_hyper_table}')
    except Exception as err:
        raise Exception(
            f'Error while creating new hyper, exception is: {str(err)}')

def publish_hyper(datasource_name_new):

    try: 
        with server.auth.sign_in(tableau_auth):
            publish_mode = TSC.Server.PublishMode.Overwrite

            project_id = None
            req_options = TSC.RequestOptions().page_size(100)
            all_projects, pagination_item = server.projects.get(req_options)
            for project in TSC.Pager(server.projects):
                if project.name == tableau_project:
                    project_id = project.id
            
            datasource = TSC.DatasourceItem(project_id)
            print_line()
            print(f'Publishing {datasource_name_new} to {tableau_site} -> {tableau_project}...')
            datasource = server.datasources.publish(datasource, f'datasources/{datasource_name_new}.hyper', publish_mode)
            print('OK -> Datasource published!') 
            print(f'      Datasource name: {datasource_name_new}')
            print(f'      Datasource ID: {datasource.id}')
    except Exception as err:
        raise Exception(
            f'Error while publishing new hyper, exception is: {str(err)}')    

# MAIN
if __name__ == '__main__':
    try:
        # Variables
        datasource_name = 'display_ads_v2'
        datasource_name_new = 'display_ads_v3'
        new_column_name = 'new_column_test'
        next_to = 'campaign_target'

        # Step 1: Existing hyper to CSV
        process_started()
        datasource_id = get_datasource_id_by_name(datasource_name)
        download_datasource_by_id(datasource_id, datasource_name)
        convert_datasource_to_hyper(datasource_name)
        hyper_to_csv(datasource_name)

        # Step 2: Editing the CSV file (add new column)
        add_column_to_csv(datasource_name, new_column_name, next_to)

        # Step 3: New CSV to new hyper (old hyper is kept for testing purposes)
        create_hyper_file_from_csv(datasource_name, datasource_name_new)
        publish_hyper(datasource_name_new)
        process_ended()
    except Exception as err:
        raise Exception(str(err))
    
server.auth.sign_out()
