def tableau_csv_pdf_to_gcp(workbook_name, view_name, sort_by, pdf_csv, bucket_name, bucket_folder, bucket_subfolder):

    from pathlib import Path
    import os
    import tableauserverclient as TSC  # version 0.19.0
    from os.path import exists
    import pandas as pd
    from io import StringIO
    from google.cloud import storage
    import shutil
    # from airflow.models import Variable

    # Ruta base
    base_path = Path(__file__).resolve().parent

    # Conexion a Tableau server (usar VPN si es prueba local)
    # TABLEAU_CONFIG = Variable.get('looker_sdk_food_and_groceries', deserialize_json=True)
    import json
    TABLEAU_CONFIG = json.load(open(os.path.join(base_path, 'tableau.json')))
    SERVER_URL = TABLEAU_CONFIG['tableau_server_url']
    USER = TABLEAU_CONFIG['tableau_user']
    PASSWORD = TABLEAU_CONFIG['tableau_password']
    SITE = TABLEAU_CONFIG['tableau_site_id']
    try:
        tableau_auth = TSC.TableauAuth(USER, PASSWORD, SITE)
        server = TSC.Server(SERVER_URL, use_server_version=True)
        server.auth.sign_in(tableau_auth)
    except Exception as err:
        raise Exception(
            'Error while connecting to Tableau server, exception is: {}'.format(err))

    with server.auth.sign_in(tableau_auth):

        workbook_id = None
        view_id = None

        # Busqueda del workbook
        req_options = TSC.RequestOptions().page_size(300)
        all_workbooks, pagination_item = server.workbooks.get(req_options)
        for workbook in [workbook for workbook in all_workbooks]:
            if workbook.name == workbook_name:
                workbook_id = workbook.id

            # Busqueda de la vista
            if workbook_id != None:
                for view in server.workbooks.get_by_id(workbook_id).views:
                    if view.name == view_name:
                        view_id = view.id

        if view_id != None:
            print('---------- DOWNLOADING TABLEAU FILES TO LOCAL ----------')
            print('Local folder: {}/temporal_folder/{}'.format(base_path, workbook_name))
            view_item = server.views.get_by_id(view_id)

            # Creacion folder local
            try:
                if not exists('{}/temporal_folder/{}'.format(base_path, workbook_name)):
                    os.makedirs(
                        '{}/temporal_folder/{}'.format(base_path, workbook_name))
            except Exception as err:
                raise Exception(
                    "Fail creating temporal folder, exception is {}".format(str(err)))

            # guardar vista como PDF
            if pdf_csv == 'PDF' or pdf_csv == 'PDF_and_CSV':
                try:
                    server.views.populate_pdf(view_item, req_options=None)
                    with open('{}/temporal_folder/{}/{}.pdf'.format(base_path, workbook_name, view_item.name), 'wb') as f:
                        f.write(view_item.pdf)
                        print("OK LOCAL SAVE: PDF '{}.pdf'".format(view_item.name))
                except Exception as err:
                    raise Exception(
                        "FAIL LOCAL SAVE: PDF '{}.pdf', exception is {}".format(view_item.name, str(err)))

            # guardar vista como CSV
            if pdf_csv == 'CSV' or pdf_csv == 'PDF_and_CSV':
                try:
                    server.views.populate_csv(view_item, req_options=None)
                    string = StringIO(b''.join(view_item.csv).decode("utf-8"))
                    df = pd.read_csv(string, sep=",")

                    # si tiene una columna "Measure Values" pivoteamos la tabla para que queden como columnas separadas
                    if 'Measure Values' in df.columns.values:
                        df['Measure Values'] = pd.to_numeric(
                            df['Measure Values'].str.replace('\\,|\\$|\\%', '', regex=True))
                        cols = [c for c in df.columns.values if c not in (
                            'Measure Values', 'Measure Names')]
                        df = pd.pivot_table(
                            df, values='Measure Values', columns='Measure Names', index=cols).reset_index()
                        df.sort_values(by=sort_by)
                        df.to_csv('{}/temporal_folder/{}/{}.csv'.format(base_path,
                                                                        workbook_name, view_item.name), index=False)
                    else:
                        df.to_csv(
                            '{}/temporal_folder/{}/{}.csv'.format(base_path, workbook_name, view_item.name))
                    print("OK LOCAL SAVE: CSV '{}.csv'".format(view_item.name))
                except Exception as err:
                    raise Exception(
                        "FAIL LOCAL SAVE: CSV '{}.csv', exception is {}".format(view_item.name, str(err)))

        if workbook_id == None or view_id == None:
            print('FAIL: Workbook or View name were not found')

    # Enviar archivos creados a un bucket en GCP
    if exists('{}/temporal_folder/{}'.format(base_path, workbook_name)):
        print('---------- SENDING LOOKER FILES TO GCP BUCKET ----------')
        print('GCP Bucket : {}/{}/{}'.format(
            bucket_name, bucket_folder, bucket_subfolder))
        # Creacion cliente de GCP
        # storage_client = storage.Client()
        gcp_json = os.path.join(base_path, 'gcp.json')
        storage_client = storage.Client.from_service_account_json(gcp_json)

        # Envio de archivos
        for file in os.listdir('{}/temporal_folder/{}'.format(base_path, workbook_name)):
            source_file_name = '{}/temporal_folder/{}/{}'.format(
                base_path, workbook_name, file)
            destination_blob_name = '{}/{}/{}'.format(
                bucket_folder, bucket_subfolder, file)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            try:
                blob.upload_from_filename(source_file_name)
                print("OK GCP SEND: '{}'".format(file))
            except Exception as err:
                raise Exception(
                    "FAIL GCP SEND: '{}', exception is {}".format(file, str(err)))
    else:
        raise Exception(
            'FAIL: Temporal folder not exists, there are no files to send fo GCP bucket')

    # Borramos la carpeta local temporal
    if exists('{}/temporal_folder'.format(base_path)):
        try:
            shutil.rmtree('{}/temporal_folder'.format(base_path))
        except Exception as err:
            print(
                'FAIL: Error while deleting the temporal folder, exception is: {}'.format(str(err)))


##############################
###### AIRFLOW DAG PART ######
##############################
WORKBOOK_NAME = "Tableau Dashboard name"
VIEW_NAME = 'Tableau view name'
SORT_BY = ['Country', 'Date']
BUCKET_NAME = 'bucket-name'
BUCKET_FOLDER = 'folder/sub_folder'


tableau_csv_pdf_to_gcp(
    workbook_name=WORKBOOK_NAME,
    view_name=VIEW_NAME,
    sort_by=SORT_BY,
    pdf_csv='CSV',
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_subfolder=WORKBOOK_NAME+'/'+VIEW_NAME
)
