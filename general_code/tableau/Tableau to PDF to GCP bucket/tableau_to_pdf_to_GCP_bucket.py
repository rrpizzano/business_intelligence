import tableauserverclient as TSC
import os
import json
import shutil

from google.cloud import storage
from pathlib import Path
from os.path import exists

tableau_server_url = "https://tableau....net"
tableau_user = "...Tableau"
tableau_password = "6..."
tableau_site_id = "P..."

base_path = Path(__file__).resolve().parent
gcp_json = os.path.join(base_path, 'gcp.json')
gcp_file = json.load(open(gcp_json))

# Conexion a Tableau Server
try:
    tableau_auth = TSC.TableauAuth(
        tableau_user, tableau_password, tableau_site_id)
    server = TSC.Server(tableau_server_url, use_server_version=True)
    server.auth.sign_in(tableau_auth)
except Exception as err:
    raise Exception(
        f'Error while connecting to Tableau server, exception is: {str(err)}')

with server.auth.sign_in(tableau_auth):

    # Buscar el id de la view por nombre de tableau y nombre de la view
    workbook_name = 'Tableau Dashboard Name'
    workbook_id = None
    view_name = "Tableau Dashboard View"
    view_id = None

    all_workbooks, pagination_item = server.workbooks.get()
    for workbook in all_workbooks:
        if workbook.name == workbook_name:
            workbook_id = workbook.id

        if workbook_id != None:
            for view in server.workbooks.get_by_id(workbook_id).views:
                if view.name == view_name:
                    view_id = view.id

    if view_id != None:
        # Crear PDF a partir de la view
        view_item = server.views.get_by_id(view_id)
        server.views.populate_pdf(view_item, req_options=None)
        temp_folder = os.makedirs('{}/temporal_folder'.format(base_path))
        try:
            with open('{}/temporal_folder/{}.pdf'.format(base_path, view_item.name), 'wb') as f:
                f.write(view_item.pdf)
                print('OK --> PDF: {} successfully saved locally'.format(view_item.name))
        except Exception as err:
            raise Exception(
                'FAIL --> Error while creating PDF, exception is: {}'.format(str(err)))

    if workbook_id == None or view_id == None:
        print('FAIL --> Workbook or View name were not found')

    # Enviar PDF creado a un bucket en GCP
    if exists('{}/temporal_folder/{}.pdf'.format(base_path, view_item.name)):
        bucket_name = 'bucket-name'
        source_file_name = "{}/temporal_folder/{}.pdf".format(
            base_path, view_item.name)
        destination_blob_name = 'folder/sub_folder/{}'.format(
            view_item.name)
        storage_client = storage.Client.from_service_account_json(gcp_json)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        try:
            # Envio PDF a bucket y lo borro localmente
            blob.upload_from_filename(source_file_name)
            print("OK --> PDF: {} successfully saved in GCP bucket: {}/folder/sub_folder".format(
                view_item.name, bucket_name))
        except Exception as err:
            raise Exception(
                'FAIL --> Error while sending PDF to GCP, exception is: {}'.format(str(err)))

        # Borramos la carpeta local temporal
        shutil.rmtree('{}/temporal_folder'.format(base_path))
    else:
        print('FAIL --> PDF not found in the local machine')
