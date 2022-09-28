import looker_sdk
import os
import time
import shutil

from looker_sdk import models
from pathlib import Path
from os.path import exists
from pathlib import Path
from google.cloud import storage

# Ruta base
base_path = Path(__file__).resolve().parent

# Dashboard de Looker
title = 'Looker Dashboard name'

# Credenciales SDK de Looker
base_url = 'https://...sa.looker.com:...'
client_id = 'gv...'
client_secret = 'bM...'

# Credenciales para GCP
gcp_json = os.path.join(base_path, 'gcp.json')
bucket_name = 'bucket-name'
bucket_folder = 'folder/sub_folder'

# Conexion al SDK de Looker (https://pypi.org/project/looker-sdk/)
try:
    with open('{}/looker.ini'.format(base_path), 'w') as f:
        f.write('[Looker]\n base_url={}\n client_id={}\n client_secret={}\n verify_ssl=True'.format(
            base_url, client_id, client_secret))
    sdk = looker_sdk.init31('{}/looker.ini'.format(base_path))
except Exception as err:
    raise Exception(
        'FAIL --> Error while connecting to Looker SDK, exception is: {}'.format(str(err)))

# Descargar (localmente) dashboard a PDF
dashboard = next(iter(sdk.search_dashboards(title=title.lower())), None)
if dashboard.id != None:
    id = dashboard.id
    task = sdk.create_dashboard_render_task(
        dashboard_id=str(id),
        result_format='pdf',
        body=models.CreateDashboardRenderTask(
            dashboard_style=None,
            dashboard_filters=None,
        ),
        width=1260,
        height=5499
    )

    if not (task and task.id):
        raise Exception(
            'FAIL --> Could not create a render task for dashboard_id: {}'.format(
                id)
        )

    # Esperamos que termine el render
    delay = 0.5
    while True:
        poll = sdk.render_task(task.id)
        if poll.status == 'failure':
            print(poll)
            raise Exception(
                'FAIL --> Render failed for dashboard: {}'.format(id)
            )
        elif poll.status == 'success':
            break

        time.sleep(delay)

    # Guardamos localmente el render en PDF
    result = sdk.render_task_results(task.id)
    try:
        os.makedirs('{}/temporal_folder'.format(base_path))
        with open('{}/temporal_folder/{}.pdf'.format(base_path, title), 'wb') as f:
            f.write(result)
            print(
                'OK --> PDF: {} successfully saved locally in a temporal folder'.format(title))
    except Exception as err:
        raise Exception(
            'FAIL --> Error saving PDF locally, exception is: {}'.format(str(err)))

    # Enviar PDF creado a un bucket en GCP
    if exists('{}/temporal_folder/{}.pdf'.format(base_path, title)):
        source_file_name = "{}/temporal_folder/{}.pdf".format(base_path, title)
        destination_blob_name = '{}/{}'.format(bucket_folder, title)
        storage_client = storage.Client.from_service_account_json(gcp_json)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        try:
            # Envio PDF a bucket en GCP
            blob.upload_from_filename(source_file_name)
            print("OK --> PDF: {} successfully saved in GCP bucket: {}/{}".format(
                title, bucket_name, bucket_folder))
        except Exception as err:
            raise Exception(
                'FAIL --> Error while sending PDF to GCP, exception is: {}'.format(str(err)))
    else:
        print('FAIL --> PDF not found locally')

    # Borramos la carpeta local temporal
    try:
        shutil.rmtree('{}/temporal_folder'.format(base_path))
    except Exception as err:
        raise Exception(
            'FAIL --> Error while deleting the temporal folder, exception is: {}'.format(str(err)))
else:
    raise Exception(
        'FAIL --> Dashboard {} not found'.format(id)
    )

# Borramos el archivo para conexion al SDK de Looker
try:
    os.remove('{}/looker.ini'.format(base_path))
except Exception as err:
    raise Exception(
        'FAIL --> Error while deleting the looker.ini file, exception is: {}'.format(str(err)))
