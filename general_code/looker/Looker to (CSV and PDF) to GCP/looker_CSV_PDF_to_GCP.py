#########################
#### Python Operator ####
#########################

def looker_csv_pdf_to_gcp(looker_title, csv_filter, pdf_filter, files_format, bucket_name, bucket_folder, bucket_subfolder):

    import looker_sdk
    from looker_sdk import models
    import logging
    import os
    import shutil
    import urllib
    import time
    from pathlib import Path
    from os.path import exists
    from google.cloud import storage
    # from airflow.models import Variable

    # Ruta base
    base_path = Path(__file__).resolve().parent

    # Conexion al SDK de Looker (https://pypi.org/project/looker-sdk/)
    # LOOKER_SDK_CONFIG = Variable.get('looker_sdk_food_and_groceries', deserialize_json=True)
    import json
    LOOKER_SDK_CONFIG = json.load(open(os.path.join(base_path, 'looker.json')))
    BASE_URL = LOOKER_SDK_CONFIG['base_url']
    CLIENT_ID = LOOKER_SDK_CONFIG['client_id']
    CLIENT_SECRET = LOOKER_SDK_CONFIG['client_secret']
    try:
        with open('{}/looker.ini'.format(base_path), 'w') as f:
            f.write('[Looker]\n base_url={}\n client_id={}\n client_secret={}\n verify_ssl=True'.format(
                BASE_URL, CLIENT_ID, CLIENT_SECRET))
        sdk = looker_sdk.init31('{}/looker.ini'.format(base_path))
    except Exception as err:
        raise Exception(
            'FAIL: Error while connecting to Looker SDK, exception is: {}'.format(str(err)))

    # Busqueda del Dashboard en la web de Looker
    try:
        dashboard = next(
            iter(sdk.search_dashboards(title=looker_title.lower())), None)
    except Exception as err:
        raise Exception(
            'FAIL: Error while searching Dashboard, exception is: {}'.format(str(err)))

    # Descarga de dashboard a PDF y/o CSVs
    if dashboard.id != None:
        print('---------- DOWNLOADING LOOKER FILES TO LOCAL ----------')
        print('Local folder: {}/temporal_folder/{}'.format(base_path, dashboard.title))
        # CSVs
        if files_format == 'CSV' or files_format == 'PDF_and_CSV':
            tiles = sdk.dashboard_dashboard_elements(dashboard_id=dashboard.id)
            for tile in tiles:
                if tile.title != None:
                    # Aplicación de filtros sobre query del tile
                    new_query = tile.query
                    dict1 = new_query.filters
                    dict2 = csv_filter
                    dict1.update((k, dict2[k])
                                 for k in dict1.keys() & dict2.keys())
                    new_query.client_id = None
                    new_query.id = None
                    new_query_results = sdk.run_inline_query(
                        body=new_query, result_format='csv')
                    # Descarga resultados CSV en temporal_folder
                    try:
                        if not exists('{}/temporal_folder/{}'.format(base_path, dashboard.title)):
                            os.makedirs(
                                '{}/temporal_folder/{}'.format(base_path, dashboard.title))
                        with open('{}/temporal_folder/{}/{}.csv'.format(base_path, dashboard.title, tile.title), 'wb') as f:
                            f.write(str.encode(new_query_results))
                            print(
                                "OK LOCAL SAVE: CSV '{}.csv'".format(tile.title))
                    except Exception as err:
                        raise Exception(
                            "FAIL LOCAL SAVE: CSV '{}.csv', exception is {}".format(tile.title, str(err)))

        # PDF
        if files_format == 'PDF' or files_format == 'PDF_and_CSV':
            id = dashboard.id
            task = sdk.create_dashboard_render_task(
                dashboard_id=str(id),
                result_format='pdf',
                body=models.CreateDashboardRenderTask(
                    dashboard_style='tiled',
                    dashboard_filters=urllib.parse.urlencode(pdf_filter)
                ),
                width=1260,
                height=5499
            )

            if not (task and task.id):
                raise Exception(
                    'FAIL: Could not create a render task for dashboard_id {}'.format(id))

            # Esperamos que termine el render del PDF (sino queda corrupto el archivo)
            delay = 5.0
            while True:
                poll = sdk.render_task(task.id)
                if poll.status == 'failure':
                    raise Exception(
                        'FAIL: Render failed for dashboard_id {}'.format(id))
                elif poll.status == 'success':
                    break
                time.sleep(delay)

            # Descarga resultados PDF en temporal_folder
            pdf_result = sdk.render_task_results(task.id)
            pdf_suffix = ''
            for key, value in pdf_filter.items():
                pdf_suffix += '_' + value
            pdf_name = dashboard.title + pdf_suffix
            try:
                if not exists('{}/temporal_folder/{}'.format(base_path, dashboard.title)):
                    os.makedirs(
                        '{}/temporal_folder/{}'.format(base_path, dashboard.title))
                with open('{}/temporal_folder/{}/{}.pdf'.format(base_path, dashboard.title, pdf_name), 'wb') as f:
                    f.write(pdf_result)
                    print(
                        "OK LOCAL SAVE: PDF '{}.pdf'".format(pdf_name))
            except Exception as err:
                raise Exception(
                    "FAIL LOCAL SAVE: PDF '{}.pdf', exception is {}".format(pdf_name, str(err)))
    else:
        print("FAIL: Dashboard '{}' was not found, make sure the name is right".format(
            looker_title))

    # Enviar archivos creados a un bucket en GCP
    if exists('{}/temporal_folder/{}'.format(base_path, dashboard.title)):
        print('---------- SENDING LOOKER FILES TO GCP BUCKET ----------')
        print('GCP Bucket : {}/{}/{}'.format(bucket_name,
              bucket_folder, bucket_subfolder))
        # Creacion cliente de GCP
        # storage_client = storage.Client()
        gcp_json = os.path.join(base_path, 'gcp.json')
        storage_client = storage.Client.from_service_account_json(gcp_json)

        # Envio de archivos
        for file in os.listdir('{}/temporal_folder/{}'.format(base_path, dashboard.title)):
            source_file_name = '{}/temporal_folder/{}/{}'.format(
                base_path, dashboard.title, file)
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
            logging.error(
                'FAIL: Error while deleting the temporal folder, exception is: {}'.format(str(err)))

    # Borramos el archivo looker.ini
    if exists('{}/looker.ini'.format(base_path)):
        try:
            os.remove('{}/looker.ini'.format(base_path))
        except Exception as err:
            logging.error(
                'FAIL: Error while deleting looker.ini file, exception is: {}'.format(str(err)))


#########################
#### Python Operator ####
#########################


def gcp_files_to_email(bucket_name, bucket_folder, bucket_subfolder, emails, email_subject):

    import os
    import logging
    import smtplib
    import shutil
    from pathlib import Path
    from os.path import exists
    from google.cloud import storage
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    # from airflow.models import Variable

    # Ruta base
    base_path = Path(__file__).resolve().parent

    # Descargar archivo desde GCP bucket a una carpeta local
    try:
        if not exists('{}/temporal_folder/{}'.format(base_path, bucket_subfolder)):
            os.makedirs(
                '{}/temporal_folder/{}'.format(base_path, bucket_subfolder))

        # storage_client = storage.Client()
        gcp_json = os.path.join(base_path, 'gcp.json')
        storage_client = storage.Client.from_service_account_json(gcp_json)

        print('---------- DOWNLOADING GCP FILES TO LOCAL ----------')
        print('GCP bucket: {}/{}/{}'.format(bucket_name,
              bucket_folder, bucket_subfolder))
        print('Local folder: temporal_folder/{}'.format(bucket_subfolder))
        for blob in storage_client.list_blobs('{}'.format(bucket_name), prefix='{}/{}'.format(bucket_folder, bucket_subfolder)):
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob.name)
            destination_file_name = '{}/temporal_folder/{}/{}'.format(
                base_path, bucket_subfolder, blob.name.split('/')[4])
            blob.download_to_filename(destination_file_name)
            print("OK LOCAL SAVE: '{}'".format(blob.name.split('/')[4]))
    except Exception as err:
        raise Exception(
            'FAIL: Error while downloading files from GCP bucket, exception is: {}'.format(str(err)))

    # Enviar email con files descargados del GCP bucket
    if exists("{}/temporal_folder/{}".format(base_path, bucket_subfolder)):
        print('---------- EMAILING DOWNLOADED GCP FILES ----------')
        # data_email = Variable.get('data_email', deserialize_json=True)
        import json
        email_json = os.path.join(base_path, 'data_email.json')
        data_email = json.load(open(email_json))

        for email in emails:

            message = MIMEMultipart()
            message['from'] = data_email['email_address']
            message['subject'] = '(NO REPLY) - outlook attached data: {}'.format(
                email_subject)
            message['to'] = email

            body = """
                <html lang="en">
                <head>
                </head>
                <body>
                    Hello, <br><br>
                    Attached you will find information related to your franchise business. <br><br>
                    Regards, <br>
                    Rodrigo Pizzano <br>
                </body>
                </html>
             """
            message.attach(MIMEText(body, 'html'))

            # Adjuntar archivos al correo
            for file in os.listdir('{}/temporal_folder/{}'.format(base_path, bucket_subfolder)):
                if os.path.splitext(file)[1] == '.pdf':
                    try:
                        with open("{}/temporal_folder/{}/{}".format(base_path, bucket_subfolder, file), 'rb') as f:
                            message.attach(MIMEApplication(
                                f.read(), name=file, _subtype='pdf'))
                    except Exception as err:
                        raise Exception(
                            'FAIL: Error while attaching PDF to email, exception is: {}'.format(str(err)))
                if os.path.splitext(file)[1] == '.csv':
                    try:
                        with open("{}/temporal_folder/{}/{}".format(base_path, bucket_subfolder, file), 'rb') as f:
                            message.attach(
                                MIMEApplication(f.read(), name=file))
                    except Exception as err:
                        raise Exception(
                            'FAIL: Error while attaching CSV to email, exception is: {}'.format(str(err)))

            # Envio de correo
            with smtplib.SMTP(host='smtp.gmail.com', port=587) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(data_email['email_address'],
                           data_email['email_password'])
                try:
                    smtp.send_message(message)
                    print('OK EMAIL: {}'.format(email))
                except Exception as err:
                    raise Exception(
                        'FAIL EMAIL: {}, exception is: {}'.format(email, str(err)))
    else:
        raise Exception(
            'FAIL: Local folder with GCP downloaded files does not exist')

    # Borramos la carpeta local temporal
    if exists('{}/temporal_folder'.format(base_path)):
        try:
            shutil.rmtree('{}/temporal_folder'.format(base_path))
        except Exception as err:
            logging.error(
                'FAIL: Error while deleting the temporal folder, exception is: {}'.format(str(err)))

##############################
###### AIRFLOW DAG PART ######
##############################


LOOKER_TITLE = 'Looker Dashboard name'
BUCKET_NAME = 'gcp-bucket-name'
BUCKET_FOLDER = 'folder/sub_folder'
FILES_FORMAT = ['CSV', 'PDF', 'PDF_and_CSV']

CSV_FILTER = {
    "Argentina": {'table_1.field_1': 'Argentina', 'table_2.field_2': "McDonald's"},
    "Chile": {'table_1.field_1': "Chile", 'table_2.field_2': "McDonald's"}
}

PDF_FILTER = {
    "Argentina": {"Field 1 Name": "Argentina", "Field 2 Name": "McDonald's"},
    "Chile": {"Field 1 Name": "Chile", "Field 2 Name": "McDonald's"}
}

EMAILS = {
    "Argentina": ["rodrigo.pizzano@outlook.com", "rrpizzano@gmail.com"],
    "Chile": ["rodrigo.pizzano@outlook.com", "rrpizzano@gmail.com"]
}

looker_csv_pdf_to_gcp(
    looker_title=LOOKER_TITLE,
    csv_filter=CSV_FILTER['Argentina'],
    pdf_filter=PDF_FILTER['Argentina'],
    files_format='PDF_and_CSV',
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_subfolder=LOOKER_TITLE+'/'+PDF_FILTER['Argentina']['Field 1 Name']
)

gcp_files_to_email(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_subfolder=LOOKER_TITLE+'/'+PDF_FILTER['Argentina']['Field 1 Name'],
    emails=EMAILS['Argentina'],
    email_subject='Testing'
)

"""
looker_csv_pdf_to_gcp(
    looker_title=LOOKER_TITLE,
    csv_filter=CSV_FILTER['Chile'],
    pdf_filter=PDF_FILTER['Chile'],
    files_format='PDF_and_CSV',
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_subfolder=LOOKER_TITLE+'/'+PDF_FILTER['Chile']['Field 1 Name']
)

gcp_files_to_email(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_subfolder=LOOKER_TITLE+'/'+PDF_FILTER['Chile']['Field 1 Name'],
    emails=EMAILS['Chile'],
    email_subject='Testing'
)
"""
