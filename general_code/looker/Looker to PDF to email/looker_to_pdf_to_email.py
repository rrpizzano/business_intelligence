#########################
#### Python Operator ####
#########################

def lookerPDFtoGCPbucketOperator(looker_title, looker_filter, pdf_suffix, bucket_name, bucket_folder):

    import looker_sdk
    from looker_sdk import models
    import logging
    import os
    import time
    import shutil
    import urllib
    from pathlib import Path
    from os.path import exists
    from google.cloud import storage
    # from airflow.models import Variable

    # Ruta base
    base_path = Path(__file__).resolve().parent

    # Conexion al SDK de Looker (https://pypi.org/project/looker-sdk/)
    # LOOKER_SDK_CONFIG = Variable.get('looker_sdk_variable_in_airflow', deserialize_json=True)
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

    # Render del Dashboard a PDF
    if dashboard.id != None:
        id = dashboard.id
        task = sdk.create_dashboard_render_task(
            dashboard_id=str(id),
            result_format='pdf',
            body=models.CreateDashboardRenderTask(
                dashboard_style='tiled',
                dashboard_filters=urllib.parse.urlencode(looker_filter)
            ),
            width=1260,
            height=5499
        )

        if not (task and task.id):
            raise Exception(
                'FAIL: Could not create a render task for dashboard_id: {}'.format(id))

        # Esperamos que termine el render del dashboard
        delay = 5.0
        while True:
            poll = sdk.render_task(task.id)
            if poll.status == 'failure':
                raise Exception(
                    'FAIL: Render failed for dashboard: {}'.format(id))
            elif poll.status == 'success':
                break
            time.sleep(delay)

        # Guardamos localmente el render en PDF
        result = sdk.render_task_results(task.id)
        try:
            if not exists('{}/temporal_folder'.format(base_path)):
                os.makedirs('{}/temporal_folder'.format(base_path))
            with open('{}/temporal_folder/{}_{}.pdf'.format(base_path, looker_title, pdf_suffix), 'wb') as f:
                f.write(result)
                logging.info(
                    'OK: PDF: {}_{} successfully saved locally in a temporal folder'.format(looker_title, pdf_suffix))
        except Exception as err:
            raise Exception(
                'FAIL: Error saving PDF locally, exception is: {}'.format(str(err)))

        # Enviar PDF creado a un bucket en GCP
        if exists('{}/temporal_folder/{}_{}.pdf'.format(base_path, looker_title, pdf_suffix)):
            #storage_client = storage.Client()
            gcp_json = os.path.join(base_path, 'gcp.json')
            storage_client = storage.Client.from_service_account_json(gcp_json)
            source_file_name = "{}/temporal_folder/{}_{}.pdf".format(
                base_path, looker_title, pdf_suffix)
            destination_blob_name = '{}/{}_{}'.format(
                bucket_folder, looker_title, pdf_suffix)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            try:
                blob.upload_from_filename(source_file_name)
                logging.info("OK: pdf {}_{} successfully saved in GCP bucket: {}/{}".format(
                    looker_title, pdf_suffix, bucket_name, bucket_folder))
            except Exception as err:
                raise Exception(
                    'FAIL: Error while sending PDF to GCP, exception is: {}'.format(str(err)))
        else:
            raise Exception('FAIL: Dashboard PDF not found locally')
    else:
        raise Exception('FAIL: Looker Dashboard does not exists')

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

def GCPbucketPDFtoEmailOperator(bucket_name, bucket_folder, bucket_file, emails):

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
        if not exists('{}/temporal_folder'.format(base_path)):
            os.makedirs('{}/temporal_folder'.format(base_path))
        destination_file_name = "{}/temporal_folder/{}.pdf".format(
            base_path, bucket_file)
        #storage_client = storage.Client()
        gcp_json = os.path.join(base_path, 'gcp.json')
        storage_client = storage.Client.from_service_account_json(gcp_json)
        bucket = storage_client.bucket(bucket_name)
        source_blob_name = '{}/{}'.format(bucket_folder, bucket_file)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        logging.info("OK: PDF successfully saved locally.")
    except Exception as err:
        raise Exception(
            'FAIL: Error while downloading PDF from GCP bucket, exception is: {}'.format(str(err)))

    # Enviar email con PDF descargado del GCP bucket
    if exists("{}/temporal_folder/{}.pdf".format(base_path, bucket_file)):

        for email in emails:
            # data_email = Variable.get('data_email', deserialize_json=True)
            data_email = {"email_address": "data-noreply@outlook.com",
                          "email_password": "y9>XaL8be=d*g"}
            message = MIMEMultipart()
            message['from'] = data_email['email_address']
            message['subject'] = '(NO REPLY) Looker Dashboard: {}'.format(
                bucket_file)
            message['to'] = email
            body = """
                <html lang="en">
                <head>  
                </head>
                <body>
                    Hello, <br><br>
                    The PDF corresponding to the Looker dashboard <strong> '{}' </strong> is attached. <br><br>
                    Regards, <br>
                    Rodrigo Pizzano <br>
                </body>
                </html>
            """.format(bucket_file)
            message.attach(MIMEText(body, 'html'))

            # Adjuntar PDF al correo
            try:
                with open("{}/temporal_folder/{}.pdf".format(base_path, bucket_file), 'rb') as f:
                    message.attach(MIMEApplication(f.read(), _subtype='pdf'))
            except Exception as err:
                raise Exception(
                    'FAIL: Error while attaching PDF to email, exception is: {}'.format(str(err)))

            # Envio de correo
            with smtplib.SMTP(host='smtp.gmail.com', port=587) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(data_email['email_address'],
                           data_email['email_password'])
                try:
                    smtp.send_message(message)
                    logging.info(
                        'OK: Message sent to: {} with PDF: {}'.format(email, bucket_file))
                except Exception as err:
                    raise Exception(
                        'FAIL: Error while sending email with PDF, exception is: {}'.format(str(err)))
    else:
        raise Exception('FAIL: PDF not found locally')

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

LOOKER_TITLE = 'Reporte Partner IB + KA (by Partner)'
BUCKET_NAME = 'us-peya-da-food-and-groceries-pro'
BUCKET_FOLDER = 'raw/pdf_looker_files'

LOOKER_FILTERS = {
    "Argentina": {"Field Name 1": "Argentina", "Filed Name 2": "McDonald's"},
    "Chile": {"Field Name 1": "Chile", "Filed Name 2": "McDonald's"},
    "Uruguay": {"Field Name 1": "Uruguay", "Filed Name 2": "McDonald's"},
    "Panamá": {"Field Name 1": "Panamá", "Filed Name 2": "McDonald's"},
    "Venezuela": {"Field Name 1": "Venezuela", "Filed Name 2": "McDonald's"},
    "Ecuador": {"Field Name 1": "Ecuador", "Filed Name 2": "McDonald's"},
    "Costa Rica": {"Field Name 1": "Costa Rica", "Filed Name 2": "McDonald's"}
}

EMAILS = {
    "Argentina": ["rodrigo.pizzano@outlook.com", "rrpizzano@gmail.com"],
    "Chile": ["rodrigo.pizzano@outlook.com", "rrpizzano@gmail.com"],
    "Uruguay": ["rodrigo.pizzano@outlook.com", "rrpizzano@gmail.com"],
    "Panamá": ["rodrigo.pizzano@outlook.com", "rrpizzano@gmail.com"],
    "Venezuela": ["rodrigo.pizzano@outlook.com", "rrpizzano@gmail.com"],
    "Ecuador": ["rodrigo.pizzano@outlook.com", "rrpizzano@gmail.com"],
    "Costa Rica": ["rodrigo.pizzano@outlook.com", "rrpizzano@gmail.com"]
}

lookerPDFtoGCPbucketOperator(
    looker_title=LOOKER_TITLE,
    looker_filter=LOOKER_FILTERS["Argentina"],
    pdf_suffix=LOOKER_FILTERS["Argentina"]["Field Name 1"],
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER
)

lookerPDFtoGCPbucketOperator(
    looker_title=LOOKER_TITLE,
    looker_filter=LOOKER_FILTERS["Chile"],
    pdf_suffix=LOOKER_FILTERS["Chile"]["Field Name 1"],
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER
)

lookerPDFtoGCPbucketOperator(
    looker_title=LOOKER_TITLE,
    looker_filter=LOOKER_FILTERS["Uruguay"],
    pdf_suffix=LOOKER_FILTERS["Uruguay"]["Field Name 1"],
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER
)

lookerPDFtoGCPbucketOperator(
    looker_title=LOOKER_TITLE,
    looker_filter=LOOKER_FILTERS["Panamá"],
    pdf_suffix=LOOKER_FILTERS["Panamá"]["Field Name 1"],
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER
)

lookerPDFtoGCPbucketOperator(
    looker_title=LOOKER_TITLE,
    looker_filter=LOOKER_FILTERS["Venezuela"],
    pdf_suffix=LOOKER_FILTERS["Venezuela"]["Field Name 1"],
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER
)

lookerPDFtoGCPbucketOperator(
    looker_title=LOOKER_TITLE,
    looker_filter=LOOKER_FILTERS["Ecuador"],
    pdf_suffix=LOOKER_FILTERS["Ecuador"]["Field Name 1"],
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER
)

lookerPDFtoGCPbucketOperator(
    looker_title=LOOKER_TITLE,
    looker_filter=LOOKER_FILTERS["Costa Rica"],
    pdf_suffix=LOOKER_FILTERS["Costa Rica"]["Field Name 1"],
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER
)

GCPbucketPDFtoEmailOperator(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_file=LOOKER_TITLE+"_"+LOOKER_FILTERS["Argentina"]["Field Name 1"],
    emails=EMAILS["Argentina"]
)

GCPbucketPDFtoEmailOperator(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_file=LOOKER_TITLE+"_"+LOOKER_FILTERS["Chile"]["Field Name 1"],
    emails=EMAILS["Chile"]
)

GCPbucketPDFtoEmailOperator(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_file=LOOKER_TITLE+"_"+LOOKER_FILTERS["Uruguay"]["Field Name 1"],
    emails=EMAILS["Uruguay"]
)

GCPbucketPDFtoEmailOperator(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_file=LOOKER_TITLE+"_"+LOOKER_FILTERS["Panamá"]["Field Name 1"],
    emails=EMAILS["Panamá"]
)

GCPbucketPDFtoEmailOperator(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_file=LOOKER_TITLE+"_"+LOOKER_FILTERS["Venezuela"]["Field Name 1"],
    emails=EMAILS["Venezuela"]
)

GCPbucketPDFtoEmailOperator(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_file=LOOKER_TITLE+"_"+LOOKER_FILTERS["Ecuador"]["Field Name 1"],
    emails=EMAILS["Ecuador"]
)

GCPbucketPDFtoEmailOperator(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    bucket_file=LOOKER_TITLE+"_"+LOOKER_FILTERS["Costa Rica"]["Field Name 1"],
    emails=EMAILS["Costa Rica"]
)
