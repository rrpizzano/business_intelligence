import os
import smtplib
import json
import shutil

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from os.path import exists
from google.cloud import storage

# Ruta base
base_path = Path(__file__).resolve().parent

# Ruta GCP bucket
bucket_name = 'us-bucket-name'
bucket_folder = 'folder/sub_folder'
bucket_file = 'File name in bucket'
bucket_file_link = 'https://looker.com/dashboards/...'

# Credenciales para correo
email_from = ''
email_from_pass = ''
email_to = 'rodrigo.pizzano@outlook.com'

# Descargar PDF del GCP bucket
try:
    gcp_json = os.path.join(base_path, 'gcp.json')
    os.makedirs('{}/temporal_folder'.format(base_path))
    source_blob_name = '{}/{}'.format(bucket_folder, bucket_file)
    destination_file_name = "{}/temporal_folder/{}.pdf".format(
        base_path, bucket_file)

    storage_client = storage.Client.from_service_account_json(gcp_json)

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print("OK --> PDF successfully saved locally.")
except Exception as err:
    raise Exception(
        'FAIL --> Error while downloading PDF from GCP bucket, exception is: {}'.format(str(err)))

# Enviar email con PDF descargado del GCP bucket
if exists("{}/temporal_folder/{}.pdf".format(base_path, bucket_file)):
    message = MIMEMultipart()
    message['from'] = email_from
    message['subject'] = '(No reply) - Looker Dashboard: {}'.format(bucket_file)
    message['to'] = email_to
    body = """
        <html lang="en">
        <head>  
        </head>
        <body>
            Hello, <br><br>
            The PDF corresponding to the Looker dashboard <strong> '{}' </strong> is attached. <br>
            If you want to go directly to the dashboard please follow this link: <strong> <a href= {} > click here </a> </strong><br><br>
            Regards, <br>
            Rodrigo Pizzano.
        </body>
        </html>
    """.format(bucket_file, bucket_file_link)
    message.attach(MIMEText(body, 'html'))

    # Adjuntar PDF al correo
    try:
        with open("{}/temporal_folder/{}.pdf".format(base_path, bucket_file), 'rb') as f:
            message.attach(MIMEApplication(f.read(), _subtype='pdf'))
    except Exception as err:
        raise Exception(
            'FAIL --> Error while attaching PDF to email, exception is: {}'.format(str(err)))

    # Envio de correo
    with smtplib.SMTP(host='smtp.gmail.com', port=587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(email_from, email_from_pass)
        try:
            smtp.send_message(message)
            print(
                'OK --> Message sent to: {} with PDF: {}'.format(email_to, bucket_file))
        except Exception as err:
            raise Exception(
                'FAIL --> Error while sending email with PDF, exception is: {}'.format(str(err)))
else:
    print('FAIL --> PDF not found locally')

# Borramos la carpeta temporal
try:
    shutil.rmtree('{}/temporal_folder'.format(base_path))
except Exception as err:
    raise Exception(
        'FAIL --> Error while deleting the temporal folder, exception is: {}'.format(str(err)))
