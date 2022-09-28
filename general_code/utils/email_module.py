def gcp_files_to_email(bucket_name, bucket_folder, emails, email_subject):
    """
    Takes all the files located in a Google Cloud Platform bucket folder and sends them by email to a specified list of recipients

    Attributes
    ----------
    bucket_name : string
        Bucket GCP name to take the files
    bucket_folder : string
        Bucket GCP folder to take the files
    emails : comma separated email addresses (Ex: rodrigo.pizzano@outlook.com, rrpizzano@gmail.com)
        List of email recipients
    email_subject : string
        Email subject details
    """

    import os
    import smtplib
    import shutil
    import zipfile
    import json
    import time
    from pathlib import Path
    from os.path import exists
    from google.cloud import storage
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from posixpath import basename
    # from airflow.models import Variable

    # Ruta base
    base_path = Path(__file__).resolve().parent

    # Descargar archivo desde GCP bucket a una carpeta local
    try:
        if not exists(f'{base_path}/{bucket_folder}'):
            os.makedirs(f'{base_path}/{bucket_folder}')

        print('\n', '---------- DOWNLOADING GCP FILES TO LOCAL ----------')
        print(f'GCP bucket: {bucket_name}/{bucket_folder}')
        print(f'Local folder: {base_path}/{bucket_folder}')

        # Creacion cliente de GCP
        # storage_client = storage.Client()
        gcp_json = os.path.join(base_path, 'config/gcp.json')
        storage_client = storage.Client.from_service_account_json(gcp_json)

        # Descarga de archivos
        for blob in storage_client.list_blobs(f'{bucket_name}', prefix=f'{bucket_folder}'):
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob.name)
            destination_file_name = f"{base_path}/{bucket_folder}/{blob.name.split('/')[-1]}"
            blob.download_to_filename(destination_file_name)
            print(f"OK LOCAL SAVE: '{blob.name.split('/')[-1]}'")

        # Zipeamos los CSVs
        with zipfile.ZipFile(f'{base_path}/{bucket_folder}/zipped_CSVs.zip', mode='w') as z:
            for file in os.listdir(f'{base_path}/{bucket_folder}'):
                if os.path.splitext(file)[1] == '.csv':
                    z.write(f'{base_path}/{bucket_folder}/{file}',
                            basename(f'{file}'))

            if os.stat(f'{base_path}/{bucket_folder}/zipped_CSVs.zip').st_size == 0:
                os.remove(f'{base_path}/{bucket_folder}/zipped_CSVs.zip')

            elif os.stat(f'{base_path}/{bucket_folder}/zipped_CSVs.zip').st_size != 0:
                print("OK LOCAL SAVE: 'zipped_CSVs.zip'")

    except Exception as err:
        raise Exception(
            f'FAIL: Error while downloading files from GCP bucket, exception is: {str(err)}')

    # Enviar email con files descargados del GCP bucket
    if exists(f'{base_path}/{bucket_folder}'):

        print('\n', '---------- EMAILING DOWNLOADED GCP FILES ----------')
        # data_email = Variable.get('data_email', deserialize_json=True)
        email_json = os.path.join(base_path, 'config/data_email.json')
        data_email = json.load(open(email_json))

        message = MIMEMultipart()
        message['from'] = data_email['email_address']
        message['to'] = ", ".join(emails)
        message['subject'] = email_subject
        body = """
            <html lang="en">
            <head>
            </head>
            <body>
                Hello, <br><br>
                Attached you will find information on the main KPIs of your interest. <br><br>
                Regards, <br>
                Rodrigo Pizzano <br><br>
            </body>
            </html>
        """
        message.attach(MIMEText(body, 'html'))

        # Adjuntar archivos al correo
        for file in os.listdir(f'{base_path}/{bucket_folder}'):
            if os.path.splitext(file)[1] == '.pdf':
                try:
                    with open(f'{base_path}/{bucket_folder}/{file}', 'rb') as f:
                        message.attach(
                            MIMEApplication(f.read(), name=file, _subtype='pdf'))
                    print(f"OK ATTACHED: '{file}'")
                except Exception as err:
                    raise Exception(
                        f'FAIL: Error while attaching PDF to email, exception is: {str(err)}')
            if os.path.splitext(file)[1] == '.zip':
                try:
                    with open(f'{base_path}/{bucket_folder}/{file}', 'rb') as f:
                        message.attach(
                            MIMEApplication(f.read(), name=file))
                    print(f"OK ATTACHED: '{file}'")
                except Exception as err:
                    raise Exception(
                        f'FAIL: Error while attaching zipped CSVs to email, exception is: {str(err)}')

        # Envio de correo
        with smtplib.SMTP(host='smtp.gmail.com', port=587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(data_email['email_address'],
                       data_email['email_password'])
            try:
                smtp.send_message(message)
                print(f'OK EMAIL: {", ".join(emails)}')
            except Exception as err:
                raise Exception(
                    f'FAIL EMAIL: {", ".join(emails)}, exception is: {str(err)}')
    else:
        raise Exception(
            'FAIL: Local folder with GCP downloaded files does not exist')

    # Borramos la carpeta local utilizada para descargar los archivos
    if exists(f'{base_path}/{bucket_folder}'):
        time.sleep(10)
        try:
            shutil.rmtree(f'{base_path}/{bucket_folder}')
            print(f'DELETED: {base_path}/{bucket_folder}')
        except Exception as err:
            raise Exception(
                f'FAIL: Error while deleting the local folder, exception is: {str(err)}')

    # Borrar carpetas vacias basura
    for folder in os.listdir(f'{base_path}'):
        if os.path.isdir(f'{base_path}/{folder}') and len(os.listdir(f'{base_path}/{folder}')) == 0:
            # os.rmdir(f'{base_path}/{folder}')
            print(folder)
