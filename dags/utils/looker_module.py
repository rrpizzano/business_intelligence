def looker_csv_pdf_to_gcp(looker_name, csv_filter, pdf_filter, files_format, bucket_name, bucket_folder):
    """
    Creates PDF and/or CSV files based on a Looker dashboard and saves them in a Google Cloud Platform bucket

    Attributes
    ----------
    looker_name : string
        Looker dashboard name to be searched in the Looker server.
    csv_filter : dictionary
        Filter for all the queries of the tiles in the Looker dashboard (this is used for CSV files)
        Value for no filters is {'none': {}}
    pdf_filter : dictionary
        Filter for all the views of the tiles in the Looker dashboard (this is used for PDF files)
        Value for no filters is {'none': {}}
    files_format : string
        Created files format, possible entries: PDF, CSV, PDF_and_CSV
    bucket_name : string
        Bucket GCP name to store the files
    bucket_folder : string
        Bucket GCP folder to store the files
    """
    import looker_sdk
    from looker_sdk import models
    import os
    import shutil
    import urllib
    import time
    from pathlib import Path
    from os.path import exists
    from google.cloud import storage
    from airflow.models import Variable

    # Ruta base
    base_path = Path(__file__).resolve().parent

    # Conexion al SDK de Looker (https://pypi.org/project/looker-sdk/)
    LOOKER_SDK_CONFIG = Variable.get(
        'name_of_the_variable_in_airflow', deserialize_json=True)
    BASE_URL = LOOKER_SDK_CONFIG['base_url']
    CLIENT_ID = LOOKER_SDK_CONFIG['client_id']
    CLIENT_SECRET = LOOKER_SDK_CONFIG['client_secret']
    try:
        with open(f'{base_path}/looker.ini', 'w') as f:
            f.write(
                f'[Looker]\n base_url={BASE_URL}\n client_id={CLIENT_ID}\n client_secret={CLIENT_SECRET}\n verify_ssl=True')
        sdk = looker_sdk.init31(f'{base_path}/looker.ini')
    except Exception as err:
        raise Exception(
            f'FAIL: Error while connecting to Looker SDK, exception is: {str(err)}')

    # Busqueda del Dashboard en la web de Looker
    try:
        dashboard = next(
            iter(sdk.search_dashboards(title=looker_name.lower())), None)
    except Exception as err:
        raise Exception(
            f'FAIL: Error while searching Dashboard, exception is: {str(err)}')

    # Descarga de dashboard a PDF y/o CSVs
    if dashboard.id != None:
        print('\n', '---------- DOWNLOADING LOOKER FILES TO LOCAL ----------')
        print(f'Local folder: {base_path}/temp')

        # CSVs
        if files_format == 'CSV' or files_format == 'PDF_and_CSV':

            tiles = sdk.dashboard_dashboard_elements(dashboard_id=dashboard.id)

            for tile in tiles:
                if tile.title != None:

                    # Aplicación de filtros sobre query del tile
                    new_query = tile.query
                    dict1 = new_query.filters
                    dict2 = csv_filter
                    dict1.update(
                        (k, dict2[k]) for k in dict1.keys() & dict2.keys())
                    new_query.client_id = None
                    new_query.id = None
                    new_query_results = sdk.run_inline_query(
                        body=new_query, result_format='csv')

                    # Descarga resultados CSV en carpeta local
                    csv_suffix = ''
                    for key, value in csv_filter.items():
                        if key != 'none':
                            csv_suffix += ' - ' + value
                    csv_name = tile.title + csv_suffix
                    try:
                        if not exists(f'{base_path}/temp'):
                            os.makedirs(f'{base_path}/temp')
                        with open(f"{base_path}/temp/{csv_name}.csv", 'wb') as f:
                            f.write(str.encode(new_query_results))
                            print(f"OK LOCAL SAVE: CSV '{csv_name}.csv'")
                    except Exception as err:
                        raise Exception(
                            f"FAIL LOCAL SAVE: CSV '{csv_name}.csv', exception is {str(err)}")

        # PDFs
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
                    f'FAIL: Could not create a render task for dashboard_id {id}')

            # Esperamos que termine el render del PDF (sino queda corrupto el archivo)
            delay = 5.0
            while True:
                poll = sdk.render_task(task.id)
                if poll.status == 'failure':
                    raise Exception(
                        f'FAIL: Render failed for dashboard_id {id}')
                elif poll.status == 'success':
                    break
                time.sleep(delay)

            # Descarga resultados PDF en carpeta local
            pdf_result = sdk.render_task_results(task.id)
            pdf_suffix = ''
            for key, value in pdf_filter.items():
                if key != 'none':
                    pdf_suffix += ' - ' + value
            pdf_name = dashboard.title + pdf_suffix
            try:
                if not exists(f'{base_path}/temp'):
                    os.makedirs(f'{base_path}/temp')
                with open(f'{base_path}/temp/{pdf_name}.pdf', 'wb') as f:
                    f.write(pdf_result)
                    print(f"OK LOCAL SAVE: PDF '{pdf_name}.pdf'")
            except Exception as err:
                raise Exception(
                    f"FAIL LOCAL SAVE: PDF '{pdf_name}.pdf', exception is {str(err)}")
    else:
        print(
            f"FAIL: Dashboard '{looker_name}' was not found, make sure the name is right")

    # Enviar archivos creados a un bucket en GCP
    if exists(f'{base_path}/temp'):
        print('\n', '---------- SENDING LOOKER FILES TO GCP BUCKET ----------')
        print(f'GCP Bucket : {bucket_name}/{bucket_folder}')

        # Creacion cliente de GCP
        storage_client = storage.Client()

        # Envio de archivos
        for file in os.listdir(f'{base_path}/temp'):
            source_file_name = f'{base_path}/temp/{file}'
            destination_blob_name = f'{bucket_folder}/{file}'
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            try:
                blob.upload_from_filename(source_file_name)
                print(f"OK GCP SEND: '{file}'")
            except Exception as err:
                raise Exception(
                    f"FAIL GCP SEND: '{file}', exception is {str(err)}")
    else:
        raise Exception(
            'FAIL: Local folder not exists, there are no files to send fo GCP bucket')

    # Borramos la carpeta local utilizada para descargar los archivos
    if exists(f'{base_path}/temp'):
        try:
            shutil.rmtree(f'{base_path}/temp')
            print(f'Temporal folder deleted: {base_path}/temp')
        except Exception as err:
            raise Exception(
                f'FAIL: Error while deleting the local folder, exception is: {str(err)}')

    # Borramos el archivo looker.ini
    if exists(f'{base_path}/looker.ini'):
        try:
            os.remove(f'{base_path}/looker.ini')
            print(f'Looker file deleted: {base_path}/looker.ini')
        except Exception as err:
            raise Exception(
                f'FAIL: Error while deleting looker.ini file, exception is: {str(err)}')


def looker_csv_pdf_to_gcp_operator(task_id, looker_name, csv_filter, pdf_filter, files_format, bucket_name, bucket_folder):
    """
    Returns an Airflow PythonVirtualenvOperator using the function 'looker_csv_pdf_to_gcp'
    Requires the install of the looker-sdk v22.0.0 package

    Attributes
    ----------
    task_id : string
        Name of the Airflow task
    looker_name : string
        Looker dashboard name to be searched in the Looker server.
    csv_filter : dictionary
        Filter for all the queries of the tiles in the Looker dashboard (this is used for CSV files)
        Value for no filters is {'none': {}}
    pdf_filter : dictionary
        Filter for all the views of the tiles in the Looker dashboard (this is used for PDF files)
        Value for no filters is {'none': {}}
    files_format : string
        Created files format, possible entries: PDF, CSV, PDF_and_CSV
    bucket_name : string
        Bucket GCP name to store the files
    bucket_folder : string
        Bucket GCP folder to store the files
    """
    from airflow.operators.python_operator import PythonVirtualenvOperator
    return PythonVirtualenvOperator(
        task_id=task_id,
        python_callable=looker_csv_pdf_to_gcp,
        op_kwargs=dict(
            looker_name=looker_name,
            csv_filter=csv_filter,
            pdf_filter=pdf_filter,
            files_format=files_format,
            bucket_name=bucket_name,
            bucket_folder=bucket_folder
        ),
        requirements=[
            "looker-sdk==22.0.0",
            "apache-airflow"
        ]
    )
