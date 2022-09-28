def tableau_csv_pdf_to_gcp(workbook_name, view_name, pdf_csv, bucket_name, bucket_folder):
    """
    Creates PDF and/or CSV files based on a Tableau dashboard view and saves them in a Google Cloud Platform bucket

    Attributes
    ----------
    workbook_name : string
        Workbook name to be searched in the Tableau server.
    view_name : string
        View name used to generate the files.
    pdf_csv : string 
        Created files format, possible entries: PDF, CSV, PDF_and_CSV
    bucket_name : string
        Bucket GCP name to store the files
    bucket_folder : string
        Bucket GCP folder to store the files
    """
    from pathlib import Path
    import os
    import tableauserverclient as TSC
    from os.path import exists
    import pandas as pd
    from io import StringIO
    from google.cloud import storage
    import shutil
    from airflow.models import Variable

    # Ruta base
    base_path = Path(__file__).resolve().parent

    # Conexion a Tableau server (usar VPN si es prueba local)
    TABLEAU_CONFIG = Variable.get('tableau_auth', deserialize_json=True)
    SERVER_URL = TABLEAU_CONFIG['server_url']
    USER = TABLEAU_CONFIG['user']
    PASSWORD = TABLEAU_CONFIG['password']
    SITE = TABLEAU_CONFIG['site_id']
    try:
        tableau_auth = TSC.TableauAuth(USER, PASSWORD, SITE)
        server = TSC.Server(SERVER_URL, use_server_version=True)
        server.auth.sign_in(tableau_auth)
    except Exception as err:
        raise Exception(
            f'Error while connecting to Tableau server, exception is: {str(err)}')

    with server.auth.sign_in(tableau_auth):

        # Busqueda del workbook
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(
            TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, workbook_name))
        matching_workbooks, pagination_item = server.workbooks.get(req_option)

        # Busqueda de la vista
        view_id = None
        if len(matching_workbooks) == 1:
            workbook = matching_workbooks[0]
            all_views = server.workbooks.populate_views(workbook)
            for view in workbook.views:
                if view.name == view_name:
                    view_id = view.id

        if view_id != None:
            print('\n', '---------- DOWNLOADING TABLEAU FILES TO LOCAL ----------')
            print(f'Local folder: {base_path}/temp')
            view_item = server.views.get_by_id(view_id)

            # Creacion folder local
            try:
                if not exists(f'{base_path}/temp'):
                    os.makedirs(f'{base_path}/temp')
            except Exception as err:
                raise Exception(
                    f'Fail creating local folder, exception is {str(err)}')

            # guardar vista como PDF
            if pdf_csv == 'PDF' or pdf_csv == 'PDF_and_CSV':
                try:
                    server.views.populate_pdf(view_item, req_options=None)
                    with open(f'{base_path}/temp/{view_item.name}.pdf', 'wb') as f:
                        f.write(view_item.pdf)
                        print(f"OK LOCAL SAVE: PDF '{view_item.name}.pdf'")
                except Exception as err:
                    raise Exception(
                        f"FAIL LOCAL SAVE: PDF '{view_item.name}.pdf', exception is {str(err)}")

            # guardar vista como CSV
            if pdf_csv == 'CSV' or pdf_csv == 'PDF_and_CSV':
                try:
                    server.views.populate_csv(view_item, req_options=None)
                    string = StringIO(b''.join(view_item.csv).decode("utf-8"))
                    df = pd.read_csv(string, sep=",")

                    # Se pivotea la tabla si contiene una columna llamada "Measure Values"
                    if 'Measure Values' in df.columns.values:
                        cols_order = df['Measure Names'].unique().tolist()
                        cols = [c for c in df.columns.values if c not in (
                            'Measure Values', 'Measure Names')]
                        df = df.pivot(
                            index=cols, columns=['Measure Names'], values='Measure Values')[cols_order]
                        df.sort_values(cols)
                        df.to_csv(
                            f'{base_path}/temp/{view_item.name}.csv')
                    else:
                        df.to_csv(
                            f'{base_path}/temp/{view_item.name}.csv')
                    print(f"OK LOCAL SAVE: CSV '{view_item.name}.csv'")
                except Exception as err:
                    raise Exception(
                        f"FAIL LOCAL SAVE: CSV '{view_item.name}.csv', exception is {str(err)}")

        if len(matching_workbooks) != 1 or view_id == None:
            print('FAIL: Workbook or View name were not found')

    # Enviar archivos creados a un bucket en GCP
    if exists(f'{base_path}/temp'):
        print('\n', '---------- SENDING TABLEAU FILES TO GCP BUCKET ----------')
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


def tableau_csv_pdf_to_gcp_operator(task_id, workbook_name, view_name, pdf_csv, bucket_name, bucket_folder):
    """
    Returns an Airflow PythonOperator using the function 'tableau_csv_pdf_to_gcp'

    Attributes
    ----------
    task_id : string
        Name of the Airflow task
    workbook_name : string
        Workbook name to be searched in the Tableau server.
    view_name : string
        View name used to generate the files.
    pdf_csv : string 
        Created files format, possible entries: PDF, CSV, PDF_and_CSV
    bucket_name : string
        Bucket GCP name to store the files
    bucket_folder : string
        Bucket GCP folder to store the files
    """
    from airflow.operators.python_operator import PythonOperator
    return PythonOperator(
        task_id=task_id,
        python_callable=tableau_csv_pdf_to_gcp,
        op_kwargs=dict(
            workbook_name=workbook_name,
            view_name=view_name,
            pdf_csv=pdf_csv,
            bucket_name=bucket_name,
            bucket_folder=bucket_folder
        )
    )
