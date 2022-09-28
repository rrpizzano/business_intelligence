import importlib.util

# import Tableau module
tableau_spec = importlib.util.spec_from_file_location(
    "mod", "/Users/Rodrigo/Documents/GitHub/rodrigopizzano/utils/tableau_module.py")
tableau_module = importlib.util.module_from_spec(tableau_spec)
tableau_spec.loader.exec_module(tableau_module)


# import email module
email_spec = importlib.util.spec_from_file_location(
    "mod", "/Users/Rodrigo/Documents/GitHub/rodrigopizzano/utils/email_module.py")
email_module = importlib.util.module_from_spec(email_spec)
email_spec.loader.exec_module(email_module)


# Variables globales
WORKBOOK_NAME = "Tableau Workbook Name"
VIEW_NAME_1 = 'Tableau View Name 1'
VIEW_NAME_2 = 'Tableau View Name 2'
BUCKET_NAME = 'bucket-name'
BUCKET_LOCATION = 'folder/sub_folder'
EMAILS = ['rodrigo.pizzano@outlook.com', 'rrpizzano@gmail.com']


# Tableau to GCP
tableau_module.tableau_csv_pdf_to_gcp(
    workbook_name=WORKBOOK_NAME,
    view_name=VIEW_NAME_1,
    pdf_csv='PDF_and_CSV',
    bucket_name=BUCKET_NAME,
    bucket_location=BUCKET_LOCATION
)

"""
tableau_module.tableau_csv_pdf_to_gcp(
    workbook_name=WORKBOOK_NAME,
    view_name=VIEW_NAME_2,
    pdf_csv='PDF_and_CSV',
    bucket_name=BUCKET_NAME,
    bucket_location=BUCKET_LOCATION
)
"""

# GCP to email
email_module.gcp_files_to_email(
    workbook_name=WORKBOOK_NAME,
    bucket_name=BUCKET_NAME,
    bucket_location=BUCKET_LOCATION,
    emails=EMAILS,
    email_peya_or_outsider='outsider'
)
