import importlib.util


# import email module
email_spec = importlib.util.spec_from_file_location(
    "mod", "/Users/Rodrigo/Documents/GitHub/rodrigopizzano/utils/email_module.py")
email_module = importlib.util.module_from_spec(email_spec)
email_spec.loader.exec_module(email_module)


# Variables globales
BUCKET_NAME = 'gcp-bucket-name'
EMAILS = ['rodrigo.pizzano@outlook.com', 'rrpizzano@gmail.com']
BUCKET_FOLDER = 'folder/sub_folder'
EMAIL_SUBJECT = "[Attached data] File Name - Some other info"

# GCP files to email
email_module.gcp_files_to_email(
    bucket_name=BUCKET_NAME,
    bucket_folder=BUCKET_FOLDER,
    emails=EMAILS,
    email_subject=EMAIL_SUBJECT
)
