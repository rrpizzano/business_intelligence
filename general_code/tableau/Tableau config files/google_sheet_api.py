import gspread
from oauth2client.service_account import ServiceAccountCredentials

SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/...'
SPREADSHEET_ID = SPREADSHEET_URL.split('/')[5]

# Credenciales + cliente
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    './Tableau config files/gcp.json', scope)
client = gspread.authorize(credentials)

# lectura google sheet
spreadsheet = client.open_by_url(SPREADSHEET_URL)
worksheet = spreadsheet.get_worksheet(0)
rows = worksheet.get_all_records()
for row in rows:
    print(row)
