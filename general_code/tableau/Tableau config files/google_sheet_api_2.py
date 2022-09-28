from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas

# URL y RANGO del archivo google sheet
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/...'
SPREADSHEET_ID = SPREADSHEET_URL.split('/')[5]
RANGE_NAME = 'Sheet1!A1:H12'

# Conexion google sheet API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
creds = service_account.Credentials.from_service_account_file(
    './Tableau config files/gcp.json', scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)

# Lectura de datos en google sheet
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
values = result.get('values', [])

df = pandas.DataFrame(values)
df.columns = df.iloc[0]
df = df[1:]

list_names = list(set((df['Name'])))
print(list_names)
