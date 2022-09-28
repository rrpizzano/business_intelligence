import json
from unittest import result
import looker_sdk
import os
from looker_sdk import models
from pathlib import Path

# Ruta base
base_path = Path(__file__).resolve().parent
LOOKER_TITLE = 'Looker Dashboard name'

# Conexion al SDK de Looker
LOOKER_SDK_CONFIG = json.load(open(os.path.join(base_path, 'looker.json')))
BASE_URL = LOOKER_SDK_CONFIG['base_url']
CLIENT_ID = LOOKER_SDK_CONFIG['client_id']
CLIENT_SECRET = LOOKER_SDK_CONFIG['client_secret']
with open('{}/looker.ini'.format(base_path), 'w') as f:
    f.write('[Looker]\n base_url={}\n client_id={}\n client_secret={}\n verify_ssl=True'.format(
        BASE_URL, CLIENT_ID, CLIENT_SECRET))
sdk = looker_sdk.init31('{}/looker.ini'.format(base_path))
dashboard = next(iter(sdk.search_dashboards(title=LOOKER_TITLE.lower())), None)

FILTER = {'table_1.field_1': 'value_1',
          'table_2.field_2': 'value_2'}

tiles = sdk.dashboard_dashboard_elements(dashboard_id=dashboard.id)

# Impresion de filtros de queries del dashboard

for tile in tiles:
    if tile.title != None:
        print(tile.title)
        print(json.dumps(tile.query.filters, indent=2, sort_keys=True))


"""
# Editar el filtro de una query
for tile in tiles:
    if tile.title == 'Evolución de Pedidos':
        print('---------- Old filter ----------')
        print(json.dumps(tile.query.filters, indent=2, sort_keys=True))

        new_query = tile.query
        dict1 = new_query.filters
        dict2 = {"table_1.field_1": "value_1",
                 "table_2.field_2": 'value_2'}
        dict1.update((k, dict2[k])for k in dict1.keys() & dict2.keys())

        print('---------- New filter ----------')
        print(json.dumps(new_query.filters, indent=2, sort_keys=True))
"""
