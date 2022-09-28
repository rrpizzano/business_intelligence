import looker_sdk
from pathlib import Path

base_path = Path(__file__).resolve().parent

LOOKER_SDK_CONFIG = {
    'base_url': 'https://...sa.looker.com...',
    'client_id': 'gv...',
    'client_secret': 'bM...'
}
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

LOOKER_TITLE = 'Looker Dashboard Name'
dashboard = next(iter(sdk.search_dashboards(title=LOOKER_TITLE.lower())), None)
print(dashboard.id)
