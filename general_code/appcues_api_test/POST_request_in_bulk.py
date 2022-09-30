"""
curl https://api.appcues.com/v2/accounts/ACCOUNT_ID/import/profiles \
  -u API_KEY:API_SECRET \
  -H 'Content-type: application/ndjson' \
  -d '
{"user_id": "some-id-here", "email": "harriet@example.com"}
{"user_id": "another-user-id", "first_name": "Snabe"}
{"user_id": "a-third-user", "email": "bumbleborn@example.com", isAdmin: true}
  '
"""

import requests
import json

appcues_keys = json.load(
    open('general_code/appcues_api_test/config/appcues_keys.json'))

data = ' {"user_id": "rodri_19", "test_1": "aaa_19", "test_2": 19, "test_3": 1663545600000, "row_num": 1} \n {"user_id": "rodri_16", "test_1": "aaa_16", "test_2": 16, "test_3": 1663286400000, "row_num": 2} '

response = requests.post(
    "https://api.appcues.com/v2/accounts/ACCOUNT_ID/import/profiles",
    headers={'Content-type': 'application/ndjson'},
    data=data,
    auth=(appcues_keys['key'], appcues_keys['secret'])
)

response_json = json.loads(response.content)
print(json.dumps(response_json, indent=2))
