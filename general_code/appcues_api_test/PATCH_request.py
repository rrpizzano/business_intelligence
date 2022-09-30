"""
curl https://api.appcues.com/v2/accounts/ACCOUNT_ID/users/USER_ID/profile \
  -u API_KEY:API_SECRET \
  -H 'Content-type: application/json' \
  -X PATCH \
  -d '
  {
    "first_name": "Harriet",
    "last_name": "Porber",
    "email": "harriet@bumbleborn.net"
  }
  '
"""

import requests
import json

appcues_keys = json.load(
    open('general_code/appcues_api_test/config/appcues_keys.json'))

user = 'rodri_1'
data = '{"test_1": "Rodrigo Pizzano", "test_2": "Ruben Ruiz"}'

response = requests.patch(
    f"{appcues_keys['endpoint']}/{user}/profile",
    headers={'Content-type': 'application/json'},
    data=data,
    auth=(appcues_keys['key'], appcues_keys['secret'])
)

response_json = json.loads(response.content)
print(json.dumps(response_json, indent=2))
