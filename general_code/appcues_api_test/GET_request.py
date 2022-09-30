import requests
import json

appcues_keys = json.load(
    open('general_code/appcues_api_test/config/appcues_keys.json'))

user = 'rodri_27'
response = requests.get(
    f"{appcues_keys['endpoint']}/{user}/profile",
    auth=(appcues_keys['key'], appcues_keys['secret'])
)
response_json = json.loads(response.content)
print(json.dumps(response_json, indent=2))
