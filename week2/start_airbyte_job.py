import requests
import json

# Get secrets
with open("secrets/airbyte_secrets.json", "r") as f:
    config = json.load(f)

api_endpoint = "http://localhost:8000/api"
api_key = config["api_key"]
connection_id = config["connection_id"]

headers = {
    "Authorization": f"Basic {api_key}",
    "Content-Type": "application/json"
}

payload = {
    "connectionId": f"{connection_id}"
}

#r = requests.post(f"{api_endpoint}/v1/connections/sync", headers=headers, data=json.dumps(payload))
#print(r.status_code)