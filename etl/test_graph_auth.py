import os
import requests
from dotenv import load_dotenv

load_dotenv()

TENANT_ID = os.getenv("GR_TENANT_ID")
CLIENT_ID = os.getenv("GR_CLIENT_ID")
CLIENT_SECRET = os.getenv("GR_CLIENT_SECRET")

if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    raise RuntimeError(
        "Missing CZBI_TENANT_ID, CZBI_CLIENT_ID or CZBI_CLIENT_SECRET in .env"
    )
url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "https://graph.microsoft.com/.default"
}

response = requests.post(url, data=data)

print("Status:", response.status_code)
print(response.json())

access_token = response.json()["access_token"]

headers = {
    "Authorization": f"Bearer {access_token}"
}

graph_response = requests.get(
    "https://graph.microsoft.com/v1.0/sites?search=*",
    headers=headers
)

print("Graph status:", graph_response.status_code)
print(graph_response.json())