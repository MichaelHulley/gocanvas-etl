import os

import requests
from dotenv import load_dotenv

load_dotenv()

TENANT_ID = os.getenv("GR_TENANT_ID")
CLIENT_ID = os.getenv("GR_CLIENT_ID")
CLIENT_SECRET = os.getenv("GR_CLIENT_SECRET")

if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    raise RuntimeError(
        "Missing GR_TENANT_ID, GR_CLIENT_ID or GR_CLIENT_SECRET in .env"
    )

# =========================
# GET ACCESS TOKEN
# =========================

url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "https://graph.microsoft.com/.default",
}

response = requests.post(url, data=data, timeout=30)

print("Token status:", response.status_code)

if response.status_code != 200:
    print("Token request failed.")
    print(response.text)
    raise SystemExit(1)

token_data = response.json()
access_token = token_data.get("access_token")

if not access_token:
    raise RuntimeError("Token response did not contain an access token.")

print("Access token obtained successfully.")

# =========================
# TEST MICROSOFT GRAPH
# =========================

headers = {
    "Authorization": f"Bearer {access_token}"
}

graph_response = requests.get(
    "https://graph.microsoft.com/v1.0/sites?search=*",
    headers=headers,
    timeout=30,
)

print("Graph status:", graph_response.status_code)

if graph_response.status_code == 200:
    sites = graph_response.json().get("value", [])

    print(f"Sites returned: {len(sites)}")

    for site in sites:
        print(
            f"- {site.get('displayName')} | "
            f"{site.get('webUrl')}"
        )
else:
    print("Graph request failed.")
    print(graph_response.text)