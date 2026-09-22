import os
import requests
from dotenv import load_dotenv

load_dotenv()

TENANT_ID = os.getenv("CZBI_TENANT_ID")
CLIENT_ID = os.getenv("CZBI_CLIENT_ID")
CLIENT_SECRET = os.getenv("CZBI_CLIENT_SECRET")

if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    raise RuntimeError(
        "Missing CZBI_TENANT_ID, CZBI_CLIENT_ID or CZBI_CLIENT_SECRET in .env"
    )

TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

token_payload = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "https://analysis.windows.net/powerbi/api/.default",
    "grant_type": "client_credentials",
}
print(f"TENANT_ID=[{TENANT_ID}]")
print(f"CLIENT_ID=[{CLIENT_ID}]")


token_response = requests.post(TOKEN_URL, data=token_payload)
print("Token status:", token_response.status_code)

if token_response.status_code != 200:
    print(token_response.text)
    raise SystemExit("Could not get token")

access_token = token_response.json()["access_token"]
print("Token length:", len(access_token))

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

# First test: list Power BI workspaces
groups_url = "https://api.powerbi.com/v1.0/myorg/groups"
groups_response = requests.get(groups_url, headers=headers)

print("Groups status:", groups_response.status_code)
print(groups_response.text)