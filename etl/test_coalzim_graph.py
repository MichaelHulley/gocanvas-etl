import os
import requests
from dotenv import load_dotenv

load_dotenv()

tenant_id = os.getenv("COALZIM_TENANT_ID")
client_id = os.getenv("COALZIM_CLIENT_ID")
client_secret = os.getenv("COALZIM_CLIENT_SECRET")
hostname = os.getenv("COALZIM_SITE_HOSTNAME")
site_path = os.getenv("COALZIM_SITE_PATH")

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

token_response = requests.post(token_url, data={
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": "https://graph.microsoft.com/.default",
})

print("tenant_id:", tenant_id)
print("client_id:", client_id)
print("secret exists:", bool(client_secret))


print("Token status:", token_response.status_code)
token_response.raise_for_status()

access_token = token_response.json()["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}

test_response = requests.get(
    "https://graph.microsoft.com/v1.0/sites?search=Knightshade",
    headers=headers
)

print("Search status:", test_response.status_code)
print(test_response.json())



site_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
site_response = requests.get(site_url, headers=headers)

print("Site status:", site_response.status_code)
print(site_response.json())