import os
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("GC_CLIENT_ID")
CLIENT_SECRET = os.getenv("GC_CLIENT_SECRET")

TOKEN_URL = "https://www.gocanvas.com/api/v3/oauth/token"


def get_access_token():
    payload = {"grant_type": "client_credentials"}

    response = requests.post(
        TOKEN_URL,
        data=payload,
        auth=(CLIENT_ID, CLIENT_SECRET)
    )

    if response.status_code != 200:
        raise Exception(f"Failed to get token: {response.text}")

    token = response.json()["access_token"]

    return token