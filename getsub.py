import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

GOCANVAS_BASE = "https://www.gocanvas.com"
TOKEN_URL = f"{GOCANVAS_BASE}/oauth/token"
API_BASE = f"{GOCANVAS_BASE}/api/v3"

GC_CLIENT_ID = os.getenv("GC_CLIENT_ID")
GC_CLIENT_SECRET = os.getenv("GC_CLIENT_SECRET")

FORM_ID = 5771040


def assert_json_response(r: requests.Response, context: str) -> dict:
    ct = (r.headers.get("Content-Type") or "").lower()
    if "application/json" not in ct:
        snippet = r.text[:400].replace("\n", " ")
        raise RuntimeError(
            f"{context}: Expected JSON but got Content-Type={ct}. "
            f"Status={r.status_code}. First chars: {snippet}"
        )
    return r.json()


def unwrap_data(obj):
    if isinstance(obj, dict) and "data" in obj:
        return obj["data"]
    return obj


def get_access_token():
    existing = os.getenv("GC_ACCESS_TOKEN")
    if existing:
        return existing

    r = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        headers={
            "Accept": "application/json",
            "User-Agent": "PostmanRuntime/7.36.0",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        auth=(GC_CLIENT_ID, GC_CLIENT_SECRET),
        timeout=60,
    )
    return assert_json_response(r, "Token request")["access_token"]


def list_submissions(token: str, form_id: int):
    url = f"{API_BASE}/submissions"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {"form_id": str(form_id)}

    r = requests.get(url, headers=headers, params=params, timeout=60)
    print("Status:", r.status_code)
    print("URL:", r.url)

    if r.status_code != 200:
        raise RuntimeError(f"List submissions failed {r.status_code}: {r.text}")

    body = assert_json_response(r, "List submissions")
    return unwrap_data(body)


token = get_access_token()
items = list_submissions(token, FORM_ID)

print(type(items))
print(json.dumps(items[:5] if isinstance(items, list) else items, indent=2)[:8000])
