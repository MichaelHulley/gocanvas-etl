# test_gocanvas_token.py
import os
import requests
from dotenv import load_dotenv

load_dotenv()

GOCANVAS_BASE = "https://www.gocanvas.com"
TOKEN_URL = f"{GOCANVAS_BASE}/oauth/token"

GC_CLIENT_ID = os.getenv("GC_CLIENT_ID")
GC_CLIENT_SECRET = os.getenv("GC_CLIENT_SECRET")

print("GC_CLIENT_ID present:", bool(GC_CLIENT_ID))
print("GC_CLIENT_SECRET present:", bool(GC_CLIENT_SECRET))
print("TOKEN_URL:", TOKEN_URL)

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
    allow_redirects=False,
)

print("Status:", r.status_code)
print("Content-Type:", r.headers.get("Content-Type"))
print(r.text[:2000])