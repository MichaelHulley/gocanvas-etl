# =========================================
# GoCanvas Forms Endpoint Extract Script
# Author: Michael Hulley
# Date: 2026-04-01
# Description:
#   Retrieves all forms from the GoCanvas API
#   forms endpoint, handles either list or
#   paginated responses, prints summaries,
#   and saves results to JSON and CSV.
# =========================================

import csv
import json
from dotenv import load_dotenv
import requests

from gocanvas_get_token import get_access_token

load_dotenv()

# =========================
# CONFIG
# =========================
BASE_URL = "https://api.gocanvas.com/api/v3/forms/"
OUTPUT_JSON = "forms_output.json"
OUTPUT_CSV = "forms_output.csv"

KEYWORDS = ["grv", "tomato", "drum", "fill", "production", "intake"]

# =========================
# GET TOKEN
# =========================
access_token = get_access_token()

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# =========================
# FETCH FORMS
# =========================
def fetch_all_forms():
    all_forms = []
    url = BASE_URL

    print("Fetching forms from GoCanvas...")

    while url:
        print(f"Calling: {url}")
        response = requests.get(url, headers=headers, timeout=60)

        if response.status_code != 200:
            print(f"ERROR: {response.status_code}")
            print(response.text)
            break

        data = response.json()

        # Case 1: Endpoint returns a plain list
        if isinstance(data, list):
            all_forms.extend(data)
            print(f"Retrieved {len(data)} forms (non-paginated list response)")
            break

        # Case 2: Endpoint returns paginated dict
        elif isinstance(data, dict):
            results = data.get("results", [])
            all_forms.extend(results)
            print(f"Retrieved {len(results)} forms (Total: {len(all_forms)})")
            url = data.get("next")

        else:
            print(f"Unexpected response type: {type(data)}")
            break

    return all_forms

# =========================
# SAVE OUTPUTS
# =========================
def save_json(forms, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(forms, f, indent=4, ensure_ascii=False)
    print(f"Saved JSON: {filename}")

def save_csv(forms, filename):
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["form_id", "name", "status", "updated_at"])

        for f in forms:
            writer.writerow([
                f.get("id"),
                f.get("name"),
                f.get("status"),
                f.get("updated_at")
            ])

    print(f"Saved CSV: {filename}")

# =========================
# PRINT SUMMARIES
# =========================
def print_form_summary(forms):
    print("\n=== FORM SUMMARY ===")
    for f in forms:
        print(f"{f.get('id')} | {f.get('name')} | {f.get('status')}")

def print_filtered_forms(forms, keywords):
    print("\n=== FILTERED (Factory Related) ===")
    found = False

    for f in forms:
        name = (f.get("name") or "").lower()
        if any(keyword in name for keyword in keywords):
            print(f"{f.get('id')} | {f.get('name')} | {f.get('status')}")
            found = True

    if not found:
        print("No forms matched the keyword filter.")

def print_most_recent_forms(forms, limit=20):
    print(f"\n=== MOST RECENT FORMS (Top {limit}) ===")

    forms_sorted = sorted(
        forms,
        key=lambda x: x.get("updated_at") or "",
        reverse=True
    )

    for f in forms_sorted[:limit]:
        print(f"{f.get('id')} | {f.get('name')} | {f.get('updated_at')}")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    forms = fetch_all_forms()

    print(f"\nTotal forms retrieved: {len(forms)}")

    if forms:
        print_form_summary(forms)
        print_filtered_forms(forms, KEYWORDS)
        print_most_recent_forms(forms, limit=20)
        save_json(forms, OUTPUT_JSON)
        save_csv(forms, OUTPUT_CSV)
    else:
        print("No forms retrieved.")