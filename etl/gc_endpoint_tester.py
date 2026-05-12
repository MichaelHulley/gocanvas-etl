# =========================================
# GoCanvas Endpoint Tester
# Author: Michael Hulley
# Date: 2026-03-23
# Description:
#   Simple script to test GoCanvas API endpoints
#   using the existing get_access_token() helper.
#   Useful for:
#   - checking list submissions responses
#   - checking submission detail
#   - inspecting multi_key / repeated structures
# =========================================

import json
import requests
from dotenv import load_dotenv
from gocanvas_get_token import get_access_token

load_dotenv()

# =========================
# CONFIG
# =========================
BASE_URL = "https://www.gocanvas.com/api/v3"

# Choose mode:
#   "list_submissions"
#   "submission_detail"
MODE = "submission_detail"

# Test values
FORM_ID = 5518337   #5794028 
SUBMISSION_ID = 238265129

# Optional query params for list_submissions
PARAMS = {
    "form_id": FORM_ID,
    "page": 1,
    "per_page": 20,
    # "created_after": "2025-01-01T00:00:00Z",
    # "created_before": "2026-03-21T23:59:59Z",
}

# Set True if you want to save output to file
SAVE_TO_FILE = True
OUTPUT_FILE = "gocanvas_test_output.json"

# Set True if you want the full raw JSON printed to console
PRINT_FULL_JSON = True


def print_response(response):
    print("\n" + "=" * 100)
    print(f"STATUS CODE : {response.status_code}")
    print(f"URL         : {response.url}")
    print("=" * 100)

    try:
        payload = response.json()

        if PRINT_FULL_JSON:
            print(json.dumps(payload, indent=2))

        if SAVE_TO_FILE:
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            print(f"\n✅ Response saved to {OUTPUT_FILE}")

        return payload

    except ValueError:
        print(response.text)
        return None


def call_endpoint(session, endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    print(f"\nCalling: {url}")
    if params:
        print(f"Params: {params}")

    response = session.get(url, params=params, timeout=60)
    return print_response(response)


def inspect_response_structure(responses):
    labels = {}
    multi_labels = {}
    multi_keys = set()

    for r in responses:
        label = (r.get("label") or "").strip()
        mk = r.get("multi_key")

        labels[label] = labels.get(label, 0) + 1

        if mk is not None:
            multi_labels[label] = multi_labels.get(label, 0) + 1
            multi_keys.add(str(mk))

    print("\nRESPONSE STRUCTURE SUMMARY")
    print(f"Total responses: {len(responses)}")
    print(f"Distinct multi_key values: {len(multi_keys)}")

    try:
        sorted_keys = sorted(multi_keys, key=lambda x: int(x) if str(x).isdigit() else str(x))
    except Exception:
        sorted_keys = sorted(multi_keys)

    print(f"multi_key values: {sorted_keys}")

    print("\nMULTI_KEY LABEL COUNTS")
    for label in sorted(multi_labels):
        print(f"{multi_labels[label]:>3}  {label}")


def inspect_submission(payload):
    if not isinstance(payload, dict):
        return

    print("\n" + "=" * 100)
    print("SUBMISSION STRUCTURE CHECK")
    print("=" * 100)

    print("Submission ID     :", payload.get("id"))
    print("Form ID           :", payload.get("form_id"))
    print("Submission Number :", payload.get("submission_number"))
    print("Submission Name   :", payload.get("submission_name"))
    print("Created At        :", payload.get("created_at"))
    print("Status            :", payload.get("status"))

    if "responses" in payload:
        responses = payload.get("responses") or []
        inspect_response_structure(responses)
        print("Any multi_key present?:", any(r.get("multi_key") is not None for r in responses))
        print(f"Top-level responses found: {len(responses)}")
        if responses:
            print("Sample response keys:", list(responses[0].keys()))

    if "repeatable_sections" in payload:
        sections = payload.get("repeatable_sections") or []
        print(f"Repeatable sections found: {len(sections)}")
        if sections:
            first = sections[0]
            print("Sample repeatable section keys:", list(first.keys()))

            section_responses = first.get("responses", [])
            print(f"Responses in first repeatable section: {len(section_responses)}")
            if section_responses:
                print("Sample repeatable response keys:", list(section_responses[0].keys()))

    if "responses" not in payload and "repeatable_sections" not in payload:
        print("No top-level responses or repeatable_sections found.")


def inspect_submission_list(payload):
    print("\n" + "=" * 100)
    print("SUBMISSION LIST CHECK")
    print("=" * 100)

    if isinstance(payload, dict):
        print("Payload keys:", list(payload.keys()))
        print("total    :", payload.get("total"))
        print("page     :", payload.get("page"))
        print("per_page :", payload.get("per_page"))

        items = payload.get("items") or payload.get("submissions") or []
    elif isinstance(payload, list):
        items = payload
        print("Payload type: list")
    else:
        items = []

    print(f"\nSubmissions returned: {len(items)}")

    if items:
        print("Sample submission keys:", list(items[0].keys()))

        print("\nSubmission rows returned:")
        for item in items:
            print(
                "id=", item.get("id"),
                "| form_id=", item.get("form_id"),
                "| sub_no=", item.get("submission_number"),
                "| created_at=", item.get("created_at"),
                "| status=", item.get("status"),
            )


def main():
    token = get_access_token()
    print("✅ Access token retrieved")

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    })

    if MODE == "list_submissions":
        payload = call_endpoint(session, "/submissions", params=PARAMS)
        inspect_submission_list(payload)

    elif MODE == "submission_detail":
        submission_id = str(SUBMISSION_ID).strip()
        payload = call_endpoint(session, f"/submissions/{submission_id}")
        inspect_submission(payload)

    else:
        raise ValueError("MODE must be 'list_submissions' or 'submission_detail'")


if __name__ == "__main__":
    main()