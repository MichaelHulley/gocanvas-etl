import os
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load existing Coalzim Graph credentials
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

TENANT_ID = os.getenv("COALZIM_TENANT_ID")
CLIENT_ID = os.getenv("COALZIM_CLIENT_ID")
CLIENT_SECRET = os.getenv("COALZIM_CLIENT_SECRET")

SITE_HOSTNAME = "coalzimzw.sharepoint.com"
SITE_PATH = "/sites/KnightshadeManagement-FactoryManagement"


def get_access_token():
    token_url = (
        f"https://login.microsoftonline.com/"
        f"{TENANT_ID}/oauth2/v2.0/token"
    )

    response = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
        },
    )

    print("Token status:", response.status_code)
    response.raise_for_status()

    return response.json()["access_token"]


def main():
    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # -------------------------------------------------
    # Test 1 - Can Graph see the Factory Management site?
    # -------------------------------------------------
    site_url = (
        f"https://graph.microsoft.com/v1.0/sites/"
        f"{SITE_HOSTNAME}:{SITE_PATH}"
    )

    print()
    print("Testing site:")
    print(SITE_PATH)

    response = requests.get(site_url, headers=headers)

    print("Site status:", response.status_code)

    if not response.ok:
        print("Graph cannot access the site.")
        print(response.text)
        return

    site = response.json()
    site_id = site["id"]

    print("SUCCESS")
    print("Site name:", site.get("displayName"))
    print("Site ID:", site_id)

    # -------------------------------------------------
    # Test 2 - Find document libraries
    # -------------------------------------------------
    drives_url = (
        f"https://graph.microsoft.com/v1.0/sites/"
        f"{site_id}/drives"
    )

    response = requests.get(drives_url, headers=headers)

    print()
    print("Drives status:", response.status_code)

    if not response.ok:
        print(response.text)
        return

    drives = response.json().get("value", [])

    print()
    print("Document libraries:")
    for drive in drives:
        print(
            " -",
            drive.get("name"),
            "| ID:",
            drive.get("id")
        )
    # -------------------------------------------------
    # Test 3 - Search for ProductionSheetsReLabelled
    # -------------------------------------------------
    print()
    print("Searching for ProductionSheetsReLabelled.xlsx...")

    for drive in drives:
        drive_id = drive["id"]

        search_url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
            f"/root/search(q='ProductionSheetsReLabelled.xlsx')"
        )

        response = requests.get(search_url, headers=headers)

        print("Search status:", response.status_code)

        if not response.ok:
            print(response.text)
            continue

        items = response.json().get("value", [])

        for item in items:
            print()
            print("FOUND:")
            print("Name:", item.get("name"))
            print("Item ID:", item.get("id"))
            print("Web URL:", item.get("webUrl"))

            parent = item.get("parentReference", {})
            print("Parent path:", parent.get("path"))

if __name__ == "__main__":
    main()