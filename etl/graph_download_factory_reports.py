import os
from pathlib import Path

import requests
from dotenv import load_dotenv


# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

TENANT_ID = os.getenv("COALZIM_TENANT_ID")
CLIENT_ID = os.getenv("COALZIM_CLIENT_ID")
CLIENT_SECRET = os.getenv("COALZIM_CLIENT_SECRET")

SITE_HOSTNAME = os.getenv("COALZIM_SITE_HOSTNAME", "coalzimzw.sharepoint.com")
SITE_PATH = os.getenv("COALZIM_SITE_PATH", "/sites/KnightshadeManagement-FactoryReporting")

REPORT_FOLDER = os.getenv(
    "REPORT_FOLDER",
    r"C:\Users\Administrator\OneDrive\KnightshadeData\Factory Reporting"
)

FILES_TO_DOWNLOAD = [
    "DrumReport.xlsx",
    "BinTippingReport.xlsx",
    "SalesReport.xlsx",
    "ReturnsReport.xlsx",
    "ReRunReport.xlsx",
    "DiscardedDrumsReport.xlsx",
    "DimGrowers.csv",
    "Deliveries.xlsx",
]


# =========================
# GRAPH HELPERS
# =========================
def get_access_token():
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    response = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
    })

    print("Token status:", response.status_code)
    response.raise_for_status()

    return response.json()["access_token"]


def graph_get(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def download_file(download_url, local_path):
    response = requests.get(download_url)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(response.content)


# =========================
# MAIN
# =========================
def main():
    Path(REPORT_FOLDER).mkdir(parents=True, exist_ok=True)

    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Get SharePoint site
    site_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_HOSTNAME}:{SITE_PATH}"
    site = graph_get(site_url, headers)
    site_id = site["id"]

    print("Site found:", site.get("displayName"))
    print("Site ID:", site_id)

    # Get default document library drive
    drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive"
    drive = graph_get(drive_url, headers)
    drive_id = drive["id"]

    print("Drive found:", drive.get("name"))
    print("Drive ID:", drive_id)

    for file_name in FILES_TO_DOWNLOAD:
        print("-" * 60)
        print("Downloading:", file_name)

        item_url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
            f"/root:/{file_name}"
        )

        try:
            item = graph_get(item_url, headers)
        except requests.HTTPError as e:
            print(f"FAILED: Could not find {file_name}")
            print(e)
            continue

        download_url = item.get("@microsoft.graph.downloadUrl")

        if not download_url:
            print(f"FAILED: No download URL for {file_name}")
            continue

        local_path = os.path.join(REPORT_FOLDER, file_name)
        download_file(download_url, local_path)

        print("Saved to:", local_path)

    print("-" * 60)
    print("Download process complete.")


if __name__ == "__main__":
    main()