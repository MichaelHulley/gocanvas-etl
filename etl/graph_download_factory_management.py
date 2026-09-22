# =========================================
# Knightshade Factory Management Downloader
# =========================================
# Downloads the live ProductionSheetsReLabelled.xlsx
# workbook from the Coalzim SharePoint
# Factory Management site using Microsoft Graph.
#
# Source:
#   Knightshade Management - Factory Management
#
# Destination:
#   C:\KnightshadeData\Factory Management
#
# Authentication:
#   Uses existing Coalzim Graph credentials
#   stored in the project .env file.
# =========================================

import os
from pathlib import Path

import requests
from dotenv import load_dotenv


# =========================================
# CONFIGURATION
# =========================================

# Project root:
# C:\PythonProjects\GoCanvas_API
BASE_DIR = Path(__file__).resolve().parents[1]

# Load existing .env
load_dotenv(BASE_DIR / ".env")


# Existing Coalzim Microsoft Graph credentials
TENANT_ID = os.getenv("COALZIM_TENANT_ID")
CLIENT_ID = os.getenv("COALZIM_CLIENT_ID")
CLIENT_SECRET = os.getenv("COALZIM_CLIENT_SECRET")


# Factory Management SharePoint Documents library
DRIVE_ID = (
    "b!VfJz2bQT5UO1x5GvDBlIFqZMMESRCQBAmoVzM5EddNco4fIQRywIRoWysTy1e-UO"
)


# LIVE ProductionSheetsReLabelled.xlsx
# Do NOT use the MIH Copy item ID.
ITEM_ID = "01OXEJWGR63JP7RTSAOVA23YIJARTAAN52"


FILE_NAME = "ProductionSheetsReLabelled.xlsx"


# Local Knightshade data landing folder
DOWNLOAD_FOLDER = Path(
    r"C:\KnightshadeData\Factory Management"
)


# =========================================
# VALIDATE CONFIGURATION
# =========================================

def validate_config():

    missing = []

    if not TENANT_ID:
        missing.append("COALZIM_TENANT_ID")

    if not CLIENT_ID:
        missing.append("COALZIM_CLIENT_ID")

    if not CLIENT_SECRET:
        missing.append("COALZIM_CLIENT_SECRET")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(missing)
        )


# =========================================
# GET MICROSOFT GRAPH TOKEN
# =========================================

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
        timeout=60,
    )

    print("Token status:", response.status_code)

    response.raise_for_status()

    return response.json()["access_token"]


# =========================================
# DOWNLOAD WORKBOOK
# =========================================

def download_workbook(token):

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Retrieve the exact SharePoint item
    item_url = (
        f"https://graph.microsoft.com/v1.0/"
        f"drives/{DRIVE_ID}/items/{ITEM_ID}"
    )

    print("-" * 60)
    print("Locating SharePoint workbook...")

    response = requests.get(
        item_url,
        headers=headers,
        timeout=60,
    )

    print("Item status:", response.status_code)

    response.raise_for_status()

    item = response.json()

    sharepoint_name = item.get("name")
    modified = item.get("lastModifiedDateTime")
    size = item.get("size")

    print("SharePoint file:", sharepoint_name)
    print("Last modified:", modified)
    print("SharePoint size:", size, "bytes")

    # Safety check:
    # Make sure Graph returned the expected LIVE workbook.
    if sharepoint_name != FILE_NAME:
        raise RuntimeError(
            f"Unexpected SharePoint file returned: {sharepoint_name}"
        )

    download_url = item.get("@microsoft.graph.downloadUrl")

    if not download_url:
        raise RuntimeError(
            "Microsoft Graph did not return a download URL."
        )

    # Make sure destination exists
    DOWNLOAD_FOLDER.mkdir(
        parents=True,
        exist_ok=True
    )

    local_file = DOWNLOAD_FOLDER / FILE_NAME

    print()
    print("Downloading workbook...")

    download_response = requests.get(
        download_url,
        timeout=300,
    )

    download_response.raise_for_status()

    # Write download to a temporary file first.
    # This avoids leaving a damaged production file if
    # the download is interrupted.
    temp_file = DOWNLOAD_FOLDER / (
        FILE_NAME + ".download"
    )

    with open(temp_file, "wb") as f:
        f.write(download_response.content)

    downloaded_size = temp_file.stat().st_size

    if downloaded_size == 0:
        temp_file.unlink(missing_ok=True)
        raise RuntimeError(
            "Downloaded file is empty."
        )

    # Replace existing local copy only after
    # successful download.
    temp_file.replace(local_file)

    print()
    print("SUCCESS")
    print("Saved to:", local_file)
    print("Downloaded bytes:", downloaded_size)

    return local_file


# =========================================
# MAIN
# =========================================

def main():

    print("=" * 60)
    print("Knightshade Factory Management Download")
    print("=" * 60)

    validate_config()

    token = get_access_token()

    local_file = download_workbook(token)

    print("-" * 60)
    print("Download process complete.")
    print("Local workbook:")
    print(local_file)
    print("=" * 60)


if __name__ == "__main__":
    main()