# =========================================
# GoCanvas Form Registry Refresh
# Author: Michael Hulley
# Project: Knightshade
#
# Description:
#   Fetches the current GoCanvas form list and inserts
#   any missing form IDs into dbo.gocanvas_form_registry.
#
# Important:
#   New forms are inserted as is_active = 0.
#   They must be reviewed before activation.
# =========================================

import os
import json
import pyodbc
import requests
from dotenv import load_dotenv
from gocanvas_get_token import get_access_token

load_dotenv()

# =========================
# CONFIG
# =========================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB") or os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER") or os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

BASE_URL = "https://www.gocanvas.com/api/v3"
REQUEST_TIMEOUT = 60


def require_env() -> None:
    missing = []

    if not SQL_SERVER:
        missing.append("SQL_SERVER")
    if not SQL_DB:
        missing.append("SQL_DB / SQL_DATABASE")
    if not SQL_USER:
        missing.append("SQL_USER / SQL_USERNAME")
    if not SQL_PASSWORD:
        missing.append("SQL_PASSWORD")

    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")


def get_sql_connection() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DB};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=no;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str)


def get_session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
    )
    return session


def fetch_forms(session: requests.Session) -> list[dict]:
    url = f"{BASE_URL}/forms"

    print(f"Calling GoCanvas forms endpoint:")
    print(f"GET {url}")

    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    payload = response.json()

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        return (
            payload.get("items")
            or payload.get("forms")
            or payload.get("data")
            or []
        )

    return []


def insert_missing_forms(cursor: pyodbc.Cursor, forms: list[dict]) -> tuple[int, int]:
    inserted_count = 0
    existing_count = 0

    for form in forms:
        form_id = form.get("id")
        form_name = form.get("name")
        status = form.get("status")
        version = form.get("version")
        root_version_id = form.get("root_version_id")

        if not form_id:
            continue

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM dbo.gocanvas_form_registry
            WHERE form_id = ?
            """,
            form_id,
        )

        exists = cursor.fetchone()[0]

        if exists:
            existing_count += 1

            # Keep the name updated in case GoCanvas renamed the form
            cursor.execute(
                """
                UPDATE dbo.gocanvas_form_registry
                SET
                    form_name = ?,
                    updated_at = SYSDATETIME()
                WHERE form_id = ?
                """,
                form_name,
                form_id,
            )

        else:
            note_text = (
                "Auto-discovered from GoCanvas forms endpoint. "
                "Inserted inactive for review. "
                f"status={status}, version={version}, root_version_id={root_version_id}"
            )

            cursor.execute(
                """
                INSERT INTO dbo.gocanvas_form_registry
                (
                    form_id,
                    form_name,
                    process_area,
                    etl_process_name,
                    target_stage_group,
                    target_object,
                    run_post_load_proc,
                    post_load_proc_name,
                    is_active,
                    notes,
                    created_at,
                    updated_at
                )
                VALUES
                (
                    ?,
                    ?,
                    'Other',
                    'gc_get_forms.py',
                    'Unmapped',
                    NULL,
                    0,
                    NULL,
                    0,
                    ?,
                    SYSDATETIME(),
                    SYSDATETIME()
                )
                """,
                form_id,
                form_name,
                note_text,
            )

            inserted_count += 1

            print(
                f"New form inserted inactive: "
                f"form_id={form_id}, name={form_name}, status={status}, version={version}"
            )

    return inserted_count, existing_count


def main() -> None:
    require_env()

    token = get_access_token()
    print("Access token retrieved")

    session = get_session(token)
    conn = get_sql_connection()
    cursor = conn.cursor()

    try:
        forms = fetch_forms(session)
        print(f"Forms returned by GoCanvas: {len(forms)}")

        inserted_count, existing_count = insert_missing_forms(cursor, forms)

        conn.commit()

        print("")
        print("=" * 80)
        print("FORM REGISTRY REFRESH SUMMARY")
        print("=" * 80)
        print(f"Existing forms checked/updated : {existing_count}")
        print(f"New forms inserted inactive   : {inserted_count}")
        print("Done.")

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()
        session.close()


if __name__ == "__main__":
    main()