# GoCanvas Drum Fill ETL Script
# Author: Michael Hulley
# Date: 2026-04-13
# Description:
#   Extracts GoCanvas submissions and responses
#   for active DrumFill forms defined in
#   dbo.gocanvas_form_registry and loads RAW data
#   into SQL Server staging tables:
#       dbo.stg_gocanvas_submission
#       dbo.stg_gocanvas_response
#
#   Incremental load logic:
#   - Reads last_created_after_utc from dbo.etl_process_control
#   - Applies a 1-day overlap buffer
#   - Upserts submission headers
#   - Deletes/reloads responses for touched submissions
#   - Updates watermark on successful completion
# =========================================

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pyodbc
import requests
from dotenv import load_dotenv
from gocanvas_get_token import get_access_token
from tqdm import tqdm

load_dotenv()

# =========================
# CONFIG
# =========================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB") or os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER") or os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

BASE_URL = "https://www.gocanvas.com/api/v3"

PROCESS_AREA = "DrumFill"
ETL_PROCESS_NAME = "gc_drum_fill_etl.py"

DEFAULT_CREATED_AFTER = "2025-01-01T00:00:00Z"
CREATED_BEFORE = None

PER_PAGE = 100
REQUEST_TIMEOUT = 60
SLEEP_BETWEEN_CALLS = 0.15
OVERLAP_DAYS = 1

DEBUG_RESPONSE_KEYS = False
PRINT_SUBMISSION_LIST = True

# Set to integer like 5 for testing, or None for full load
TEST_LIMIT = None

# =========================
# COUNTERS
# =========================
submissions_fetched_count = 0
submissions_after_filter_count = 0
responses_fetched_count = 0
submissions_inserted = 0
submissions_updated = 0
responses_inserted = 0
responses_deleted = 0
submission_errors = 0

# =========================
# GLOBALS
# =========================
conn: Optional[pyodbc.Connection] = None
cursor: Optional[pyodbc.Cursor] = None
session: Optional[requests.Session] = None


# =========================
# HELPERS
# =========================
def require_env() -> None:
    required = {
        "SQL_SERVER": SQL_SERVER,
        "SQL_DB/SQL_DATABASE": SQL_DB,
        "SQL_USER/SQL_USERNAME": SQL_USER,
        "SQL_PASSWORD": SQL_PASSWORD,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


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


def get_form_ids_from_registry(
    db_cursor: pyodbc.Cursor,
    process_area: str,
    etl_process_name: Optional[str] = None,
) -> list[int]:
    sql = """
        SELECT form_id
        FROM dbo.gocanvas_form_registry
        WHERE process_area = ?
          AND is_active = 1
    """
    params: list[Any] = [process_area]

    if etl_process_name:
        sql += " AND etl_process_name = ?"
        params.append(etl_process_name)

    sql += " ORDER BY form_id"

    db_cursor.execute(sql, params)
    return [row[0] for row in db_cursor.fetchall()]


def ensure_etl_control_row(db_cursor: pyodbc.Cursor, process_name: str) -> None:
    db_cursor.execute(
        """
        IF NOT EXISTS (
            SELECT 1
            FROM dbo.etl_process_control
            WHERE process_name = ?
        )
        INSERT INTO dbo.etl_process_control
        (
            process_name,
            last_successful_run_utc,
            last_created_after_utc,
            last_status,
            last_message,
            updated_at_utc
        )
        VALUES (?, NULL, NULL, NULL, NULL, SYSUTCDATETIME())
        """,
        process_name,
        process_name,
    )


def get_last_created_after(db_cursor: pyodbc.Cursor, process_name: str) -> Optional[datetime]:
    db_cursor.execute(
        """
        SELECT last_created_after_utc
        FROM dbo.etl_process_control
        WHERE process_name = ?
        """,
        process_name,
    )
    row = db_cursor.fetchone()
    return row[0] if row and row[0] else None


def update_etl_status(
    db_cursor: pyodbc.Cursor,
    process_name: str,
    status: str,
    message: Optional[str] = None,
    last_created_after_utc: Optional[datetime] = None,
    success: bool = False,
) -> None:
    if success and last_created_after_utc is not None:
        db_cursor.execute(
            """
            UPDATE dbo.etl_process_control
            SET
                last_successful_run_utc = SYSUTCDATETIME(),
                last_created_after_utc = ?,
                last_status = ?,
                last_message = ?,
                updated_at_utc = SYSUTCDATETIME()
            WHERE process_name = ?
            """,
            last_created_after_utc,
            status,
            message,
            process_name,
        )
    else:
        db_cursor.execute(
            """
            UPDATE dbo.etl_process_control
            SET
                last_status = ?,
                last_message = ?,
                updated_at_utc = SYSUTCDATETIME()
            WHERE process_name = ?
            """,
            status,
            message,
            process_name,
        )


def resolve_created_after(db_cursor: pyodbc.Cursor, process_name: str) -> str:
    last_created_after = get_last_created_after(db_cursor, process_name)

    if not last_created_after:
        return DEFAULT_CREATED_AFTER

    if last_created_after.tzinfo is None:
        last_created_after = last_created_after.replace(tzinfo=timezone.utc)

    created_after_dt = last_created_after - timedelta(days=OVERLAP_DAYS)
    return created_after_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_api_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


# =========================
# API SESSION
# =========================
def get_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
    )
    return s


def api_get(url: str, params: Optional[dict[str, Any]] = None) -> Any:
    assert session is not None
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


# =========================
# ENDPOINT: LIST SUBMISSIONS
# =========================
def get_submission_list_page(form_id: int, page: int, created_after: Optional[str]) -> Any:
    params: dict[str, Any] = {
        "form_id": form_id,
        "page": page,
        "per_page": PER_PAGE,
    }

    if created_after:
        params["created_after"] = created_after

    if CREATED_BEFORE:
        params["created_before"] = CREATED_BEFORE

    print("\nCalling submissions list endpoint...")
    print(f"   GET {BASE_URL}/submissions")
    print(f"   Request params: {params}")

    url = f"{BASE_URL}/submissions"
    return api_get(url, params=params)


def get_all_submission_summaries(form_id: int, created_after: Optional[str]) -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []
    page = 1

    while True:
        payload = get_submission_list_page(form_id, page, created_after)

        if isinstance(payload, dict):
            raw_items = payload.get("items") or payload.get("submissions") or payload.get("data") or []
        elif isinstance(payload, list):
            raw_items = payload
        else:
            raw_items = []

        items = [x for x in raw_items if x.get("form_id") == form_id]

        if not raw_items:
            break

        all_items.extend(items)

        print(
            f"   Page {page}: fetched {len(raw_items)} submissions, "
            f"kept {len(items)} for exact form_id={form_id}"
        )

        if PRINT_SUBMISSION_LIST and raw_items:
            print("   Raw form_ids returned:", sorted({x.get("form_id") for x in raw_items}))

        if len(raw_items) < PER_PAGE:
            break

        page += 1
        time.sleep(SLEEP_BETWEEN_CALLS)

    return all_items


# =========================
# ENDPOINT: SUBMISSION DETAIL
# =========================
def get_submission_detail(submission_id: Any) -> Any:
    submission_id = str(submission_id).strip()
    url = f"{BASE_URL}/submissions/{submission_id}"
    return api_get(url)


# =========================
# VALUE PARSERS
# =========================
def parse_numeric(field_type: Optional[str], value: Any) -> Optional[float]:
    if value is None:
        return None

    if field_type not in ("Decimal", "Calculation"):
        return None

    text = str(value).strip()
    if text == "":
        return None

    try:
        num = float(text)
        if abs(num) >= 1000000000000:
            return None
        return num
    except ValueError:
        return None


def parse_date_value(field_type: Optional[str], value: Any) -> Optional[datetime.date]:
    if value is None or field_type != "Date":
        return None

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


def parse_time_value(field_type: Optional[str], value: Any) -> Optional[datetime.time]:
    if value is None or field_type != "Time":
        return None

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%I:%M %p", "%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).time().replace(microsecond=0)
        except ValueError:
            continue

    return None


# =========================
# SQL STAGING LOAD: SUBMISSION HEADER
# =========================
def upsert_submission(sub: dict[str, Any]) -> None:
    global submissions_inserted, submissions_updated
    assert cursor is not None

    cursor.execute(
        """
        UPDATE dbo.stg_gocanvas_submission
        SET
            client_guid = ?,
            form_id = ?,
            submission_number = ?,
            submission_name = ?,
            created_at_utc = ?,
            revision = ?,
            revised_at_utc = ?,
            department_id = ?,
            user_id = ?,
            status = ?,
            raw_json = ?,
            loaded_at_utc = SYSUTCDATETIME()
        WHERE submission_id = ?
        """,
        sub.get("client_guid"),
        sub.get("form_id"),
        sub.get("submission_number"),
        sub.get("submission_name"),
        sub.get("created_at"),
        1 if sub.get("revision") else 0,
        sub.get("revised_at"),
        sub.get("department_id"),
        sub.get("user_id"),
        sub.get("status"),
        json.dumps(sub, ensure_ascii=False),
        sub.get("id"),
    )

    if cursor.rowcount == 0:
        cursor.execute(
            """
            INSERT INTO dbo.stg_gocanvas_submission
            (
                submission_id,
                client_guid,
                form_id,
                submission_number,
                submission_name,
                created_at_utc,
                revision,
                revised_at_utc,
                department_id,
                user_id,
                status,
                raw_json,
                loaded_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
            """,
            sub.get("id"),
            sub.get("client_guid"),
            sub.get("form_id"),
            sub.get("submission_number"),
            sub.get("submission_name"),
            sub.get("created_at"),
            1 if sub.get("revision") else 0,
            sub.get("revised_at"),
            sub.get("department_id"),
            sub.get("user_id"),
            sub.get("status"),
            json.dumps(sub, ensure_ascii=False),
        )
        submissions_inserted += 1
    else:
        submissions_updated += 1


# =========================
# SQL STAGING LOAD: RESPONSE DELETE
# =========================
def delete_existing_submission_responses(submission_id: Any) -> None:
    global responses_deleted
    assert cursor is not None

    cursor.execute(
        "DELETE FROM dbo.stg_gocanvas_response WHERE submission_id = ?",
        submission_id,
    )
    responses_deleted += cursor.rowcount


# =========================
# SQL STAGING LOAD: RESPONSE DETAIL
# =========================
def insert_response_row(submission: dict[str, Any], response: dict[str, Any], response_ordinal: int) -> None:
    global responses_inserted
    assert cursor is not None

    response_value = response.get("value")
    field_type = response.get("type")

    value_text = None if response_value is None else str(response_value)
    value_numeric = parse_numeric(field_type, response_value)
    value_date = parse_date_value(field_type, response_value)
    value_time = parse_time_value(field_type, response_value)
    value_json = json.dumps(response, ensure_ascii=False)

    cursor.execute(
        """
        INSERT INTO dbo.stg_gocanvas_response
        (
            response_id,
            submission_id,
            form_id,
            entry_id,
            field_type,
            label,
            export_label,
            displayed,
            value_text,
            value_numeric,
            value_date,
            value_time,
            value_json,
            loaded_at_utc,
            multi_key,
            response_ordinal
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), ?, ?)
        """,
        response.get("id"),
        submission.get("id"),
        submission.get("form_id"),
        response.get("entry_id"),
        field_type,
        response.get("label"),
        response.get("export_label"),
        1 if response.get("displayed") else 0,
        value_text,
        value_numeric,
        value_date,
        value_time,
        value_json,
        response.get("multi_key"),
        response_ordinal,
    )

    responses_inserted += 1


def load_submission_and_responses(submission: dict[str, Any]) -> None:
    global responses_fetched_count

    upsert_submission(submission)

    submission_id = submission.get("id")
    responses = submission.get("responses", []) or []

    if DEBUG_RESPONSE_KEYS and responses:
        print("Sample response keys:", list(responses[0].keys()))

    delete_existing_submission_responses(submission_id)

    for i, response in enumerate(responses, start=1):
        insert_response_row(submission, response, i)

    responses_fetched_count += len(responses)


def print_multi_key_summary(submission: dict[str, Any]) -> None:
    responses = submission.get("responses", []) or []

    header_count = 0
    detail_count = 0
    multi_keys: set[str] = set()

    for r in responses:
        mk = r.get("multi_key")
        if mk is None:
            header_count += 1
        else:
            detail_count += 1
            multi_keys.add(str(mk))

    print(
        f"    responses={len(responses)}, "
        f"header_rows={header_count}, "
        f"detail_rows={detail_count}, "
        f"multi_groups={len(multi_keys)}"
    )


# =========================
# MAIN
# =========================
def main() -> None:
    global conn, cursor, session
    global submissions_fetched_count, submissions_after_filter_count, submission_errors

    require_env()

    token = get_access_token()
    print("Access token retrieved")

    conn = get_sql_connection()
    cursor = conn.cursor()
    session = get_session(token)

    ensure_etl_control_row(cursor, ETL_PROCESS_NAME)
    conn.commit()

    created_after = resolve_created_after(cursor, ETL_PROCESS_NAME)
    print(f"Using CREATED_AFTER = {created_after}")

    update_etl_status(
        cursor,
        ETL_PROCESS_NAME,
        status="RUNNING",
        message=f"Started incremental load from {created_after}",
    )
    conn.commit()

    form_ids = get_form_ids_from_registry(
        db_cursor=cursor,
        process_area=PROCESS_AREA,
        etl_process_name=ETL_PROCESS_NAME,
    )

    print(f"Active forms from registry for {PROCESS_AREA}: {form_ids}")

    if not form_ids:
        raise Exception(
            f"No active forms found in dbo.gocanvas_form_registry "
            f"for process_area={PROCESS_AREA}, etl_process_name={ETL_PROCESS_NAME}"
        )

    max_created_at: Optional[datetime] = None

    for form_id in form_ids:
        print("\n" + "=" * 80)
        print(f"Processing form_id={form_id}")
        print("=" * 80)

        submission_summaries = get_all_submission_summaries(form_id, created_after)
        submissions_fetched_count += len(submission_summaries)

        if PRINT_SUBMISSION_LIST:
            print("\nSubmission IDs returned by list endpoint:")
            for s in submission_summaries:
                print(
                    "id=", s.get("id"),
                    "| form_id=", s.get("form_id"),
                    "| sub_no=", s.get("submission_number"),
                    "| created_at=", s.get("created_at"),
                    "| status=", s.get("status"),
                )

        if TEST_LIMIT:
            submission_summaries = submission_summaries[:TEST_LIMIT]

        submissions_after_filter_count += len(submission_summaries)
        print(f"Total submissions fetched for form {form_id}: {len(submission_summaries)}")

        for item in tqdm(submission_summaries, desc=f"Form {form_id} submissions"):
            submission_id = item.get("id")
            if not submission_id:
                continue

            item_created_at = parse_api_datetime(item.get("created_at"))
            if item_created_at and (max_created_at is None or item_created_at > max_created_at):
                max_created_at = item_created_at

            try:
                print(f"\nFetching submission detail for ID: {submission_id}")
                print(f"   GET {BASE_URL}/submissions/{submission_id}")
                submission = get_submission_detail(submission_id)

                detail_created_at = parse_api_datetime(submission.get("created_at"))
                if detail_created_at and (max_created_at is None or detail_created_at > max_created_at):
                    max_created_at = detail_created_at

                load_submission_and_responses(submission)
                print_multi_key_summary(submission)
                conn.commit()
                time.sleep(SLEEP_BETWEEN_CALLS)

            except Exception as sub_err:
                submission_errors += 1
                conn.rollback()
                print(f"Error on submission_id={submission_id}: {sub_err}")

    if max_created_at is None:
        last_created_after = get_last_created_after(cursor, ETL_PROCESS_NAME)
        if last_created_after is not None:
            max_created_at = last_created_after

    if max_created_at is not None:
        if max_created_at.tzinfo is None:
            max_created_at = max_created_at.replace(tzinfo=timezone.utc)
        max_created_at = max_created_at.astimezone(timezone.utc).replace(tzinfo=None)

        update_etl_status(
            cursor,
            ETL_PROCESS_NAME,
            status="SUCCESS",
            message=(
                f"Loaded submissions={submissions_after_filter_count}, "
                f"errors={submission_errors}"
            ),
            last_created_after_utc=max_created_at,
            success=True,
        )
    else:
        update_etl_status(
            cursor,
            ETL_PROCESS_NAME,
            status="SUCCESS",
            message="No submissions returned; watermark unchanged",
            success=False,
        )

    conn.commit()

    print("\n" + "=" * 80)
    print("LOAD SUMMARY")
    print("=" * 80)
    print(f"submissions_fetched_count      = {submissions_fetched_count}")
    print(f"submissions_after_filter_count = {submissions_after_filter_count}")
    print(f"responses_fetched_count        = {responses_fetched_count}")
    print(f"submissions_inserted           = {submissions_inserted}")
    print(f"submissions_updated            = {submissions_updated}")
    print(f"responses_inserted             = {responses_inserted}")
    print(f"responses_deleted              = {responses_deleted}")
    print(f"submission_errors              = {submission_errors}")
    print(f"max_created_at                 = {max_created_at}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        if conn:
            try:
                if cursor:
                    update_etl_status(
                        cursor,
                        ETL_PROCESS_NAME,
                        status="FAILED",
                        message=str(e)[:1000],
                        success=False,
                    )
                    conn.commit()
            except Exception:
                pass
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()