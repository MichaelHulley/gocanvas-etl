# =========================================
# GoCanvas Shift Report ETL Script
# Author: Michael Hulley
# Date: 2026-04-14
# Description:
#   Extracts GoCanvas submissions and responses
#   for all active Shift Report forms assigned
#   to this ETL process and loads RAW data into:
#       dbo.stg_gocanvas_submission
#       dbo.stg_gocanvas_response
# =========================================

import json
import os
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
PROCESS_NAME = "gc_shift_report_etl.py"
FACT_PROC = None
LIMIT = None
DEBUG_RESPONSE_KEYS = False

DEFAULT_CREATED_AFTER = "2025-01-01T00:00:00Z"
OVERLAP_HOURS = 6
PER_PAGE = 100
REQUEST_TIMEOUT = 60

DEBUG_LIST_ENDPOINT = True
ZERO_MATCH_PAGES_AFTER_MATCH = 2

# =========================
# RUNTIME STATE
# =========================
run_id = None
submissions_fetched_count = 0
submissions_after_filter_count = 0
responses_fetched_count = 0
submissions_inserted = 0
submissions_updated = 0
responses_inserted = 0
responses_updated = 0

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


def safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def parse_decimal(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def parse_date_value(value: Any):
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return None

    txt = str(value).strip()

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            continue

    return None


def parse_time_value(value: Any):
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return None

    txt = str(value).strip()

    for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p"):
        try:
            return datetime.strptime(txt, fmt).time()
        except ValueError:
            continue

    return None


def parse_api_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def get_sql_connection() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DB};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=Yes;"
        f"Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str, timeout=60)


def get_form_ids_for_process(db_cursor: pyodbc.Cursor, process_name: str) -> list[int]:
    db_cursor.execute(
        """
        SELECT form_id
        FROM dbo.gocanvas_form_registry
        WHERE etl_process_name = ?
          AND is_active = 1
        ORDER BY form_id
        """,
        process_name,
    )
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


def update_etl_control(
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

    created_after_dt = last_created_after - timedelta(hours=OVERLAP_HOURS)
    return created_after_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def start_etl_run_log(db_cursor: pyodbc.Cursor, process_name: str) -> int:
    db_cursor.execute(
        """
        INSERT INTO dbo.etl_run_log (
            process_name,
            started_at_utc,
            status
        )
        OUTPUT INSERTED.run_id
        VALUES (?, SYSUTCDATETIME(), ?)
        """,
        process_name,
        "RUNNING",
    )
    return db_cursor.fetchone()[0]


def finish_etl_run_log(
    db_cursor: pyodbc.Cursor,
    run_id_value: int,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    db_cursor.execute(
        """
        UPDATE dbo.etl_run_log
        SET
            ended_at_utc = SYSUTCDATETIME(),
            status = ?,
            submissions_fetched = ?,
            submissions_after_filter = ?,
            responses_fetched = ?,
            submissions_inserted = ?,
            submissions_updated = ?,
            responses_inserted = ?,
            responses_updated = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        status,
        submissions_fetched_count,
        submissions_after_filter_count,
        responses_fetched_count,
        submissions_inserted,
        submissions_updated,
        responses_inserted,
        responses_updated,
        error_message[:4000] if error_message else None,
        run_id_value,
    )


# =========================
# API
# =========================
def get_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s


def fetch_submissions_for_form(
    api_session: requests.Session,
    form_id: int,
    created_after: Optional[str] = None,
) -> list[dict[str, Any]]:
    submissions: list[dict[str, Any]] = []
    page = 1
    seen_match = False
    zero_match_pages_after_match = 0

    while True:
        params = {
            "form_id": form_id,
            "page": page,
            "per_page": PER_PAGE,
        }
        if created_after:
            params["created_after"] = created_after

        response = api_session.get(
            f"{BASE_URL}/submissions",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        if DEBUG_LIST_ENDPOINT:
            print("\n" + "=" * 80)
            print(f"Calling submissions endpoint for form_id={form_id}, page={page}")
            print(f"GET {BASE_URL}/submissions")
            print(f"Params: {params}")
            print(f"Status: {response.status_code}")

        if response.status_code != 200:
            print(
                f"Failed to fetch submissions for form {form_id}, "
                f"status={response.status_code}"
            )
            print(response.text[:500])
            break

        data = response.json()

        if DEBUG_LIST_ENDPOINT:
            print(f"Payload type: {type(data).__name__}")
            if isinstance(data, dict):
                print(f"Top-level keys: {list(data.keys())[:20]}")

        if isinstance(data, dict) and "data" in data:
            batch = data["data"]
        elif isinstance(data, dict):
            batch = data.get("items") or data.get("submissions") or data.get("data") or []
        elif isinstance(data, list):
            batch = data
        else:
            batch = []

        if DEBUG_LIST_ENDPOINT:
            print(f"Raw batch count: {len(batch)}")
            if batch and isinstance(batch[0], dict):
                raw_form_ids = sorted({x.get('form_id') for x in batch if isinstance(x, dict)})
                print(f"Raw form_ids returned: {raw_form_ids}")

        if not batch:
            if DEBUG_LIST_ENDPOINT:
                print("No batch returned, stopping pagination.")
            break

        filtered_batch = [s for s in batch if s.get("form_id") == form_id]

        if DEBUG_LIST_ENDPOINT:
            print(f"Filtered batch count: {len(filtered_batch)}")

        if filtered_batch:
            seen_match = True
            zero_match_pages_after_match = 0
            submissions.extend(filtered_batch)
        elif seen_match:
            zero_match_pages_after_match += 1
            if DEBUG_LIST_ENDPOINT:
                print(
                    f"Zero-match pages after match: "
                    f"{zero_match_pages_after_match}/{ZERO_MATCH_PAGES_AFTER_MATCH}"
                )
            if zero_match_pages_after_match >= ZERO_MATCH_PAGES_AFTER_MATCH:
                if DEBUG_LIST_ENDPOINT:
                    print("Stopping early after consecutive zero-match pages.")
                break

        if len(batch) < PER_PAGE:
            if DEBUG_LIST_ENDPOINT:
                print("Batch smaller than PER_PAGE, stopping pagination.")
            break

        page += 1

    return submissions


def fetch_submission_detail(api_session: requests.Session, submission_id: Any) -> Optional[dict[str, Any]]:
    response = api_session.get(f"{BASE_URL}/submissions/{submission_id}", timeout=REQUEST_TIMEOUT)

    if response.status_code != 200:
        print(
            f"Failed to fetch detail for submission {submission_id}, "
            f"status={response.status_code}"
        )
        print(response.text[:500])
        return None

    return response.json()


# =========================
# RESPONSE TRANSFORM
# =========================
def build_response_rows(
    detail: dict[str, Any],
    fallback_form_id: Optional[int] = None,
    debug_response_keys: bool = False,
) -> list[tuple]:
    response_rows = []

    if not isinstance(detail, dict):
        return response_rows

    sub_id = detail.get("id")
    form_id = detail.get("form_id") or fallback_form_id
    responses = detail.get("responses", [])

    if not sub_id or not form_id:
        print(f"Skipping submission because id/form_id missing: {sub_id}, {form_id}")
        return response_rows

    if debug_response_keys and responses:
        sample = responses[0]
        if isinstance(sample, str):
            sample = json.loads(sample)
        print("DEBUG response keys:", sorted(sample.keys()))

    for r in responses:
        if isinstance(r, str):
            r = json.loads(r)

        response_id = r.get("id")
        if not response_id:
            continue

        entry_id = r.get("entry_id")
        field_type = r.get("field_type") or r.get("type")
        label = safe_str(r.get("label"))
        export_label = safe_str(r.get("export_label"))

        displayed = r.get("displayed")
        if isinstance(displayed, (dict, list)):
            displayed = None
        elif displayed is not None:
            displayed = bool(displayed)

        raw_value = r.get("value_text")
        if raw_value is None:
            raw_value = r.get("value")

        value_json = None
        if isinstance(raw_value, (dict, list)):
            value_json = json.dumps(raw_value, ensure_ascii=False)
            value_text = value_json
        else:
            value_text = safe_str(raw_value)

        value_numeric = r.get("value_numeric")
        if value_numeric is None:
            value_numeric = parse_decimal(raw_value)
        elif isinstance(value_numeric, (dict, list)):
            value_numeric = None

        value_date = r.get("value_date")
        if value_date is None:
            value_date = parse_date_value(raw_value)
        elif isinstance(value_date, (dict, list)):
            value_date = None

        value_time = r.get("value_time")
        if value_time is None:
            value_time = parse_time_value(raw_value)
        elif isinstance(value_time, (dict, list)):
            value_time = None

        multi_key = None
        for key_name in ("multi_key", "key", "field_key"):
            if r.get(key_name) is not None:
                multi_key = safe_str(r.get(key_name))
                break

        response_rows.append(
            (
                response_id,
                sub_id,
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
                multi_key,
            )
        )

    return response_rows


# =========================
# SQL LOAD
# =========================
def merge_submissions(db_cursor: pyodbc.Cursor, all_submissions: list[dict[str, Any]]) -> None:
    global submissions_inserted, submissions_updated

    db_cursor.execute(
        """
        IF OBJECT_ID('tempdb..#submissions') IS NOT NULL DROP TABLE #submissions;
        CREATE TABLE #submissions (
            submission_id BIGINT NOT NULL,
            form_id INT NOT NULL,
            submission_number NVARCHAR(100) NULL,
            submission_name NVARCHAR(500) NULL,
            created_at_utc DATETIME2(0) NOT NULL,
            revision BIT NULL,
            status NVARCHAR(50) NULL
        );
        """
    )

    submission_rows = []
    for s in all_submissions:
        submission_rows.append(
            (
                s.get("id"),
                s.get("form_id"),
                safe_str(s.get("submission_number")),
                safe_str(s.get("submission_name")),
                s.get("created_at"),
                s.get("revision"),
                safe_str(s.get("status")),
            )
        )

    db_cursor.executemany(
        """
        INSERT INTO #submissions (
            submission_id,
            form_id,
            submission_number,
            submission_name,
            created_at_utc,
            revision,
            status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        submission_rows,
    )

    db_cursor.execute(
        """
        IF OBJECT_ID('tempdb..#submission_merge_actions') IS NOT NULL DROP TABLE #submission_merge_actions;
        CREATE TABLE #submission_merge_actions (action_name NVARCHAR(10));

        MERGE dbo.stg_gocanvas_submission AS tgt
        USING #submissions AS src
            ON tgt.submission_id = src.submission_id
        WHEN MATCHED THEN
            UPDATE SET
                form_id = src.form_id,
                submission_number = src.submission_number,
                submission_name = src.submission_name,
                created_at_utc = src.created_at_utc,
                revision = src.revision,
                status = src.status,
                loaded_at_utc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                submission_id,
                form_id,
                submission_number,
                submission_name,
                created_at_utc,
                revision,
                status,
                loaded_at_utc
            )
            VALUES (
                src.submission_id,
                src.form_id,
                src.submission_number,
                src.submission_name,
                src.created_at_utc,
                src.revision,
                src.status,
                SYSUTCDATETIME()
            )
        OUTPUT $action INTO #submission_merge_actions(action_name);
        """
    )

    db_cursor.execute(
        """
        SELECT action_name, COUNT(*)
        FROM #submission_merge_actions
        GROUP BY action_name
        """
    )
    action_counts = {row[0]: row[1] for row in db_cursor.fetchall()}
    submissions_inserted = action_counts.get("INSERT", 0)
    submissions_updated = action_counts.get("UPDATE", 0)


def merge_responses(db_cursor: pyodbc.Cursor, response_rows: list[tuple]) -> None:
    global responses_inserted, responses_updated

    db_cursor.execute(
        """
        IF OBJECT_ID('tempdb..#responses') IS NOT NULL DROP TABLE #responses;

        CREATE TABLE #responses (
            response_id BIGINT NOT NULL,
            submission_id BIGINT NOT NULL,
            form_id INT NOT NULL,
            entry_id BIGINT NULL,
            field_type NVARCHAR(255) NULL,
            label NVARCHAR(1000) NULL,
            export_label NVARCHAR(1000) NULL,
            displayed BIT NULL,
            value_text NVARCHAR(MAX) NULL,
            value_numeric DECIMAL(18,6) NULL,
            value_date DATE NULL,
            value_time TIME(0) NULL,
            value_json NVARCHAR(MAX) NULL,
            loaded_at_utc DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
            multi_key NVARCHAR(500) NULL
        );
        """
    )
    for row in response_rows:
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
            multi_key,
        ) = row

        if field_type and len(str(field_type)) > 255:
            print(f"field_type overflow | response_id={response_id} | len={len(str(field_type))}")

        if label and len(str(label)) > 1000:
            print(f"label overflow | response_id={response_id} | len={len(str(label))}")

        if export_label and len(str(export_label)) > 1000:
            print(f"export_label overflow | response_id={response_id} | len={len(str(export_label))}")

        if multi_key and len(str(multi_key)) > 500:
            print(f"multi_key overflow | response_id={response_id} | len={len(str(multi_key))}")
    
    db_cursor.fast_executemany = False

    db_cursor.executemany(
        """
        INSERT INTO #responses (
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
            multi_key
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        response_rows,
    )

    db_cursor.execute(
        """
        IF OBJECT_ID('tempdb..#response_merge_actions') IS NOT NULL DROP TABLE #response_merge_actions;
        CREATE TABLE #response_merge_actions (action_name NVARCHAR(10));

        ;WITH src AS (
            SELECT
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
                ROW_NUMBER() OVER (
                    PARTITION BY response_id
                    ORDER BY loaded_at_utc DESC
                ) AS rn
            FROM #responses
        ),
        deduped AS (
            SELECT
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
                multi_key
            FROM src
            WHERE rn = 1
        )
        MERGE dbo.stg_gocanvas_response AS tgt
        USING deduped AS src
            ON tgt.response_id = src.response_id
        WHEN MATCHED THEN
            UPDATE SET
                submission_id = src.submission_id,
                form_id = src.form_id,
                entry_id = src.entry_id,
                field_type = src.field_type,
                label = src.label,
                export_label = src.export_label,
                displayed = src.displayed,
                value_text = src.value_text,
                value_numeric = src.value_numeric,
                value_date = src.value_date,
                value_time = src.value_time,
                value_json = src.value_json,
                loaded_at_utc = SYSUTCDATETIME(),
                multi_key = src.multi_key
        WHEN NOT MATCHED THEN
            INSERT (
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
                multi_key
            )
            VALUES (
                src.response_id,
                src.submission_id,
                src.form_id,
                src.entry_id,
                src.field_type,
                src.label,
                src.export_label,
                src.displayed,
                src.value_text,
                src.value_numeric,
                src.value_date,
                src.value_time,
                src.value_json,
                SYSUTCDATETIME(),
                src.multi_key
            )
        OUTPUT $action INTO #response_merge_actions(action_name);
        """
    )

    db_cursor.execute(
        """
        SELECT action_name, COUNT(*)
        FROM #response_merge_actions
        GROUP BY action_name
        """
    )
    action_counts = {row[0]: row[1] for row in db_cursor.fetchall()}
    responses_inserted = action_counts.get("INSERT", 0)
    responses_updated = action_counts.get("UPDATE", 0)


# =========================
# MAIN
# =========================
def main() -> None:
    global conn, cursor, session, run_id
    global submissions_fetched_count, submissions_after_filter_count, responses_fetched_count

    require_env()

    token = get_access_token()
    print("Access token retrieved")

    conn = get_sql_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True
    print("Connected to SQL Server")

    ensure_etl_control_row(cursor, PROCESS_NAME)
    conn.commit()

    created_after = resolve_created_after(cursor, PROCESS_NAME)
    print(f"Using CREATED_AFTER = {created_after}")

    update_etl_control(
        cursor,
        PROCESS_NAME,
        status="RUNNING",
        message=f"Started incremental load from {created_after}",
    )
    conn.commit()

    form_ids = get_form_ids_for_process(cursor, PROCESS_NAME)
    print(f"Forms configured for {PROCESS_NAME}: {form_ids}")

    if not form_ids:
        raise Exception(f"No active forms found in dbo.gocanvas_form_registry for {PROCESS_NAME}")

    run_id = start_etl_run_log(cursor, PROCESS_NAME)
    conn.commit()
    print(f"ETL log started, run_id={run_id}")

    session = get_session(token)

    all_submissions: list[dict[str, Any]] = []
    max_created_at: Optional[datetime] = None

    for form_id in form_ids:
        form_submissions = fetch_submissions_for_form(
            api_session=session,
            form_id=form_id,
            created_after=created_after,
        )

        print(f"form_id={form_id} final returned submissions={len(form_submissions)}")

        for s in form_submissions:
            created_dt = parse_api_datetime(s.get("created_at"))
            if created_dt and (max_created_at is None or created_dt > max_created_at):
                max_created_at = created_dt

        all_submissions.extend(form_submissions)

    submissions_fetched_count = len(all_submissions)

    deduped = {}
    for s in all_submissions:
        sid = s.get("id")
        if sid is not None:
            deduped[sid] = s

    all_submissions = list(deduped.values())

    if LIMIT:
        all_submissions = all_submissions[:LIMIT]

    submissions_after_filter_count = len(all_submissions)
    print(f"Total submissions after dedupe/filter: {submissions_after_filter_count}")

    if not all_submissions:
        print("No new submissions found. Nothing to do.")

        finish_etl_run_log(cursor, run_id, "SUCCESS", None)
        update_etl_control(
            cursor,
            PROCESS_NAME,
            status="SUCCESS",
            message="No new submissions found; watermark unchanged",
            success=False,
        )
        conn.commit()
        return

    merge_submissions(cursor, all_submissions)
    conn.commit()
    print(f"Submissions merged | inserted={submissions_inserted}, updated={submissions_updated}")

    response_rows: list[tuple] = []

    print("Fetching submission details / responses...")
    for s in tqdm(all_submissions, desc="Responses", unit="submission"):
        sub_id = s.get("id")
        form_id = s.get("form_id")

        detail = fetch_submission_detail(session, sub_id)
        if not detail:
            continue

        detail_created_dt = parse_api_datetime(detail.get("created_at"))
        if detail_created_dt and (max_created_at is None or detail_created_dt > max_created_at):
            max_created_at = detail_created_dt

        rows = build_response_rows(
            detail=detail,
            fallback_form_id=form_id,
            debug_response_keys=DEBUG_RESPONSE_KEYS,
        )
        response_rows.extend(rows)

    responses_fetched_count = len(response_rows)
    print(f"Total responses fetched: {responses_fetched_count}")

    if response_rows:
        merge_responses(cursor, response_rows)
        conn.commit()
        print(f"Responses merged | inserted={responses_inserted}, updated={responses_updated}")
    else:
        print("No response rows returned")

    if FACT_PROC:
        print(f"Running stored procedure: {FACT_PROC}...")
        cursor.execute(f"EXEC {FACT_PROC}")
        conn.commit()
        print("Stored procedure executed successfully")
    else:
        print("No post-load stored procedure configured")

    if max_created_at is not None:
        if max_created_at.tzinfo is None:
            max_created_at = max_created_at.replace(tzinfo=timezone.utc)
        max_created_at = max_created_at.astimezone(timezone.utc).replace(tzinfo=None)

        update_etl_control(
            cursor,
            PROCESS_NAME,
            status="SUCCESS",
            message=(
                f"Loaded submissions={submissions_after_filter_count}, "
                f"responses={responses_fetched_count}"
            ),
            last_created_after_utc=max_created_at,
            success=True,
        )
    else:
        update_etl_control(
            cursor,
            PROCESS_NAME,
            status="SUCCESS",
            message="Completed successfully; watermark unchanged",
            success=False,
        )

    finish_etl_run_log(cursor, run_id, "SUCCESS", None)
    conn.commit()
    print("ETL completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_message = str(e)
        print(f"ETL failed: {error_message}")

        if cursor and run_id:
            try:
                finish_etl_run_log(cursor, run_id, "FAILED", error_message)
                update_etl_control(
                    cursor,
                    PROCESS_NAME,
                    status="FAILED",
                    message=error_message[:1000],
                    success=False,
                )
                conn.commit()
                print("ETL failure logged")
            except Exception as log_error:
                print(f"Failed to log ETL failure: {log_error}")

        if conn:
            conn.rollback()
        raise

    finally:
        if session:
            session.close()
        if cursor:
            cursor.close()
        if conn:
            conn.close()