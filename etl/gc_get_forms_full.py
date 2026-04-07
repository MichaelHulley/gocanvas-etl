# =========================================
# GoCanvas ETL Script
# Rewritten core sections for safer loading
# =========================================

import json
import os
from datetime import datetime, timedelta

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
PROCESS_NAME = "gc_get_forms"
FACT_PROC = "dbo.usp_gocanvas_stage_to_fact" #dbo.usp_load_fact_tomato_intake
LIMIT = None
DEBUG_RESPONSE_KEYS = True

FORM_IDS = sorted(
    {
      #  5757557,
      #  5525962,
      #  5525968,
      #  5612718,
      #  5612894,
      #  5648299,
      #  5676079,
      #  5679477,
      #  5682489,
      #  5686251,
        5700339,  # Factory Managers Daily Report
      #  5761346,
      #  5771040,
      #  5356418,
      #  5500213,
      #  5404597,
    }
)


def safe_str(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def parse_decimal(value):
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def parse_date_value(value):
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return None

    txt = str(value).strip()

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            continue

    return None


def parse_time_value(value):
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return None

    txt = str(value).strip()

    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(txt, fmt).time()
        except ValueError:
            continue

    return None


def get_form_last_run(cursor, form_id):
    cursor.execute(
        """
        SELECT MAX(created_at_utc)
        FROM dbo.stg_gocanvas_submission
        WHERE form_id = ?
        """,
        form_id,
    )
    return cursor.fetchone()[0]


def fetch_submissions_for_form(session, form_id, created_after=None):
    submissions = []
    page = 1

    while True:
        params = {"form_id": form_id, "page": page, "per_page": 100}
        if created_after:
            params["created_after"] = created_after

        response = session.get(f"{BASE_URL}/submissions", params=params, timeout=60)
        if response.status_code != 200:
            print(
                f"❌ Failed to fetch submissions for form {form_id}, "
                f"status={response.status_code}"
            )
            print(response.text[:300])
            break

        data = response.json()

        if isinstance(data, dict) and "data" in data:
            batch = data["data"]
        elif isinstance(data, list):
            batch = data
        else:
            batch = []

        if not batch:
            break

        # hard exact-form filter
        batch = [s for s in batch if s.get("form_id") == form_id]

        submissions.extend(batch)

        if len(batch) < 100:
            break

        page += 1

    return submissions


def fetch_submission_detail(session, submission_id):
    response = session.get(f"{BASE_URL}/submissions/{submission_id}", timeout=60)
    if response.status_code != 200:
        print(
            f"❌ Failed to fetch detail for submission {submission_id}, "
            f"status={response.status_code}"
        )
        print(response.text[:300])
        return None
    return response.json()


def build_response_rows(detail, fallback_form_id=None, debug_response_keys=False):
    response_rows = []
    debug_logged = False

    if not isinstance(detail, dict):
        return response_rows

    sub_id = detail.get("id")
    form_id = detail.get("form_id") or fallback_form_id
    responses = detail.get("responses", [])

    if not sub_id or not form_id:
        print(f"⚠ Skipping submission because id/form_id missing: {sub_id}, {form_id}")
        return response_rows

    if debug_response_keys and responses and not debug_logged:
        sample = responses[0]
        if isinstance(sample, str):
            sample = json.loads(sample)
        print("DEBUG response keys:", sorted(sample.keys()))
        debug_logged = True

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


def print_row_length_warnings(response_rows):
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

        if field_type and len(str(field_type)) > 100:
            print(f"⚠ field_type too long | response_id={response_id} | len={len(str(field_type))}")

        if label and len(str(label)) > 500:
            print(f"⚠ label too long | response_id={response_id} | len={len(str(label))}")

        if export_label and len(str(export_label)) > 500:
            print(f"⚠ export_label too long | response_id={response_id} | len={len(str(export_label))}")

        if multi_key and len(str(multi_key)) > 200:
            print(f"⚠ multi_key too long | response_id={response_id} | len={len(str(multi_key))}")


run_id = None
submissions_fetched_count = 0
submissions_after_filter_count = 0
responses_fetched_count = 0
submissions_inserted = 0
submissions_updated = 0
responses_inserted = 0
responses_updated = 0

conn = None
cursor = None
session = None

try:
    token = get_access_token()
    print("✅ Access token retrieved")

    headers = {"Authorization": f"Bearer {token}"}

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DB};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=Yes;"
    )

    conn = pyodbc.connect(conn_str, timeout=60)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    print("✅ Connected to SQL Server")

    cursor.execute(
        """
        INSERT INTO dbo.etl_run_log (
            process_name,
            started_at_utc,
            status
        )
        OUTPUT INSERTED.run_id
        VALUES (?, SYSUTCDATETIME(), ?)
        """,
        PROCESS_NAME,
        "RUNNING",
    )
    run_id = cursor.fetchone()[0]
    conn.commit()
    print(f"✅ ETL log started, run_id={run_id}")

    session = requests.Session()
    session.headers.update(headers)

    all_submissions = []

    for form_id in FORM_IDS:
        last_run = get_form_last_run(cursor, form_id)

        if last_run:
            lookback_dt = last_run - timedelta(days=1)
            created_after = lookback_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f"✅ form_id={form_id} incremental from {created_after}")
        else:
            created_after = None
            print(f"✅ form_id={form_id} has no previous load; running full load")

        form_submissions = fetch_submissions_for_form(
            session=session,
            form_id=form_id,
            created_after=created_after,
        )

        # python-side safety filter
        if created_after:
            before_count = len(form_submissions)
            form_submissions = [
                s
                for s in form_submissions
                if s.get("created_at") and s["created_at"] > created_after
            ]
            after_count = len(form_submissions)
            print(f"Filtered form {form_id}: {before_count} -> {after_count}")

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
    print(f"✅ Total submissions after dedupe/filter: {submissions_after_filter_count}")

    if not all_submissions:
        print("✅ No new submissions found. Nothing to do.")
        cursor.execute(
            """
            UPDATE dbo.etl_run_log
            SET
                ended_at_utc = SYSUTCDATETIME(),
                status = ?,
                submissions_fetched = ?,
                submissions_after_filter = ?,
                responses_fetched = 0,
                submissions_inserted = 0,
                submissions_updated = 0,
                responses_inserted = 0,
                responses_updated = 0,
                error_message = NULL
            WHERE run_id = ?
            """,
            "SUCCESS",
            submissions_fetched_count,
            submissions_after_filter_count,
            run_id,
        )
        conn.commit()
        raise SystemExit(0)

    # =========================
    # LOAD SUBMISSIONS TEMP TABLE
    # =========================
    cursor.execute(
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

    cursor.executemany(
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
    conn.commit()

    cursor.execute(
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
                status = src.status
        WHEN NOT MATCHED THEN
            INSERT (
                submission_id,
                form_id,
                submission_number,
                submission_name,
                created_at_utc,
                revision,
                status
            )
            VALUES (
                src.submission_id,
                src.form_id,
                src.submission_number,
                src.submission_name,
                src.created_at_utc,
                src.revision,
                src.status
            )
        OUTPUT $action INTO #submission_merge_actions(action_name);
        """
    )
    conn.commit()

    cursor.execute(
        """
        SELECT action_name, COUNT(*)
        FROM #submission_merge_actions
        GROUP BY action_name
        """
    )
    submission_action_counts = {row[0]: row[1] for row in cursor.fetchall()}
    submissions_inserted = submission_action_counts.get("INSERT", 0)
    submissions_updated = submission_action_counts.get("UPDATE", 0)

    print(
        f"✅ Submissions merged | inserted={submissions_inserted}, updated={submissions_updated}"
    )

    # =========================
    # FETCH DETAILS / RESPONSES
    # =========================
    response_rows = []

    print("Fetching submission details / responses...")
    for s in tqdm(all_submissions, desc="Responses", unit="submission"):
        sub_id = s.get("id")
        form_id = s.get("form_id")

        detail = fetch_submission_detail(session, sub_id)
        if not detail:
            continue

        rows = build_response_rows(
            detail=detail,
            fallback_form_id=form_id,
            debug_response_keys=DEBUG_RESPONSE_KEYS,
        )
        response_rows.extend(rows)

    responses_fetched_count = len(response_rows)
    print(f"✅ Total responses fetched: {responses_fetched_count}")

    print_row_length_warnings(response_rows)

    cursor.execute(
        """
        IF OBJECT_ID('tempdb..#responses') IS NOT NULL DROP TABLE #responses;

        CREATE TABLE #responses (
            response_id BIGINT NOT NULL,
            submission_id BIGINT NOT NULL,
            form_id INT NOT NULL,
            entry_id BIGINT NULL,
            field_type NVARCHAR(100) NULL,
            label NVARCHAR(500) NULL,
            export_label NVARCHAR(500) NULL,
            displayed BIT NULL,
            value_text NVARCHAR(MAX) NULL,
            value_numeric DECIMAL(18,6) NULL,
            value_date DATE NULL,
            value_time TIME(0) NULL,
            value_json NVARCHAR(MAX) NULL,
            loaded_at_utc DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
            multi_key NVARCHAR(200) NULL
        );
        """
    )

    cursor.executemany(
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
    conn.commit()

    cursor.execute(
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
                loaded_at_utc = src.loaded_at_utc,
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
                src.loaded_at_utc,
                src.multi_key
            )
        OUTPUT $action INTO #response_merge_actions(action_name);
        """
    )
    conn.commit()

    cursor.execute(
        """
        SELECT action_name, COUNT(*)
        FROM #response_merge_actions
        GROUP BY action_name
        """
    )
    response_action_counts = {row[0]: row[1] for row in cursor.fetchall()}
    responses_inserted = response_action_counts.get("INSERT", 0)
    responses_updated = response_action_counts.get("UPDATE", 0)

    print(
        f"✅ Responses merged | inserted={responses_inserted}, updated={responses_updated}"
    )

    print(f"Running stored procedure: {FACT_PROC}...")
    cursor.execute(f"EXEC {FACT_PROC}")
    conn.commit()
    print("✅ Stored procedure executed successfully")

    cursor.execute(
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
            error_message = NULL
        WHERE run_id = ?
        """,
        "SUCCESS",
        submissions_fetched_count,
        submissions_after_filter_count,
        responses_fetched_count,
        submissions_inserted,
        submissions_updated,
        responses_inserted,
        responses_updated,
        run_id,
    )
    conn.commit()
    print("✅ ETL completed successfully")

except Exception as e:
    error_message = str(e)
    print(f"❌ ETL failed: {error_message}")

    if cursor and run_id:
        try:
            cursor.execute(
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
                "FAILED",
                submissions_fetched_count,
                submissions_after_filter_count,
                responses_fetched_count,
                submissions_inserted,
                submissions_updated,
                responses_inserted,
                responses_updated,
                error_message[:4000],
                run_id,
            )
            conn.commit()
            print("✅ ETL failure logged")
        except Exception as log_error:
            print(f"❌ Failed to log ETL failure: {log_error}")

    raise

finally:
    if session:
        session.close()
    if cursor:
        cursor.close()
    if conn:
        conn.close()