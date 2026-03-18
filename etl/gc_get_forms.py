import os
import json
import requests
import pyodbc
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import timedelta
from gocanvas_get_token import get_access_token

load_dotenv()

# =========================
# CONFIG
# =========================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB") or os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER") or os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

FORM_IDS = [
    5757557,
    5525962, 5525968, 5612718, 5612894, 5648299,
    5676079, 5679477, 5682489, 5686251, 5761346, 5771040
]

LIMIT = None
BASE_URL = "https://www.gocanvas.com/api/v3"
PROCESS_NAME = "tomato_intake"

# =========================
# COUNTERS FOR LOGGING
# =========================
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
    # =========================
    # TOKEN
    # =========================
    token = get_access_token()
    print("✅ Access token retrieved")

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # =========================
    # SQL CONNECTION
    # =========================
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

    # =========================
    # START ETL LOG
    # =========================
    cursor.execute("""
    INSERT INTO dbo.etl_run_log (
        process_name,
        started_at_utc,
        status
    )
    OUTPUT INSERTED.run_id
    VALUES (?, SYSUTCDATETIME(), ?)
    """, PROCESS_NAME, "RUNNING")

    run_id = cursor.fetchone()[0]
    conn.commit()
    print(f"✅ ETL log started, run_id={run_id}")

    # =========================
    # LAST LOAD TIME
    # =========================
    cursor.execute("""
    SELECT MAX(created_at_utc)
    FROM dbo.stg_gocanvas_submission
    """)
    last_run = cursor.fetchone()[0]

    if last_run:
        lookback_dt = last_run - timedelta(days=1)
        created_after = lookback_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"✅ Incremental load with lookback from: {created_after}")
    else:
        created_after = None
        print("✅ No previous load found; running full load")

    # =========================
    # HTTP SESSION
    # =========================
    session = requests.Session()
    session.headers.update(headers)

    # =========================
    # FETCH SUBMISSIONS
    # =========================
    def fetch_submissions(form_ids, created_after=None):
        all_submissions = []

        for form_id in form_ids:
            print(f"Fetching submissions for form_id: {form_id}")
            page = 1

            while True:
                url = f"{BASE_URL}/submissions"
                params = {
                    "form_id": form_id,
                    "page": page,
                    "per_page": 100
                }

                if created_after:
                    params["created_after"] = created_after

                response = session.get(url, params=params, timeout=60)

                if response.status_code != 200:
                    print(f"❌ Failed to fetch submissions for form {form_id}, status {response.status_code}")
                    print(response.text[:300])
                    break

                data = response.json()

                if isinstance(data, dict) and "data" in data:
                    submissions = data["data"]
                elif isinstance(data, list):
                    submissions = data
                else:
                    submissions = []

                if not submissions:
                    break

                all_submissions.extend(submissions)

                if len(submissions) < 100:
                    break

                page += 1

        return all_submissions

    all_submissions = fetch_submissions(FORM_IDS, created_after=created_after)
    submissions_fetched_count = len(all_submissions)

    # =========================
    # PYTHON-SIDE DEDUPE
    # =========================
    deduped = {}
    for s in all_submissions:
        if isinstance(s, str):
            s = json.loads(s)
        sid = s.get("id")
        if sid is not None:
            deduped[sid] = s

    all_submissions = list(deduped.values())

    # =========================
    # PYTHON-SIDE INCREMENTAL FILTER
    # =========================
    if last_run:
        print("Applying Python-side incremental filter...")
        before_count = len(all_submissions)

        all_submissions = [
            s for s in all_submissions
            if s.get("created_at") and s["created_at"] > created_after
        ]

        after_count = len(all_submissions)
        print(f"Filtered submissions: {before_count} → {after_count}")

    if LIMIT:
        all_submissions = all_submissions[:LIMIT]

    submissions_after_filter_count = len(all_submissions)
    print(f"✅ Total submissions after dedupe/filter: {submissions_after_filter_count}")

    if not all_submissions:
        print("✅ No new submissions found. Nothing to do.")

        cursor.execute("""
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
        0,
        submissions_inserted,
        submissions_updated,
        responses_inserted,
        responses_updated,
        run_id
        )
        conn.commit()
        raise SystemExit()

    # =========================
    # LOAD SUBMISSIONS TO TEMP TABLE
    # =========================
    cursor.execute("""
    IF OBJECT_ID('tempdb..#submissions') IS NOT NULL DROP TABLE #submissions;

    CREATE TABLE #submissions (
        submission_id BIGINT NOT NULL,
        form_id BIGINT NULL,
        submission_number NVARCHAR(100) NULL,
        submission_name NVARCHAR(255) NULL,
        created_at_utc DATETIME2 NULL,
        revision BIT NOT NULL,
        status NVARCHAR(50) NULL
    );
    """)

    submission_rows = []
    for s in all_submissions:
        submission_rows.append((
            s.get("id"),
            s.get("form_id"),
            s.get("submission_number"),
            s.get("submission_name"),
            s.get("created_at"),
            bool(s.get("revision", False)),
            s.get("status", "completed")
        ))

    print("Loading submissions into temp table...")
    cursor.executemany("""
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
    """, submission_rows)
    conn.commit()

    # =========================
    # MERGE SUBMISSIONS WITH ACTION COUNTS
    # =========================
    print("Merging submissions into stg_gocanvas_submission...")

    cursor.execute("""
    IF OBJECT_ID('tempdb..#submission_merge_actions') IS NOT NULL DROP TABLE #submission_merge_actions;

    CREATE TABLE #submission_merge_actions (
        action_name NVARCHAR(10)
    );
    """)

    cursor.execute("""
    ;WITH src AS (
        SELECT
            submission_id,
            form_id,
            submission_number,
            submission_name,
            created_at_utc,
            revision,
            status,
            ROW_NUMBER() OVER (
                PARTITION BY submission_id
                ORDER BY created_at_utc DESC, form_id DESC
            ) AS rn
        FROM #submissions
    ),
    deduped AS (
        SELECT
            submission_id,
            form_id,
            submission_number,
            submission_name,
            created_at_utc,
            revision,
            status
        FROM src
        WHERE rn = 1
    )
    MERGE dbo.stg_gocanvas_submission AS tgt
    USING deduped AS src
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
    """)
    conn.commit()

    cursor.execute("""
    SELECT action_name, COUNT(*)
    FROM #submission_merge_actions
    GROUP BY action_name
    """)
    submission_action_counts = {row[0]: row[1] for row in cursor.fetchall()}

    submissions_inserted = submission_action_counts.get("INSERT", 0)
    submissions_updated = submission_action_counts.get("UPDATE", 0)

    print(f"✅ Submissions merged | inserted={submissions_inserted}, updated={submissions_updated}")

    # =========================
    # FETCH SUBMISSION DETAILS / RESPONSES
    # =========================
    response_rows = []

    print("Fetching submission details / responses...")
    for s in tqdm(all_submissions, desc="Responses", unit="submission"):
        sub_id = s.get("id")

        url = f"{BASE_URL}/submissions/{sub_id}"
        response = session.get(url, timeout=60)

        if response.status_code != 200:
            print(f"❌ Failed to fetch submission detail for submission {sub_id}, status {response.status_code}")
            print(response.text[:200])
            continue

        detail = response.json()

        form_id = s.get("form_id")
        if form_id is None and isinstance(detail, dict):
            form_id = detail.get("form_id")

        if not sub_id or not form_id:
            print(f"⚠ Skipping submission {sub_id} because form_id is missing")
            continue

        if isinstance(detail, dict):
            responses = detail.get("responses", [])
        else:
            responses = []

        for r in responses:
            if isinstance(r, str):
                r = json.loads(r)

            value_text = r.get("value_text")
            if value_text is None:
                value_text = r.get("value")

            if isinstance(value_text, (list, dict)):
                value_text = json.dumps(value_text, ensure_ascii=False)
            elif value_text is not None:
                value_text = str(value_text)

            value_numeric = r.get("value_numeric")
            if isinstance(value_numeric, (list, dict)):
                value_numeric = None

            value_date = r.get("value_date")
            if isinstance(value_date, (list, dict)):
                value_date = None

            value_time = r.get("value_time")
            if isinstance(value_time, (list, dict)):
                value_time = None

            displayed = r.get("displayed")
            if isinstance(displayed, (list, dict)):
                displayed = None

            response_id = r.get("id")
            label = r.get("label")

            if not response_id:
                continue

            response_rows.append((
                response_id,
                sub_id,   # entry_id
                sub_id,   # submission_id
                form_id,
                label,
                value_text,
                value_numeric,
                value_date,
                value_time,
                displayed
            ))

    responses_fetched_count = len(response_rows)
    print(f"✅ Total responses fetched: {responses_fetched_count}")

    bad_rows = [row for row in response_rows if row[3] is None]
    if bad_rows:
        print(f"❌ Found {len(bad_rows)} response rows with NULL form_id")
        print("Sample bad row:", bad_rows[0])
        raise SystemExit("NULL form_id found in response rows")

    # =========================
    # LOAD RESPONSES TO TEMP TABLE
    # =========================
    cursor.execute("""
    IF OBJECT_ID('tempdb..#responses') IS NOT NULL DROP TABLE #responses;

    CREATE TABLE #responses (
        response_id BIGINT NOT NULL,
        entry_id BIGINT NOT NULL,
        submission_id BIGINT NOT NULL,
        form_id BIGINT NOT NULL,
        label NVARCHAR(255) NULL,
        value_text NVARCHAR(MAX) NULL,
        value_numeric DECIMAL(18,3) NULL,
        value_date DATE NULL,
        value_time TIME NULL,
        displayed BIT NULL
    );
    """)

    print("Loading responses into temp table...")
    cursor.executemany("""
    INSERT INTO #responses (
        response_id,
        entry_id,
        submission_id,
        form_id,
        label,
        value_text,
        value_numeric,
        value_date,
        value_time,
        displayed
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, response_rows)
    conn.commit()

    # =========================
    # MERGE RESPONSES WITH ACTION COUNTS
    # =========================
    print("Merging responses into stg_gocanvas_response...")

    cursor.execute("""
    IF OBJECT_ID('tempdb..#response_merge_actions') IS NOT NULL DROP TABLE #response_merge_actions;

    CREATE TABLE #response_merge_actions (
        action_name NVARCHAR(10)
    );
    """)

    cursor.execute("""
    ;WITH src AS (
        SELECT
            response_id,
            entry_id,
            submission_id,
            form_id,
            label,
            value_text,
            value_numeric,
            value_date,
            value_time,
            displayed,
            ROW_NUMBER() OVER (
                PARTITION BY response_id
                ORDER BY submission_id DESC
            ) AS rn
        FROM #responses
    ),
    deduped AS (
        SELECT
            response_id,
            entry_id,
            submission_id,
            form_id,
            label,
            value_text,
            value_numeric,
            value_date,
            value_time,
            displayed
        FROM src
        WHERE rn = 1
    )
    MERGE dbo.stg_gocanvas_response AS tgt
    USING deduped AS src
        ON tgt.response_id = src.response_id
    WHEN MATCHED THEN
        UPDATE SET
            entry_id = src.entry_id,
            submission_id = src.submission_id,
            form_id = src.form_id,
            label = src.label,
            value_text = src.value_text,
            value_numeric = src.value_numeric,
            value_date = src.value_date,
            value_time = src.value_time,
            displayed = src.displayed
    WHEN NOT MATCHED THEN
        INSERT (
            response_id,
            entry_id,
            submission_id,
            form_id,
            label,
            value_text,
            value_numeric,
            value_date,
            value_time,
            displayed
        )
        VALUES (
            src.response_id,
            src.entry_id,
            src.submission_id,
            src.form_id,
            src.label,
            src.value_text,
            src.value_numeric,
            src.value_date,
            src.value_time,
            src.displayed
        )
    OUTPUT $action INTO #response_merge_actions(action_name);
    """)
    conn.commit()

    cursor.execute("""
    SELECT action_name, COUNT(*)
    FROM #response_merge_actions
    GROUP BY action_name
    """)
    response_action_counts = {row[0]: row[1] for row in cursor.fetchall()}

    responses_inserted = response_action_counts.get("INSERT", 0)
    responses_updated = response_action_counts.get("UPDATE", 0)

    print(f"✅ Responses merged | inserted={responses_inserted}, updated={responses_updated}")

    # =========================
    # RUN FACT LOAD
    # =========================
    print("Running stored procedure: usp_load_fact_tomato_intake...")
    cursor.execute("EXEC dbo.usp_load_fact_tomato_intake")
    conn.commit()
    print("✅ Stored procedure executed successfully")

    # =========================
    # UPDATE ETL LOG - SUCCESS
    # =========================
    cursor.execute("""
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
    run_id
    )
    conn.commit()
    print("✅ ETL log updated")

except Exception as e:
    print(f"❌ ETL failed: {e}")

    if cursor is not None and conn is not None and run_id is not None:
        try:
            cursor.execute("""
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
            str(e),
            run_id
            )
            conn.commit()
            print("✅ ETL failure logged")
        except Exception as log_error:
            print(f"❌ Failed to write ETL failure log: {log_error}")

    raise

finally:
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()
    if session is not None:
        session.close()