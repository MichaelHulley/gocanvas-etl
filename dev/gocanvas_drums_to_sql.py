import os
import sys
from datetime import datetime
from typing import Any, Dict, List

import pyodbc
from dotenv import load_dotenv

load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
ODBC_DRIVER = "ODBC Driver 18 for SQL Server"

FORM_ID = 5771040

MERGE_DRUM_SQL = """
MERGE dbo.stg_gocanvas_drum AS tgt
USING (
    SELECT
        ? AS submission_id,
        ? AS multi_key,
        ? AS form_id,
        ? AS submission_number,
        ? AS created_at_utc,
        ? AS status,
        ? AS series_number,
        ? AS start_date,
        ? AS shift_time,
        ? AS shift_start_time,
        ? AS shift_supervisor,
        ? AS quality_assistant,
        ? AS drum_fillers,
        ? AS drum_number,
        ? AS barcode,
        ? AS drum_type,
        ? AS foil_bag_batch_number,
        ? AS product_batch_number,
        ? AS filling_time,
        ? AS lid_seal_status,
        ? AS packaging_weight,
        ? AS content_weight,
        ? AS gross_weight,
        ? AS comment_text
) AS src
ON tgt.submission_id = src.submission_id
AND tgt.multi_key = src.multi_key
WHEN MATCHED THEN
    UPDATE SET
        form_id               = src.form_id,
        submission_number     = src.submission_number,
        created_at_utc        = src.created_at_utc,
        status                = src.status,
        series_number         = src.series_number,
        start_date            = src.start_date,
        shift_time            = src.shift_time,
        shift_start_time      = src.shift_start_time,
        shift_supervisor      = src.shift_supervisor,
        quality_assistant     = src.quality_assistant,
        drum_fillers          = src.drum_fillers,
        drum_number           = src.drum_number,
        barcode               = src.barcode,
        drum_type             = src.drum_type,
        foil_bag_batch_number = src.foil_bag_batch_number,
        product_batch_number  = src.product_batch_number,
        filling_time          = src.filling_time,
        lid_seal_status       = src.lid_seal_status,
        packaging_weight      = src.packaging_weight,
        content_weight        = src.content_weight,
        gross_weight          = src.gross_weight,
        comment_text          = src.comment_text,
        loaded_at_utc         = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        submission_id, multi_key, form_id, submission_number, created_at_utc, status,
        series_number, start_date, shift_time, shift_start_time, shift_supervisor,
        quality_assistant, drum_fillers, drum_number, barcode, drum_type,
        foil_bag_batch_number, product_batch_number, filling_time, lid_seal_status,
        packaging_weight, content_weight, gross_weight, comment_text
    )
    VALUES (
        src.submission_id, src.multi_key, src.form_id, src.submission_number, src.created_at_utc, src.status,
        src.series_number, src.start_date, src.shift_time, src.shift_start_time, src.shift_supervisor,
        src.quality_assistant, src.drum_fillers, src.drum_number, src.barcode, src.drum_type,
        src.foil_bag_batch_number, src.product_batch_number, src.filling_time, src.lid_seal_status,
        src.packaging_weight, src.content_weight, src.gross_weight, src.comment_text
    );
"""


def get_conn() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
        "Encrypt=no;"
    )
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn


def parse_date_mmddyyyy(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value).strip(), "%m/%d/%Y").date()
    except ValueError:
        return None


def parse_time_flexible(value):
    if not value:
        return None
    value = str(value).strip()
    for fmt in ("%H:%M:%S", "%H:%M", "%I:%M %p"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            pass
    return None


def normalize_text(value):
    if value is None:
        return None
    s = str(value).strip()
    return s if s != "" else None


def to_decimal(value):
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fetch_submission_headers(cur: pyodbc.Cursor) -> Dict[int, Dict[str, Any]]:
    sql = """
    SELECT
        submission_id,
        form_id,
        submission_number,
        created_at_utc,
        status
    FROM dbo.stg_gocanvas_submission
    WHERE form_id = ?
    """
    rows = cur.execute(sql, FORM_ID).fetchall()

    headers: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        headers[int(r.submission_id)] = {
            "submission_id": int(r.submission_id),
            "form_id": r.form_id,
            "submission_number": r.submission_number,
            "created_at_utc": r.created_at_utc,
            "status": r.status,
            "series_number": None,
            "start_date": None,
            "shift_time": None,
            "shift_start_time": None,
            "shift_supervisor": None,
            "quality_assistant": None,
            "drum_fillers": None,
        }
    return headers


def fetch_submission_responses(cur: pyodbc.Cursor):
    sql = """
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
        multi_key
    FROM dbo.stg_gocanvas_response
    WHERE form_id = ?
    ORDER BY submission_id, multi_key, response_id
    """
    return list(cur.execute(sql, FORM_ID).fetchall())


def assign_header_field(header: Dict[str, Any], label: str, value_text: Any):
    value = normalize_text(value_text)

    if label in ("Seies Number", "Series Number"):
        header["series_number"] = value
    elif label == "Start Date":
        header["start_date"] = parse_date_mmddyyyy(value)
    elif label == "Shift Time":
        header["shift_time"] = value
    elif label == "Shift Start Time":
        header["shift_start_time"] = parse_time_flexible(value)
    elif label == "Shift Supervisor":
        header["shift_supervisor"] = value
    elif label == "Quality Assistant":
        header["quality_assistant"] = value
    elif label == "Drum Fillers":
        header["drum_fillers"] = value


def assign_drum_field(
    drum_row: Dict[str, Any], label: str, value_text: Any, value_numeric: Any
):
    text_value = normalize_text(value_text)

    if label == "Drum Number":
        drum_row["drum_number"] = to_decimal(
            value_numeric if value_numeric is not None else value_text
        )
    elif label == "Barcode":
        drum_row["barcode"] = text_value
    elif label == "Drum Type":
        drum_row["drum_type"] = text_value
    elif label == "Foil Bag Batch Number":
        source_val = value_numeric if value_numeric is not None else value_text
        drum_row["foil_bag_batch_number"] = normalize_text(source_val)
    elif label == "Product Batch Number":
        drum_row["product_batch_number"] = text_value
    elif label == "Filling Time":
        drum_row["filling_time"] = parse_time_flexible(text_value)
    elif label == "Lid Seal Good/Bad":
        drum_row["lid_seal_status"] = text_value
    elif label == "Packaging weight":
        drum_row["packaging_weight"] = to_decimal(
            value_numeric if value_numeric is not None else value_text
        )
    elif label == "Content Weight":
        drum_row["content_weight"] = to_decimal(
            value_numeric if value_numeric is not None else value_text
        )
    elif label == "Gross Weight":
        drum_row["gross_weight"] = to_decimal(
            value_numeric if value_numeric is not None else value_text
        )
    elif label == "Comment":
        drum_row["comment_text"] = text_value


def build_drum_rows(
    headers: Dict[int, Dict[str, Any]], responses
) -> List[Dict[str, Any]]:
    drums_by_key: Dict[tuple, Dict[str, Any]] = {}

    for r in responses:
        submission_id = int(r.submission_id)
        label = (r.label or "").strip()
        multi_key = normalize_text(r.multi_key)

        if submission_id not in headers:
            continue

        if not multi_key:
            assign_header_field(headers[submission_id], label, r.value_text)
            continue

        key = (submission_id, multi_key)

        if key not in drums_by_key:
            hdr = headers[submission_id]
            drums_by_key[key] = {
                "submission_id": submission_id,
                "multi_key": multi_key,
                "form_id": hdr["form_id"],
                "submission_number": hdr["submission_number"],
                "created_at_utc": hdr["created_at_utc"],
                "status": hdr["status"],
                "series_number": None,
                "start_date": None,
                "shift_time": None,
                "shift_start_time": None,
                "shift_supervisor": None,
                "quality_assistant": None,
                "drum_fillers": None,
                "drum_number": None,
                "barcode": None,
                "drum_type": None,
                "foil_bag_batch_number": None,
                "product_batch_number": None,
                "filling_time": None,
                "lid_seal_status": None,
                "packaging_weight": None,
                "content_weight": None,
                "gross_weight": None,
                "comment_text": None,
            }

        assign_drum_field(
            drums_by_key[key],
            label,
            r.value_text,
            r.value_numeric,
        )

    for (submission_id, _), drum in drums_by_key.items():
        hdr = headers[submission_id]
        drum["series_number"] = hdr["series_number"]
        drum["start_date"] = hdr["start_date"]
        drum["shift_time"] = hdr["shift_time"]
        drum["shift_start_time"] = hdr["shift_start_time"]
        drum["shift_supervisor"] = hdr["shift_supervisor"]
        drum["quality_assistant"] = hdr["quality_assistant"]
        drum["drum_fillers"] = hdr["drum_fillers"]

    return list(drums_by_key.values())


def upsert_drum_rows(cur: pyodbc.Cursor, drum_rows: List[Dict[str, Any]]):
    params = []
    for d in drum_rows:
        params.append(
            (
                d["submission_id"],
                d["multi_key"],
                d["form_id"],
                d["submission_number"],
                d["created_at_utc"],
                d["status"],
                d["series_number"],
                d["start_date"],
                d["shift_time"],
                d["shift_start_time"],
                d["shift_supervisor"],
                d["quality_assistant"],
                d["drum_fillers"],
                d["drum_number"],
                d["barcode"],
                d["drum_type"],
                d["foil_bag_batch_number"],
                d["product_batch_number"],
                d["filling_time"],
                d["lid_seal_status"],
                d["packaging_weight"],
                d["content_weight"],
                d["gross_weight"],
                d["comment_text"],
            )
        )

    cur.fast_executemany = True
    cur.executemany(MERGE_DRUM_SQL, params)


def main():
    conn = get_conn()
    try:
        cur = conn.cursor()

        headers = fetch_submission_headers(cur)
        responses = fetch_submission_responses(cur)

        print(f"Found {len(headers)} staged submissions for form_id={FORM_ID}")
        print(f"Found {len(responses)} staged responses for form_id={FORM_ID}")

        drum_rows = build_drum_rows(headers, responses)
        print(f"Built {len(drum_rows)} drum rows")

        if drum_rows:
            upsert_drum_rows(cur, drum_rows)
            conn.commit()

        print("Done.")
    except Exception as e:
        conn.rollback()
        print("ERROR:", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
