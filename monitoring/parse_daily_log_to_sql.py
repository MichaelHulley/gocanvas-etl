# =========================================
# Knightshade ETL Log Parser → SQL Loader
# Author: Michael Hulley
# =========================================

import os
import re
from datetime import datetime

import pyodbc
from dotenv import load_dotenv

load_dotenv()

# =========================
# CONFIG
# =========================
LOG_FOLDER = r"C:\Users\mrdhulley\PythonProjects\GoCanvas_API\logs"

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE") or os.getenv("SQL_DB")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

if not SQL_SERVER:
    raise RuntimeError("Missing SQL_SERVER in .env")
if not SQL_DATABASE:
    raise RuntimeError("Missing SQL_DATABASE or SQL_DB in .env")
if not SQL_USER:
    raise RuntimeError("Missing SQL_USER in .env")
if not SQL_PASSWORD:
    raise RuntimeError("Missing SQL_PASSWORD in .env")


# =========================
# GET LATEST LOG FILE
# =========================
def get_latest_log_file(folder: str) -> str:
    files = [
        f for f in os.listdir(folder)
        if f.lower().endswith(".log")
    ]

    if not files:
        raise FileNotFoundError(f"No log files found in {folder}")

    files.sort(
        key=lambda x: os.path.getmtime(os.path.join(folder, x)),
        reverse=True
    )

    return os.path.join(folder, files[0])


# =========================
# DATE PARSING
# =========================
def parse_bat_datetime(value: str):
    """
    Parses BAT timestamps like:
    02/05/2026 11:43:19.75
    02/05/2026 11:43:19
    """
    if not value:
        return None

    value = value.strip()

    # Remove decimal seconds
    if "." in value:
        value = value.split(".")[0]

    return datetime.strptime(value, "%d/%m/%Y %H:%M:%S")


# =========================
# STATUS HELPERS
# =========================
def detect_step_status(content: str, start_text: str, success_text: str | None = None) -> str:
    """
    Simple status detection:
    - NOT RUN if start marker not found
    - SUCCESS if start marker found and no obvious failure
    """
    if start_text not in content:
        return "NOT RUN"

    if success_text and success_text not in content:
        return "FAILED"

    return "SUCCESS"


# =========================
# PARSE LOG FILE
# =========================
def parse_log(file_path: str) -> dict:
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    content = "".join(lines)

    # Timestamps from explicit BAT markers
    start_time = None
    end_time = None
    runtime_minutes = None

    start_match = re.search(r"START_TIME=(.+)", content)
    end_match = re.search(r"END_TIME=(.+)", content)

    try:
        if start_match:
            start_time = parse_bat_datetime(start_match.group(1))

        if end_match:
            end_time = parse_bat_datetime(end_match.group(1))

        if start_time and end_time:
            runtime_minutes = round((end_time - start_time).total_seconds() / 60, 2)

    except Exception:
        start_time = None
        end_time = None
        runtime_minutes = None

    # Step statuses
    intake_status = detect_step_status(
        content,
        "Running Intake ETL",
        "ETL completed successfully"
    )

    drum_fill_status = detect_step_status(
        content,
        "Running Drum Fill ETL",
        "LOAD SUMMARY"
    )

    shift_status = detect_step_status(
        content,
        "Running Shift Report ETL",
        "ETL completed successfully"
    )

    email_status = detect_step_status(
        content,
        "Sending Daily Report",
        "Email sent successfully"
    )

    # Overall status
    if "FAILED" in content:
        overall_status = "FAILED"
    elif "Completed successfully" in content and end_time is not None:
        overall_status = "SUCCESS"
    else:
        overall_status = "FAILED"

    # Last useful message
    last_message = None
    for line in reversed(lines):
        clean = line.strip()
        if clean:
            last_message = clean
            break

    return {
        "log_file_name": os.path.basename(file_path),
        "run_started_at": start_time,
        "run_completed_at": end_time,
        "overall_status": overall_status,
        "intake_status": intake_status,
        "drum_fill_status": drum_fill_status,
        "shift_status": shift_status,
        "email_status": email_status,
        "runtime_minutes": runtime_minutes,
        "last_message": last_message,
    }


# =========================
# LOAD INTO SQL
# =========================
def load_to_sql(data: dict) -> None:
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO dbo.etl_daily_run_status
        (
            log_file_name,
            run_started_at,
            run_completed_at,
            overall_status,
            intake_status,
            drum_fill_status,
            shift_report_status,
            email_status,
            total_runtime_minutes,
            last_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        data["log_file_name"],
        data["run_started_at"],
        data["run_completed_at"],
        data["overall_status"],
        data["intake_status"],
        data["drum_fill_status"],
        data["shift_status"],
        data["email_status"],
        data["runtime_minutes"],
        data["last_message"],
    )

    conn.commit()
    cursor.close()
    conn.close()


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    try:
        log_file = get_latest_log_file(LOG_FOLDER)
        parsed = parse_log(log_file)

        print("Parsed log:")
        for key, value in parsed.items():
            print(f"{key}: {value}")

        load_to_sql(parsed)
        print("Log successfully loaded to SQL")

    except Exception as e:
        print("ERROR:", str(e))
        raise