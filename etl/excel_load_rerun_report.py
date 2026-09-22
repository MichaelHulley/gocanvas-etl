# =========================================
# Script: excel_load_rerun_report.py
# Author: Michael Hulley
# Project: Knightshade Factory Reporting
# Purpose:
#   Load ReRunReport.xlsx into
#   dbo.stg_rerun_report
#
# Source:
#   C:\KnightshadeData\Factory Reporting\ReRunReport.xlsx
#
# Target:
#   dbo.stg_rerun_report
#
# Created: 2026-06-14
# Updated: 2026-09-10
#   - Added Week # -> week_number
#   - Improved RL / RA / RB rerun classification
# =========================================

import os

from pathlib import Path
from datetime import datetime, time

import pandas as pd
import pyodbc
import re

from dotenv import load_dotenv


# =========================
# CONFIG
# =========================

BASE_DIR = Path(__file__).resolve().parents[1]

load_dotenv(BASE_DIR / ".env")

REPORT_FOLDER = os.getenv(
    "REPORT_FOLDER",
    r"C:\KnightshadeData\Factory Reporting"
)

EXCEL_FILE = os.path.join(
    REPORT_FOLDER,
    "ReRunReport.xlsx"
)

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE") or os.getenv("SQL_DB")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

ODBC_DRIVER = os.getenv(
    "ODBC_DRIVER",
    "ODBC Driver 18 for SQL Server"
)

SOURCE_FILE_NAME = "ReRunReport.xlsx"


# =========================
# HELPERS
# =========================

def clean_text(value):

    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    return value


def to_decimal(value):

    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    value = value.replace(",", "")

    parsed = pd.to_numeric(
        value,
        errors="coerce"
    )

    if pd.isna(parsed):
        return None

    return float(parsed)


def to_int(value):

    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    parsed = pd.to_numeric(
        value,
        errors="coerce"
    )

    if pd.isna(parsed):
        return None

    return int(parsed)


def to_date(value):

    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    parsed = pd.to_datetime(
        value,
        errors="coerce"
    )

    if pd.isna(parsed):
        return None

    return parsed.date()

def to_time(value):

    if pd.isna(value):
        return None

    if isinstance(value, time):
        return value

    if isinstance(value, datetime):
        return value.time()

    value = str(value).strip()

    if value == "":
        return None

    parsed = pd.to_datetime(value, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed.time()

def get_effective_rerun_date(date_rerun, comment):

    comment = clean_text(comment)

    # Prefer rerun date written in Comment3
    if comment:

        match = re.search(
            r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b",
            comment
        )

        if match:

            day = int(match.group(1))
            month = int(match.group(2))
            year = int(match.group(3))

            if year < 100:
                year += 2000

            try:
                return datetime(
                    year,
                    month,
                    day
                ).date()

            except ValueError:
                pass

    # Fall back to DateRerun
    return to_date(date_rerun)


def classify_rerun(reason):

    if pd.isna(reason):
        return None

    reason = str(reason).strip()
    reason_upper = reason.upper()
    reason_lower = reason.lower()

    # Primary classification from factory rerun codes
    if reason_upper.startswith("RL"):
        return "EndOfLine"

    if reason_upper.startswith("RA"):
        return "AscepticBag"

    if reason_upper.startswith("RB"):
        return "Breakdown"

    # Fallback for historical/free-text descriptions
    if "eol" in reason_lower or "end of line" in reason_lower:
        return "EndOfLine"

    if (
        "asceptic" in reason_lower
        or "aseptic" in reason_lower
        or "damaged drum" in reason_lower
        or "damaged lid" in reason_lower
    ):
        return "AscepticBag"

    if (
        "breakdown" in reason_lower
        or "pump" in reason_lower
        or "mechanical" in reason_lower
    ):
        return "Breakdown"

    return "Other"


def get_connection():

    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=Yes;"
    )

    return pyodbc.connect(conn_str)


# =========================
# MAIN LOAD
# =========================

def main():

    print("Loading Excel file:")
    print(EXCEL_FILE)

    if not os.path.exists(EXCEL_FILE):
        raise FileNotFoundError(
            f"Excel file not found: {EXCEL_FILE}"
        )

    df = pd.read_excel(EXCEL_FILE)

    df.columns = df.columns.str.strip()

    print("Raw rows:", len(df))
    print("Raw columns:", list(df.columns))

    # =========================
    # COLUMN MAPPING
    # =========================

    df = df.rename(columns={

        "BarCodeNumber": "bar_code_number",
        "DrumStatus": "drum_status",
        "Comment3": "comment3",
        "DateRerun": "date_rerun",
        "SeriesRerun": "series_rerun",
        "SupervisedBy": "supervised_by",

        "ReasonforReRun": "reason_for_rerun",
        "ReasonforRerun": "reason_for_rerun",

        "ReRunCategory": "rerun_category"

     })

    print("Renamed columns:", list(df.columns))

    # =========================
    # REQUIRED COLUMNS
    # =========================

    required_columns = [

        "bar_code_number",
        "drum_status",
        "comment3",
        "date_rerun",
        "series_rerun",
        "supervised_by",
        "reason_for_rerun",
  #      "week_number",
    ]

    missing = [
        c
        for c in required_columns
        if c not in df.columns
    ]

    if missing:

        raise ValueError(
            f"Missing expected columns in Excel file: {missing}. "
            f"Available columns after rename: {list(df.columns)}"
        )

    # =========================
    # RERUN CATEGORY
    # =========================

    if "rerun_category" in df.columns:

        supplied_category = (
            df["rerun_category"]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
        )

        derived_category = (
            df["reason_for_rerun"]
            .apply(classify_rerun)
        )

        df["rerun_category"] = (
            supplied_category
            .fillna(derived_category)
        )

    else:

        df["rerun_category"] = (
            df["reason_for_rerun"]
            .apply(classify_rerun)
        )


    # =========================
    # REMOVE EMPTY ROWS
    # =========================

    df = df.dropna(how="all")

    # Keep rows containing useful rerun information.
    #
    # A barcode may occasionally be blank in the source,
    # so retain the row for staging/audit purposes.
    # Reporting calculations will require a valid barcode
    # when joining to stg_drum_report.

    df = df[

        (df["bar_code_number"].notna()) |
        (df["date_rerun"].notna()) |
        (df["series_rerun"].notna()) |
        (df["reason_for_rerun"].notna())

    ]

    print("Rows after blank removal:", len(df))

    # =========================
    # DIAGNOSTICS
    # =========================

    print("Sample series_rerun values:")
    print(
        df["series_rerun"]
        .dropna()
        .head(10)
        .tolist()
    )

    print("Sample rerun_category values:")
    print(
        df["rerun_category"]
        .dropna()
        .head(10)
        .tolist()
    )

    
    print(
        "Max bar_code_number:",
        pd.to_numeric(
            df["bar_code_number"],
            errors="coerce"
        ).max()
    )

    
    print(
        "Rows with valid barcode:",
        pd.to_numeric(
            df["bar_code_number"],
            errors="coerce"
        ).notna().sum()
    )

    # =========================
    # DATABASE CONNECTION
    # =========================

    conn = get_connection()

    cursor = conn.cursor()

    cursor.fast_executemany = False

    print(
        "Truncating dbo.stg_rerun_report..."
    )

    cursor.execute(
        "TRUNCATE TABLE dbo.stg_rerun_report;"
    )

    # =========================
    # INSERT
    # =========================

    insert_sql = """
    INSERT INTO dbo.stg_rerun_report (

        source_file_name,
        source_row_number,
        bar_code_number,
        drum_status,
        comment3,
        date_rerun,
        effective_rerun_date,
        series_rerun,
        supervised_by,
        reason_for_rerun,
        rerun_category

    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    rows_to_insert = []

    for idx, row in df.iterrows():
        rows_to_insert.append((

            SOURCE_FILE_NAME,
            int(idx) + 2,

            to_int(
                row["bar_code_number"]
            ),

            clean_text(
                row["drum_status"]
            ),

            clean_text(
                row["comment3"]
            ),

            to_date(
                row["date_rerun"]
            ),

            get_effective_rerun_date(
                row["date_rerun"],
                row["comment3"]
            ),

            clean_text(
                row["series_rerun"]
            ),

            clean_text(
                row["supervised_by"]
            ),

            clean_text(
                row["reason_for_rerun"]
            ),

            clean_text(
                row["rerun_category"]
            ),

        ))



    print(
        "Inserting rows:",
        len(rows_to_insert)
    )

    for i, row_data in enumerate(
        rows_to_insert,
        start=1
    ):

        if i % 100 == 0:
            print(f"Inserting row {i}")

        try:

            cursor.execute(
                insert_sql,
                row_data
            )

        except Exception:

            print(
                "Failed insert at prepared row:",
                i
            )

            print("Row data:")
            print(row_data)

            raise

    conn.commit()

    cursor.close()
    conn.close()

    print("Load complete.")

    print(
        f"Loaded {len(rows_to_insert)} rows "
        "into dbo.stg_rerun_report"
    )


if __name__ == "__main__":
    main()