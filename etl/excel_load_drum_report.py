# =========================================
# Script: excel_load_drum_report.py
# Author: Michael Hulley
# Project: Knightshade Factory Reporting
# Purpose:
#   Load DrumReport.xlsx into
#   dbo.stg_drum_report
#
# Source:
#   C:\KnightshadeData\Factory Reporting\DrumReport.xlsx
#
# Target:
#   dbo.stg_drum_report
#
# Created: 2026-06-10
# =========================================


import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyodbc
from dotenv import load_dotenv


# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

REPORT_FOLDER = os.getenv("REPORT_FOLDER", r"C:\Users\Administrator\OneDrive\KnightshadeData\Factory Reporting")
EXCEL_FILE = os.path.join(REPORT_FOLDER, "DrumReport.xlsx")

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE") or os.getenv("SQL_DB")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

SOURCE_FILE_NAME = "DrumReport.xlsx"


# =========================
# HELPERS
# =========================

def clean_text_limited(value, max_length: int):
    if pd.isna(value):
        return None

    text = str(value).strip()

    if not text:
        return None

    # Treat strings made only of punctuation/backticks as invalid.
    if not any(char.isalnum() for char in text):
        return None

    return text[:max_length]

def clean_text(value):
    if pd.isna(value):
        return None
    return str(value).strip()


def to_decimal(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    value = value.replace(",", "")

    parsed = pd.to_numeric(value, errors="coerce")

    if pd.isna(parsed):
        return None

    return float(parsed)

def to_int(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    return int(float(value))

def to_date(value):
    if pd.isna(value) or value == "":
        return None
    return pd.to_datetime(value).date()


from datetime import datetime, time

def to_time(value):
    if pd.isna(value) or value == "":
        return None

    # Excel is already returning a time object
    if isinstance(value, time):
        return value

    # Datetime -> extract time portion
    if isinstance(value, datetime):
        return value.time()

    parsed = pd.to_datetime(value, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed.time()


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
        raise FileNotFoundError(f"Excel file not found: {EXCEL_FILE}")

    df = pd.read_excel(EXCEL_FILE)
    df.columns = df.columns.str.strip()

    print("Raw rows:", len(df))
    print("Raw columns:", list(df.columns))

    # Standardise column names from Excel to SQL-friendly names
    df = df.rename(columns={
        "Date": "drum_date",
        "FillingTime": "filling_time",
        "BarCodeNumber": "drum_number",
        "DrumNumber": "drum_number",
        "Brix": "brix",
        "NetWeight": "net_weight_kg",
        "ProductType": "quality_parameter",
        "QualityParameter": "quality_parameter",
        "QualityKey": "quality_key",
        "pH": "ph",
        "Series": "series",
        "DrumStatus": "drum_status",
        "Shift": "shift",
        "Year": "production_year",
        "BagType": "bag_type",
    })

    print("Columns after rename:", list(df.columns))
    print("Bag type column exists:", "bag_type" in df.columns)

    if "bag_type" in df.columns:
        print("Bag type non-null count:", df["bag_type"].notna().sum())
        print("Sample BagType values:")
        print(df["bag_type"].dropna().head(20).tolist())

    # Remove dummy rows and empty drum records
    df = df[
        (df["drum_number"].notna()) &
        (df["drum_number"] != 0)
    ]

    required_columns = [
        "drum_date",
        "filling_time",
        "drum_number",
        "brix",
        "net_weight_kg",
        "quality_parameter",
        "quality_key",
        "ph",
        "series",
        "drum_status",
        "shift",
        "production_year",
        "bag_type",
    ]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in Excel file: {missing}")

    df = df[
        (df["drum_number"].notna()) &
        (df["drum_number"] != 0)
    ]

    # Remove fully blank rows
    df = df.dropna(how="all")

    print("Rows after blank removal:", len(df))
    
    print("Max brix:", pd.to_numeric(df["brix"], errors="coerce").max())
    print("Max ph:", pd.to_numeric(df["ph"], errors="coerce").max())
    print("Max net_weight_kg:", pd.to_numeric(df["net_weight_kg"], errors="coerce").max())
    print("Max production_year:", pd.to_numeric(df["production_year"], errors="coerce").max())
    
    # NEW DEBUG
    print("Max drum_number:", pd.to_numeric(df["drum_number"], errors="coerce").max())
    print("Max source row:", len(df) + 1)
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = False

    print("Truncating dbo.stg_drum_report...")
    cursor.execute("TRUNCATE TABLE dbo.stg_drum_report;")

    insert_sql = """
        INSERT INTO dbo.stg_drum_report (
            source_file_name,
            source_row_number,
            drum_date,
            filling_time,
            drum_number,
            brix,
            net_weight_kg,
            quality_parameter,
            quality_key,
            ph,
            series,
            drum_status,
            shift,
            production_year,
            bag_type
        )
        VALUES (?, ?, ?, ? ,? ,? , ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    rows_to_insert = []

    for idx, row in df.iterrows():
        rows_to_insert.append((
            SOURCE_FILE_NAME,
            int(idx) + 2,  # Excel row number, assuming row 1 is headers
            to_date(row["drum_date"]),
            to_time(row["filling_time"]),
            to_int(row["drum_number"]),
            to_decimal(row["brix"]),
            to_decimal(row["net_weight_kg"]),
            clean_text(row["quality_parameter"]),
            clean_text(row["quality_key"]),
            to_decimal(row["ph"]),
            clean_text(row["series"]),
            clean_text(row["drum_status"]),
            clean_text(row["shift"]),
            to_int(row["production_year"]),
            clean_text_limited(row["bag_type"], 100)
        ))

    print("Inserting rows:", len(rows_to_insert))
    for i, row_data in enumerate(rows_to_insert, start=1):

        if i % 100 == 0:
            print(f"Inserting row {i}")

        try:
            cursor.execute(insert_sql, row_data)

        except Exception as e:
            print("Failed insert at prepared row:", i)
            print("Row data:")
            print(row_data)
            raise

    conn.commit()
    cursor.close()
    conn.close()

    print("Load complete.")
    print(f"Loaded {len(rows_to_insert)} rows into dbo.stg_drum_report")


if __name__ == "__main__":
    main()