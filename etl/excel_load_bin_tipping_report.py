# =========================================
# Script: excel_load_bin_tipping_report.py
# Author: Michael Hulley
# Project: Knightshade Factory Reporting
# Purpose:
#   Load BinTippingReport.xlsx into
#   dbo.stg_bin_tipping_report
#
# Source:
#   C:\KnightshadeData\Factory Reporting\BinTippingReport.xlsx
#
# Target:
#   dbo.stg_bin_tipping_report
#
# Created: 2026-06-14
# =========================================

import os
from pathlib import Path
from datetime import datetime, time

import pandas as pd
import pyodbc
from dotenv import load_dotenv


# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

REPORT_FOLDER = os.getenv("REPORT_FOLDER", r"C:\KnightshadeData\Factory Reporting")
EXCEL_FILE = os.path.join(REPORT_FOLDER, "BinTippingReport.xlsx")

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE") or os.getenv("SQL_DB")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

SOURCE_FILE_NAME = "BinTippingReport.xlsx"


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

    parsed = pd.to_numeric(value, errors="coerce")

    if pd.isna(parsed):
        return None

    return int(parsed)


def to_date(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    parsed = pd.to_datetime(value, errors="coerce")

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
    xls = pd.ExcelFile(EXCEL_FILE)

    print("\nExcel worksheets:")
    for sheet in xls.sheet_names:
        print("  ", repr(sheet))

    print("\nColumns by worksheet:")
    for sheet in xls.sheet_names:
        test_df = pd.read_excel(EXCEL_FILE, sheet_name=sheet, nrows=5)
        print(f"\nSheet: {sheet}")
        for col in test_df.columns:
            print("  ", repr(col))
        df = pd.read_excel(EXCEL_FILE)
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(r"\s+", " ", regex=True)
        print("Raw rows:", len(df))
        print("Raw columns:", list(df.columns))
    #    print("\nColumns containing 'weigh':")
    #    for col in df.columns:
    #        if "weigh" in str(col).lower():
    #            print(repr(col))

    df = df.rename(columns={
        "LoadNumber": "load_number",
        "TippingStartDate": "tipping_start_date",
        "TippingEndDate": "tipping_end_date",
        "TippingStartTime": "tipping_start_time",
        "TippingEndTime": "tipping_end_time",
        "NumberofShifts": "number_of_shifts",
        "Rejected(kgs)": "rejected_kg",
        "SeedsandSkins(kgs)": "seeds_skins_kg",
        "SeriesNumber": "series_no",
        "NetWeight": "net_weight_kg",
        "NumberOfBins": "number_of_bins",
        "Year": "production_year",
   #     "Weighbridge net weight of fruit delivered (KG)": "weighbridge_net_weight_kg",
    })

    print("Renamed columns:", list(df.columns))
  #  print("\nWeighbridge column test:")
 #   print("Column exists:", "weighbridge_net_weight_kg" in df.columns)

    #if "weighbridge_net_weight_kg" in df.columns:
    #    print("Non-null count:", df["weighbridge_net_weight_kg"].notna().sum())
     #   print("First 25 values:")
     #   print(df["weighbridge_net_weight_kg"].head(25).tolist())
    required_columns = [
        "load_number",
        "tipping_start_date",
        "tipping_end_date",
        "tipping_start_time",
        "tipping_end_time",
        "number_of_shifts",
        "rejected_kg",
        "seeds_skins_kg",
        "series_no",
        "net_weight_kg",
      #  "weighbridge_net_weight_kg",
        "number_of_bins",
    ]

    missing = [c for c in required_columns if c not in df.columns]

    if missing:
        raise ValueError(
            f"Missing expected columns in Excel file: {missing}. "
            f"Available columns after rename: {list(df.columns)}"
        )

    df = df.dropna(how="all")

    df = df[
        (df["load_number"].notna()) &
        (df["load_number"] != 0)
    ]

    print("Rows after blank removal:", len(df))

    print("Max load_number:", pd.to_numeric(df["load_number"], errors="coerce").max())
    print("Sample series_no values:")
    print(df["series_no"].dropna().head(10).tolist())
    print("Max net_weight_kg:", pd.to_numeric(df["net_weight_kg"], errors="coerce").max())
    print("Max number_of_bins:", pd.to_numeric(df["number_of_bins"], errors="coerce").max())
    print("Max rejected_kg:", pd.to_numeric(df["rejected_kg"], errors="coerce").max())
    print("Max seeds_skins_kg:", pd.to_numeric(df["seeds_skins_kg"], errors="coerce").max())
    print("Max source row:", len(df) + 1)
  #  print(
   #     "Max weighbridge_net_weight_kg:",
   #     pd.to_numeric(
   #         df["weighbridge_net_weight_kg"],
   #         errors="coerce"
   #     ).max()
    #)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = False

    print("Truncating dbo.stg_bin_tipping_report...")
    cursor.execute("TRUNCATE TABLE dbo.stg_bin_tipping_report;")

    insert_sql = """
    INSERT INTO dbo.stg_bin_tipping_report (
        source_file_name,
        source_row_number,
        load_number,
        tipping_start_date,
        tipping_end_date,
        tipping_start_time,
        tipping_end_time,
        number_of_shifts,
        rejected_kg,
        seeds_skins_kg,
        series_no,
        net_weight_kg,
        number_of_bins
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    rows_to_insert = []

    for idx, row in df.iterrows():        

        rows_to_insert.append((
            SOURCE_FILE_NAME,
            int(idx) + 2,
            to_int(row["load_number"]),
            to_date(row["tipping_start_date"]),
            to_date(row["tipping_end_date"]),
            to_time(row["tipping_start_time"]),
            to_time(row["tipping_end_time"]),
            to_decimal(row["number_of_shifts"]),
            to_decimal(row["rejected_kg"]),
            to_decimal(row["seeds_skins_kg"]),
            clean_text(row["series_no"]),
            to_decimal(row["net_weight_kg"]),
        #    to_decimal(row["weighbridge_net_weight_kg"]),
            to_decimal(row["number_of_bins"]),
        ))

    print("Inserting rows:", len(rows_to_insert))

    for i, row_data in enumerate(rows_to_insert, start=1):
        if i % 100 == 0:
            print(f"Inserting row {i}")

        try:
            cursor.execute(insert_sql, row_data)

        except Exception:
            print("Failed insert at prepared row:", i)
            print("Row data:")
            print(row_data)
            raise

    conn.commit()
    cursor.close()
    conn.close()

    print("Load complete.")
    print(f"Loaded {len(rows_to_insert)} rows into dbo.stg_bin_tipping_report")


if __name__ == "__main__":
    main()