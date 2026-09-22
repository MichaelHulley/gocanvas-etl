import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv

load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE") or os.getenv("SQL_DB")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

OUTPUT_FILE = r"C:\KnightshadeData\Exports\KnightshadeSQLExtract.xlsx"

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASSWORD};"
    "TrustServerCertificate=Yes;"
)

query = """
SELECT
    drum_date,
    filling_time,
    drum_no,
    raw_drum_number,
    brix,
    quality_parameter,
    product_group,
    product_description,
    quality_key,
    quality_key_description,
    ph,
    series,
    drum_status,
    shift,
    Season,
    drum_report_category,
    include_in_good_quality,
    include_in_saleable,
    include_in_non_conforming
FROM dbo.vw_drum_report
ORDER BY drum_date, filling_time, drum_no;
"""

with pyodbc.connect(conn_str) as conn:
    df = pd.read_sql(query, conn)

print("Columns exported:")
print(list(df.columns))

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="DrumReport", index=False)

print(f"Exported {len(df)} rows to {OUTPUT_FILE}")