# =========================================
# Daily Tomato Intake Email Report
# =========================================

import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE") or os.getenv("SQL_DB")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

SMTP_SERVER = "smtp-relay.brevo.com"
SMTP_PORT = 587
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME") or os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

print("SQL_SERVER:", SQL_SERVER)
print("SQL_DATABASE:", SQL_DATABASE)
print("SQL_USER:", SQL_USER)
print("EMAIL_USERNAME:", EMAIL_USERNAME)
print("EMAIL_PASSWORD loaded:", EMAIL_PASSWORD is not None)
print("EMAIL_PASSWORD length:", len(EMAIL_PASSWORD) if EMAIL_PASSWORD else None)

RECIPIENTS = [
    "mrdhulley@hotmail.com","mrdhulley@gmail.com"
]

BASE_DIR = os.path.dirname(__file__)
sql_path = os.path.join(BASE_DIR, "daily_report.sql")

with open(sql_path, "r") as f:
    SQL_QUERY = f.read()

# =========================
# SQL QUERY
# =========================

# =========================
# CONNECT TO SQL
# =========================
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};"
    "Encrypt=no;"
)

cursor = conn.cursor()
cursor.execute(SQL_QUERY)

# =========================
# FETCH MULTIPLE RESULT SETS
# =========================
def fetch_df(cursor):
    if cursor.description is None:
        return None
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame.from_records(rows, columns=columns)

def next_dataframe(cursor):
    while True:
        df = fetch_df(cursor)
        if df is not None:
            return df
        if not cursor.nextset():
            return None

df_summary = next_dataframe(cursor)
df_quality = next_dataframe(cursor)
df_ex_counts = next_dataframe(cursor)
df_ex_detail = next_dataframe(cursor)


# =========================
# FORMAT HTML
# =========================
def df_to_html(df):
    return df.to_html(index=False, border=0, justify="center")

report_date = df_summary['report_date'][0]

html = f"""
<html>
<head>
<style>
    body {{ font-family: Arial; }}
    h2 {{ color: #2E86C1; }}
    table {{ border-collapse: collapse; width: 80%; margin-bottom: 20px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: center; }}
    th {{ background-color: #2E86C1; color: white; }}
</style>
</head>
<body>

<h2>🍅 Daily Tomato Intake Report - {report_date}</h2>

<h3>1. Daily Summary</h3>
{df_to_html(df_summary)}

<h3>2. Quality Breakdown</h3>
{df_to_html(df_quality)}

<h3>3. Exceptions Summary</h3>
{df_to_html(df_ex_counts)}

<h3>4. Exception Details</h3>
{df_to_html(df_ex_detail)}

</body>
</html>
"""

# =========================
# SEND EMAIL
# =========================
msg = MIMEMultipart("alternative")
msg["Subject"] = f"Daily Tomato Intake Report - {report_date}"
msg["From"] = "Knightshade Reports <mrdhulley@hotmail.com>"
msg["To"] = ", ".join(RECIPIENTS)

msg.attach(MIMEText(html, "html"))

with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
    server.sendmail(EMAIL_USERNAME, RECIPIENTS, msg.as_string())

print("✅ Email sent successfully!")