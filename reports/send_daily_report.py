# =========================================
# Factory Daily Management Email Report
# =========================================

import os
import smtplib
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape

import pandas as pd
import pyodbc
from dotenv import load_dotenv

load_dotenv()

# =========================
# CONFIG
# =========================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE") or os.getenv("SQL_DB")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

ODBC_DRIVER = os.getenv("ODBC_DRIVER") or "ODBC Driver 18 for SQL Server"
SQL_PROC = "EXEC dbo.usp_factory_manager_report @report_date = ?"

SMTP_SERVER = os.getenv("SMTP_SERVER") or "smtp-relay.brevo.com"
SMTP_PORT = int(os.getenv("SMTP_PORT") or 587)
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME") or os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

FROM_EMAIL = os.getenv("FROM_EMAIL") or "mrdhulley@hotmail.com"
FROM_NAME = os.getenv("FROM_NAME") or "Knightshade Reports"

EMAIL_SUBJECT_PREFIX = os.getenv("EMAIL_SUBJECT_PREFIX") or ""
EMAIL_INTRO = os.getenv("EMAIL_INTRO") or ""
EMAIL_INTRO_2 = os.getenv("EMAIL_INTRO_2") or ""
EMAIL_SIGNOFF = os.getenv("EMAIL_SIGNOFF") or ""
EMAIL_SIGNATURE = os.getenv("EMAIL_SIGNATURE") or ""

RECIPIENTS_RAW = os.getenv("REPORT_RECIPIENTS")
if RECIPIENTS_RAW:
    RECIPIENTS = [x.strip() for x in RECIPIENTS_RAW.split(",") if x.strip()]
else:
    RECIPIENTS = [
        "mrdhulley@hotmail.com",
        "paul@zimconcentrates.com",
        "warrick@coalzim.com",
        "rich@insikalodge.com"
    ]

REQUIRED_VARS = {
    "SQL_SERVER": SQL_SERVER,
    "SQL_DATABASE": SQL_DATABASE,
    "SQL_USER": SQL_USER,
    "SQL_PASSWORD": SQL_PASSWORD,
    "EMAIL_USERNAME": EMAIL_USERNAME,
    "EMAIL_PASSWORD": EMAIL_PASSWORD,
}

MISSING_VARS = [k for k, v in REQUIRED_VARS.items() if not v]
if MISSING_VARS:
    raise ValueError(f"Missing environment variables: {', '.join(MISSING_VARS)}")


# =========================
# REPORT DATE
# =========================
def get_report_date() -> date:
    """
    Priority:
    1. REPORT_DATE env var in YYYY-MM-DD format
    2. yesterday
    """
    report_date_env = os.getenv("REPORT_DATE")
    if report_date_env:
        return datetime.strptime(report_date_env, "%Y-%m-%d").date()

    return date.today() - timedelta(days=1)


# =========================
# DATABASE HELPERS
# =========================
def get_connection() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def dataframe_from_current_resultset(cursor: pyodbc.Cursor) -> pd.DataFrame | None:
    if cursor.description is None:
        return None

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame.from_records(rows, columns=columns)


def fetch_all_resultsets(cursor: pyodbc.Cursor) -> list[pd.DataFrame]:
    """
    Read all tabular result sets in order.
    """
    results: list[pd.DataFrame] = []

    while True:
        if cursor.description is not None:
            df = dataframe_from_current_resultset(cursor)
            if df is not None:
                results.append(df)

        if not cursor.nextset():
            break

    return results


def fetch_report_data(report_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    conn = get_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(SQL_PROC, report_date)

        resultsets = fetch_all_resultsets(cursor)
        cursor.close()

        print(f"Result sets returned: {len(resultsets)}")
        for i, df in enumerate(resultsets, start=1):
            print(f"Result set {i}: {len(df)} rows, columns={list(df.columns)}")

        df_header = (
            resultsets[0]
            if len(resultsets) > 0
            else pd.DataFrame(columns=["report_date", "week_start_date", "month_start_date", "year_start_date"])
        )

        df_report = (
            resultsets[1]
            if len(resultsets) > 1
            else pd.DataFrame(columns=["section", "metric", "Day", "WTD", "MTD", "YTD"])
        )

        return df_header, df_report

    finally:
        conn.close()


# =========================
# DISPLAY HELPERS
# =========================
def format_date_value(value) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value)


def format_email_intro(text1: str, text2: str) -> str:
    parts: list[str] = []

    if text1.strip():
        parts.append(f"<p>{escape(text1.strip())}</p>")

    if text2.strip():
        parts.append(f"<p>{escape(text2.strip())}</p>")

    return "".join(parts)


def format_email_signoff(signoff: str, signature: str) -> str:
    parts: list[str] = []

    if signoff.strip():
        parts.append(f"<p>{escape(signoff.strip())}</p>")

    if signature.strip():
        parts.append(f"<p><strong>{escape(signature.strip())}</strong></p>")

    return "".join(parts)


def classify_metric(metric: str) -> str:
    metric_lower = metric.lower()

    if "brix" in metric_lower:
        return "decimal_2"
    if "% good" in metric_lower:
        return "decimal_2"
    if "ratio" in metric_lower:
        return "decimal_2"
    if "productivity" in metric_lower:
        return "decimal_2"
    if "value" in metric_lower:
        return "currency_0"
    if "weight" in metric_lower:
        return "number_2"
    if "losses" in metric_lower:
        return "number_2"
    if "processed" in metric_lower:
        return "number_2"
    if "produced" in metric_lower:
        return "number_2"
    if "drums" in metric_lower:
        return "number_2"
    if "bins" in metric_lower:
        return "number_2"
    if "downtime" in metric_lower:
        return "number_2"
    if "efficiency" in metric_lower:
        return "decimal_2"
    if "throughput" in metric_lower:
        return "number_2"

    return "number_2"


def format_number(value, style: str) -> str:
    if pd.isna(value) or value is None or value == "":
        return ""

    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)

    if style == "currency_0":
        return f"${num:,.0f}"
    if style == "decimal_2":
        return f"{num:,.2f}"
    if style == "number_2":
        return f"{num:,.2f}"

    return f"{num:,.2f}"


def metric_alert_class(metric: str, value) -> str:
    if pd.isna(value) or value == "":
        return ""

    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""

    metric_lower = metric.lower()

    if metric_lower == "% good" and num < 90:
        return "alert-red"

    if metric_lower == "conversion ratio" and num < 0.20:
        return "alert-amber"

    if metric_lower == "broken bins" and num > 3:
        return "alert-amber"

    if metric_lower == "total losses" and num > 5000:
        return "alert-red"

    return ""


def clean_report_df(df_report: pd.DataFrame) -> pd.DataFrame:
    if df_report.empty:
        return df_report

    df = df_report.copy()

    for col in ["section", "metric"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    if {"section", "metric"}.issubset(df.columns):
        df = df[~((df["section"].str.strip() == "") & (df["metric"].str.strip() == ""))].copy()

    return df.reset_index(drop=True)


def build_report_table_html(df_report: pd.DataFrame) -> str:
    if df_report.empty:
        return '<p class="no-data">No report data available.</p>'

    rows_html: list[str] = []
    current_section = None
    value_cols = ["Day", "WTD", "MTD", "YTD"]

    for _, row in df_report.iterrows():
        section = str(row.get("section", "") or "")
        metric = str(row.get("metric", "") or "")

        if section and section != current_section:
            rows_html.append(
                f"""
                <tr class="section-row">
                    <td colspan="5">{escape(section)}</td>
                </tr>
                """
            )
            current_section = section

        style = classify_metric(metric)

        value_cells = []
        for col in value_cols:
            raw_value = row.get(col)
            formatted_value = format_number(raw_value, style)
            alert_class = metric_alert_class(metric, raw_value)
            value_cells.append(
                f'<td class="{alert_class}">{escape(formatted_value)}</td>'
            )

        rows_html.append(
            f"""
            <tr>
                <td class="metric-cell">{escape(metric)}</td>
                {''.join(value_cells)}
            </tr>
            """
        )

    return f"""
    <table class="report-table">
        <thead>
            <tr>
                <th>Metric</th>
                <th>Day</th>
                <th>WTD</th>
                <th>MTD</th>
                <th>YTD</th>
            </tr>
        </thead>
        <tbody>
            {''.join(rows_html)}
        </tbody>
    </table>
    """


def build_html(header: dict, df_report: pd.DataFrame) -> str:
    report_date = header.get("report_date", "")
    week_start = header.get("week_start_date", "")
    month_start = header.get("month_start_date", "")
    year_start = header.get("year_start_date", "")

    email_intro_html = format_email_intro(EMAIL_INTRO, EMAIL_INTRO_2)
    email_signoff_html = format_email_signoff(EMAIL_SIGNOFF, EMAIL_SIGNATURE)
    report_table_html = build_report_table_html(df_report)

    return f"""
    <html>
    <head>
    <meta charset="utf-8">
    <style>
        body {{
            font-family: Arial, Helvetica, sans-serif;
            background-color: #f4f6f8;
            color: #2b2b2b;
            margin: 0;
            padding: 24px;
        }}
        .container {{
            max-width: 1220px;
            margin: 0 auto;
            background: #ffffff;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        }}
        h2 {{
            color: #2E86C1;
            margin-top: 0;
            margin-bottom: 10px;
            font-size: 28px;
            font-weight: 700;
        }}
        .subheader {{
            margin-bottom: 24px;
            color: #555;
            font-size: 13px;
            line-height: 1.8;
            border-bottom: 1px solid #e1e6ea;
            padding-bottom: 14px;
        }}
        .message-block {{
            margin-bottom: 20px;
            color: #444;
            font-size: 14px;
            line-height: 1.6;
        }}
        .message-block p {{
            margin: 0 0 10px 0;
        }}
        table.report-table {{
            border-collapse: collapse;
            width: 100%;
            font-size: 14px;
        }}
        table.report-table th,
        table.report-table td {{
            border: 1px solid #d9dee3;
            padding: 10px 10px;
            text-align: right;
        }}
        table.report-table th:first-child,
        table.report-table td:first-child {{
            text-align: left;
        }}
        table.report-table th {{
            background-color: #2E86C1;
            color: white;
            font-weight: bold;
            font-size: 15px;
        }}
        table.report-table tr:nth-child(even) td {{
            background-color: #f9fbfc;
        }}
        .section-row td {{
            background-color: #d6eaf8 !important;
            font-weight: bold;
            color: #1f4e79;
            text-align: left !important;
            font-size: 15px;
            padding-top: 12px;
            padding-bottom: 12px;
        }}
        .metric-cell {{
            white-space: nowrap;
        }}
        .alert-red {{
            background-color: #fdecea !important;
            color: #b71c1c;
            font-weight: bold;
        }}
        .alert-amber {{
            background-color: #fff8e1 !important;
            color: #8a6d1d;
            font-weight: bold;
        }}
        .signoff-block {{
            margin-top: 24px;
            font-size: 14px;
            color: #333;
        }}
        .signoff-block p {{
            margin: 4px 0;
        }}
        .no-data {{
            color: #777;
            font-style: italic;
            margin: 6px 0 0 0;
        }}
        .footer {{
            margin-top: 28px;
            font-size: 12px;
            color: #777;
            border-top: 1px solid #e1e6ea;
            padding-top: 12px;
        }}
    </style>
    </head>
    <body>
        <div class="container">
            <h2>🏭 Factory Daily Report</h2>

            <div class="subheader">
                <strong>Report Date:</strong> {escape(report_date)}<br>
                <strong>Week Start:</strong> {escape(week_start)}
                &nbsp;&nbsp;|&nbsp;&nbsp;
                <strong>Month Start:</strong> {escape(month_start)}
                &nbsp;&nbsp;|&nbsp;&nbsp;
                <strong>Year Start:</strong> {escape(year_start)}
            </div>

            <div class="message-block">
                {email_intro_html}
            </div>

            {report_table_html}

            <div class="signoff-block">
                {email_signoff_html}
            </div>

            <div class="footer">
                Generated automatically by Knightshade reporting.
            </div>
        </div>
    </body>
    </html>
    """


# =========================
# EMAIL
# =========================
def send_email(subject: str, html: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.sendmail(FROM_EMAIL, RECIPIENTS, msg.as_string())


# =========================
# MAIN
# =========================
def main() -> None:
    report_date = get_report_date()
    print(f"Running factory report for {report_date}")

    df_header, df_report = fetch_report_data(report_date)
    df_report = clean_report_df(df_report)

    if df_header.empty:
        header = {
            "report_date": report_date.strftime("%Y-%m-%d"),
            "week_start_date": "",
            "month_start_date": "",
            "year_start_date": "",
        }
    else:
        header_row = df_header.iloc[0]
        header = {
            "report_date": format_date_value(header_row.get("report_date")),
            "week_start_date": format_date_value(header_row.get("week_start_date")),
            "month_start_date": format_date_value(header_row.get("month_start_date")),
            "year_start_date": format_date_value(header_row.get("year_start_date")),
        }

    subject = f"{EMAIL_SUBJECT_PREFIX}🏭 Factory Daily Report - {header['report_date']}"
    html = build_html(header, df_report)

    send_email(subject, html)
    print("✅ Email sent successfully!")


if __name__ == "__main__":
    main()