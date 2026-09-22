# =========================================
# Factory Daily Series Email Report
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

SQL_PROC = "EXEC dbo.usp_factory_series_daily_report @report_date = ?"

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
    RECIPIENTS = [
        x.strip()
        for x in RECIPIENTS_RAW.split(",")
        if x.strip()
    ]
else:
    RECIPIENTS = [
        "mrdhulley@hotmail.com"
        # ,
        # "warwick@coalzim.com",
        # "admire@zimconcentrates.com",
        # "rich@insikalodge.com",
        # "kath@zimconcentrates.com"
    ]


REQUIRED_VARS = {
    "SQL_SERVER": SQL_SERVER,
    "SQL_DATABASE": SQL_DATABASE,
    "SQL_USER": SQL_USER,
    "SQL_PASSWORD": SQL_PASSWORD,
    "EMAIL_USERNAME": EMAIL_USERNAME,
    "EMAIL_PASSWORD": EMAIL_PASSWORD,
}


MISSING_VARS = [
    key
    for key, value in REQUIRED_VARS.items()
    if not value
]

if MISSING_VARS:
    raise ValueError(
        f"Missing environment variables: {', '.join(MISSING_VARS)}"
    )


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
        return datetime.strptime(
            report_date_env,
            "%Y-%m-%d"
        ).date()

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


def dataframe_from_current_resultset(
    cursor: pyodbc.Cursor
) -> pd.DataFrame | None:

    if cursor.description is None:
        return None

    columns = [
        col[0]
        for col in cursor.description
    ]

    rows = cursor.fetchall()

    return pd.DataFrame.from_records(
        rows,
        columns=columns
    )


def fetch_all_resultsets(
    cursor: pyodbc.Cursor
) -> list[pd.DataFrame]:

    results: list[pd.DataFrame] = []

    while True:

        if cursor.description is not None:

            df = dataframe_from_current_resultset(
                cursor
            )

            if df is not None:
                results.append(df)

        if not cursor.nextset():
            break

    return results


def fetch_report_data(
    report_date: date
) -> tuple[pd.DataFrame, pd.DataFrame]:

    conn = get_connection()

    try:

        cursor = conn.cursor()

        cursor.execute(
            SQL_PROC,
            report_date
        )

        resultsets = fetch_all_resultsets(
            cursor
        )

        cursor.close()

        print(
            f"Result sets returned: {len(resultsets)}"
        )

        for i, df in enumerate(
            resultsets,
            start=1
        ):

            print(
                f"Result set {i}: "
                f"{len(df)} rows, "
                f"columns={list(df.columns)}"
            )


        df_header = (
            resultsets[0]
            if len(resultsets) > 0
            else pd.DataFrame(
                columns=[
                    "report_date",
                    "series_1",
                    "series_2",
                    "series_3",
                ]
            )
        )


        df_report = (
            resultsets[1]
            if len(resultsets) > 1
            else pd.DataFrame()
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

    if isinstance(
        value,
        (pd.Timestamp, datetime)
    ):
        return value.strftime("%d %b %Y")

    if isinstance(value, date):
        return value.strftime("%d %b %Y")

    try:
        parsed = pd.to_datetime(value)
        return parsed.strftime("%d %b %Y")

    except Exception:
        return str(value)


def format_email_intro(
    text1: str,
    text2: str
) -> str:

    parts: list[str] = []

    if text1.strip():
        parts.append(
            f"<p>{escape(text1.strip())}</p>"
        )

    if text2.strip():
        parts.append(
            f"<p>{escape(text2.strip())}</p>"
        )

    return "".join(parts)


def format_email_signoff(
    signoff: str,
    signature: str
) -> str:

    parts: list[str] = []

    if signoff.strip():

        parts.append(
            f"<p>{escape(signoff.strip())}</p>"
        )

    if signature.strip():

        parts.append(
            f"<p><strong>"
            f"{escape(signature.strip())}"
            f"</strong></p>"
        )

    return "".join(parts)


# =========================
# METRIC FORMATTING
# =========================

def classify_metric(metric: str) -> str:

    metric_lower = metric.lower().strip()

    if "%" in metric:
        return "percent"

    if "cost" in metric_lower:
        return "currency"

    if "conversion ratio" in metric_lower:
        return "decimal"

    if "brix" in metric_lower:
        return "decimal"

    return "number"


def format_number(
    value,
    style: str
) -> str:

    if value is None or pd.isna(value) or value == "":
        return "-"

    try:
        num = float(value)

    except (TypeError, ValueError):
        return str(value)

    if style == "currency":
        return f"${num:,.2f}"

    if style == "percent":
        return f"{num:,.2f}%"

    if style == "decimal":
        return f"{num:,.2f}"

    return f"{num:,.2f}"


# =========================
# CLEAN REPORT DATA
# =========================

def clean_report_df(
    df_report: pd.DataFrame
) -> pd.DataFrame:

    if df_report is None or df_report.empty:
        return pd.DataFrame()

    df = df_report.copy()

    for col in [
        "section",
        "metric",
    ]:

        if col in df.columns:

            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
            )


    if {
        "section",
        "metric"
    }.issubset(df.columns):

        df = df[
            ~(
                (
                    df["section"]
                    .str.strip()
                    == ""
                )
                &
                (
                    df["metric"]
                    .str.strip()
                    == ""
                )
            )
        ].copy()


    if "sort_order" in df.columns:

        df = df.sort_values(
            "sort_order"
        )


    return df.reset_index(
        drop=True
    )


# =========================
# SECTION CLASS
# =========================

def section_class(
    section: str
) -> str:

    section_lower = (
        section
        .strip()
        .lower()
    )

    if section_lower == "key factory kpis":
        return "kpi"

    if section_lower == "processing":
        return "processing"

    if section_lower == "paste production":
        return "paste"

    if section_lower == "costs":
        return "costs"

    return "default"


# =========================
# REPORT TABLE
# =========================

def build_report_table_html(
    df_report: pd.DataFrame
) -> str:

    if df_report is None or df_report.empty:

        return (
            '<p class="no-data">'
            'No report data available.'
            '</p>'
        )


    value_cols = [
        col
        for col in df_report.columns
        if col not in [
            "sort_order",
            "section",
            "metric",
        ]
    ]


    if not value_cols:

        return (
            '<p class="no-data">'
            'No report data available.'
            '</p>'
        )


    colspan = len(value_cols) + 1


    # ---------------------------------------------
    # Column headings
    # ---------------------------------------------

    header_cells = [
        '<th class="metric-header">Metric</th>'
    ]


    for col in value_cols:

        heading = str(col)

        if heading == "Previous Series":
            heading = "Previous Series"

        header_cells.append(
            f"<th>{escape(heading)}</th>"
        )


    # ---------------------------------------------
    # Rows
    # ---------------------------------------------

    rows_html: list[str] = []

    current_section = None


    for _, row in df_report.iterrows():

        section = str(
            row.get(
                "section",
                ""
            ) or ""
        ).strip()


        metric = str(
            row.get(
                "metric",
                ""
            ) or ""
        ).strip()


        css_section = section_class(
            section
        )


        # -----------------------------------------
        # Section heading
        # -----------------------------------------

        if (
            section
            and section != current_section
        ):

            rows_html.append(
                f"""
                <tr class="section-row section-{css_section}">
                    <td colspan="{colspan}">
                        {escape(section.upper())}
                    </td>
                </tr>
                """
            )

            current_section = section


        # -----------------------------------------
        # Values
        # -----------------------------------------

        style = classify_metric(
            metric
        )

        value_cells: list[str] = []


        for col in value_cols:

            raw_value = row.get(col)

            formatted_value = (
                format_number(
                    raw_value,
                    style
                )
            )

            value_cells.append(
                f"""
                <td class="value-cell">
                    {escape(formatted_value)}
                </td>
                """
            )


        rows_html.append(
            f"""
            <tr class="metric-row section-{css_section}">

                <td class="metric-cell">
                    {escape(metric)}
                </td>

                {''.join(value_cells)}

            </tr>
            """
        )


    return f"""
    <table
        class="report-table"
        cellpadding="0"
        cellspacing="0"
    >

        <thead>

            <tr>
                {''.join(header_cells)}
            </tr>

        </thead>


        <tbody>

            {''.join(rows_html)}

        </tbody>

    </table>
    """


# =========================
# HTML REPORT
# =========================

def build_html(
    header: dict,
    df_report: pd.DataFrame
) -> str:

    report_date = header.get(
        "report_date",
        ""
    )


    email_intro_html = (
        format_email_intro(
            EMAIL_INTRO,
            EMAIL_INTRO_2
        )
    )


    email_signoff_html = (
        format_email_signoff(
            EMAIL_SIGNOFF,
            EMAIL_SIGNATURE
        )
    )


    report_table_html = (
        build_report_table_html(
            df_report
        )
    )


    return f"""
    <!DOCTYPE html>

    <html>

    <head>

        <meta charset="utf-8">

        <meta
            name="viewport"
            content="width=device-width"
        >


        <style>

            body {{
                margin: 0;
                padding: 20px;
                background-color: #ffffff;
                color: #111111;
                font-family:
                    Arial,
                    Helvetica,
                    sans-serif;
            }}


            .container {{
                width: 100%;
                max-width: 1080px;
                margin: 0 auto;
            }}


            /* =========================
               EMAIL INTRO
               ========================= */

            .message-block {{
                font-size: 14px;
                line-height: 1.5;
                margin-bottom: 12px;
            }}


            .message-block p {{
                margin: 0 0 8px 0;
            }}


            /* =========================
               TITLE
               ========================= */

            .title-table {{
                width: 100%;
                border-collapse: collapse;
                background-color: #174f78;
            }}


            .title-table td {{
                border: 0;
                color: white;
            }}


            .title-left {{
                padding:
                    7px 10px
                    3px 10px;
                font-size: 21px;
                font-weight: bold;
                text-align: left;
            }}


            .title-right {{
                padding:
                    7px 10px
                    3px 10px;
                font-size: 16px;
                font-weight: bold;
                text-align: right;
                white-space: nowrap;
            }}


            .subtitle {{
                background-color: #174f78;
                color: #ffffff;
                padding:
                    1px 10px
                    8px 10px;
                font-size: 16px;
            }}


            /* =========================
               REPORT TABLE
               ========================= */

            table.report-table {{
                width: 100%;
                border-collapse: collapse;
                table-layout: fixed;
                font-size: 14px;
            }}


            table.report-table th {{
                background-color: #174f78;
                color: #ffffff;
                border:
                    1px solid #667f91;
                padding: 5px 8px;
                font-size: 15px;
                font-weight: bold;
                text-align: center;
            }}


            table.report-table th.metric-header {{
                text-align: left;
                width: 44%;
            }}


            table.report-table th:nth-child(2),
            table.report-table th:nth-child(3),
            table.report-table th:nth-child(4) {{
                width: 9%;
            }}


            table.report-table th:nth-child(5) {{
                width: 15%;
            }}


            table.report-table th:nth-child(6) {{
                width: 11%;
            }}


            table.report-table td {{
                border:
                    1px solid #7d8b91;
                padding: 4px 8px;
                vertical-align: middle;
            }}


            .metric-cell {{
                text-align: left;
            }}


            .value-cell {{
                text-align: center;
                white-space: nowrap;
            }}


            /* =========================
               SECTION HEADINGS
               ========================= */

            .section-row td {{
                font-size: 15px;
                font-weight: bold;
                text-align: left;
                padding: 5px 8px;
            }}


            /* =========================
               KPI SECTION
               ========================= */

            .section-kpi td {{
                background-color: #e5f0df;
            }}


            /* =========================
               PROCESSING
               ========================= */

            .section-processing td {{
                background-color: #dce7f5;
            }}


            /* =========================
               PASTE PRODUCTION
               ========================= */

            .section-paste td {{
                background-color: #fff600;
            }}


            /* =========================
               COSTS
               ========================= */

            .section-costs td {{
                background-color: #ffc000;
            }}


            .section-default td {{
                background-color: #f2f2f2;
            }}


            /* =========================
               SIGNOFF
               ========================= */

            .signoff-block {{
                margin-top: 16px;
                font-size: 14px;
                line-height: 1.5;
            }}


            .signoff-block p {{
                margin: 4px 0;
            }}


            .footer {{
                margin-top: 16px;
                border-top:
                    1px solid #cccccc;
                padding-top: 8px;
                font-size: 11px;
                color: #777777;
            }}


            .no-data {{
                color: #777777;
                font-style: italic;
            }}

        </style>

    </head>


    <body>

        <div class="container">


            <div class="message-block">
                {email_intro_html}
            </div>


            <table class="title-table">

                <tr>

                    <td class="title-left">
                        Daily Factory Report
                    </td>

                    <td class="title-right">
                        Report Date:
                        {escape(report_date)}
                    </td>

                </tr>

            </table>


            <div class="subtitle">
                Series Based Reporting
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

def send_email(
    subject: str,
    html: str
) -> None:

    msg = MIMEMultipart(
        "alternative"
    )

    msg["Subject"] = subject

    msg["From"] = (
        f"{FROM_NAME} <{FROM_EMAIL}>"
    )

    msg["To"] = ", ".join(
        RECIPIENTS
    )

    msg.attach(
        MIMEText(
            html,
            "html",
            "utf-8"
        )
    )


    with smtplib.SMTP(
        SMTP_SERVER,
        SMTP_PORT
    ) as server:

        server.ehlo()

        server.starttls()

        server.ehlo()

        server.login(
            EMAIL_USERNAME,
            EMAIL_PASSWORD
        )

        server.sendmail(
            FROM_EMAIL,
            RECIPIENTS,
            msg.as_string()
        )


# =========================
# MAIN
# =========================

def main() -> None:

    report_date = get_report_date()

    print(
        f"Running factory series report "
        f"for {report_date}"
    )


    # ---------------------------------------------
    # Fetch SQL report
    # ---------------------------------------------

    df_header, df_report = (
        fetch_report_data(
            report_date
        )
    )


    df_report = clean_report_df(
        df_report
    )


    # ---------------------------------------------
    # Header / Series
    # ---------------------------------------------

    if df_header.empty:

        header = {
            "report_date":
                report_date.strftime(
                    "%d %b %Y"
                ),
            "series_1": "",
            "series_2": "",
            "series_3": "",
        }

        series = [
            "Series 1",
            "Series 2",
            "Series 3",
        ]


    else:

        header_row = (
            df_header.iloc[0]
        )


        raw_series = [
            header_row.get("series_1"),
            header_row.get("series_2"),
            header_row.get("series_3"),
        ]


        header = {

            "report_date":
                format_date_value(
                    header_row.get(
                        "report_date"
                    )
                ),

            "series_1":
                raw_series[0],

            "series_2":
                raw_series[1],

            "series_3":
                raw_series[2],
        }


        # -----------------------------------------
        # Convert:
        # 26-032 -> Series 32
        # 26-031 -> Series 31
        # 26-030 -> Series 30
        # -----------------------------------------

        series = []

        for item in raw_series:

            if item is None or pd.isna(item):

                series.append("Series")

                continue


            item_text = str(item)


            if "-" in item_text:

                suffix = (
                    item_text
                    .split("-")[-1]
                )

                try:
                    suffix = str(
                        int(suffix)
                    )
                except ValueError:
                    pass

                series.append(
                    f"Series {suffix}"
                )

            else:

                series.append(
                    f"Series {item_text}"
                )


    # ---------------------------------------------
    # Rename SQL result columns
    # ---------------------------------------------

    if len(series) >= 3:

        df_report = (
            df_report.rename(
                columns={
                    "Series_1": series[0],
                    "Series_2": series[1],
                    "Series_3": series[2],
                    "Previous_Series":
                        "Previous Series",
                }
            )
        )


    print(
        f"Report columns: "
        f"{series[0]}, "
        f"{series[1]}, "
        f"{series[2]}, "
        f"Previous Series, "
        f"YTD"
    )


    # ---------------------------------------------
    # Email
    # ---------------------------------------------

    subject = (
        f"{EMAIL_SUBJECT_PREFIX}"
        f"Factory Daily Report - "
        f"{header['report_date']}"
    )


    html = build_html(
        header,
        df_report
    )


    send_email(
        subject,
        html
    )


    print(
        "✅ Email sent successfully!"
    )


if __name__ == "__main__":
    main()