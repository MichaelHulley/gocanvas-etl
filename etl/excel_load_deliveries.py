# =========================================
# Script: excel_load_deliveries.py
# Author: Michael Hulley
# Project: Knightshade Factory Reporting
#
# Purpose:
#   Load Deliveries.xlsx into dbo.stg_deliveries
#
# Source:
#   C:\KnightshadeData\Factory Reporting\Deliveries.xlsx
#
# Target:
#   dbo.stg_deliveries
#
# =========================================

import os
from pathlib import Path
from datetime import datetime, time, timedelta

import pandas as pd
import pyodbc
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
    "Deliveries.xlsx"
)

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE") or os.getenv("SQL_DB")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

ODBC_DRIVER = os.getenv(
    "ODBC_DRIVER",
    "ODBC Driver 18 for SQL Server"
)

SOURCE_FILE_NAME = "Deliveries.xlsx"


# =========================
# HELPERS
# =========================

def clean_text(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    # Excel / GoCanvas often returns numeric zero
    # for otherwise empty text fields.
    if value in ("nan", "None"):
        return None

    return value


def to_decimal(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    # Some Excel values have been entered like:
    # '-278.00
    value = value.replace("'", "")
    value = value.replace(",", "")

    parsed = pd.to_numeric(value, errors="coerce")

    if pd.isna(parsed):
        return None

    return float(parsed)


def to_int(value):
    decimal_value = to_decimal(value)

    if decimal_value is None:
        return None

    return int(decimal_value)


def to_date(value):
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.date()

    if isinstance(value, datetime):
        return value.date()

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
        return value.replace(microsecond=0)

    if isinstance(value, datetime):
        return value.time().replace(microsecond=0)

    if isinstance(value, pd.Timestamp):
        return value.time().replace(microsecond=0)

    value = str(value).strip()

    if value == "":
        return None

    parsed = pd.to_datetime(
        value,
        errors="coerce"
    )

    if pd.isna(parsed):
        return None

    return parsed.time().replace(microsecond=0)


def get_week_start(value):
    delivery_date = to_date(value)

    if delivery_date is None:
        return None

    # Monday = 0
    return delivery_date - timedelta(
        days=delivery_date.weekday()
    )


def get_week_end(value):
    week_start = get_week_start(value)

    if week_start is None:
        return None

    return week_start + timedelta(days=6)


def determine_intake_weight(row):
    """
    Determines the weight selected by the factory
    for intake reporting.

    If no valid weight source has been selected,
    fall back to weighbridge net weight.
    """

    source = clean_text(
        row.get("weight_source_for_intake")
    )

    weighbridge_weight = to_decimal(
        row.get("weighbridge_net_weight_kg")
    )

    check_weight = to_decimal(
        row.get("check_weight_net_product_kg")
    )

    if source:
        source_lower = source.lower()

        if "weighbridge" in source_lower:
            return weighbridge_weight

        if "check" in source_lower:
            return check_weight

    # Fallback where the factory has not selected
    # a valid weight source.
    return weighbridge_weight


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

    print("=" * 60)
    print("Loading Deliveries.xlsx")
    print("=" * 60)

    print("Excel file:")
    print(EXCEL_FILE)

    if not os.path.exists(EXCEL_FILE):
        raise FileNotFoundError(
            f"Excel file not found: {EXCEL_FILE}"
        )

    # =========================
    # READ EXCEL
    # =========================

    df = pd.read_excel(EXCEL_FILE)

    # Remove leading/trailing and repeated spaces
    # from column headings.
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(
        r"\s+",
        " ",
        regex=True
    )

    print("Raw rows:", len(df))
    print("Raw columns:", len(df.columns))

    # =========================
    # COLUMN MAPPING
    # =========================

    df = df.rename(columns={

        "Submission Form Version":
            "submission_form_version",

        "Load number":
            "load_number",

        "Date":
            "delivery_date",

        "Time":
            "delivery_time",

        "Grower name":
            "grower_name",

        "Farm name":
            "farm_name",

        "Total number of wooden bins delivered full":
            "total_bins_delivered_full",

        "Bin weight delivered":
            "bin_weight_delivered_kg",

        "Total number of empty bins returned":
            "total_empty_bins_returned",

        "Bin weight returned":
            "bin_weight_returned_kg",

        "Bin weight balance to add or subtract from the load":
            "bin_weight_balance_kg",

        "Driver name":
            "driver_name",

        "Driver ID Number":
            "driver_id_number",

        "Truck registration number":
            "truck_registration_number",

        "Trailer registration number 1":
            "trailer_registration_number_1",

        "Trailer registration number 2":
            "trailer_registration_number_2",

        "Field number":
            "field_number",

        "Any Chemicals Applied That May Affect Product":
            "chemicals_applied_may_affect_product",

        "Names of last applied pesticide/herbicide, fungicide applied":
            "last_pesticide_herbicide_fungicide",

        "Active ingredients of chemical product/s applied":
            "chemical_active_ingredients",

        "Date of application":
            "chemical_application_date",

        "Pre-harvest interval from application":
            "pre_harvest_interval",

        "Is fruit safe to process (completed required pre-harvest delay)":
            "fruit_safe_to_process",

        "Weighbridge weight in, full load (KG)":
            "weighbridge_weight_in_kg",

        "Weighbridge weight out, Empty with bins (KG)":
            "weighbridge_weight_out_kg",

        "Weighbridge net weight of fruit delivered (KG)":
            "weighbridge_net_weight_kg",

        "Check weight done on platform scale":
            "check_weight_done",

        "Use weighbridge weight or check weight for intake":
            "weight_source_for_intake",

        "Check weight of full bins on small scale":
            "check_weight_full_bins_kg",

        "Check weight net product before grading (KG)":
            "check_weight_net_product_kg",

        "Kg Difference between check weight and weigh bridge weight":
            "check_vs_weighbridge_difference_kg",

        "Rotten, Insect damage or physical damage":
            "rotten_damage_assessment",

        "Rotten/Damaged weight (Kg)":
            "rotten_damaged_weight_kg",

        "% of Load Rotten/Damaged fruit":
            "rotten_damaged_pct",

        "Green fruit not processable":
            "green_not_processable_1",

        "Green fruit not processable.1":
            "green_not_processable_2",

        "% of load green fruit not processable":
            "green_not_processable_pct",

        "Good quality acceptable for processing":
            "good_quality_assessment",

        "Good quality acceptable fruit weight (KG) of sample":
            "good_quality_sample_weight_kg",

        "% of Load good quality acceptable fruit":
            "good_quality_pct",

        "pH of good quality Tomatoes":
            "good_quality_ph",

        "Fruit brix of good quality fruit":
            "good_quality_brix",

        "USD Good quality fruit value in US$/Kg at measured brix":
            "good_quality_value_per_kg_usd",

        "Sunburnt or Orange fruit slightly sub-standard":
            "sunburnt_orange_assessment",

        "pH of Sunburnt or orange Tomatoes":
            "sunburnt_orange_ph",

        "Sunburnt or Orange fruit slightly sub-standardweight (KG)":
            "sunburnt_orange_sample_weight_kg",

        "% of Load Sunburnt or Orange, slightly sub-standard fruit":
            "sunburnt_orange_pct",

        "Fruit brix of slightly sub-standard fruit":
            "sunburnt_orange_brix",

        "USD Slightly sub-standard fruit value in US$/Kg at measured brix":
            "sunburnt_orange_value_per_kg_usd",

        "Comments":
            "comments",

        "USD $ value of good quality acceptable fruit US$":
            "good_quality_value_usd",

        "USD $ Delivered value of orange, sunburnt or slightly sub-standard fruit":
            "sunburnt_orange_value_usd",

        "USD $ Total value":
            "total_value_usd",

        "($) Price Per KG":
            "price_per_kg_usd",

        "USD $ value of acceptable fruit":
            "acceptable_fruit_value_usd_2",

        "USD $ value of orange, sunburnt or slightly sub-standard fruit":
            "sunburnt_orange_value_usd_2",

        "USD $ Total value .1":
            "total_value_usd_2",

        "($) Price Per KG.1":
            "price_per_kg_usd_2",

        "Name of grader/evaluator performing the inspection":
            "grader_name",

        "Comments on load and transporter":
            "transporter_comments",

        "Management name:":
            "management_name",
    })

    print("Columns after rename:", len(df.columns))

    # =========================
    # REQUIRED COLUMNS
    # =========================

    required_columns = [
        "load_number",
        "delivery_date",
        "grower_name",
        "farm_name",
        "weighbridge_net_weight_kg",
        "weight_source_for_intake",
    ]

    missing = [
        col
        for col in required_columns
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required Deliveries columns: {missing}"
        )

    # =========================
    # REMOVE BLANK ROWS
    # =========================

    df = df.dropna(how="all")

    # Convert load number to numeric.
    # The factory workbook contains unused/template rows
    # populated with Load number = 0.
    df["load_number"] = pd.to_numeric(
        df["load_number"],
        errors="coerce"
    )

    # Keep only genuine delivery records.
    df = df[
        df["load_number"].notna()
        & (df["load_number"] > 0)
    ].copy()

    print(
        "Rows after removing blank/template rows:",
        len(df)
    )

    # =========================
    # DUPLICATE LOAD CONTROL
    # =========================

    df["duplicate_load_count"] = (
        df.groupby("load_number")["load_number"]
        .transform("count")
    )

    df["is_duplicate_load"] = (
        df["duplicate_load_count"] > 1
    )

    duplicate_df = (
        df[df["is_duplicate_load"]]
        [
            [
                "load_number",
                "delivery_date",
                "grower_name",
                "farm_name",
                "weighbridge_net_weight_kg",
                "duplicate_load_count",
            ]
        ]
        .sort_values(
            ["load_number", "delivery_date"]
        )
    )

    if not duplicate_df.empty:
        print()
        print("WARNING - DUPLICATE LOAD NUMBERS FOUND")
        print("-" * 60)
        print(duplicate_df.to_string(index=False))
        print("-" * 60)

    # =========================
    # DERIVED FIELDS
    # =========================

    df["delivery_week_start"] = (
        df["delivery_date"].apply(get_week_start)
    )

    df["delivery_week_end"] = (
        df["delivery_date"].apply(get_week_end)
    )

    df["intake_weight_kg"] = df.apply(
        determine_intake_weight,
        axis=1
    )

    print(
        "Total weighbridge net weight kg:",
        pd.to_numeric(
            df["weighbridge_net_weight_kg"],
            errors="coerce"
        ).sum()
    )

    print(
        "Total selected intake weight kg:",
        pd.to_numeric(
            df["intake_weight_kg"],
            errors="coerce"
        ).sum()
    )

    # =========================
    # SQL CONNECTION
    # =========================

    conn = get_connection()
    cursor = conn.cursor()

    print(
        "Truncating dbo.stg_deliveries..."
    )

    cursor.execute(
        "TRUNCATE TABLE dbo.stg_deliveries;"
    )

    # =========================
    # INSERT SQL
    # =========================

    insert_sql = """
    INSERT INTO dbo.stg_deliveries
    (
        source_file_name,
        source_row_number,

        submission_form_version,
        load_number,
        delivery_date,
        delivery_time,
        delivery_week_start,
        delivery_week_end,

        grower_name,
        farm_name,

        total_bins_delivered_full,
        bin_weight_delivered_kg,
        total_empty_bins_returned,
        bin_weight_returned_kg,
        bin_weight_balance_kg,

        driver_name,
        driver_id_number,
        truck_registration_number,
        trailer_registration_number_1,
        trailer_registration_number_2,

        field_number,
        chemicals_applied_may_affect_product,
        last_pesticide_herbicide_fungicide,
        chemical_active_ingredients,
        chemical_application_date,
        pre_harvest_interval,
        fruit_safe_to_process,

        weighbridge_weight_in_kg,
        weighbridge_weight_out_kg,
        weighbridge_net_weight_kg,

        check_weight_done,
        weight_source_for_intake,
        check_weight_full_bins_kg,
        check_weight_net_product_kg,
        check_vs_weighbridge_difference_kg,
        intake_weight_kg,

        rotten_damage_assessment,
        rotten_damaged_weight_kg,
        rotten_damaged_pct,

        green_not_processable_1,
        green_not_processable_2,
        green_not_processable_pct,

        good_quality_assessment,
        good_quality_sample_weight_kg,
        good_quality_pct,
        good_quality_ph,
        good_quality_brix,
        good_quality_value_per_kg_usd,

        sunburnt_orange_assessment,
        sunburnt_orange_ph,
        sunburnt_orange_sample_weight_kg,
        sunburnt_orange_pct,
        sunburnt_orange_brix,
        sunburnt_orange_value_per_kg_usd,

        comments,

        good_quality_value_usd,
        sunburnt_orange_value_usd,
        total_value_usd,
        price_per_kg_usd,

        acceptable_fruit_value_usd_2,
        sunburnt_orange_value_usd_2,
        total_value_usd_2,
        price_per_kg_usd_2,

        grader_name,
        transporter_comments,
        management_name,

        duplicate_load_count,
        is_duplicate_load
    )
    VALUES
    (
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?
    );
    """

    rows_to_insert = []

    for idx, row in df.iterrows():

        rows_to_insert.append(
            (
                SOURCE_FILE_NAME,
                int(idx) + 2,

                to_int(row.get("submission_form_version")),
                to_int(row.get("load_number")),
                to_date(row.get("delivery_date")),
                to_time(row.get("delivery_time")),
                row.get("delivery_week_start"),
                row.get("delivery_week_end"),

                clean_text(row.get("grower_name")),
                clean_text(row.get("farm_name")),

                to_decimal(row.get("total_bins_delivered_full")),
                to_decimal(row.get("bin_weight_delivered_kg")),
                to_decimal(row.get("total_empty_bins_returned")),
                to_decimal(row.get("bin_weight_returned_kg")),
                to_decimal(row.get("bin_weight_balance_kg")),

                clean_text(row.get("driver_name")),
                clean_text(row.get("driver_id_number")),
                clean_text(row.get("truck_registration_number")),
                clean_text(row.get("trailer_registration_number_1")),
                clean_text(row.get("trailer_registration_number_2")),

                clean_text(row.get("field_number")),
                clean_text(row.get("chemicals_applied_may_affect_product")),
                clean_text(row.get("last_pesticide_herbicide_fungicide")),
                clean_text(row.get("chemical_active_ingredients")),
                to_date(row.get("chemical_application_date")),
                clean_text(row.get("pre_harvest_interval")),
                clean_text(row.get("fruit_safe_to_process")),

                to_decimal(row.get("weighbridge_weight_in_kg")),
                to_decimal(row.get("weighbridge_weight_out_kg")),
                to_decimal(row.get("weighbridge_net_weight_kg")),

                clean_text(row.get("check_weight_done")),
                clean_text(row.get("weight_source_for_intake")),
                to_decimal(row.get("check_weight_full_bins_kg")),
                to_decimal(row.get("check_weight_net_product_kg")),
                to_decimal(row.get("check_vs_weighbridge_difference_kg")),
                to_decimal(row.get("intake_weight_kg")),

                clean_text(row.get("rotten_damage_assessment")),
                to_decimal(row.get("rotten_damaged_weight_kg")),
                to_decimal(row.get("rotten_damaged_pct")),

                clean_text(row.get("green_not_processable_1")),
                clean_text(row.get("green_not_processable_2")),
                to_decimal(row.get("green_not_processable_pct")),

                clean_text(row.get("good_quality_assessment")),
                to_decimal(row.get("good_quality_sample_weight_kg")),
                to_decimal(row.get("good_quality_pct")),
                to_decimal(row.get("good_quality_ph")),
                to_decimal(row.get("good_quality_brix")),
                to_decimal(row.get("good_quality_value_per_kg_usd")),

                clean_text(row.get("sunburnt_orange_assessment")),
                to_decimal(row.get("sunburnt_orange_ph")),
                to_decimal(row.get("sunburnt_orange_sample_weight_kg")),
                to_decimal(row.get("sunburnt_orange_pct")),
                to_decimal(row.get("sunburnt_orange_brix")),
                to_decimal(row.get("sunburnt_orange_value_per_kg_usd")),

                clean_text(row.get("comments")),

                to_decimal(row.get("good_quality_value_usd")),
                to_decimal(row.get("sunburnt_orange_value_usd")),
                to_decimal(row.get("total_value_usd")),
                to_decimal(row.get("price_per_kg_usd")),

                to_decimal(row.get("acceptable_fruit_value_usd_2")),
                to_decimal(row.get("sunburnt_orange_value_usd_2")),
                to_decimal(row.get("total_value_usd_2")),
                to_decimal(row.get("price_per_kg_usd_2")),

                clean_text(row.get("grader_name")),
                clean_text(row.get("transporter_comments")),
                clean_text(row.get("management_name")),

                to_int(row.get("duplicate_load_count")),
                bool(row.get("is_duplicate_load")),
            )
        )

    print("Rows prepared:", len(rows_to_insert))

    # Easier to debug initially than fast_executemany.
    cursor.fast_executemany = False

    for i, row_data in enumerate(
        rows_to_insert,
        start=1
    ):
        try:
            cursor.execute(
                insert_sql,
                row_data
            )

        except Exception:
            print()
            print("FAILED INSERT")
            print("Prepared row:", i)
            print("Source Excel row:", row_data[1])
            print("Load number:", row_data[3])
            print()
            raise

        if i % 100 == 0:
            print(
                f"Inserted {i} rows..."
            )

    conn.commit()

    cursor.close()
    conn.close()

    print()
    print("=" * 60)
    print("Deliveries load complete.")
    print(
        f"Loaded {len(rows_to_insert)} rows "
        "into dbo.stg_deliveries"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()