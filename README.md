# GoCanvas ETL Pipeline

This project loads GoCanvas submission and response data into SQL Server staging tables and then populates `fact_tomato_intake` via a stored procedure.

## Overview

The pipeline performs the following steps:

1. Retrieves an OAuth access token from GoCanvas
2. Fetches submissions from configured GoCanvas form IDs
3. Applies incremental filtering with a lookback window
4. Retrieves submission detail and embedded responses
5. Loads data into SQL Server staging tables
6. Executes `dbo.usp_load_fact_tomato_intake`
7. Logs ETL run metrics to `dbo.etl_run_log`

## Project Structure

```text
GoCanvas_API/
├── etl/
│   ├── gc_get_forms.py
│   └── gocanvas_get_token.py
├── .gitignore
├── requirements.txt
└── README.md
Main Components
etl/gc_get_forms.py

Main ETL script. Handles:

GoCanvas submission extraction

response extraction

incremental logic

deduplication

SQL staging loads

MERGE operations

ETL logging

stored procedure execution

etl/gocanvas_get_token.py

Helper script that retrieves a valid GoCanvas OAuth token.

Requirements

Python 3.x

SQL Server

ODBC Driver 18 for SQL Server

GoCanvas API credentials

Python Packages

Install dependencies with:

pip install -r requirements.txt

Typical packages used:

requests

pyodbc

python-dotenv

tqdm

Environment Variables

Create a .env file in the project root.

Example:

SQL_SERVER=192.168.1.2,1433
SQL_DB=KNIGHTSHADE_MGT
SQL_USER=your_sql_username
SQL_PASSWORD=your_sql_password

GOCANVAS_CLIENT_ID=your_client_id
GOCANVAS_CLIENT_SECRET=your_client_secret
SQL Dependencies

The script expects the following database objects to exist:

dbo.stg_gocanvas_submission

dbo.stg_gocanvas_response

dbo.etl_run_log

dbo.usp_load_fact_tomato_intake

How It Works
Incremental Load

The script checks:

SELECT MAX(created_at_utc)
FROM dbo.stg_gocanvas_submission

It then applies:

a 1-day lookback window

Python-side filtering after fetch

deduplication before load

This makes the pipeline safer against late-arriving records and API inconsistencies.

Submission and Response Loading

The script:

stages data in temp tables

performs set-based MERGE operations into staging

captures insert/update counts using OUTPUT $action

ETL Logging

Each run is logged to:

dbo.etl_run_log

Captured fields include:

process name

start/end time

status

submissions fetched

submissions after filter

responses fetched

insert/update counts

error message on failure

Running the Script

From the repo root:

python etl/gc_get_forms.py
Branching Strategy

main = stable / production-ready code

dev = active development and testing

Typical workflow:

git checkout dev
# make changes
git add .
git commit -m "Describe change"
git push

When ready to promote:

git checkout main
git merge dev
git push
Versioning

Releases are tagged in Git.

Example:

git tag -a v1.0 -m "Initial production-ready GoCanvas ETL"
git push origin v1.0
Current Status

This project currently supports:

multi-form submission extraction

response extraction via submission detail endpoint

incremental loads with overlap window

SQL staging and fact table loading

ETL run logging

Git version control and tagged releases

Future Improvements

Planned enhancements:

faster parallel response fetching

dedicated ETL control / watermark table

improved handling of revised submissions

scheduled execution

Power BI reporting layer

Author

Michael Hulley