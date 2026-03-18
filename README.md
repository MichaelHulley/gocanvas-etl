# GoCanvas ETL

This project loads GoCanvas submission and response data into SQL Server staging tables and then populates `fact_tomato_intake` via stored procedure.

## Requirements

- Python 3.x
- SQL Server
- ODBC Driver 18 for SQL Server

## Environment variables

Create a `.env` file with:

- `SQL_SERVER`
- `SQL_DB` or `SQL_DATABASE`
- `SQL_USER` or `SQL_USERNAME`
- `SQL_PASSWORD`
- `GOCANVAS_CLIENT_ID`
- `GOCANVAS_CLIENT_SECRET`

## Main script

`etl/gc_get_forms.py`

## What it does

1. Gets GoCanvas OAuth token
2. Fetches submissions incrementally
3. Fetches submission detail / responses
4. Loads staging tables
5. Runs `dbo.usp_load_fact_tomato_intake`
6. Logs ETL run to `dbo.etl_run_log`