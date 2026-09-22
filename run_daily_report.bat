@echo off
cd /d C:\PythonProjects\GoCanvas_API
call venv\Scripts\activate
python .\reports\send_daily_report.py >> C:\PythonProjects\GoCanvas_API\daily_report_log.txt 2>&1