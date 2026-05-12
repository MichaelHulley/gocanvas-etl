@echo off
cd /d C:\Users\mrdhulley\PythonProjects\GoCanvas_API

call venv\Scripts\activate

python reports\send_daily_report.py >> logs\daily_report.log 2>&1
