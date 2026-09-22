@echo off
setlocal

chcp 65001 > nul
set PYTHONIOENCODING=utf-8

cd /d C:\PythonProjects\GoCanvas_API

if not exist logs mkdir logs

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set TIMESTAMP=%%i
set LOGFILE=logs\daily_factory_report_%TIMESTAMP%.log

call venv\Scripts\activate.bat

echo ========================================== >> %LOGFILE%
echo Starting Knightshade daily load and reports >> %LOGFILE%
echo START_TIME=%date% %time% >> %LOGFILE%
echo ========================================== >> %LOGFILE%

echo Downloading Factory Reporting files from SharePoint... >> %LOGFILE%
python etl\graph_download_factory_reports.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Loading Drum Report Excel into SQL staging... >> %LOGFILE%
python etl\excel_load_drum_report.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Loading Bin Tipping Report Excel into SQL staging... >> %LOGFILE%
python etl\excel_load_bin_tipping_report.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Loading Rerun Report Excel into SQL staging... >> %LOGFILE%
python etl\excel_load_rerun_report.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Refreshing GoCanvas form registry... >> %LOGFILE%
python etl\gc_refresh_form_registry.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Running Intake ETL... >> %LOGFILE%
python etl\gc_intake_etl.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Running Shift Report ETL... >> %LOGFILE%
python etl\gc_shift_report_etl.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Sending Factory Series Report... >> %LOGFILE%
python reports\send_daily_series_report.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo ========================================== >> %LOGFILE%
echo Completed successfully >> %LOGFILE%
echo END_TIME=%date% %time% >> %LOGFILE%
echo ========================================== >> %LOGFILE%

echo Logging run status to SQL... >> %LOGFILE%
python monitoring\parse_daily_log_to_sql.py "%LOGFILE%" >> %LOGFILE% 2>&1
exit /b 0

:fail
echo ========================================== >> %LOGFILE%
echo FAILED >> %LOGFILE%
echo END_TIME=%date% %time% >> %LOGFILE%
echo ========================================== >> %LOGFILE%

echo Logging run status to SQL... >> %LOGFILE%
python monitoring\parse_daily_log_to_sql.py "%LOGFILE%" >> %LOGFILE% 2>&1

exit /b 1