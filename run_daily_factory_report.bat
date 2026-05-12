@echo off
setlocal

cd /d C:\Users\mrdhulley\PythonProjects\GoCanvas_API

if not exist logs mkdir logs

REM Build safe timestamp for unique log file
set YYYY=%date:~-4%
set MM=%date:~3,2%
set DD=%date:~0,2%
set HH=%time:~0,2%
set HH=%HH: =0%
set MIN=%time:~3,2%
set SEC=%time:~6,2%

set LOGFILE=logs\daily_factory_report_%YYYY%%MM%%DD%_%HH%%MIN%%SEC%.log

call venv\Scripts\activate.bat

echo ========================================== >> %LOGFILE%
echo Starting Knightshade daily load and report >> %LOGFILE%
echo START_TIME=%date% %time% >> %LOGFILE%
echo ========================================== >> %LOGFILE%

echo Running Intake ETL... >> %LOGFILE%
python etl\gc_intake_etl.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Running Drum Fill ETL... >> %LOGFILE%
python etl\gc_drum_fill_etl.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Running Shift Report ETL... >> %LOGFILE%
python etl\gc_shift_report_etl.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo Sending Daily Report... >> %LOGFILE%
python reports\send_daily_report.py >> %LOGFILE% 2>&1
if errorlevel 1 goto fail

echo ========================================== >> %LOGFILE%
echo Completed successfully >> %LOGFILE%
echo END_TIME=%date% %time% >> %LOGFILE%
echo ========================================== >> %LOGFILE%

echo Logging run status to SQL... >> %LOGFILE%
python monitoring\parse_daily_log_to_sql.py >> %LOGFILE% 2>&1

exit /b 0
:fail
echo ========================================== >> %LOGFILE%
echo FAILED >> %LOGFILE%
echo END_TIME=%date% %time% >> %LOGFILE%
echo ========================================== >> %LOGFILE%

echo Logging failed run status to SQL... >> %LOGFILE%
python monitoring\parse_daily_log_to_sql.py >> %LOGFILE% 2>&1

exit /b 1