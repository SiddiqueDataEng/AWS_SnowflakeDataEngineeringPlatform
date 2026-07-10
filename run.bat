@echo off
setlocal EnableDelayedExpansion

:: ============================================================
:: run.bat  --  Pakistani AWS + Snowflake Data Platform
:: Run any or all pipeline components from a menu or directly.
::
:: Usage:
::   run.bat              -> interactive menu
::   run.bat 0            -> full pipeline
::   run.bat 1            -> data generator only
::   run.bat 9            -> Kafka produce + consume
::   run.bat dashboard    -> launch Streamlit dashboard
::   run.bat checkdb      -> print DB row counts
::   run.bat install      -> pip install requirements
:: ============================================================

set "BASE=%~dp0"
set "PY=python"
set "LOGDIR=%BASE%logs"

:: Force UTF-8 so emoji in Python scripts prints correctly
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
chcp 65001 >nul 2>&1
for /f "tokens=1-3 delims=/ " %%a in ("%DATE%") do set "DT=%%c-%%a-%%b"
for /f "tokens=1-3 delims=:." %%a in ("%TIME: =0%") do set "TM=%%a-%%b-%%c"
set "TS=%DT%_%TM%"

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

:: Direct invocation
if not "%~1"=="" (
    call :DISPATCH %~1
    exit /b %ERRORLEVEL%
)

:MENU
cls
echo.
echo  +--------------------------------------------------+
echo  ^|   PK  Pakistani AWS + Snowflake Data Platform    ^|
echo  +--------------------------------------------------+
echo.
echo   [0]  Run FULL pipeline  (all 11 steps)
echo   ----------------------------------------------
echo   [1]  Step 01  Generate Pakistani data
echo   [2]  Step 02  Setup DB + COPY INTO
echo   [3]  Step 03  Streams + CDC
echo   [4]  Step 04  Tasks + Scheduling
echo   [5]  Step 05  UDF simulation
echo   [6]  Step 06  External Functions (Lambda)
echo   [7]  Step 07  AWS Python integrations
echo   [8]  Step 08  Airflow DAG simulation
echo   [9]  Step 09  Kafka streaming (produce+consume)
echo   [10] Step 10  Data Governance + Security
echo   [11] Step 11  Snowpark + ML Forecast
echo   ----------------------------------------------
echo   [dashboard]  Launch BI Dashboard
echo   [checkdb]    Print DB table row counts
echo   [install]    pip install requirements
echo   [q]          Quit
echo.
set /p "CHOICE=  Select: "

if /i "%CHOICE%"=="q" exit /b 0
call :DISPATCH %CHOICE%
echo.
pause
goto MENU

:: ────────────────────────────────────────────────────────────
:DISPATCH
set "C=%~1"
if "%C%"==""     goto :UNKNOWN

if /i "%C%"=="0"         goto :ALL
if /i "%C%"=="1"         goto :S1
if /i "%C%"=="2"         goto :S2
if /i "%C%"=="3"         goto :S3
if /i "%C%"=="4"         goto :S4
if /i "%C%"=="5"         goto :S5
if /i "%C%"=="6"         goto :S6
if /i "%C%"=="7"         goto :S7
if /i "%C%"=="8"         goto :S8
if /i "%C%"=="9"         goto :S9
if /i "%C%"=="10"        goto :S10
if /i "%C%"=="11"        goto :S11
if /i "%C%"=="dashboard" goto :DASH
if /i "%C%"=="checkdb"   goto :CHKDB
if /i "%C%"=="install"   goto :INSTALL
goto :UNKNOWN

:: ────────────────────────────────────────────────────────────
:ALL
call :RUN "Full pipeline" "%BASE%run_pipeline.py"
exit /b %ERRORLEVEL%

:S1
call :RUN "01 Data Generator" "%BASE%02_data_generator\pk_data_generator.py"
exit /b %ERRORLEVEL%

:S2
call :RUN "02 Setup DB" "%BASE%03_data_loading\setup_db.py"
exit /b %ERRORLEVEL%

:S3
call :RUN "03 Streams CDC" "%BASE%04_streams_cdc\streams_cdc.py"
exit /b %ERRORLEVEL%

:S4
call :RUN "04 Tasks" "%BASE%05_tasks_scheduling\tasks.py"
exit /b %ERRORLEVEL%

:S5
call :RUN "05 UDF" "%BASE%06_udf\udf_simulation.py"
exit /b %ERRORLEVEL%

:S6
call :RUN "06 External Funcs" "%BASE%07_external_functions\lambda_currency.py"
exit /b %ERRORLEVEL%

:S7
call :RUN "07 AWS Python" "%BASE%08_aws_python\snowflake_connector.py"
exit /b %ERRORLEVEL%

:S8
call :RUN "08 Airflow DAG" "%BASE%08_aws_python\airflow_dags\pk_sales_dag.py"
exit /b %ERRORLEVEL%

:S9
echo   [-->] Step 09a - Kafka Producer...
set "LOG=%LOGDIR%\step_09a_%TS%.log"
%PY% "%BASE%09_kafka_streaming\pk_kafka_producer.py" 50 > "%LOG%" 2>&1
type "%LOG%"
echo   [-->] Step 09b - Kafka Consumer...
set "LOG=%LOGDIR%\step_09b_%TS%.log"
%PY% "%BASE%09_kafka_streaming\pk_kafka_consumer.py" >> "%LOG%" 2>&1
type "%LOG%"
exit /b %ERRORLEVEL%

:S10
call :RUN "10 Governance" "%BASE%10_governance_security\governance.py"
exit /b %ERRORLEVEL%

:S11
call :RUN "11 Snowpark ML" "%BASE%11_snowpark\snowpark_simulation.py"
exit /b %ERRORLEVEL%

:DASH
echo   [-->] Launching BI Dashboard at http://localhost:8501
start "" cmd /k "%PY% -m streamlit run "%BASE%superset_dashboard.py" --server.port 8501"
echo   [OK]  BI Dashboard started -- open http://localhost:8501
exit /b 0

:CHKDB
echo   [-->] Checking DB...
%PY% "%BASE%check_db.py"
exit /b %ERRORLEVEL%

:INSTALL
echo   [-->] Installing requirements...
%PY% -m pip install -r "%BASE%requirements.txt"
exit /b %ERRORLEVEL%

:UNKNOWN
echo   [ERR] Unknown component: %C%
echo         Valid: 0-11, dashboard, checkdb, install
exit /b 1

:: ────────────────────────────────────────────────────────────
:: :RUN  label  script
::   Runs a python script, logs to file, prints output
:: ────────────────────────────────────────────────────────────
:RUN
set "LABEL=%~1"
set "SCRIPT=%~2"
set "LOG=%LOGDIR%\step_%LABEL: =_%_%TS%.log"
echo.
echo   ====================================================
echo    STEP: %LABEL%
echo   ====================================================
%PY% "%SCRIPT%" > "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"
type "%LOG%"
if %RC%==0 (
    echo.
    echo   [OK]  %LABEL% completed.
) else (
    echo.
    echo   [ERR] %LABEL% failed (exit %RC%)
)
echo   [LOG] %LOG%
exit /b %RC%
