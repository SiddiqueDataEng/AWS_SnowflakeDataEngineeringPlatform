#Requires -Version 5.1
<#
.SYNOPSIS
    Pakistani AWS + Snowflake Data Platform - PowerShell launcher.

.PARAMETER Component
    0=all pipeline, 1-11=individual step, dashboard, checkdb, install
    Omit to show interactive menu.

.PARAMETER LogOutput
    Save output to logs/ directory (default: true)

.EXAMPLE
    .\run.ps1               # interactive menu
    .\run.ps1 0             # full pipeline
    .\run.ps1 1             # data generator only
    .\run.ps1 dashboard     # launch Streamlit
    .\run.ps1 9             # Kafka streaming
#>
param(
    [string]$Component  = "",
    [bool]  $LogOutput  = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

$BASE   = $PSScriptRoot
$LOGDIR = Join-Path $BASE "logs"
$PY     = "python"
$TS     = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"

# Force UTF-8 output so emoji / Unicode prints correctly on Windows
$env:PYTHONUTF8    = "1"
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

if (-not (Test-Path $LOGDIR)) { New-Item -ItemType Directory -Path $LOGDIR | Out-Null }

# ── Color helpers ─────────────────────────────────────────────
function hdr  ([string]$m) { Write-Host "`n  $('='*56)" -ForegroundColor Cyan;  Write-Host "   $m" -ForegroundColor Cyan; Write-Host "  $('='*56)" -ForegroundColor Cyan }
function ok   ([string]$m) { Write-Host "  [OK]  $m" -ForegroundColor Green }
function err  ([string]$m) { Write-Host "  [ERR] $m" -ForegroundColor Red }
function inf  ([string]$m) { Write-Host "  [-->] $m" -ForegroundColor Yellow }

# ── Run a python script with optional log ─────────────────────
function Invoke-Step {
    param([string]$Label, [string]$Script, [string[]]$ScriptArgs = @())

    hdr "STEP: $Label"
    $log = Join-Path $LOGDIR "step_${Label}_${TS}.log"
    $sw  = [Diagnostics.Stopwatch]::StartNew()

    if ($LogOutput) {
        & $PY "-W" "ignore::DeprecationWarning" $Script @ScriptArgs 2>&1 | Tee-Object -FilePath $log
        $rc = $LASTEXITCODE
    } else {
        & $PY "-W" "ignore::DeprecationWarning" $Script @ScriptArgs
        $rc = $LASTEXITCODE
    }
    if ($rc -eq 0) { ok  "$Label done in $($sw.Elapsed.TotalSeconds.ToString('F1'))s" }
    else           { err "$Label failed (exit $rc)" }
    if ($LogOutput) { inf "Log -> $log" }
    return $rc
}

# ── Step registry ─────────────────────────────────────────────
$STEPS = [ordered]@{
    "1"   = @{ Label="01 Data Generator";        Script="02_data_generator\pk_data_generator.py";       Args=@()     }
    "2"   = @{ Label="02 Setup DB + Load";        Script="03_data_loading\setup_db.py";                  Args=@()     }
    "3"   = @{ Label="03 Streams CDC";            Script="04_streams_cdc\streams_cdc.py";                Args=@()     }
    "4"   = @{ Label="04 Tasks Scheduling";       Script="05_tasks_scheduling\tasks.py";                 Args=@()     }
    "5"   = @{ Label="05 UDF Simulation";         Script="06_udf\udf_simulation.py";                     Args=@()     }
    "6"   = @{ Label="06 External Functions";     Script="07_external_functions\lambda_currency.py";     Args=@()     }
    "7"   = @{ Label="07 AWS Python";             Script="08_aws_python\snowflake_connector.py";         Args=@()     }
    "8"   = @{ Label="08 Airflow DAG";            Script="08_aws_python\airflow_dags\pk_sales_dag.py";   Args=@()     }
    "9a"  = @{ Label="09 Kafka Producer";         Script="09_kafka_streaming\pk_kafka_producer.py";      Args=@("50") }
    "9b"  = @{ Label="09 Kafka Consumer";         Script="09_kafka_streaming\pk_kafka_consumer.py";      Args=@()     }
    "10"  = @{ Label="10 Governance Security";    Script="10_governance_security\governance.py";         Args=@()     }
    "11"  = @{ Label="11 Snowpark ML";            Script="11_snowpark\snowpark_simulation.py";            Args=@()     }
    "chk" = @{ Label="Check DB";                  Script="check_db.py";                                  Args=@()     }
}

# ── Run one component by key ──────────────────────────────────
function Run-Component ([string]$key) {
    $key = $key.ToLower().Trim()

    if ($key -eq "0") {
        hdr "FULL PIPELINE (all 11 steps)"
        $log = Join-Path $LOGDIR "pipeline_full_${TS}.log"
        if ($LogOutput) { & $PY (Join-Path $BASE "run_pipeline.py") 2>&1 | Tee-Object -FilePath $log }
        else             { & $PY (Join-Path $BASE "run_pipeline.py") }
        if ($LASTEXITCODE -eq 0) { ok "Full pipeline complete" } else { err "Pipeline failed" }
        return
    }

    if ($key -eq "9") {
        # Producer then consumer
        $s = $STEPS["9a"]; Invoke-Step $s.Label (Join-Path $BASE $s.Script) $s.Args
        $s = $STEPS["9b"]; Invoke-Step $s.Label (Join-Path $BASE $s.Script) $s.Args
        return
    }

    if ($key -eq "checkdb" -or $key -eq "chk") {
        $s = $STEPS["chk"]; Invoke-Step $s.Label (Join-Path $BASE $s.Script) $s.Args
        return
    }

    if ($key -eq "dashboard") {
        hdr "Launching BI Dashboard"
        $script = Join-Path $BASE "superset_dashboard.py"
        inf "Starting at http://localhost:8501 ..."
        Start-Process powershell -ArgumentList "-NoExit -Command `"& $PY -m streamlit run '$script' --server.port 8501`""
        ok "BI Dashboard started -- open http://localhost:8501"
        return
    }

    if ($key -eq "install") {
        hdr "pip install requirements"
        & $PY -m pip install -r (Join-Path $BASE "requirements.txt")
        if ($LASTEXITCODE -eq 0) { ok "Install complete" } else { err "Install failed" }
        return
    }

    if ($STEPS.Contains($key)) {
        $s = $STEPS[$key]
        Invoke-Step $s.Label (Join-Path $BASE $s.Script) $s.Args
    } else {
        err "Unknown component: '$key'"
        inf "Valid choices: 0-11, dashboard, checkdb, install"
    }
}

# ── Interactive menu ──────────────────────────────────────────
function Show-Menu {
    while ($true) {
        Clear-Host
        Write-Host ""
        Write-Host "  +--------------------------------------------------+" -ForegroundColor Cyan
        Write-Host "  |  PK  Pakistani AWS + Snowflake Data Platform      |" -ForegroundColor Cyan
        Write-Host "  +--------------------------------------------------+" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "   [0]  Run FULL pipeline  (all 11 steps)"            -ForegroundColor White
        Write-Host "   -----------------------------------------------"   -ForegroundColor DarkGray
        Write-Host "   [1]  Step 01  Generate Pakistani data"             -ForegroundColor Green
        Write-Host "   [2]  Step 02  Setup DB + COPY INTO"                -ForegroundColor Green
        Write-Host "   [3]  Step 03  Streams + CDC"                       -ForegroundColor Green
        Write-Host "   [4]  Step 04  Tasks + Scheduling"                  -ForegroundColor Green
        Write-Host "   [5]  Step 05  UDF simulation"                      -ForegroundColor Green
        Write-Host "   [6]  Step 06  External Functions (Lambda)"         -ForegroundColor Green
        Write-Host "   [7]  Step 07  AWS Python integrations"             -ForegroundColor Green
        Write-Host "   [8]  Step 08  Airflow DAG"                         -ForegroundColor Green
        Write-Host "   [9]  Step 09  Kafka streaming (produce+consume)"   -ForegroundColor Green
        Write-Host "   [10] Step 10  Data Governance + Security"          -ForegroundColor Green
        Write-Host "   [11] Step 11  Snowpark + ML Forecast"              -ForegroundColor Green
        Write-Host "   -----------------------------------------------"   -ForegroundColor DarkGray
        Write-Host "   [dashboard]  Launch BI Dashboard"               -ForegroundColor Magenta
        Write-Host "   [checkdb]    Print DB table row counts"            -ForegroundColor Yellow
        Write-Host "   [install]    pip install requirements"             -ForegroundColor Yellow
        Write-Host "   [q]          Quit"                                 -ForegroundColor DarkGray
        Write-Host ""

        $choice = Read-Host "  Select"
        if ($choice -in @("q","Q","quit","exit")) { break }

        Run-Component $choice
        Write-Host ""
        Read-Host "  Press Enter to return to menu"
    }
}

# ── Entry point ───────────────────────────────────────────────
if ($Component -eq "") { Show-Menu } else { Run-Component $Component }
