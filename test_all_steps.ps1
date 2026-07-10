# test_all_steps.ps1 — run every step and report pass/fail
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$BASE = $PSScriptRoot
$PY   = "python"

$STEPS = [ordered]@{
    "1"  = "02_data_generator\pk_data_generator.py"
    "2"  = "03_data_loading\setup_db.py"
    "3"  = "04_streams_cdc\streams_cdc.py"
    "4"  = "05_tasks_scheduling\tasks.py"
    "5"  = "06_udf\udf_simulation.py"
    "6"  = "07_external_functions\lambda_currency.py"
    "7"  = "08_aws_python\snowflake_connector.py"
    "8"  = "08_aws_python\airflow_dags\pk_sales_dag.py"
    "9a" = "09_kafka_streaming\pk_kafka_producer.py"
    "9b" = "09_kafka_streaming\pk_kafka_consumer.py"
    "10" = "10_governance_security\governance.py"
    "11" = "11_snowpark\snowpark_simulation.py"
}

$results = @()
foreach ($key in $STEPS.Keys) {
    $script = Join-Path $BASE $STEPS[$key]
    $args_  = if ($key -eq "9a") { @("50") } else { @() }
    $out    = & $PY "-W" "ignore::DeprecationWarning" $script @args_ 2>&1
    $rc     = $LASTEXITCODE
    $status = if ($rc -eq 0) { "PASS" } else { "FAIL" }
    $results += [PSCustomObject]@{ Step=$key; Status=$status; ExitCode=$rc }
    Write-Host "  Step $key  $status" -ForegroundColor $(if ($rc -eq 0) {"Green"} else {"Red"})
}

Write-Host ""
Write-Host "  ── Summary ──" -ForegroundColor Cyan
$results | Format-Table -AutoSize
$failed = ($results | Where-Object { $_.Status -eq "FAIL" }).Count
if ($failed -eq 0) { Write-Host "  All steps PASSED" -ForegroundColor Green }
else               { Write-Host "  $failed step(s) FAILED" -ForegroundColor Red }
