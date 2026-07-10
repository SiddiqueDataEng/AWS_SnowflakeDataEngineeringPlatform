@echo off
setlocal EnableDelayedExpansion

:: ============================================================
:: deploy.bat — Terraform deploy for PK AWS + Snowflake platform
:: Usage: deploy.bat [action] [env]
::   action : init | plan | apply | destroy | output | validate
::   env    : dev | prod
::
:: Examples:
::   deploy.bat plan dev
::   deploy.bat apply dev
::   deploy.bat apply prod
::   deploy.bat destroy dev
:: ============================================================

set "ACTION=%~1"
set "ENV=%~2"
set "TF_DIR=%~dp0"
set "LOG_DIR=%TF_DIR%..\logs"

if "%ACTION%"=="" set "ACTION=plan"
if "%ENV%"==""    set "ENV=dev"

:: Validate env
if /i not "%ENV%"=="dev" if /i not "%ENV%"=="prod" (
    echo   [ERROR] env must be dev or prod
    exit /b 1
)

set "TFVARS=%TF_DIR%environments\%ENV%\terraform.tfvars"
set "TIMESTAMP=%DATE:~-4%-%DATE:~3,2%-%DATE:~0,2%"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%TFVARS%" (
    echo   [ERROR] Missing tfvars: %TFVARS%
    exit /b 1
)

echo.
echo  ====================================================
echo   Terraform Deploy -- %ACTION% / %ENV%
echo  ====================================================
echo   tfvars : %TFVARS%
echo.

:: Check terraform exists
where terraform >nul 2>&1
if errorlevel 1 (
    echo   [ERROR] terraform not found. Install from https://developer.hashicorp.com/terraform/install
    exit /b 1
)

pushd "%TF_DIR%"

if /i "%ACTION%"=="init"     goto DO_INIT
if /i "%ACTION%"=="plan"     goto DO_PLAN
if /i "%ACTION%"=="apply"    goto DO_APPLY
if /i "%ACTION%"=="destroy"  goto DO_DESTROY
if /i "%ACTION%"=="output"   goto DO_OUTPUT
if /i "%ACTION%"=="validate" goto DO_VALIDATE
if /i "%ACTION%"=="fmt"      goto DO_FMT
echo   [ERROR] Unknown action: %ACTION%
popd
exit /b 1

:DO_INIT
echo   [INIT] Initialising providers...
terraform init -upgrade -input=false
goto DONE

:DO_PLAN
terraform init -upgrade -input=false
echo   [PLAN] Planning %ENV% deployment...
terraform plan -var-file="%TFVARS%" -input=false
goto DONE

:DO_APPLY
terraform init -upgrade -input=false
if /i "%ENV%"=="prod" (
    echo.
    echo   WARNING: You are deploying to PRODUCTION.
    set /p "CONFIRM=  Type YES to confirm: "
    if /i not "!CONFIRM!"=="YES" (
        echo   Aborted.
        popd
        exit /b 0
    )
)
terraform apply -var-file="%TFVARS%" -input=false
if errorlevel 0 terraform output
goto DONE

:DO_DESTROY
echo.
echo   WARNING: This will DESTROY all %ENV% resources.
set /p "CONFIRM=  Type destroy-%ENV% to confirm: "
if /i not "!CONFIRM!"=="destroy-%ENV%" (
    echo   Aborted.
    popd
    exit /b 0
)
terraform init -input=false
terraform destroy -var-file="%TFVARS%" -input=false
goto DONE

:DO_OUTPUT
terraform init -input=false
terraform output -var-file="%TFVARS%"
goto DONE

:DO_VALIDATE
terraform init -backend=false -input=false
terraform validate
goto DONE

:DO_FMT
terraform fmt -recursive .
goto DONE

:DONE
set "EXIT=%ERRORLEVEL%"
popd
if %EXIT%==0 (
    echo.
    echo   [OK] %ACTION% %ENV% completed successfully.
) else (
    echo.
    echo   [FAIL] %ACTION% %ENV% failed with exit code %EXIT%.
)
exit /b %EXIT%
