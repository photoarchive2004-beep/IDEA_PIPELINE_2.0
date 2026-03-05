@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
if not defined STAGE_B1_LLM_LIMIT set "STAGE_B1_LLM_LIMIT=10"

set "SCOPE=balanced"
set "N=300"
set "LOGDIR=%~dp0launcher_logs"
set "LASTLOG=%LOGDIR%\runB_last.log"
set "ERRLOG=%LOGDIR%\runB_launcher_error.log"

echo Clean-out: previous out will be archived to out\_archive\<timestamp>
echo Stage B1 search mode? [B]ALANCED / [F]OCUSED / [W]IDE (auto B in 8s)
choice /C BFW /N /T 8 /D B >nul
if errorlevel 3 (
  set "SCOPE=wide"
  set "N=450"
)
if errorlevel 2 (
  set "SCOPE=focused"
  set "N=180"
)
if errorlevel 1 (
  set "SCOPE=balanced"
  set "N=300"
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\run_b_launcher.ps1" -Scope %SCOPE% -N %N%
set "RC=%ERRORLEVEL%"

if not "%RC%"=="0" (
  echo.
  echo FAILED code %RC%
  echo Logs: "%LOGDIR%"
  if exist "%LASTLOG%" (
    start "runB_last.log" notepad "%LASTLOG%"
  ) else (
    start "runB_launcher_error.log" notepad "%ERRLOG%"
  )
  echo Press any key to close...
  pause >nul
  exit /b %RC%
)

echo.
echo OK
exit /b 0
