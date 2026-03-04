@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

set "SCOPE=balanced"
set "N=300"
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
pause
