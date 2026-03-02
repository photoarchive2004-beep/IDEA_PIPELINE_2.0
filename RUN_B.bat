@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

set "MODE=BALANCED"
echo Stage B search mode? [B]ALANCED / [F]OCUSED / [W]IDE (auto B in 8s)
choice /C BFW /N /T 8 /D B >nul
if errorlevel 3 set "MODE=WIDE"
if errorlevel 2 set "MODE=FOCUSED"
if errorlevel 1 set "MODE=BALANCED"

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\run_b.ps1" -Mode %MODE%
pause
