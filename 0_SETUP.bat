@echo off
setlocal EnableExtensions
cd /d "%~dp0"

rem ASCII-only .bat to avoid cmd.exe encoding issues
set "LOG=%CD%\setup_log.txt"

> "%LOG%" echo [INFO] === 0_SETUP start ===
>>"%LOG%" echo [INFO] Root=%CD%

where powershell >nul 2>nul
if errorlevel 1 (
  >>"%LOG%" echo [ERROR] PowerShell not found.
  echo PowerShell not found.
  pause
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%CD%\tools\setup.ps1" -Root "%CD%" -LogPath "%LOG%"
set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" (
  echo [ERROR] Setup failed. See: "%LOG%"
  >>"%LOG%" echo [ERROR] Setup failed.
  pause
  exit /b %EC%
)

echo [OK] Setup complete.
>>"%LOG%" echo [OK] Setup complete.
echo [NEXT] Put your OPENALEX_API_KEY into config\secrets.env (if you plan to run Stage B).
pause
exit /b 0
