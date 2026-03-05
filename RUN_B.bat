@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul

set "ROOT=%~dp0"
pushd "%ROOT%" >nul

echo Stage B1 search mode? [B]ALANCED / [F]OCUSED / [W]IDE (auto B in 8s)
choice /c BFW /n /t 8 /d B >nul
set "SCOPE=balanced"
if errorlevel 3 set "SCOPE=wide"
if errorlevel 2 set "SCOPE=focused"

set "N=300"
if not "%STAGE_B1_N%"=="" set "N=%STAGE_B1_N%"

set "IDEA_DIR="
if exist "ideas\_ACTIVE_PATH.txt" (
  for /f "usebackq delims=" %%A in ("ideas\_ACTIVE_PATH.txt") do set "IDEA_DIR=%%A"
)

if not "!IDEA_DIR!"=="" (
  if not exist "!IDEA_DIR!" (
    if exist "%ROOT%!IDEA_DIR!" set "IDEA_DIR=%ROOT%!IDEA_DIR!"
  )
)

if "!IDEA_DIR!"=="" (
  for /f "delims=" %%D in ('dir /b /ad /o-n "ideas\IDEA-*" 2^>nul') do (
    set "IDEA_DIR=%ROOT%ideas\%%D"
    goto :gotIdea
  )
)

:gotIdea
if "!IDEA_DIR!"=="" (
  echo ERROR: Нет папок ideas\IDEA-*.
  echo Сначала запусти 1_NEW_IDEA.bat.
  echo.
  pause
  popd >nul
  exit /b 1
)

set "PS1=%ROOT%tools\run_b_launcher.ps1"
if not exist "%PS1%" (
  echo ERROR: Не найден файл: %PS1%
  echo.
  pause
  popd >nul
  exit /b 1
)

echo.
echo Running Stage B1...
echo   idea:  "!IDEA_DIR!"
echo   scope: "!SCOPE!"
echo   N:     "!N!"
echo.

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%PS1%" -Scope "!SCOPE!" -N !N! -IdeaDir "!IDEA_DIR!"
set "RC=%ERRORLEVEL%"

if not "%RC%"=="0" (
  echo.
  echo ==========================================
  echo Stage B1 FAILED with code %RC%
  echo Log: %ROOT%launcher_logs\runB_last.log
  echo ==========================================
  if exist "%ROOT%launcher_logs\runB_last.log" start notepad.exe "%ROOT%launcher_logs\runB_last.log"
  echo.
  pause
  popd >nul
  exit /b %RC%
)

echo.
echo ✅ Stage B1 OK.
popd >nul
exit /b 0