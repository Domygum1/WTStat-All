@echo off
setlocal
cd /d "%~dp0"
title WTStat ALL - Replay Sampler - Port 8767
set "PYTHON_EXE=%~dp0..\WTStat\.venv\Scripts\python.exe"
if exist "%PYTHON_EXE%" (
  "%PYTHON_EXE%" -c "import fastapi,uvicorn,requests" >nul 2>nul
  if not errorlevel 1 goto :ready
)
set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
  python -m venv .venv
  if errorlevel 1 goto :fail
)
"%PYTHON_EXE%" -c "import fastapi,uvicorn,requests" >nul 2>nul
if errorlevel 1 (
  "%PYTHON_EXE%" -m pip install -r requirements.txt
  if errorlevel 1 goto :fail
)
:ready
if not exist "..\WTStat\bin\wtstat_results.exe" (
  echo Missing WTStat parser. Run ..\WTStat\start.bat first.
  goto :fail
)
echo WTStat all: http://127.0.0.1:8767
start "" "http://127.0.0.1:8767"
"%PYTHON_EXE%" -m uvicorn server:app --host 127.0.0.1 --port 8767
exit /b %errorlevel%
:fail
echo Startup failed. See README.md.
pause
exit /b 1
