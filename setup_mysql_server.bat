@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
  python -m venv .venv
  if errorlevel 1 goto :fail
)
"%PYTHON_EXE%" -m pip install -r requirements-mysql.txt
if errorlevel 1 goto :fail
"%PYTHON_EXE%" setup_mysql_server.py
if errorlevel 1 goto :fail
echo.
echo Setup completed.
pause
exit /b 0
:fail
echo MySQL setup failed; no sampler data was deleted.
pause
exit /b 1
