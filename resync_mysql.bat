@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo 请先运行 start.bat。
  pause
  exit /b 1
)
".venv\Scripts\python.exe" resync_mysql.py
pause
