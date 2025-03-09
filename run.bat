@echo off
echo ===============================================================
echo       Map_researcher0.3V - Starting Application
echo ===============================================================
echo.

:: Determine which Python to use
if exist venv\Scripts\python.exe (
    set PY_CMD=venv\Scripts\python
) else (
    echo [INFO] Virtual environment not found, using system Python
    set PY_CMD=python
)

:: Run bootstrap first
echo Running system checks...
%PY_CMD% bootstrap.py

:: Run the main application
echo.
echo Starting Map_researcher0.3V...
%PY_CMD% main.py %* 2> logs\error.log

if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] An application error occurred.
    echo Check logs\error.log for details.
    echo.
    type logs\error.log
    pause
)