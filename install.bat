@echo off
echo ===============================================================
echo       Map_researcher0.4V - Simple Installation
echo ===============================================================
echo.

:: Create necessary directories
if not exist data mkdir data
if not exist exports mkdir exports
if not exist config mkdir config
if not exist logs mkdir logs
if not exist maps mkdir maps
if not exist cache mkdir cache

:: Check Python version
echo Checking Python version...
python --version 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.6+ and make sure it's in your PATH.
    goto :end
)

:: Ask about virtual environment
echo.
echo Do you want to:
echo 1) Install in a virtual environment (recommended)
echo 2) Install globally
echo 3) Skip installation and use fallbacks
set /p option="Enter option number (1-3): "

if "%option%"=="1" (
    echo Creating virtual environment...
    python -m venv venv
    if %ERRORLEVEL% neq 0 (
        echo [ERROR] Failed to create virtual environment.
        echo Continuing with global installation...
        set py_cmd=python
        set pip_cmd=pip
    ) else (
        set py_cmd=venv\Scripts\python
        set pip_cmd=venv\Scripts\pip
    )
) else if "%option%"=="2" (
    set py_cmd=python
    set pip_cmd=pip
) else (
    echo Skipping installation, will use fallbacks...
    goto :create_launcher
)

:: Install dependencies
echo.
echo Installing required packages...
%pip_cmd% install -U pip setuptools wheel

echo Installing core dependencies...
%pip_cmd% install requests pandas colorama

echo Installing UI dependencies...
%pip_cmd% install tabulate rich

echo Installing visualization dependencies...
%pip_cmd% install folium

echo Installing database dependencies...
%pip_cmd% install psycopg2-binary || %pip_cmd% install psycopg2 || echo [WARNING] PostgreSQL support not available.

echo Installing data analysis dependencies...
%pip_cmd% install numpy openpyxl beautifulsoup4

echo Installing visualization extensions...
%pip_cmd% install matplotlib

echo Installing machine learning dependencies (optional, may fail on some systems)...
%pip_cmd% install scikit-learn || echo [WARNING] scikit-learn not installed, some clustering features will be limited.



:create_launcher
:: Create run.bat
echo.
echo Creating launcher...
echo @echo off > run.bat
echo echo Starting Map_researcher0.3V... >> run.bat
echo. >> run.bat
echo if exist venv\Scripts\python.exe ( >> run.bat
echo    venv\Scripts\python main.py %%* >> run.bat
echo ) else ( >> run.bat
echo    python main.py %%* >> run.bat
echo ) >> run.bat

echo.
echo Installation complete!
echo To run the application, type "run.bat" or double-click on it.
echo.

:end
pause