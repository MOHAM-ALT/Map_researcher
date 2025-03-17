@echo off
echo ===============================================================
echo       Map_researcher0.5V - Installation
echo ===============================================================
echo.

:: Create necessary directories
if not exist data mkdir data
if not exist exports mkdir exports
if not exist config mkdir config
if not exist logs mkdir logs
if not exist maps mkdir maps
if not exist models mkdir models
if not exist web_discovery mkdir web_discovery
if not exist location_analysis mkdir location_analysis
if not exist machine_learning mkdir machine_learning
if not exist analysis mkdir analysis
if not exist reviews_analysis mkdir reviews_analysis
if not exist visualization mkdir visualization

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
%pip_cmd% install requests pandas colorama numpy

echo Installing UI dependencies...
%pip_cmd% install tabulate rich

echo Installing web scraping and analysis dependencies...
%pip_cmd% install beautifulsoup4 selenium webdriver-manager

echo Installing visualization dependencies...
%pip_cmd% install folium matplotlib

echo Installing data analysis and ML dependencies...
%pip_cmd% install scikit-learn nltk networkx

echo Installing database dependencies...
%pip_cmd% install openpyxl
%pip_cmd% install psycopg2-binary || %pip_cmd% install psycopg2 || echo [WARNING] PostgreSQL support not available.

echo Installing utility dependencies...
%pip_cmd% install tqdm pyyaml

:: Download NLTK data
echo.
echo Downloading NLTK data...
%py_cmd% -m nltk.downloader punkt stopwords -q

:: Create default config file if it doesn't exist
echo.
echo Setting up configuration...
if not exist config\config.json (
    echo Creating default configuration file...
    echo { > config\config.json
    echo   "api_keys": { >> config\config.json
    echo     "google_maps": "", >> config\config.json
    echo     "openstreetmap": "" >> config\config.json
    echo   }, >> config\config.json
    echo   "search_settings": { >> config\config.json
    echo     "default_radius": 5000, >> config\config.json
    echo     "max_results": 100 >> config\config.json
    echo   }, >> config\config.json
    echo   "database": { >> config\config.json
    echo     "type": "sqlite", >> config\config.json
    echo     "path": "data/hotels.db" >> config\config.json
    echo   } >> config\config.json
    echo } >> config\config.json
    echo Default configuration created. Please edit config\config.json to add your API keys.
) else (
    echo Configuration file already exists.
)

:create_launcher
:: Create run.bat
echo.
echo Creating launcher...
echo @echo off > run.bat
echo echo Starting Map_researcher0.5V... >> run.bat
echo. >> run.bat
echo if exist venv\Scripts\python.exe ( >> run.bat
echo    venv\Scripts\python main.py %%* >> run.bat
echo ) else ( >> run.bat
echo    python main.py %%* >> run.bat
echo ) >> run.bat

echo.
echo Installation complete!
echo.
echo IMPORTANT NOTES:
echo -----------------------------------------------------
echo 1. Edit config\config.json to add your API keys
echo 2. To run the application, type "run.bat" or double-click on it
echo 3. For advanced features, make sure to install webdriver for Selenium
echo    (Chrome/Firefox) - see documentation for details
echo -----------------------------------------------------
echo.

:end
pause