@echo off
echo.
echo ============================================
echo  Survivor Trading Strategy - Start Services
echo ============================================
echo.

:: Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    exit /b 1
)

:: Run the unified service runner
python run_services.py
