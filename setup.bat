@echo off
setlocal enabledelayedexpansion

:: ─────────────────────────────────────────────────────────────────────────────
::  Lead Scraper — One-click setup for Windows
:: ─────────────────────────────────────────────────────────────────────────────

echo.
echo ╔══════════════════════════════════════╗
echo ║      Lead Scraper  —  Setup          ║
echo ╚══════════════════════════════════════╝
echo.

:: ── 1. Python check ──────────────────────────────────────────────────────────
echo [1/5] Checking Python...
where python >nul 2>&1
if %errorlevel% neq 0 (
    where python3 >nul 2>&1
    if !errorlevel! neq 0 (
        echo        ERROR: Python 3.10+ not found.
        echo        Please install from https://python.org/downloads
        echo        Make sure to check "Add Python to PATH" during install!
        pause
        exit /b 1
    )
    set PYTHON=python3
) else (
    set PYTHON=python
)

for /f "tokens=2 delims= " %%v in ('%PYTHON% --version 2^>^&1') do set PYVER=%%v
echo        Found Python %PYVER%

:: ── 2. Virtual environment ────────────────────────────────────────────────────
echo [2/5] Setting up virtual environment...
if not exist ".venv\" (
    %PYTHON% -m venv .venv
    echo        Created .venv
) else (
    echo        .venv already exists
)

:: ── 3. Install dependencies ───────────────────────────────────────────────────
echo [3/5] Installing dependencies...
call .venv\Scripts\activate.bat
pip install --upgrade pip -q
pip install -r requirements.txt -q
pip install -e . -q
echo        All packages installed

:: ── 4. Optional Playwright ────────────────────────────────────────────────────
echo [4/5] Optional: Google Maps (Playwright)
echo        Playwright adds ~300MB Chromium download for Google Maps scraping.
set /p INSTALL_PW="       Install Playwright? [y/N] "
if /i "%INSTALL_PW%"=="y" (
    pip install playwright -q
    playwright install chromium
    echo        Playwright installed
) else (
    echo        Skipped.
)

:: ── 5. Done ───────────────────────────────────────────────────────────────────
echo [5/5] Done!
echo.
echo ╔══════════════════════════════════════════════╗
echo ║  Setup complete! How to start:               ║
echo ║                                              ║
echo ║  Web UI (browser):  start.bat               ║
echo ║  Terminal TUI:      start.bat --tui          ║
echo ║  CLI scrape:        start.bat "query" -l loc ║
echo ╚══════════════════════════════════════════════╝
echo.

:: Create start.bat
(
echo @echo off
echo call .venv\Scripts\activate.bat
echo python -m scraper %%*
) > start.bat

echo Starting web UI now...
echo.
.venv\Scripts\python.exe -m scraper
pause
