@echo off
setlocal enabledelayedexpansion
:: ─────────────────────────────────────────────────────────────────────────────
::  Lead Scraper — Update script (Windows)
::  Pulls latest code from GitHub and reinstalls dependencies if needed.
:: ─────────────────────────────────────────────────────────────────────────────

echo.
echo ╔══════════════════════════════════════╗
echo ║      Lead Scraper  —  Updater        ║
echo ╚══════════════════════════════════════╝
echo.

:: ── Git check ─────────────────────────────────────────────────────────────────
where git >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: git not found. Install Git from https://git-scm.com
    pause
    exit /b 1
)

:: ── Ensure we're inside the repo ─────────────────────────────────────────────
if not exist ".git\" (
    echo ERROR: Run this script from the lead-scraper directory.
    pause
    exit /b 1
)

:: ── Fetch ─────────────────────────────────────────────────────────────────────
echo [1/4] Checking for updates...
git fetch origin main --quiet

for /f %%i in ('git rev-parse HEAD') do set LOCAL=%%i
for /f %%i in ('git rev-parse origin/main') do set REMOTE=%%i

if "%LOCAL%"=="%REMOTE%" (
    echo        Already up to date.
    echo.
    goto done_no_update
)

:: Count commits behind
for /f %%c in ('git rev-list --count HEAD..origin/main') do set BEHIND=%%c
echo        %BEHIND% new commit(s) available:
git log --oneline HEAD..origin/main
echo.

:: ── Pull ──────────────────────────────────────────────────────────────────────
echo [2/4] Pulling latest code...
git pull origin main --quiet
echo        Code updated

:: ── Reinstall deps if changed ─────────────────────────────────────────────────
echo [3/4] Checking dependencies...
git diff HEAD@{1} HEAD --name-only 2>nul | findstr /i "requirements.txt pyproject.toml" >nul 2>&1
if %errorlevel% equ 0 (
    echo        Dependencies changed - reinstalling...
    if exist ".venv\Scripts\pip.exe" (
        .venv\Scripts\pip.exe install -r requirements.txt -q
        .venv\Scripts\pip.exe install -e . -q
        echo        Dependencies updated
    ) else (
        echo        Virtual env not found - run setup.bat first.
    )
) else (
    echo        No dependency changes
)

:: ── Done ──────────────────────────────────────────────────────────────────────
echo [4/4] Done!
echo.
echo Update complete! Run start.bat to launch.
echo.
pause
exit /b 0

:done_no_update
echo Already on the latest version.
echo Run start.bat to launch.
echo.
pause
