@echo off
REM start_bot.bat - run the scheduler 24/7 (seed/snapshot/update jobs + 05:00 UTC reset).
REM Uses uv to manage the environment (https://docs.astral.sh/uv/).
setlocal
cd /d "%~dp0.."

where uv >nul 2>nul || ( echo uv not found. Install it: https://docs.astral.sh/uv/ & pause & exit /b 1 )

REM Load local DB credentials if present (run\_env.bat is gitignored).
if exist "run\_env.bat" call "run\_env.bat"

uv sync
if errorlevel 1 ( echo uv sync failed. & pause & exit /b 1 )

echo === Starting Clash Legends Bot scheduler (Ctrl+C to stop) ===
uv run python -m legendsbot.main
pause
