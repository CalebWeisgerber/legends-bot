@echo off
REM populate_db.bat - one-shot: seed Legend clans, then run one full update pass.
REM Uses uv to manage the environment (https://docs.astral.sh/uv/).
setlocal
REM Repo root = parent of this run\ folder.
cd /d "%~dp0.."

where uv >nul 2>nul || ( echo uv not found. Install it: https://docs.astral.sh/uv/ & pause & exit /b 1 )

REM Load local DB credentials if present (run\_env.bat is gitignored).
if exist "run\_env.bat" call "run\_env.bat"

echo === Syncing dependencies (uv) ===
uv sync
if errorlevel 1 ( echo uv sync failed. & pause & exit /b 1 )

echo.
echo === Seeding Legend clans ===
uv run python -m legendsbot.seed
if errorlevel 1 ( echo Seeding failed - check DB/keys. & pause & exit /b 1 )

echo.
echo === Update pass: fetch players, write trophies/logs ===
uv run python -m legendsbot.pipeline
if errorlevel 1 ( echo Update pass failed. & pause & exit /b 1 )

echo.
echo === Done. DB populated. ===
pause
