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

REM Capture stdout+stderr. startup_stack.ps1 launches this with -WindowStyle
REM Hidden and no redirect, so an unhandled exception printed its traceback to
REM a console nobody could see and the process just vanished: 8 outages >=30min
REM in the 58 days to 2026-08-23 (153h dead, worst 99h) left ZERO diagnostic
REM evidence, not even a Windows event. Rotate on launch so the file can't grow
REM unbounded while the previous run's dying words survive exactly one restart
REM -- that's the crash worth reading. PYTHONUNBUFFERED=1 matters: without it a
REM hard exit can lose the buffered tail, which is the part that says why.
set "LB_LOG=%CD%\state\bot_run.log"
if not exist "%CD%\state" mkdir "%CD%\state" >nul 2>&1
if exist "%LB_LOG%" move /Y "%LB_LOG%" "%LB_LOG%.prev" >nul 2>&1
set "PYTHONUNBUFFERED=1"
REM Redirected stdout defaults to the ANSI codepage, so every emoji log line
REM raised UnicodeEncodeError *from print()* -- inside except handlers, which
REM turned it into lost work (see legendsbot/__init__.py). legendsbot/__init__
REM also reconfigures the streams; this covers anything importing it later.
set "PYTHONIOENCODING=utf-8:replace"

echo === Starting Clash Legends Bot scheduler (Ctrl+C to stop) ===
uv run python -m legendsbot.main >> "%LB_LOG%" 2>&1
echo [%DATE% %TIME%] scheduler EXITED with errorlevel %ERRORLEVEL% >> "%LB_LOG%"
pause
