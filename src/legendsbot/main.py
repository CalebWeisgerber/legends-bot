# main.py

import asyncio
import os
import time
from random import randint
from datetime import datetime
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from legendsbot.pipeline import main as run_full_update
from legendsbot.pipeline import run_snapshot_reset_clans
from legendsbot.seed import fetch_all_clans
from legendsbot.scan_state import seed_recently_ran, update_recently_ran
from legendsbot.league_history import ingest as run_league_history
from legendsbot.db import make_pool

API_LOCK = asyncio.Lock()
DEBUG_VERBOSE = os.getenv("BOT_DEBUG", "").lower() in ("1", "true", "yes", "on")
QUIET_MODE = os.getenv("BOT_QUIET", "").lower() in ("1", "true", "yes", "on")

def _log(message: str, force: bool = False) -> None:
    if QUIET_MODE and not force:
        return
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def _debug(message: str) -> None:
    if DEBUG_VERBOSE:
        _log(f"[DEBUG] {message}")

if DEBUG_VERBOSE:
    _log("Debug logging enabled (BOT_DEBUG)", force=True)
if QUIET_MODE:
    _log("Quiet mode enabled (BOT_QUIET)", force=True)

# ==========================================
#        TASK DEFINITIONS
# ==========================================

async def task_pre_reset_seed():
    _debug("[PRE-RESET SEED] Waiting for lock...")
    async with API_LOCK:
        try:
            start = time.perf_counter()
            _log("[PRE-RESET SEED] Starting deep scan...")
            await fetch_all_clans()
            duration = time.perf_counter() - start
            _log(f"[PRE-RESET SEED] Complete in {duration:.2f}s.")
        except Exception as exc:
            _log(f"[PRE-RESET SEED] FAILED: {exc}", force=True)

async def task_reset_snapshot():
    _debug("[SNAPSHOT] Waiting for lock (Priority)...")
    async with API_LOCK:
        try:
            start = time.perf_counter()
            _log("[SNAPSHOT] Taking Daily Snapshot...")
            await run_snapshot_reset_clans()
            duration = time.perf_counter() - start
            _log(f"[SNAPSHOT] Snapshot Complete in {duration:.2f}s.")
        except Exception as exc:
            _log(f"[SNAPSHOT] FAILED: {exc}", force=True)

async def sequence_morning_seed():
    _debug("[MORNING] Waiting for lock...")
    async with API_LOCK:
        try:
            start = time.perf_counter()
            _log("[MORNING] Starting Morning Seed + Update...")
            await fetch_all_clans()
            await run_full_update()
            duration = time.perf_counter() - start
            _log(f"[MORNING] Complete in {duration:.2f}s.")
        except Exception as exc:
            _log(f"[MORNING] Failed: {exc}", force=True)

async def task_routine_update():
    if API_LOCK.locked():
        _debug("[UPDATE] Skipped: API is busy.")
        return

    async with API_LOCK:
        try:
            start = time.perf_counter()
            _log("[UPDATE] Starting routine stats update...")
            await run_full_update()
            duration = time.perf_counter() - start
            _log(f"[UPDATE] Complete in {duration:.2f}s.")
        except Exception as exc:
            _log(f"[UPDATE] Failed: {exc}", force=True)

async def task_league_history():
    """Daily: snapshot each active player's league-season summary."""
    async with API_LOCK:
        try:
            start = time.perf_counter()
            _log("[LEAGUE-HISTORY] Ingesting season summaries...")
            pool = await make_pool(minsize=5, maxsize=50)
            try:
                await run_league_history(pool)
            finally:
                pool.close()
                await pool.wait_closed()
            duration = time.perf_counter() - start
            _log(f"[LEAGUE-HISTORY] Complete in {duration:.2f}s.")
        except Exception as exc:
            _log(f"[LEAGUE-HISTORY] Failed: {exc}", force=True)

async def run_startup():
    _log("Bot starting. Checking if startup sync is needed...")

    if seed_recently_ran() and update_recently_ran():
        _log("Startup work skipped: recent seed/update detected.")
        return

    _log("Waiting for lock for startup tasks...")
    async with API_LOCK:
        try:
            if not seed_recently_ran():
                _log("Startup seed running...")
                await fetch_all_clans()
            else:
                _log("Skipping seed; ran recently.")

            if not update_recently_ran():
                _log("Startup update running...")
                await run_full_update()
            else:
                _log("Skipping update; ran recently.")
        except Exception as exc:
            _log(f"Startup tasks failed: {exc}", force=True)

# ==========================================
#           MAIN ENTRY POINT
# ==========================================

async def main():
    _log("--- Clash Legends Bot Scheduler Starting ---", force=QUIET_MODE)
    
    scheduler = AsyncIOScheduler()
    utc_tz = pytz.utc

    scheduler.add_job(
        task_pre_reset_seed,
        trigger=CronTrigger(hour=4, minute=50, timezone=utc_tz),
        id="seed_pre_reset",
        name="Pre-Reset Seeding"
    )

    scheduler.add_job(
        task_reset_snapshot,
        trigger=CronTrigger(hour=4, minute=59, timezone=utc_tz),
        id="snapshot_reset",
        name="Daily Snapshot"
    )

    scheduler.add_job(
        sequence_morning_seed,
        trigger=CronTrigger(hour=17, minute=0, timezone=utc_tz),
        id="seq_morning",
        name="Morning Sequence"
    )

    scheduler.add_job(
        task_routine_update,
        trigger=IntervalTrigger(minutes=3, jitter=randint(10, 30)),
        id="routine_update",
        name="Routine Update"
    )

    # League-season summaries (heavy: 1 call/active player). Daily after reset.
    scheduler.add_job(
        task_league_history,
        trigger=CronTrigger(hour=6, minute=0, timezone=utc_tz),
        id="league_history",
        name="League History"
    )

    _debug("\nActive Schedule (UTC Times):")
    if DEBUG_VERBOSE:
        for job in scheduler.get_jobs():
            _debug(f" - {job.name}: {job.trigger}")
        _debug("-" * 40)

    scheduler.start()
    asyncio.create_task(run_startup())

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        _log("🛑 Scheduler cancelled.", force=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        _log("Bot shutting down gracefully.", force=True)
