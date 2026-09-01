# main.py

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from random import randint
from datetime import datetime, timedelta, timezone
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from legendsbot.pipeline import main as run_full_update
from legendsbot.pipeline import run_snapshot_reset_clans
from legendsbot.seed import fetch_all_clans
from legendsbot.scan_state import (seed_recently_ran, update_recently_ran,
                                   _load_state)
from legendsbot.league_history import ingest as run_league_history
from legendsbot.pipeline import close_shared, get_shared_pool, get_shared_session

API_LOCK = asyncio.Lock()
# Who holds API_LOCK and since when. Every scheduled task serialises on this
# lock, and task_routine_update SKIPS (rather than queues) while it's held --
# so a task that hangs while holding it silently stops all ingestion with the
# process still alive, the scheduler still ticking, and nothing logged. That
# is the signature of the outages measured 2026-08-23: 8 stretches >=30min in
# 58 days (153h dead, 88.8% uptime, worst 99h), none of which produced a
# Windows event or a single log line, each ending only at a machine reboot.
_lock_holder: str | None = None
_lock_since: float = 0.0
# Warn once per this many consecutive skips so a stuck lock is loud but a
# normal long pass doesn't spam.
LOCK_STUCK_WARN_SECONDS = 600
_skip_streak = 0
# Proof-of-life cadence, and how long ingestion may go without a successful
# update before the heartbeat shouts (routine updates run every 3 min, so 20
# min is ~6 missed cycles -- well clear of one slow pass).
HEARTBEAT_SECONDS = int(os.getenv("BOT_HEARTBEAT_SECONDS", "300"))
STALE_INGEST_ALERT_MINUTES = int(os.getenv("BOT_STALE_INGEST_MINUTES", "20"))
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


@asynccontextmanager
async def _hold_lock(name: str):
    """Take API_LOCK and record WHO holds it and SINCE WHEN, so a stall names
    the culprit task instead of just going quiet. Every task that serialises on
    API_LOCK must acquire it through here."""
    global _lock_holder, _lock_since
    async with API_LOCK:
        _lock_holder, _lock_since = name, time.monotonic()
        try:
            yield
        finally:
            _lock_holder, _lock_since = None, 0.0


async def _heartbeat() -> None:
    """Periodic proof-of-life with the one fact that matters: when did we last
    actually ingest anything. Without this, 'process is running' was the only
    signal available and it stayed true through every one of the 2026 outages.
    """
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        try:
            state = _load_state()
            last = state.get("last_update_run")
            age = "never"
            if last:
                delta = datetime.now(timezone.utc) - datetime.fromisoformat(last)
                age = f"{delta.total_seconds()/60:.0f} min ago"
            holder = (f"'{_lock_holder}' for "
                      f"{(time.monotonic() - _lock_since)/60:.1f} min"
                      if _lock_holder else "free")
            stale = last and (datetime.now(timezone.utc)
                              - datetime.fromisoformat(last)) > timedelta(
                                  minutes=STALE_INGEST_ALERT_MINUTES)
            _log(f"[HEARTBEAT] last successful update: {age} | "
                 f"API_LOCK: {holder}"
                 + ("  ⚠️ INGESTION STALE" if stale else ""),
                 force=bool(stale))
        except Exception:
            logging.getLogger(__name__).exception("[HEARTBEAT] failed")

# ==========================================
#        TASK DEFINITIONS
# ==========================================

async def task_pre_reset_seed():
    _debug("[PRE-RESET SEED] Waiting for lock...")
    async with _hold_lock("pre_reset_seed"):
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
    async with _hold_lock("reset_snapshot"):
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
    async with _hold_lock("morning_seed"):
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
    global _skip_streak
    if API_LOCK.locked():
        # NEVER let this be silent. This used to be _debug(), i.e. invisible
        # unless BOT_DEBUG was set, so a stuck lock looked exactly like a
        # healthy idle bot while ingestion was totally stopped.
        _skip_streak += 1
        held = time.monotonic() - _lock_since if _lock_since else 0.0
        if held >= LOCK_STUCK_WARN_SECONDS:
            _log(f"[UPDATE] ⚠️ STALLED: skipped {_skip_streak} run(s); "
                 f"API_LOCK held by '{_lock_holder}' for {held/60:.1f} min. "
                 f"Ingestion is STOPPED until that task returns.", force=True)
        else:
            _debug(f"[UPDATE] Skipped: '{_lock_holder}' busy {held:.0f}s.")
        return

    _skip_streak = 0
    async with _hold_lock("routine_update"):
        try:
            start = time.perf_counter()
            _log("[UPDATE] Starting routine stats update...")
            await run_full_update()
            duration = time.perf_counter() - start
            _log(f"[UPDATE] Complete in {duration:.2f}s.")
        except Exception as exc:
            # force=True: a swallowed traceback here is how a systemic failure
            # (bad credentials, API change, pool exhaustion) stays invisible.
            _log(f"[UPDATE] Failed: {exc!r}", force=True)
            logging.getLogger(__name__).exception("[UPDATE] traceback")

async def task_league_history():
    """Daily: snapshot each active player's league-season summary."""
    async with _hold_lock("league_history"):
        try:
            start = time.perf_counter()
            _log("[LEAGUE-HISTORY] Ingesting season summaries...")
            await run_league_history()
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
    async with _hold_lock("startup"):
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

    # Build the DB pool and HTTP session ONCE, here, for the life of the
    # process. Every task below reuses them via legendsbot.pipeline's shared
    # accessors. They used to be created and destroyed on every 3-minute pass
    # (~480 pools/day, each up to 100 connections), which is what made RSS
    # climb steadily for days on end.
    await get_shared_pool()
    await get_shared_session()

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
    asyncio.create_task(_heartbeat())

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        _log("🛑 Scheduler cancelled.", force=True)
    finally:
        scheduler.shutdown(wait=False)
        await close_shared()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        _log("Bot shutting down gracefully.", force=True)
