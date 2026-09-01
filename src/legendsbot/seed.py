import asyncio
import aiohttp
import aiomysql
import json
import os
import time
from itertools import cycle
from typing import List, Tuple
from urllib.parse import quote
from pathlib import Path
from datetime import datetime, timedelta, timezone

from legendsbot.db import upsert_clan, normalize_tag as db_normalize_tag, get_all_clan_tags
from legendsbot.pipeline import (  # {Legend I, II, III} ids + shared resources
    LEGEND_TIER_IDS,
    close_shared,
    get_shared_limiters,
    get_shared_pool,
    get_shared_session,
)
from legendsbot.scan_state import record_seed_run
from legendsbot.paths import KEYS_FILE, LOCATION_IDS_FILE, KNOWN_CLANS_FILE, STATE_DIR

API_BASE = "https://api.clashofclans.com/v1"
RANKING_ENDPOINT = "/locations/{loc_id}/rankings/clans"
PLAYER_RANKING_ENDPOINT = "/locations/{loc_id}/rankings/players"
PER_KEY_RPS = 29
BURST = 29

# Only scan clans with this many points to find Legends.
# 10000 is very low. 35000 is safer to avoid garbage clans.
MIN_CLAN_POINTS = 35000 

with open(KEYS_FILE, "r", encoding="utf-8") as f:
    KEYS: List[str] = list(json.load(f).values())

with open(LOCATION_IDS_FILE, "r", encoding="utf-8") as f:
    LOC_DATA = json.load(f)
LOCATION_IDS = [str(i["id"]) for i in LOC_DATA.get("items", []) if "id" in i]

SEED_STATE_PATH = STATE_DIR / "seed_state.json"
DEBUG_VERBOSE = os.getenv("BOT_DEBUG", "").lower() in ("1", "true", "yes", "on")

def _debug(message: str) -> None:
    if DEBUG_VERBOSE:
        print(f"[DEBUG] {message}")

if DEBUG_VERBOSE:
    print("[DEBUG] async_seed_clans debug logging enabled (BOT_DEBUG)")

# How fresh a seed must be to skip re-running (in hours)
MIN_SEED_INTERVAL_HOURS = 12  # tune this if you want

def _seed_recently_ran() -> bool:
    """
    Returns True if blind seeding ran within MIN_SEED_INTERVAL_HOURS.
    Uses seed_state.json alongside this script.
    """
    try:
        if not SEED_STATE_PATH.exists():
            return False

        raw = SEED_STATE_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        ts_str = data.get("last_seed_run")
        if not ts_str:
            return False

        last = datetime.fromisoformat(ts_str)
        now = datetime.now(timezone.utc)

        # If the parsed datetime is naive, assume UTC
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)

        return (now - last) < timedelta(hours=MIN_SEED_INTERVAL_HOURS)
    except Exception as e:
        print(f"⚠️ Failed to read seed_state.json: {e}")
        return False


def _update_seed_timestamp() -> None:
    """
    Write the current time as the last seed run to seed_state.json.
    """
    try:
        payload = {"last_seed_run": datetime.now(timezone.utc).isoformat()}
        SEED_STATE_PATH.write_text(json.dumps(payload), encoding="utf-8")
    except Exception as e:
        print(f"⚠️ Could not write seed_state.json: {e}")


# Rate limiting, the HTTP session and the DB pool all come from
# legendsbot.pipeline's process-wide shared resources. Seeding used to build its
# own of each per run, which both leaked (see pipeline's shared-resource note)
# and doubled the effective per-key request rate whenever a seed overlapped
# another task.


async def fetch_ranking_items(session, limiter, loc_id: str, key: str) -> List[Tuple[str, str]]:
    url = API_BASE + RANKING_ENDPOINT.format(loc_id=loc_id)
    headers = {"Authorization": f"Bearer {key}"}
    for _ in range(3):
        await limiter.throttle()
        try:
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("items", [])
                    results = []
                    for item in items:
                        if item.get("clanPoints", 0) >= MIN_CLAN_POINTS:
                            results.append((item.get("name"), item.get("tag")))
                    return results
                elif resp.status == 429:
                    retry = int(resp.headers.get("Retry-After", 1))
                    await asyncio.sleep(retry)
                elif resp.status == 404:
                    return []
                else:
                    await asyncio.sleep(1)
        except Exception:
            await asyncio.sleep(1)
    return []

async def fetch_ranking_player_clans(session, limiter, loc_id: str, key: str) -> List[str]:
    """Return clan tags of Legend players in this location's player ranking.

    Clan-points rankings miss clans that contain top players but don't rank by
    points. Player rankings surface those clans directly, widening discovery.
    """
    url = API_BASE + PLAYER_RANKING_ENDPOINT.format(loc_id=loc_id)
    headers = {"Authorization": f"Bearer {key}"}
    for _ in range(3):
        await limiter.throttle()
        try:
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    clan_tags = []
                    for item in data.get("items", []):
                        tier = item.get("leagueTier") or {}
                        if tier.get("id") not in LEGEND_TIER_IDS:
                            continue
                        ctag = (item.get("clan") or {}).get("tag")
                        if ctag:
                            clan_tags.append(ctag)
                    return clan_tags
                elif resp.status == 429:
                    await asyncio.sleep(int(resp.headers.get("Retry-After", 1)))
                elif resp.status == 404:
                    return []
                else:
                    await asyncio.sleep(1)
        except Exception:
            await asyncio.sleep(1)
    return []

async def fetch_all_clans(processes: int = 10, force: bool = False):
    print(f"Starting Blind Seeding (Min Points: {MIN_CLAN_POINTS})...")

    # Skip heavy seeding if we've run it recently and not forcing
    if not force and _seed_recently_ran():
        print(f"⏭️ Blind seeding skipped: ran within the last {MIN_SEED_INTERVAL_HOURS} hours.")
        return

    start = time.perf_counter()

    pool = await get_shared_pool()

    _debug("Loading existing clan tags...")
    async with pool.acquire() as conn:
        known_clans = set(await get_all_clan_tags(conn))
    _debug(f"Loaded {len(known_clans)} known clans.")

    limiter_map = await get_shared_limiters()
    session = await get_shared_session()

    key_cycle = cycle(KEYS)

    new_clans_to_insert = {}

    # Source 1: clan-points rankings (clans with MIN_CLAN_POINTS+).
    clan_tasks = []
    # Source 2: player rankings -> clans of Legend ranked players.
    player_tasks = []
    _debug(f"Fetching clan + player rankings for {len(LOCATION_IDS)} locations...")
    for loc_id in LOCATION_IDS:
        key = next(key_cycle)
        limiter = limiter_map[key]
        clan_tasks.append(fetch_ranking_items(session, limiter, loc_id, key))
        key2 = next(key_cycle)
        player_tasks.append(fetch_ranking_player_clans(session, limiter_map[key2], loc_id, key2))

    results = await asyncio.gather(*clan_tasks)
    player_results = await asyncio.gather(*player_tasks)

    total_found = 0
    for batch in results:
        for name, tag in batch:
            total_found += 1
            norm = db_normalize_tag(tag)
            if norm not in known_clans:
                new_clans_to_insert[tag] = name

    # Merge clans discovered via player rankings (name unknown here; the
    # update pass fills the real name on first member scan).
    player_rank_clans = 0
    for batch in player_results:
        for tag in batch:
            norm = db_normalize_tag(tag)
            if norm not in known_clans and tag not in new_clans_to_insert:
                new_clans_to_insert[tag] = "From Player Ranks"
                player_rank_clans += 1
    print(f"Player-ranking discovery added {player_rank_clans} new clan(s).")

    # Manually-registered clans: the API has no "list all Legend players"
    # endpoint, so Legend clans that never rank in any location's top-200 are
    # invisible to ranking-based discovery. known_clans.txt lets us register
    # them permanently as discovery roots (see add_clans.py).
    # Format: one clan tag per line WITHOUT the leading '#'. Lines starting
    # with '#' are comments (a leading '#' would be ambiguous with tags).
    known_file = KNOWN_CLANS_FILE
    manual_added = 0
    if known_file.exists():
        for line in known_file.read_text(encoding="utf-8").splitlines():
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            norm = db_normalize_tag(t)      # strip stray '#'/whitespace, uppercase
            raw = "#" + norm                # canonical raw tag for upsert_clan
            if norm not in known_clans and raw not in new_clans_to_insert:
                new_clans_to_insert[raw] = "Manual Seed"
                manual_added += 1
        if manual_added:
            print(f"known_clans.txt: {manual_added} manually-registered clan(s) queued.")

    print(f"Scan complete. Found {total_found} total. {len(new_clans_to_insert)} are NEW.")
    _update_seed_timestamp()
    
    if new_clans_to_insert:
        print("Inserting new clans into DB...")
        async with pool.acquire() as conn:
            count = 0
            for tag, name in new_clans_to_insert.items():
                await upsert_clan(conn, name, tag)
                count += 1
                if count % 1000 == 0:
                    _debug(f"  Inserted {count}...")
                    
    elapsed = time.perf_counter() - start
    print(f"Seeding complete in {elapsed:.2f}s")
    record_seed_run()

if __name__ == "__main__":
    async def _standalone() -> None:
        try:
            await fetch_all_clans(force=True)
        finally:
            await close_shared()

    asyncio.run(_standalone())
