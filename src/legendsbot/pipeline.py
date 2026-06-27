import asyncio
import json
import os
import time
from itertools import cycle
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote
from pathlib import Path
from datetime import datetime, timedelta, timezone

import aiohttp
import aiomysql

from legendsbot.db import (
    bulk_update_seen,
    delete_player_and_logs,
    get_all_player_tags,
    make_pool,
    safe_delete_clan,
    snapshot_reset_clan_tags,
    upsert_clan,
    upsert_player,
    mark_clan_active,
    mark_clan_inactive
)
from legendsbot.scan_state import record_update_run
from legendsbot.paths import KEYS_FILE

API_BASE = "https://api.clashofclans.com/v1"
CLAN_ENDPOINT = "/clans/{clan_tag}"
PLAYER_ENDPOINT = "/players/{player_tag}"
BATTLELOG_ENDPOINT = "/players/{player_tag}/battlelog"
LEAGUEHISTORY_ENDPOINT = "/players/{player_tag}/leaguehistory"

# --- CONFIGURATION ---
# Clash API limit is ~30-40 req/s per key; 29 stays safely under 30 to avoid
# any 429 back-off. With N keys this is ~29*N req/s total.
PER_KEY_RPS = 29
BURST = 29

# --- Legend tier targeting ---
# Apr 2026 split Legend League into 3 tiers; Jun 2026 labeled them
# "Legend I" / "Legend II" / "Legend III" (see Clash API changelog).
# We track Legend I and Legend II ONLY (Legend III excluded by design).
#
# Match is name-based (authoritative labels, exact-match, no prefix test so
# "Legend III" can't sneak in). The id set is a stronger reinforcement once the
# live leagueTier.id values are confirmed via verify_tiers.py on the API host.
LEGEND_TIER_NAMES = {"legend i", "legend ii"}   # excludes "legend iii"
# Verified via /leaguetiers (verify_tiers.py / probe_leagues.py):
#   105000036 Legend I, 105000035 Legend II, 105000034 Legend III (excluded).
LEGEND_TIER_IDS: set[int] = {105000036, 105000035}

STALE_CLAN_MINUTES = 4  # Only rescan clans whose member scan is older than this

with open(KEYS_FILE, "r", encoding="utf-8") as f:
    KEYS: List[str] = list(json.load(f).values())

CONCURRENCY = min(max(100, len(KEYS) * BURST), 200)
DEBUG_VERBOSE = os.getenv("BOT_DEBUG", "").lower() in ("1", "true", "yes", "on")

def _debug(message: str) -> None:
    if DEBUG_VERBOSE:
        print(f"[DEBUG] {message}")
def _progress(message: str) -> None:
    # Lightweight progress that still shows in quiet mode
    print(message)

class RateLimiter:
    def __init__(self, rate: int, burst: int):
        self.rate = max(rate, 1)
        self.burst = max(burst, 1)
        self.semaphore = asyncio.Semaphore(self.burst)
        self._refill_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if not self._refill_task:
            self._refill_task = asyncio.create_task(self._refill_loop())

    async def stop(self) -> None:
        if self._refill_task:
            self._refill_task.cancel()
            try:
                await self._refill_task
            except asyncio.CancelledError:
                pass
            self._refill_task = None

    async def _refill_loop(self) -> None:
        delay = 1 / self.rate
        try:
            while True:
                await asyncio.sleep(delay)
                if self.semaphore._value < self.burst:
                    self.semaphore.release()
        except asyncio.CancelledError:
            return

    async def throttle(self) -> None:
        await self.semaphore.acquire()

def is_legend(player_like: Dict[str, Any]) -> bool:
    """
    Return True only for Legend I / Legend II players (Legend III excluded).

    Uses the new leagueTier field (present on both /players and clan member
    entries). Trophies are NOT used to determine Legend membership anymore.
    """
    if not isinstance(player_like, dict):
        return False

    tier = player_like.get("leagueTier") or {}
    tier_id = tier.get("id")
    tier_name = (tier.get("name") or "").strip().lower()

    # Id match (strongest) once verified ids are populated.
    if tier_id is not None and tier_id in LEGEND_TIER_IDS:
        return True

    # Exact-name match. NOT a prefix test: "legend iii" must never match.
    return tier_name in LEGEND_TIER_NAMES

def _enc_tag(tag: str) -> str:
    if not tag.startswith("#"):
        tag = f"#{tag}"
    return quote(tag)

async def fetch_clan(session: aiohttp.ClientSession, limiter: RateLimiter, tag: str, key: str) -> Dict[str, Any]:
    url = f"{API_BASE}{CLAN_ENDPOINT.format(clan_tag=_enc_tag(tag))}"
    headers = {"Authorization": f"Bearer {key}"}
    for _ in range(3):
        await limiter.throttle()
        try:
            async with session.get(url, headers=headers, timeout=12) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 404:
                    return {"not_found": True}
                if resp.status == 429:
                    await asyncio.sleep(int(resp.headers.get("Retry-After", 1)))
                    continue
        except Exception as exc:
            # Don't print stack trace for timeouts, just retry
            await asyncio.sleep(0.5)
    return {"error": True}

async def fetch_player(session: aiohttp.ClientSession, limiter: RateLimiter, tag: str, key: str) -> Dict[str, Any]:
    url = f"{API_BASE}{PLAYER_ENDPOINT.format(player_tag=_enc_tag(tag))}"
    headers = {"Authorization": f"Bearer {key}"}
    for _ in range(3):
        await limiter.throttle()
        try:
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 404:
                    return {"not_found": True}
                if resp.status == 429:
                    await asyncio.sleep(int(resp.headers.get("Retry-After", 1)))
                    continue
        except Exception:
            await asyncio.sleep(0.5)
    return {"error": True}

async def _fetch_items(session, limiter, url, key) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {key}"}
    for _ in range(3):
        await limiter.throttle()
        try:
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 404:
                    return {"not_found": True}
                if resp.status == 429:
                    await asyncio.sleep(int(resp.headers.get("Retry-After", 1)))
                    continue
                # 403/503 etc. (battlelog has been disabled before) -> give up softly
                return {"error": True}
        except Exception:
            await asyncio.sleep(0.5)
    return {"error": True}

async def fetch_battlelog(session, limiter, tag, key) -> Dict[str, Any]:
    url = f"{API_BASE}{BATTLELOG_ENDPOINT.format(player_tag=_enc_tag(tag))}"
    return await _fetch_items(session, limiter, url, key)

async def fetch_leaguehistory(session, limiter, tag, key) -> Dict[str, Any]:
    url = f"{API_BASE}{LEAGUEHISTORY_ENDPOINT.format(player_tag=_enc_tag(tag))}"
    return await _fetch_items(session, limiter, url, key)

def _parse_battle_ts(s: str) -> Optional[datetime]:
    # Clash compact ISO, e.g. "20260625T032732.000Z" (UTC). Store naive UTC.
    try:
        return datetime.strptime(s, "%Y%m%dT%H%M%S.%fZ")
    except Exception:
        return None

def parse_legend_battles(items: List[Dict[str, Any]], since: Optional[datetime]) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
    """Return (new legend battles after `since`, new max battle time)."""
    battles: List[Dict[str, Any]] = []
    max_t = since
    for it in items or []:
        if (it.get("battleType") or "").lower() != "legend":
            continue
        t = _parse_battle_ts(it.get("battleTimestamp", ""))
        if t is None:
            continue
        if since is not None and t <= since:
            continue
        battles.append({
            "time": t,
            "is_attack": bool(it.get("attack")),
            "opponent_tag": it.get("opponentPlayerTag"),
            "opponent_name": it.get("opponentName"),
            "opponent_th": it.get("opponentTownHallLevel"),
            "stars": it.get("stars"),
            "destruction": it.get("destructionPercentage"),
            "duration": it.get("battleTime"),
        })
        if max_t is None or t > max_t:
            max_t = t
    return battles, max_t

async def load_battle_cursors(pool: aiomysql.Pool) -> Dict[str, Optional[datetime]]:
    cursors: Dict[str, Optional[datetime]] = {}
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT player_tag, last_battle_time FROM player WHERE is_active = 1"
            )
            for tag, ts in await cur.fetchall():
                clean = tag.upper() if tag else ""
                if clean:
                    cursors[clean] = ts
    return cursors

async def get_clans_to_scan(conn) -> List[str]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT DISTINCT clan_tag
            FROM clan
            WHERE is_active = 1
              AND (last_member_scan IS NULL OR last_member_scan < UTC_TIMESTAMP() - INTERVAL %s MINUTE)
            """
            ,
            (STALE_CLAN_MINUTES,)
        )
        rows = await cur.fetchall()
        return [row[0] for row in rows if row and row[0]]

async def get_players_in_clan(conn, clan_tag: str) -> List[str]:
    async with conn.cursor() as cur:
        await cur.execute("SELECT player_tag FROM player WHERE current_clan_tag = %s", (clan_tag,))
        rows = await cur.fetchall()
        return [row[0] for row in rows]

async def load_trophy_cache(pool: aiomysql.Pool) -> Dict[str, int]:
    cache: Dict[str, int] = {}
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT player_tag, trophies FROM player WHERE is_active = 1")
            for tag, trophies in await cur.fetchall():
                clean_tag = tag.upper() if tag else ""
                if clean_tag:
                    cache[clean_tag] = int(trophies or 0)
    return cache

async def touch_clan_timestamp(pool: aiomysql.Pool, clan_tag: str):
    """Updates scan timestamp even if fetch failed, so we don't spam 429s."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
             await cur.execute("UPDATE clan SET last_member_scan = UTC_TIMESTAMP() WHERE clan_tag = %s", (clan_tag,))
        await conn.commit()

async def process_clan(
    pool: aiomysql.Pool,
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    key: str,
    clan_tag: str,
    trophy_cache: Dict[str, int],
    battle_cursor: Dict[str, Any],
) -> Tuple[Set[str], Set[str], int]:
    seen_tags: Set[str] = set()
    changed_tags: Set[str] = set()
    legend_seen_count = 0

    clan = await fetch_clan(session, limiter, clan_tag, key)

    if clan.get("error"):
        # NETWORK FAIL: Mark timestamp so we don't retry immediately, but keep active status
        await touch_clan_timestamp(pool, clan_tag)
        async with pool.acquire() as conn:
            safe_members = await get_players_in_clan(conn, clan_tag)
            seen_tags.update(t.replace("#", "").upper() for t in safe_members)
        return seen_tags, changed_tags, legend_seen_count

    if clan.get("not_found"):
        async with pool.acquire() as conn:
            await safe_delete_clan(conn, clan_tag)
        return seen_tags, changed_tags, legend_seen_count

    members = clan.get("memberList") or []
    clan_name = clan.get("name", "Unknown Clan")

    # ---- Pass 1: classify members from the clan memberList (no DB/network) ----
    # memberList carries name/trophies/leagueTier. Members whose trophies changed
    # get a /battlelog fetch for exact attack/defense counts + per-battle detail.
    non_legend_tracked: List[Tuple[str, int]] = []
    tags_to_bulk_update: List[str] = []
    changed_members: List[Tuple[str, str, Any, int]] = []  # (tag, raw_tag, name, trophies)
    legend_count_in_clan = 0

    for member in members:
        raw_tag = member.get("tag")
        if not raw_tag:
            continue
        tag = raw_tag.strip("#").upper()
        new_trophies = int(member.get("trophies", 0))

        # Legend status from leagueTier only (Legend I/II), never trophies.
        if not is_legend(member):
            if tag in trophy_cache:
                non_legend_tracked.append((tag, new_trophies))
            continue

        seen_tags.add(tag)
        legend_count_in_clan += 1
        legend_seen_count += 1

        previous = trophy_cache.get(tag)
        if previous is not None and previous == new_trophies:
            tags_to_bulk_update.append(tag)        # fast path: nothing changed
        else:
            changed_members.append((tag, raw_tag, member.get("name"), new_trophies))

    # ---- Fetch /battlelog for changed members (counts + per-battle detail) ----
    fetched: Dict[str, Dict[str, Any]] = {}
    for tag, raw_tag, name, new_trophies in changed_members:
        bl = await fetch_battlelog(session, limiter, raw_tag, key)
        if bl.get("error") or bl.get("not_found"):
            # battlelog can be temporarily disabled; still record the trophy
            # change (magnitude) without battle detail rather than skipping.
            battles, max_t = [], battle_cursor.get(tag)
        else:
            battles, max_t = parse_legend_battles(bl.get("items", []), battle_cursor.get(tag))
        fetched[tag] = {
            "tag": tag,
            "name": name,
            "trophies": new_trophies,
            "clan": {"tag": clan_tag, "name": clan_name},
            "battles": battles,
            "last_battle_time": max_t,
        }

    # ---- DB transaction (no network inside) ----
    async with pool.acquire() as conn:
        try:
            await conn.begin()
            await upsert_clan(conn, clan_name, clan_tag, commit=False)

            for tag, new_trophies in non_legend_tracked:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE player SET is_active = 0, trophies = %s, last_seen = CURDATE() WHERE player_tag = %s",
                        (new_trophies, tag),
                    )
                trophy_cache.pop(tag, None)

            for tag, payload in fetched.items():
                await upsert_player(conn, payload, commit=False)
                changed_tags.add(tag)
                trophy_cache[tag] = int(payload["trophies"] or 0)
                if payload.get("last_battle_time") is not None:
                    battle_cursor[tag] = payload["last_battle_time"]

            if tags_to_bulk_update:
                await bulk_update_seen(conn, tags_to_bulk_update, clan_tag, commit=False)

            if legend_count_in_clan == 0:
                await mark_clan_inactive(conn, clan_tag)
            else:
                await mark_clan_active(conn, clan_tag)

            await conn.commit()

        except Exception as e:
            await conn.rollback()
            print(f"⚠️ DB Transaction failed for clan {clan_tag}: {e}")
            await touch_clan_timestamp(pool, clan_tag)

    return seen_tags, changed_tags, legend_seen_count

async def process_unseen(
    pool: aiomysql.Pool,
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    key: str,
    player_tag: str,
    trophy_cache: Dict[str, int],
    battle_cursor: Dict[str, Any],
) -> str:
    data = await fetch_player(session, limiter, player_tag, key)

    if data.get("error"):
        return "error"

    if data.get("not_found") or not is_legend(data):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE player SET is_active = 0, last_seen = CURDATE() WHERE player_tag = %s",
                    (player_tag,)
                )
            await conn.commit()
        trophy_cache.pop(player_tag, None)
        return "archived"

    # Still Legend: fetch battlelog for counts/detail (clanless players only
    # reach the DB here, so this is the sole path for their battles).
    bl = await fetch_battlelog(session, limiter, player_tag, key)
    since = battle_cursor.get(player_tag)
    if bl.get("error") or bl.get("not_found"):
        battles, max_t = [], since
    else:
        battles, max_t = parse_legend_battles(bl.get("items", []), since)

    async with pool.acquire() as conn:
        try:
            await conn.begin()
            new_clan = data.get("clan") or {}
            payload = {
                "tag": player_tag,
                "name": data.get("name"),
                "trophies": data.get("trophies"),
                "clan": {"tag": new_clan.get("tag"), "name": new_clan.get("name", "")},
                "battles": battles,
                "last_battle_time": max_t,
            }
            await upsert_player(conn, payload, commit=False)
            await conn.commit()
            if max_t is not None:
                battle_cursor[player_tag] = max_t
            return "updated"

        except Exception:
            await conn.rollback()
            return "error"

_SEM: Optional[asyncio.Semaphore] = None

async def _bounded(coro):
    assert _SEM is not None
    async with _SEM:
        return await coro

async def update_all(pool: aiomysql.Pool) -> None:
    global _SEM
    _SEM = asyncio.Semaphore(CONCURRENCY)
    success = False

    limiters = {key: RateLimiter(PER_KEY_RPS, BURST) for key in KEYS}
    for limiter in limiters.values():
        await limiter.start()

    start_time = time.perf_counter()

    try:
        connector = aiohttp.TCPConnector(limit=0, limit_per_host=0, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with pool.acquire() as conn:
                all_clans = await get_clans_to_scan(conn)
            print(f"Updating {len(all_clans)} clans")

            trophy_cache = await load_trophy_cache(pool)
            battle_cursor = await load_battle_cursors(pool)
            key_cycle = cycle(KEYS)
            seen: Set[str] = set()

            clan_tasks = []
            for clan_tag in all_clans:
                key = next(key_cycle)
                clan_tasks.append(
                    asyncio.create_task(
                        _bounded(
                            process_clan(
                                pool=pool,
                                session=session,
                                limiter=limiters[key],
                                key=key,
                                clan_tag=clan_tag,
                                trophy_cache=trophy_cache,
                                battle_cursor=battle_cursor,
                            )
                        )
                    )
                )

            processed = 0
            total_trophy_changes = 0 # NEW COUNTER
            
            progress_log_threshold = 500
            for future in asyncio.as_completed(clan_tasks):
                try:
                    # FIX: Capture the changed_tags here
                    seen_tags, changed_tags, _ = await future
                    seen.update(seen_tags)
                    total_trophy_changes += len(changed_tags)
                    
                    processed += 1
                    if processed >= progress_log_threshold:
                        _progress(f"[CLANS] {processed}/{len(all_clans)} | legends: {len(seen)} | hits: {total_trophy_changes}")
                        progress_log_threshold += 500
                except Exception as exc:
                    print(f"Clan task failed: {exc}")

            async with pool.acquire() as conn:
                all_players = await get_all_player_tags(conn)

            unseen = sorted(set(all_players) - seen)
            
            # Reconcile Unseen
            unseen_tasks = []
            if unseen:
                _progress(f"[UNSEEN] Reconciling {len(unseen)} players...")
                for player_tag in unseen:
                    key = next(key_cycle)
                    unseen_tasks.append(
                        asyncio.create_task(
                            _bounded(
                                process_unseen(
                                    pool=pool,
                                    session=session,
                                    limiter=limiters[key],
                                    key=key,
                                    player_tag=player_tag,
                                    trophy_cache=trophy_cache,
                                    battle_cursor=battle_cursor,
                                )
                            )
                        )
                    )

            reconciled_updated = 0
            reconciled_archived = 0
            
            if unseen_tasks:
                completed = 0
                unseen_log_threshold = 200
                for future in asyncio.as_completed(unseen_tasks):
                    try:
                        result = await future
                        if result == "updated":
                            reconciled_updated += 1
                        elif result == "archived":
                            reconciled_archived += 1
                        completed += 1
                        if completed >= unseen_log_threshold:
                            _progress(f"[UNSEEN] {completed}/{len(unseen)} reconciled")
                            unseen_log_threshold += 200
                    except Exception:
                        pass

            elapsed = time.perf_counter() - start_time
            print(f"Run Complete in {elapsed:.1f}s")
            print(f" > Clan Members Changed: {total_trophy_changes}")
            print(f" > Unseen Updated:       {reconciled_updated}")
            print(f" > Unseen Archived:      {reconciled_archived}")
            
            success = True
    finally:
        for limiter in limiters.values():
            await limiter.stop()
    if success:
        record_update_run()

async def run_snapshot_reset_clans() -> None:
    pool = await make_pool(minsize=5, maxsize=50)
    try:
        async with pool.acquire() as conn:
            await snapshot_reset_clan_tags(conn)
    finally:
        pool.close()
        await pool.wait_closed()

async def main() -> None:
    pool = await make_pool(minsize=10, maxsize=100)
    try:
        await update_all(pool)
    finally:
        pool.close()
        await pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
