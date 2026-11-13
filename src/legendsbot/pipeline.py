import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from itertools import cycle
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote
from pathlib import Path
from datetime import datetime, timedelta, timezone

import aiohttp
import aiomysql

from legendsbot.db import (
    delete_player_and_logs,
    get_all_player_tags,
    make_pool,
    safe_delete_clan,
    snapshot_reset_clan_tags,
    upsert_clan,
    upsert_player,
    mark_clan_active,
    mark_clan_inactive,
    reconcile_mirror_duplicates,
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
# We track all three Legend tiers (Legend I, II, III).
#
# Match is name-based (authoritative labels, exact-match). The id set is a
# stronger reinforcement once the live leagueTier.id values are confirmed via
# verify_tiers.py on the API host.
LEGEND_TIER_NAMES = {"legend i", "legend ii", "legend iii"}

# --- Ranked battle types ---
# battlelog `battleType` for a RANKED (trophy) battle depends on the player's
# Legend tier -- verified 2026-08-23 against live battlelogs, 24 players, all
# battles since 08-21:
#     Legend I    -> "legend"  (247 rows, 0 "ranked")
#     Legend II   -> "ranked"  ( 82 rows, 0 "legend")
#     Legend III  -> "ranked"  (118 rows, 0 "legend")
# Attack counts confirm the mapping: every sampled active player shows exactly
# 16 of their tier's type over 2 days == the 8/day ranked allowance.
#
# "homeVillage" is NOT a ranked battle -- it is ordinary loot raiding and shows
# up at EVERY tier (95/91/107 rows in the same sample), on both offense and
# defense, with no trophy effect. Including it would inject junk into
# day.num_attacks / num_defenses, so it stays excluded.
#
# Filtering on "legend" alone (the pre-2026-08-23 behaviour) silently dropped
# every battle for every Legend II/III player: 167,146 active players in
# actively-scanned clans had last_battle_time IS NULL, and only ~9% of the
# tracked base had ever recorded a single battle.
RANKED_BATTLE_TYPES = {"legend", "ranked"}
# Verified via /leaguetiers (verify_tiers.py / probe_leagues.py):
#   105000036 Legend I, 105000035 Legend II, 105000034 Legend III.
LEGEND_TIER_IDS: set[int] = {105000036, 105000035, 105000034}

STALE_CLAN_MINUTES = 4  # Only rescan clans whose member scan is older than this

# --- Process-lifetime shared resources ---
# The DB pool, the HTTP session and the per-key rate limiters used to be built
# and torn down on EVERY pass (routine update runs every 3 min, so ~480/day).
# Each cycle opened up to 100 fresh MySQL connections and an unlimited-size
# aiohttp connector, then closed them; on Windows/CPython the freed heap is
# rarely returned to the OS and TLS teardown on the Proactor loop leaves
# transports lingering, so RSS ratcheted upward for as long as the process
# lived. They are now created once, lazily, and reused for the life of the
# process (see close_shared() for shutdown).

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

_PASS_METRICS: ContextVar = ContextVar("pass_metrics", default=None)


def _metric(group, key, value=1) -> None:
    try:
        metrics = _PASS_METRICS.get()
        if metrics is not None:
            bucket = metrics[group]
            bucket[key] = bucket.get(key, 0) + value
    except Exception:
        pass


def _measure_pass(function):
    @wraps(function)
    async def measured(*args, **kwargs):
        metrics = {
            "requests": {}, "responses": {}, "http_headers_seconds": {},
            "limiter_seconds": {}, "battle_rows": {}, "phase_seconds": {},
            "inserted_battle_rows": "unknown",
            "successful_battlelog_freshness": "unknown",
        }
        token = _PASS_METRICS.set(metrics)
        started = time.perf_counter()
        outcome = "incomplete"
        try:
            result = await function(*args, **kwargs)
            outcome = "complete"
            return result
        finally:
            _PASS_METRICS.reset(token)
            try:
                metrics["elapsed_seconds"] = time.perf_counter() - started
                metrics["outcome"] = outcome
                print("[PASS_METRICS] " + json.dumps(metrics, sort_keys=True))
            except Exception:
                pass
    return measured


async def _measured_throttle(limiter, endpoint) -> None:
    started = time.perf_counter()
    try:
        await limiter.throttle()
    finally:
        _metric("limiter_seconds", endpoint, time.perf_counter() - started)


@asynccontextmanager
async def _measured_request(session, endpoint, url, **kwargs):
    started = time.perf_counter()
    responded = False
    _metric("requests", endpoint)
    try:
        async with session.get(url, **kwargs) as response:
            responded = True
            status = response.status
            status = str(status) if isinstance(status, int) and 100 <= status <= 599 else "other"
            _metric("responses", endpoint + ":" + status)
            _metric("http_headers_seconds", endpoint, time.perf_counter() - started)
            yield response
    finally:
        if not responded:
            _metric("responses", endpoint + ":no_response")
            _metric("http_headers_seconds", endpoint, time.perf_counter() - started)


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

# --- Shared resource accessors ---------------------------------------------
# All lazily built on first use inside the running loop, then reused forever.
# Every entry point (routine update, seed, snapshot, league history) goes
# through these; nothing else may create its own pool/session/limiters.

_SHARED_POOL: Optional[aiomysql.Pool] = None
_SHARED_SESSION: Optional[aiohttp.ClientSession] = None
_SHARED_LIMITERS: Optional[Dict[str, RateLimiter]] = None
_SHARED_LOCK: Optional[asyncio.Lock] = None

# Pool sizing: the old per-cycle pool was minsize=10/maxsize=100. A single
# long-lived pool needs the same ceiling (CONCURRENCY clan tasks each acquire a
# connection for their DB phase) but can keep a smaller idle floor.
POOL_MIN = int(os.getenv("BOT_POOL_MIN", "10"))
POOL_MAX = int(os.getenv("BOT_POOL_MAX", "100"))


def _shared_lock() -> asyncio.Lock:
    global _SHARED_LOCK
    if _SHARED_LOCK is None:
        _SHARED_LOCK = asyncio.Lock()
    return _SHARED_LOCK


async def get_shared_pool() -> aiomysql.Pool:
    """The one DB pool for this process."""
    global _SHARED_POOL
    if _SHARED_POOL is None:
        async with _shared_lock():
            if _SHARED_POOL is None:
                _SHARED_POOL = await make_pool(minsize=POOL_MIN, maxsize=POOL_MAX)
    return _SHARED_POOL


async def get_shared_session() -> aiohttp.ClientSession:
    """The one aiohttp session for this process.

    Recreated only if something closed it, which would otherwise wedge every
    subsequent fetch with 'Session is closed'.
    """
    global _SHARED_SESSION
    if _SHARED_SESSION is None or _SHARED_SESSION.closed:
        async with _shared_lock():
            if _SHARED_SESSION is None or _SHARED_SESSION.closed:
                connector = aiohttp.TCPConnector(
                    limit=0, limit_per_host=0, ttl_dns_cache=300
                )
                _SHARED_SESSION = aiohttp.ClientSession(connector=connector)
    return _SHARED_SESSION


async def get_shared_limiters() -> Dict[str, RateLimiter]:
    """Per-API-key token buckets, started once and never stopped.

    Sharing them across passes is also more correct than the old per-pass
    limiters: two overlapping tasks (e.g. league history and a routine update)
    used to get a fresh full-burst bucket each, i.e. 2x the intended rate.
    """
    global _SHARED_LIMITERS
    if _SHARED_LIMITERS is None:
        async with _shared_lock():
            if _SHARED_LIMITERS is None:
                limiters = {key: RateLimiter(PER_KEY_RPS, BURST) for key in KEYS}
                for limiter in limiters.values():
                    await limiter.start()
                _SHARED_LIMITERS = limiters
    return _SHARED_LIMITERS


async def close_shared() -> None:
    """Tear down the shared resources. Only for process shutdown."""
    global _SHARED_POOL, _SHARED_SESSION, _SHARED_LIMITERS
    if _SHARED_LIMITERS is not None:
        for limiter in _SHARED_LIMITERS.values():
            await limiter.stop()
        _SHARED_LIMITERS = None
    if _SHARED_SESSION is not None and not _SHARED_SESSION.closed:
        await _SHARED_SESSION.close()
    _SHARED_SESSION = None
    if _SHARED_POOL is not None:
        _SHARED_POOL.close()
        await _SHARED_POOL.wait_closed()
        _SHARED_POOL = None


async def run_bounded(items, make_coro, concurrency: int, on_result=None) -> None:
    """Run `make_coro(item)` over `items` with at most `concurrency` in flight.

    Replaces "create a Task per item up front, then gate execution with a
    semaphore": that allocated one Task + one coroutine frame for every clan
    and every unseen player before any of them ran (tens of thousands of live
    objects per pass), which is pure memory pressure with no throughput gain.
    Here only `concurrency` coroutines exist at a time.

    `on_result(result)` is called for each successful item. Exceptions are
    isolated per item so one failure can never orphan the rest of the batch.
    """
    if not items:
        return
    queue: asyncio.Queue = asyncio.Queue()
    for item in items:
        queue.put_nowait(item)

    async def worker() -> None:
        while True:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                result = await make_coro(item)
            except Exception as exc:
                # The reporting must never itself be able to kill the worker --
                # that is exactly how an unprintable character used to escalate
                # a log line into a lost batch.
                try:
                    print(f"Task failed for {item!r}: {exc!r}")
                except Exception:
                    pass
                continue
            if on_result is not None:
                on_result(result)

    workers = [asyncio.create_task(worker())
               for _ in range(min(concurrency, len(items)))]
    try:
        await asyncio.gather(*workers)
    finally:
        for w in workers:
            w.cancel()

def is_legend(player_like: Dict[str, Any]) -> bool:
    """
    Return True for any Legend tier player (Legend I, II, or III).

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
        await _measured_throttle(limiter, "clan")
        try:
            async with _measured_request(session, "clan", url, headers=headers, timeout=12) as resp:
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
        await _measured_throttle(limiter, "player")
        try:
            async with _measured_request(session, "player", url, headers=headers, timeout=10) as resp:
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

async def _fetch_items(session, limiter, url, key, endpoint="other") -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {key}"}
    for _ in range(3):
        await _measured_throttle(limiter, endpoint)
        try:
            async with _measured_request(session, endpoint, url, headers=headers, timeout=10) as resp:
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
    data = await _fetch_items(session, limiter, url, key, "battlelog")
    items = data.get("items")
    if isinstance(items, list):
        _metric("battle_rows", "returned", len(items))
    else:
        _metric("battle_rows", "unknown_fetches")
    return data

async def fetch_leaguehistory(session, limiter, tag, key) -> Dict[str, Any]:
    url = f"{API_BASE}{LEAGUEHISTORY_ENDPOINT.format(player_tag=_enc_tag(tag))}"
    return await _fetch_items(session, limiter, url, key, "leaguehistory")

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
        if (it.get("battleType") or "").lower() not in RANKED_BATTLE_TYPES:
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
            # Raw army link, stored verbatim (no parsing here). Seen on most
            # attack rows and some defense rows; None when the API omits it.
            "army_share_code": it.get("armyShareCode"),
        })
        if max_t is None or t > max_t:
            max_t = t
    _metric("battle_rows", "new_cursor_candidates", len(battles))
    return battles, max_t

async def load_active_player_state(
    pool: aiomysql.Pool,
) -> Tuple[Dict[str, int], Dict[str, Optional[datetime]]]:
    """Both per-pass caches in one scan: (trophies, battlelog cursors).

    These were two separate functions issuing the same
    `... FROM player WHERE is_active = 1` back to back on two connections,
    reading every active row twice per pass. There is nothing to index here --
    both genuinely want the whole active playerbase -- so the fix is to read
    it once.
    """
    cache: Dict[str, int] = {}
    cursors: Dict[str, Optional[datetime]] = {}
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT player_tag, trophies, last_battle_time "
                "FROM player WHERE is_active = 1"
            )
            for tag, trophies, ts in await cur.fetchall():
                clean = tag.upper() if tag else ""
                if clean:
                    cache[clean] = int(trophies or 0)
                    cursors[clean] = ts
    return cache, cursors

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
    clan_observed_at = datetime.now(timezone.utc).replace(tzinfo=None)

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
    # memberList carries name/trophies/leagueTier. EVERY legend member gets a
    # /battlelog fetch, every pass -- a "trophies unchanged since last poll"
    # fast path used to skip this, but a legend player can attack AND get
    # defended against within one 3-min cycle and net out to the exact same
    # trophy count, which silently ate the whole battle (both sides) with no
    # trace. Legend rosters are the entire point of this bot and are small
    # relative to non-legend membership, so paying for a full fetch on all of
    # them each cycle is the correct trade, not an optimization worth losing
    # correctness for.
    non_legend_tracked: List[Tuple[str, int]] = []
    changed_members: List[Tuple[str, str, Any, int, Any]] = []  # (tag, raw_tag, name, trophies, tier_id)
    legend_count_in_clan = 0

    for member in members:
        raw_tag = member.get("tag")
        if not raw_tag:
            continue
        tag = raw_tag.strip("#").upper()
        new_trophies = int(member.get("trophies", 0))

        # Legend status from leagueTier only (Legend I/II/III), never trophies.
        if not is_legend(member):
            if tag in trophy_cache:
                non_legend_tracked.append((tag, new_trophies))
            continue

        seen_tags.add(tag)
        legend_count_in_clan += 1
        legend_seen_count += 1

        tier_id = (member.get("leagueTier") or {}).get("id")
        changed_members.append((tag, raw_tag, member.get("name"),
                                new_trophies, tier_id))

    # ---- Fetch /battlelog for changed members (counts + per-battle detail) ----
    # Concurrently, not one after another: these are pure network waits and a
    # roster of N legends used to cost N sequential round trips (~150ms each)
    # before the clan's DB phase could start. This does NOT raise the request
    # rate -- every call still passes through limiter.throttle(), so the
    # PER_KEY_RPS ceiling is unchanged; it just stops leaving it idle.
    # return_exceptions so one member's failure can't drop the whole roster.
    fetched: Dict[str, Dict[str, Any]] = {}
    battlelogs = await asyncio.gather(
        *(fetch_battlelog(session, limiter, raw_tag, key)
          for _, raw_tag, _, _, _ in changed_members),
        return_exceptions=True,
    )
    for (tag, raw_tag, name, new_trophies, tier_id), bl in zip(changed_members, battlelogs):
        if isinstance(bl, BaseException) or not isinstance(bl, dict):
            bl = {"error": True}
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
            "league_tier_id": tier_id,
            "clan": {"tag": clan_tag, "name": clan.get("name")},
            "clan_observed_at": clan_observed_at,
            "clan_source": "clan_roster",
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
                # upsert_clan above already wrote this clan for the whole
                # roster; mark_clan_active below re-asserts is_active. Repeating
                # the clan upsert per member was pure duplicate work.
                await upsert_player(conn, payload, commit=False,
                                    clan_already_upserted=True)
                changed_tags.add(tag)
                trophy_cache[tag] = int(payload["trophies"] or 0)
                if payload.get("last_battle_time") is not None:
                    battle_cursor[tag] = payload["last_battle_time"]

            # commit=False: these used to commit internally, which ended this
            # transaction here and left the `except` below rolling back an
            # already-committed batch. It also cost a second fsync per clan.
            if legend_count_in_clan == 0:
                await mark_clan_inactive(conn, clan_tag, commit=False)
            else:
                await mark_clan_active(conn, clan_tag, commit=False)

            await conn.commit()

        except Exception as e:
            await conn.rollback()
            # No emoji, and never let the report raise: this handler used to
            # explode on its own "⚠️" under a cp1252-encoded stdout, which threw
            # the failure out of the clan task instead of recording it.
            try:
                print(f"[WARN] DB Transaction failed for clan {clan_tag}: {e!r}")
            except Exception:
                pass
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
    clan_observed_at = datetime.now(timezone.utc).replace(tzinfo=None)

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
                "league_tier_id": (data.get("leagueTier") or {}).get("id"),
                "clan": {"tag": new_clan.get("tag"), "name": new_clan.get("name", "")},
                "clan_observed_at": clan_observed_at,
                "clan_source": "player_profile",
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

@_measure_pass
async def update_all(pool: Optional[aiomysql.Pool] = None) -> None:
    """One full pass. `pool` is accepted for backwards compatibility; when it is
    omitted the process-wide shared pool is used."""
    if pool is None:
        pool = await get_shared_pool()
    session = await get_shared_session()
    limiters = await get_shared_limiters()

    start_time = time.perf_counter()

    async with pool.acquire() as conn:
        all_clans = await get_clans_to_scan(conn)
    print(f"Updating {len(all_clans)} clans")

    trophy_cache, battle_cursor = await load_active_player_state(pool)
    key_cycle = cycle(KEYS)
    seen: Set[str] = set()

    processed = 0
    total_trophy_changes = 0
    progress_log_threshold = 500

    async def run_clan(clan_tag: str):
        key = next(key_cycle)
        return await process_clan(
            pool=pool,
            session=session,
            limiter=limiters[key],
            key=key,
            clan_tag=clan_tag,
            trophy_cache=trophy_cache,
            battle_cursor=battle_cursor,
        )

    def on_clan(result) -> None:
        nonlocal processed, total_trophy_changes, progress_log_threshold
        seen_tags, changed_tags, _ = result
        seen.update(seen_tags)
        total_trophy_changes += len(changed_tags)
        processed += 1
        if processed >= progress_log_threshold:
            _progress(f"[CLANS] {processed}/{len(all_clans)} | legends: {len(seen)} | hits: {total_trophy_changes}")
            progress_log_threshold += 500

    phase_started = time.perf_counter()
    await run_bounded(all_clans, run_clan, CONCURRENCY, on_clan)
    _metric("phase_seconds", "clans", time.perf_counter() - phase_started)
    phase_started = time.perf_counter()

    async with pool.acquire() as conn:
        all_players = await get_all_player_tags(conn)

    unseen = sorted(set(all_players) - seen)

    reconciled_updated = 0
    reconciled_archived = 0
    completed = 0
    unseen_log_threshold = 200

    async def run_unseen(player_tag: str):
        key = next(key_cycle)
        return await process_unseen(
            pool=pool,
            session=session,
            limiter=limiters[key],
            key=key,
            player_tag=player_tag,
            trophy_cache=trophy_cache,
            battle_cursor=battle_cursor,
        )

    def on_unseen(result) -> None:
        nonlocal reconciled_updated, reconciled_archived, completed, unseen_log_threshold
        if result == "updated":
            reconciled_updated += 1
        elif result == "archived":
            reconciled_archived += 1
        completed += 1
        if completed >= unseen_log_threshold:
            _progress(f"[UNSEEN] {completed}/{len(unseen)} reconciled")
            unseen_log_threshold += 200

    if unseen:
        _progress(f"[UNSEEN] Reconciling {len(unseen)} players...")
        await run_bounded(unseen, run_unseen, CONCURRENCY, on_unseen)

    _metric("phase_seconds", "unseen", time.perf_counter() - phase_started)
    phase_started = time.perf_counter()

    # Heal the concurrent-transaction race where a mirror row and its
    # native counterpart both landed this pass (native wins).
    try:
        async with pool.acquire() as conn:
            healed = await reconcile_mirror_duplicates(conn)
    except Exception as exc:
        healed = 0
        print(f"Mirror reconcile failed: {exc}")

    _metric("phase_seconds", "reconcile", time.perf_counter() - phase_started)
    elapsed = time.perf_counter() - start_time
    print(f"Run Complete in {elapsed:.1f}s")
    print(f" > Clan Members Changed: {total_trophy_changes}")
    print(f" > Unseen Updated:       {reconciled_updated}")
    print(f" > Unseen Archived:      {reconciled_archived}")
    if healed:
        print(f" > Mirror dupes healed:  {healed}")

    record_update_run()

async def run_snapshot_reset_clans() -> None:
    pool = await get_shared_pool()
    async with pool.acquire() as conn:
        await snapshot_reset_clan_tags(conn)

async def main() -> None:
    await update_all()

if __name__ == "__main__":
    async def _standalone() -> None:
        try:
            await main()
        finally:
            await close_shared()

    asyncio.run(_standalone())
