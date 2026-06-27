"""league_history.py — ingest per-league-season summaries.

Fetches /players/{tag}/leaguehistory for every active player and upserts the
rows into league_season. League history only changes at season boundaries, so
running this once a day (e.g. after the daily reset) is plenty.

    python -m legendsbot.league_history
"""
import asyncio
from itertools import cycle
from typing import List

import aiohttp

from legendsbot.db import make_pool, upsert_league_season
from legendsbot.pipeline import KEYS, RateLimiter, PER_KEY_RPS, BURST, fetch_leaguehistory

CONCURRENCY = min(max(100, len(KEYS) * BURST), 200)


async def _active_player_tags(pool) -> List[str]:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT player_tag FROM player WHERE is_active = 1")
            return [r[0] for r in await cur.fetchall()]


async def ingest(pool) -> None:
    limiters = {k: RateLimiter(PER_KEY_RPS, BURST) for k in KEYS}
    for lim in limiters.values():
        await lim.start()
    key_cycle = cycle(KEYS)
    sem = asyncio.Semaphore(CONCURRENCY)

    tags = await _active_player_tags(pool)
    print(f"league-history: {len(tags)} active players")

    async def one(tag: str):
        key = next(key_cycle)
        async with sem:
            data = await fetch_leaguehistory(session, limiters[key], tag, key)
        if data.get("error") or data.get("not_found"):
            return
        items = data.get("items", [])
        if items:
            async with pool.acquire() as conn:
                await upsert_league_season(conn, tag, items)

    try:
        connector = aiohttp.TCPConnector(limit=0, limit_per_host=0, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [asyncio.create_task(one(t)) for t in tags]
            done = 0
            for fut in asyncio.as_completed(tasks):
                await fut
                done += 1
                if done % 2000 == 0:
                    print(f"  {done}/{len(tags)}")
    finally:
        for lim in limiters.values():
            await lim.stop()
    print("league-history: done")


async def main() -> None:
    pool = await make_pool(minsize=5, maxsize=50)
    try:
        await ingest(pool)
    finally:
        pool.close()
        await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
