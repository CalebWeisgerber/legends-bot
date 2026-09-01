"""league_history.py — ingest per-league-season summaries.

Fetches /players/{tag}/leaguehistory for every active player and upserts the
rows into league_season. League history only changes at season boundaries, so
running this once a day (e.g. after the daily reset) is plenty.

    python -m legendsbot.league_history
"""
import asyncio
from itertools import cycle
from typing import List, Optional

from legendsbot.db import upsert_league_season
from legendsbot.pipeline import (
    KEYS,
    BURST,
    close_shared,
    fetch_leaguehistory,
    get_shared_limiters,
    get_shared_pool,
    get_shared_session,
    run_bounded,
)

CONCURRENCY = min(max(100, len(KEYS) * BURST), 200)


async def _active_player_tags(pool) -> List[str]:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT player_tag FROM player WHERE is_active = 1")
            return [r[0] for r in await cur.fetchall()]


async def ingest(pool=None) -> None:
    """`pool` is accepted for backwards compatibility; omitted means the shared
    process-wide pool."""
    if pool is None:
        pool = await get_shared_pool()
    session = await get_shared_session()
    limiters = await get_shared_limiters()
    key_cycle = cycle(KEYS)

    tags = await _active_player_tags(pool)
    print(f"league-history: {len(tags)} active players")

    done = 0

    async def one(tag: str):
        key = next(key_cycle)
        data = await fetch_leaguehistory(session, limiters[key], tag, key)
        if data.get("error") or data.get("not_found"):
            return
        items = data.get("items", [])
        if items:
            async with pool.acquire() as conn:
                await upsert_league_season(conn, tag, items)

    def on_done(_result) -> None:
        nonlocal done
        done += 1
        if done % 2000 == 0:
            print(f"  {done}/{len(tags)}")

    # run_bounded isolates per-tag exceptions: one bad player used to abort the
    # await loop and leave every sibling task orphaned but still referenced.
    await run_bounded(tags, one, CONCURRENCY, on_done)
    print("league-history: done")


async def main() -> None:
    try:
        await ingest()
    finally:
        await close_shared()


if __name__ == "__main__":
    asyncio.run(main())
