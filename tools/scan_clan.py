"""scan_clan.py — immediately scan one clan and upsert its Legend I/II members.

A targeted version of the update pass's process_clan, for when you've just
registered a clan (add_clans.py) and want its members in the DB now without
running a full 7000-clan update.

Usage:
    python scan_clan.py "#CLANTAG"
"""
import asyncio
import sys
from pathlib import Path

# Windows consoles default to cp1252; player names contain unicode/emoji.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Make the legendsbot package importable when run directly from tools/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import aiohttp

from legendsbot.db import get_pool, upsert_player, normalize_tag
from legendsbot.pipeline import (fetch_clan, fetch_battlelog, parse_legend_battles,
    is_legend, RateLimiter, PER_KEY_RPS, BURST, KEYS)


async def main(raw_tag: str):
    key = KEYS[0]
    limiter = RateLimiter(PER_KEY_RPS, BURST)
    await limiter.start()
    pool = await get_pool()
    try:
        async with aiohttp.ClientSession() as session:
            clan = await fetch_clan(session, limiter, raw_tag, key)
            if clan.get("error") or clan.get("not_found"):
                print(f"Could not fetch clan {raw_tag}: {clan}")
                return
            name = clan.get("name", "Unknown Clan")
            ctag = normalize_tag(raw_tag)
            members = clan.get("memberList") or []
            legends = [m for m in members if is_legend(m)]
            print(f"{name} #{ctag}: {len(legends)} Legend I/II members")

            async with pool.acquire() as conn:
                for m in legends:
                    # memberList lacks battle data -> fetch /battlelog for counts.
                    bl = await fetch_battlelog(session, limiter, m.get("tag"), key)
                    if bl.get("error") or bl.get("not_found"):
                        battles, max_t = [], None
                    else:
                        battles, max_t = parse_legend_battles(bl.get("items", []), None)
                    n_att = sum(1 for b in battles if b["is_attack"])
                    payload = {
                        "tag": m.get("tag"),
                        "name": m.get("name"),
                        "trophies": m.get("trophies"),
                        "clan": {"tag": ctag, "name": name},
                        "battles": battles,
                        "last_battle_time": max_t,
                    }
                    await upsert_player(conn, payload)
                    print(f"  upserted {m.get('name')} ({m.get('tag')}) "
                          f"trophies={m.get('trophies')} "
                          f"attacks={n_att} defenses={len(battles) - n_att}")
    finally:
        await limiter.stop()
        pool.close()
        await pool.wait_closed()
    print("Done.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python scan_clan.py "#CLANTAG"')
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
