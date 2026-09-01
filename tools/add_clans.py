"""add_clans.py — register clan(s) as permanent discovery roots.

Why: the Clash API has no endpoint listing all Legend players. Ranking-based
discovery only reaches clans in a location's top-200. A Legend player in a
clan that ranks nowhere is otherwise undiscoverable. This:

  1. validates each clan via the API (must currently have a Legend member),
  2. inserts it into the `clan` table immediately (so the next update pass scans
     its members and picks up those Legend players),
  3. appends it to known_clans.txt so future re-seeds keep it.

Usage:
    python add_clans.py "#CLANTAG" [more tags...]
"""
import asyncio
import json
import sys
from pathlib import Path
from urllib.parse import quote

# Make the legendsbot package importable when run directly from tools/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import aiohttp

from legendsbot.db import get_pool, upsert_clan, normalize_tag
from legendsbot.pipeline import is_legend  # Legend tier check
from legendsbot.paths import KEYS_FILE, KNOWN_CLANS_FILE

API_BASE = "https://api.clashofclans.com/v1"
KNOWN_FILE = KNOWN_CLANS_FILE

with open(KEYS_FILE, "r", encoding="utf-8") as f:
    KEY = list(json.load(f).values())[0]


def _append_to_known(norm_tag: str) -> None:
    existing = set()
    if KNOWN_FILE.exists():
        for line in KNOWN_FILE.read_text(encoding="utf-8").splitlines():
            t = line.strip()
            if t and not t.startswith("#"):
                existing.add(normalize_tag(t))
    if norm_tag not in existing:
        with open(KNOWN_FILE, "a", encoding="utf-8") as fh:
            fh.write(f"\n{norm_tag}\n")


async def add_one(session, pool, raw_tag: str) -> None:
    norm = normalize_tag(raw_tag)
    url = f"{API_BASE}/clans/{quote('#' + norm)}"
    async with session.get(url, headers={"Authorization": f"Bearer {KEY}"}, timeout=15) as resp:
        if resp.status == 404:
            print(f"  #{norm}: NOT FOUND (404). Skipped.")
            return
        if resp.status != 200:
            print(f"  #{norm}: HTTP {resp.status}. Skipped.")
            return
        clan = await resp.json()

    name = clan.get("name", "Unknown")
    members = clan.get("memberList") or []
    legends = [m for m in members if is_legend(m)]
    if not legends:
        print(f"  #{norm} ({name}): no Legend members right now. "
              f"Adding anyway as a discovery root; update pass will skip if empty.")

    async with pool.acquire() as conn:
        await upsert_clan(conn, name, norm)
    _append_to_known(norm)
    print(f"  #{norm} ({name}): inserted. Legend members found: {len(legends)}")


async def main(tags):
    pool = await get_pool()
    try:
        async with aiohttp.ClientSession() as session:
            for t in tags:
                await add_one(session, pool, t)
    finally:
        pool.close()
        await pool.wait_closed()
    print("Done. Run the update pass (populate_db.bat or async_fetch_pipeline.py) "
          "to pull the members now.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python add_clans.py "#CLANTAG" [more...]')
        sys.exit(1)
    asyncio.run(main(sys.argv[1:]))
