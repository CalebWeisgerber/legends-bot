import asyncio
import json
import sys
from pathlib import Path

# Make the legendsbot package importable when run directly from tools/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import aiohttp

from legendsbot.db import get_pool, delete_player_and_logs, normalize_tag
from legendsbot.pipeline import fetch_player, RateLimiter, is_legend
from legendsbot.paths import KEYS_FILE

PER_KEY_RPS = 30
BURST_CAPACITY = 35

# Load keys
with open(KEYS_FILE, "r", encoding="utf-8") as f:
    API_KEYS = list(json.load(f).values())


import traceback

async def prune_worker(worker_id: int, key: str, player_tags: list[str], pool):
    limiter = RateLimiter(PER_KEY_RPS, BURST_CAPACITY)
    await limiter.start()  # <== ADD THIS LINE

    async with aiohttp.ClientSession() as session:
        for i, tag in enumerate(player_tags):
            tag_norm = normalize_tag(tag)

            try:
                await limiter.throttle()

                data = await fetch_player(session, limiter, tag_norm, key)

                if not data or not is_legend(data):
                    async with pool.acquire() as conn:
                        await delete_player_and_logs(conn, tag_norm)
                    print(f"[Key {worker_id}] 🗑️ Deleted {tag_norm}")
                else:
                    print(f"[Key {worker_id}] ✅ Kept {tag_norm} (Legend)")

            except Exception as e:
                print(f"[Key {worker_id}] ❌ Error with {tag_norm}: {e}")
                traceback.print_exc()

            if i % 5 == 0:
                print(f"[Key {worker_id}] Progress: {i}/{len(player_tags)}")




async def main():
    print("🔧 Connecting to DB…")
    pool = await get_pool()

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT player_tag FROM player")
            rows = await cur.fetchall()
            all_tags = [row[0] for row in rows]

    # Split player tags among workers
    per_worker = len(all_tags) // len(API_KEYS) + 1
    tasks = []

    for i, key in enumerate(API_KEYS):
        tags = all_tags[i * per_worker: (i + 1) * per_worker]
        if not tags:
            print(f"[Key {i}] ⚠️ No players assigned to this key.")
            continue
        print(f"[Key {i}] Assigned {len(tags)} players.")
        tasks.append(prune_worker(i, key, tags, pool))

    print(f"🚀 Starting {len(tasks)} prune workers, processing {len(all_tags)} players total…")
    print(f"🚀 Launching {len(tasks)} workers…")
    await asyncio.gather(*tasks, return_exceptions=True)
    print("✅ Done.")


if __name__ == "__main__":
    asyncio.run(main())
