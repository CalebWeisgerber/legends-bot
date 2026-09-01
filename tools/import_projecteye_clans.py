"""import_projecteye_clans.py — pull opponent clans discovered by Project Eye
into this bot's discovery roots.

Project Eye (the stream watcher) resolves every attack its streamers make and
writes each opponent's clan tag to its opponent_clans.txt. Those clans hosted
a Legend player *by definition* (they just got attacked in Legends), but many
never crack a location top-list, so normal seeding can't find them. This
registers each new one exactly like add_clans.py (validate → insert into
`clan` → append to known_clans.txt), and remembers what it already imported
in state/projecteye_imported.txt so re-runs only process new tags.

Usage:
    python tools/import_projecteye_clans.py            # default Project Eye path
    python tools/import_projecteye_clans.py <file>     # explicit tag list
"""
import asyncio
import sys
from pathlib import Path

# Make the legendsbot package importable when run directly from tools/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import aiohttp

from add_clans import add_one  # same validate+insert+remember flow
from legendsbot.db import get_pool, normalize_tag
from legendsbot.paths import STATE_DIR

DEFAULT_SOURCE = Path(
    r"C:\Users\caleb\Desktop\TwitchProject\Project Eye\opponent_clans.txt")
IMPORTED_FILE = STATE_DIR / "projecteye_imported.txt"


def _read_tags(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [normalize_tag(ln.strip())
            for ln in path.read_text(encoding="utf-8").splitlines()
            if ln.strip()]


async def main(source: Path) -> None:
    tags = _read_tags(source)
    done = set(_read_tags(IMPORTED_FILE))
    todo = [t for t in dict.fromkeys(tags) if t not in done]
    if not todo:
        print(f"Nothing new ({len(tags)} tags in {source.name}, all imported).")
        return
    print(f"{len(todo)} new clan(s) from {source}")

    pool = await get_pool()
    try:
        async with aiohttp.ClientSession() as session:
            for t in todo:
                await add_one(session, pool, t)
                with open(IMPORTED_FILE, "a", encoding="utf-8") as fh:
                    fh.write(t + "\n")
    finally:
        pool.close()
        await pool.wait_closed()
    print("Done. The routine update pass will scan the new clans automatically.")


if __name__ == "__main__":
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SOURCE
    asyncio.run(main(src))
