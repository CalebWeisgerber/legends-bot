"""import_archive_clans.py — one-time bulk seed of clan tags from archive dumps.

Pulls clan tags from the historical exports in archive/data-dumps/ and inserts any
not already in the `clan` table. The next update pass scans them; clans without a
Legend member auto-deactivate, so importing dead clans is self-cleaning.

  python tools/import_archive_clans.py            # dry run: report counts only
  python tools/import_archive_clans.py --write     # actually insert
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from legendsbot.db import get_pool, normalize_tag, get_all_clan_tags

DUMPS = Path(__file__).resolve().parents[1] / "archive" / "data-dumps"


def _collect() -> dict:
    """Return {normalized_tag: name_or_None} from every clan-tag source."""
    out: dict[str, str | None] = {}

    def add(raw, name=None):
        n = normalize_tag(raw)
        if n and (n not in out or (name and not out[n])):
            out[n] = name

    txt = DUMPS / "clan_tags.txt"
    if txt.exists():
        for line in txt.read_text(encoding="utf-8").splitlines():
            if line.strip():
                add(line)

    for fname, tagkey, namekey in [
        ("clan_tags2.json", "tag", "name"),
        ("top_players.json", "clan_tag", None),
        ("legends_output copy.json", "clan_tag", None),
        ("legends_output.json", "clan_tag", None),
    ]:
        f = DUMPS / fname
        if not f.exists():
            continue
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  (skip {fname}: {e})")
            continue
        for item in data:
            if isinstance(item, dict) and item.get(tagkey):
                add(item[tagkey], item.get(namekey) if namekey else None)

    return out


async def main(write: bool):
    found = _collect()
    print(f"Collected {len(found)} unique clan tags from archive dumps.")

    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            existing = set(await get_all_clan_tags(conn))
        new = {t: n for t, n in found.items() if t not in existing}
        print(f"Already in DB: {len(found) - len(new)} | NEW: {len(new)}")

        if not write:
            print("\nDry run. Re-run with --write to insert the NEW clans.")
            return
        if not new:
            print("Nothing to insert.")
            return

        rows = [(t, (n or "From Archive")) for t, n in new.items()]
        inserted = 0
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(rows), 1000):
                    batch = rows[i:i + 1000]
                    await cur.executemany(
                        """INSERT INTO clan (clan_tag, clan_name) VALUES (%s, %s)
                           ON DUPLICATE KEY UPDATE clan_name = VALUES(clan_name)""",
                        batch,
                    )
                    inserted += len(batch)
                    print(f"  inserted {inserted}/{len(rows)}")
            await conn.commit()
        print(f"Done. Inserted {inserted} clans. Run an update pass to scan them.")
    finally:
        pool.close()
        await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main(write="--write" in sys.argv))
