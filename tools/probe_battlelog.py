"""probe_battlelog.py — inspect the live ranked/legend API shapes before coding.

Pulls a top global Legend player and dumps:
  - new ranked-related fields on /players/{tag}
  - /players/{tag}/battlelog   (per-battle: type, time, trophy change, opponent)
  - /players/{tag}/leaguehistory
Run on an IP-whitelisted host:  python tools/probe_battlelog.py
"""
import json
import sys
from pathlib import Path
from urllib.parse import quote

import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from legendsbot.paths import KEYS_FILE

API = "https://api.clashofclans.com/v1"
KEY = list(json.load(open(KEYS_FILE, encoding="utf-8")).values())[0]
H = {"Authorization": f"Bearer {KEY}"}


def get(path):
    return requests.get(API + path, headers=H, timeout=15)


def main():
    tag = sys.argv[1] if len(sys.argv) > 1 else None
    if not tag:
        r = get("/locations/global/rankings/players?limit=1")
        tag = r.json()["items"][0]["tag"]
    print(f"Sample player: {tag}\n")

    p = get(f"/players/{quote(tag)}").json()
    print("=== player: ranked-ish top-level keys ===")
    for k in sorted(p.keys()):
        if k in ("trophies", "attackWins", "defenseWins", "leagueTier", "league",
                 "rankedTrophies", "ranked", "bestTrophies"):
            print(f"  {k} = {json.dumps(p.get(k))[:200]}")

    print("\n=== /battlelog ===")
    bl = get(f"/players/{quote(tag)}/battlelog")
    print("HTTP", bl.status_code)
    if bl.status_code == 200:
        items = bl.json().get("items", [])
        from collections import Counter
        types = Counter(it.get("battleType") for it in items)
        print(f"{len(items)} entries; battleType counts: {dict(types)}")
        keys = set()
        for it in items:
            keys.update(it.keys())
        print(f"union of keys: {sorted(keys)}")
        # Dump first LEGEND/RANKED entry fully (drop the bulky loot arrays).
        for it in items:
            bt = (it.get("battleType") or "").lower()
            if "legend" in bt or "ranked" in bt:
                slim = {k: v for k, v in it.items()
                        if k not in ("lootedResources", "extraLootedResources", "availableLoot", "armyShareCode")}
                print("\nFirst legend/ranked entry (loot stripped):")
                print(json.dumps(slim, indent=2, ensure_ascii=False))
                break

    print("\n=== /leaguehistory (first 2 entries, raw) ===")
    lh = get(f"/players/{quote(tag)}/leaguehistory")
    print("HTTP", lh.status_code)
    if lh.status_code == 200:
        for it in lh.json().get("items", [])[:2]:
            print(json.dumps(it, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
