"""discover_tiers.py — confirm the live Legend tier ids/names from the API.

Merges the old verify_tiers + probe_leagues scratch scripts:
  1. /leaguetiers           -> authoritative id/name for every league tier
  2. /locations/global/rankings/players -> sanity-check leagueTier on real players

Run on an IP-whitelisted host:  python tools/discover_tiers.py
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
from legendsbot.pipeline import LEGEND_TIER_IDS

API = "https://api.clashofclans.com/v1"
KEY = list(json.load(open(KEYS_FILE, encoding="utf-8")).values())[0]
H = {"Authorization": f"Bearer {KEY}"}


def get(path):
    return requests.get(API + path, headers=H, timeout=15)


def main():
    r = get("/leaguetiers")
    if r.status_code != 200:
        print(f"/leaguetiers -> HTTP {r.status_code}: {r.text[:300]}")
        if r.status_code == 403:
            print("Key not whitelisted for this IP — run on the bot host.")
        return

    print("=== /leaguetiers (Legend tiers) ===")
    for it in r.json().get("items", []):
        if "legend" in (it.get("name") or "").lower():
            tracked = "TRACKED" if it.get("id") in LEGEND_TIER_IDS else "excluded"
            print(f"  {it.get('id')}  {it.get('name'):<12} [{tracked}]")

    print(f"\nLEGEND_TIER_IDS currently in code: {sorted(LEGEND_TIER_IDS)}")

    print("\n=== sample: top global ranked players ===")
    rp = get("/locations/global/rankings/players?limit=3")
    if rp.status_code == 200:
        for p in rp.json().get("items", []):
            lt = p.get("leagueTier") or {}
            print(f"  {p.get('name')} {p.get('tag')}  leagueTier={lt.get('id')} {lt.get('name')}")


if __name__ == "__main__":
    main()
