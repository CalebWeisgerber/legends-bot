# debug_player.py — single-player debug. Dumps the new leagueTier field and runs
# the bot's is_legend() check. (Key must be IP-whitelisted for this machine.)
import json
import sys
from pathlib import Path
from urllib.parse import quote

import requests

# Make the legendsbot package importable when run directly from tools/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from legendsbot.pipeline import is_legend
from legendsbot.paths import KEYS_FILE

# Set the player tag you want to check (or pass as argv[1]).
MY_TAG = sys.argv[1] if len(sys.argv) > 1 else "#PLAYERTAG"

with open(KEYS_FILE, "r", encoding="utf-8") as f:
    api_key = list(json.load(f).values())[0]

headers = {"Authorization": f"Bearer {api_key}"}
url = f"https://api.clashofclans.com/v1/players/{quote(MY_TAG)}"

resp = requests.get(url, headers=headers, timeout=15)
if resp.status_code != 200:
    print(f"HTTP {resp.status_code}: {resp.text[:400]}")
    raise SystemExit(1)

data = resp.json()

print("--- DEBUG REPORT ---")
print(f"Name:     {data.get('name')}")
print(f"Trophies: {data.get('trophies')}")
print(f"league:      {data.get('league')}")
print(f"leagueTier:  {data.get('leagueTier')}")
print("-" * 20)
print(f"is_legend() -> {is_legend(data)}  (True for Legend I / II / III)")
