"""Canonical filesystem locations for the legendsbot package.

Resolves everything relative to the repo root so code works no matter the
current working directory. Layout:
    <root>/config   keys.json, location_ids.json, known_clans.txt
    <root>/state    scan_state.json, last_scan.json, seed_state.json
    <root>/sql      schema.sql, migrations/, leaderboard.sql
"""
from pathlib import Path

# src/legendsbot/paths.py -> parents[2] == repo root
ROOT = Path(__file__).resolve().parents[2]

CONFIG_DIR = ROOT / "config"
STATE_DIR = ROOT / "state"
SQL_DIR = ROOT / "sql"

KEYS_FILE = CONFIG_DIR / "keys.json"
LOCATION_IDS_FILE = CONFIG_DIR / "location_ids.json"
KNOWN_CLANS_FILE = CONFIG_DIR / "known_clans.txt"

# Ensure state dir exists for runtime json writers.
STATE_DIR.mkdir(parents=True, exist_ok=True)
