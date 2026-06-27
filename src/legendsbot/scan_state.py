import json
from datetime import datetime, timedelta, timezone

from legendsbot.paths import STATE_DIR

# Runtime state file (kept name last_scan.json for backward compatibility).
STATE_PATH = STATE_DIR / "last_scan.json"

# how long until we skip reseeding / updating
MIN_SEED_INTERVAL_HOURS = 12
MIN_UPDATE_INTERVAL_MINUTES = 3   # adjust if needed


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    try:
        STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"⚠️ Failed to write scan_state.json: {e}")


def record_seed_run():
    state = _load_state()
    state["last_seed_run"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)


def record_update_run():
    state = _load_state()
    state["last_update_run"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)


def seed_recently_ran() -> bool:
    state = _load_state()
    ts = state.get("last_seed_run")
    if not ts:
        return False

    last = datetime.fromisoformat(ts)
    now = datetime.now(timezone.utc)
    return (now - last) < timedelta(hours=MIN_SEED_INTERVAL_HOURS)


def update_recently_ran() -> bool:
    state = _load_state()
    ts = state.get("last_update_run")
    if not ts:
        return False

    last = datetime.fromisoformat(ts)
    now = datetime.now(timezone.utc)
    return (now - last) < timedelta(minutes=MIN_UPDATE_INTERVAL_MINUTES)
