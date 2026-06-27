"""Unit tests for the tier detection + battlelog parsing — the logic most
likely to silently break on an API change. No network or DB."""
from datetime import datetime

from legendsbot.pipeline import is_legend, parse_legend_battles, _parse_battle_ts


# --- is_legend: Legend I/II only, Legend III excluded ---

def test_is_legend_by_id():
    assert is_legend({"leagueTier": {"id": 105000036, "name": "Legend I"}})
    assert is_legend({"leagueTier": {"id": 105000035, "name": "Legend II"}})
    assert not is_legend({"leagueTier": {"id": 105000034, "name": "Legend III"}})


def test_is_legend_by_name_when_id_unknown():
    assert is_legend({"leagueTier": {"name": "Legend I"}})
    assert is_legend({"leagueTier": {"name": "legend ii"}})
    # Prefix-style false positive must NOT match.
    assert not is_legend({"leagueTier": {"name": "Legend III"}})


def test_is_legend_rejects_non_legend_and_garbage():
    assert not is_legend({"leagueTier": {"id": 1, "name": "Titan I"}})
    assert not is_legend({})              # no tier
    assert not is_legend({"leagueTier": None})
    assert not is_legend(None)            # non-dict
    assert not is_legend("nope")


# --- battle timestamp parsing ---

def test_parse_battle_ts():
    assert _parse_battle_ts("20260625T032732.000Z") == datetime(2026, 6, 25, 3, 27, 32)
    assert _parse_battle_ts("garbage") is None
    assert _parse_battle_ts("") is None


# --- parse_legend_battles ---

def _battle(ts, attack=True, btype="legend"):
    return {
        "battleType": btype,
        "battleTimestamp": ts,
        "attack": attack,
        "opponentPlayerTag": "#ABC",
        "opponentName": "Foe",
        "opponentTownHallLevel": 17,
        "stars": 3,
        "destructionPercentage": 100,
        "battleTime": 90,
    }


def test_parse_filters_non_legend_battletype():
    items = [_battle("20260625T010000.000Z", btype="war")]
    battles, max_t = parse_legend_battles(items, None)
    assert battles == []
    assert max_t is None


def test_parse_collects_legend_and_tracks_max():
    items = [
        _battle("20260625T010000.000Z", attack=True),
        _battle("20260625T020000.000Z", attack=False),
    ]
    battles, max_t = parse_legend_battles(items, None)
    assert len(battles) == 2
    assert [b["is_attack"] for b in battles] == [True, False]
    assert max_t == datetime(2026, 6, 25, 2, 0, 0)


def test_parse_since_cursor_excludes_old_and_equal():
    since = datetime(2026, 6, 25, 1, 0, 0)
    items = [
        _battle("20260625T010000.000Z"),  # == since -> excluded
        _battle("20260625T013000.000Z"),  # newer -> kept
    ]
    battles, max_t = parse_legend_battles(items, since)
    assert len(battles) == 1
    assert max_t == datetime(2026, 6, 25, 1, 30, 0)


def test_parse_skips_bad_timestamp():
    items = [_battle("not-a-time")]
    battles, max_t = parse_legend_battles(items, None)
    assert battles == []
    assert max_t is None
