"""Unit tests for the tier detection + battlelog parsing — the logic most
likely to silently break on an API change. No network or DB."""
from datetime import datetime

from legendsbot.pipeline import is_legend, parse_legend_battles, _parse_battle_ts


# --- is_legend: Legend I/II/III all match ---

def test_is_legend_by_id():
    assert is_legend({"leagueTier": {"id": 105000036, "name": "Legend I"}})
    assert is_legend({"leagueTier": {"id": 105000035, "name": "Legend II"}})
    assert is_legend({"leagueTier": {"id": 105000034, "name": "Legend III"}})


def test_is_legend_by_name_when_id_unknown():
    assert is_legend({"leagueTier": {"name": "Legend I"}})
    assert is_legend({"leagueTier": {"name": "legend ii"}})
    assert is_legend({"leagueTier": {"name": "Legend III"}})


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

def _battle(ts, attack=True, btype="legend", army_share_code=None):
    battle = {
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
    if army_share_code is not None:
        battle["armyShareCode"] = army_share_code
    return battle


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


# --- armyShareCode extraction (migration 0005) ---

def test_parse_extracts_army_share_code_verbatim():
    code = "h1p9e17_48-2m1p16e4_34-6p17e43_49-7p4e52_59i1x5"
    items = [_battle("20260625T010000.000Z", attack=True, army_share_code=code)]
    battles, _ = parse_legend_battles(items, None)
    assert battles[0]["army_share_code"] == code


def test_parse_army_share_code_none_when_absent():
    items = [_battle("20260625T010000.000Z", attack=True)]
    battles, _ = parse_legend_battles(items, None)
    assert battles[0]["army_share_code"] is None


def test_parse_army_share_code_kept_on_defense_rows_too():
    # Live probe (July 2026) showed the API includes armyShareCode on some
    # DEFENSE rows as well -- the parser must not filter it by is_attack.
    code = "h0p9e14_32-1p11e16_48-2p7e19_4"
    items = [_battle("20260625T010000.000Z", attack=False, army_share_code=code)]
    battles, _ = parse_legend_battles(items, None)
    assert battles[0]["is_attack"] is False
    assert battles[0]["army_share_code"] == code
