"""Integration tests for army_share_code capture + mirror-row harvesting
(migration 0005). Unlike test_pipeline.py, these need a real MySQL connection
-- db.py's dedupe/mirror logic runs SQL (TIMESTAMPDIFF, GREATEST, ON DUPLICATE
KEY UPDATE) that isn't worth faking with a mock cursor. They read DB_PASSWORD
from run/_env.bat (same file the bot itself uses) and skip entirely if no
local MySQL is reachable, so they're safe to run anywhere but don't block
`pytest` in an environment without the dev DB.

All rows are written under dedicated ZZTESTMIRROR* tags and deleted in the
scenario's teardown, so this never touches real tracked players.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("aiomysql")

from legendsbot.db import _pt_now_and_day, make_pool, upsert_player  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TAG_A = "ZZTESTMIRRORA"
TAG_B = "ZZTESTMIRRORB"
ARMY_CODE = "h1p9e17_48-2m1p16e4_34-6p17e43_49-7p4e52_59i1x5"


def _load_db_password() -> None:
    if os.getenv("DB_PASSWORD"):
        return
    env_file = ROOT / "run" / "_env.bat"
    if not env_file.exists():
        return
    m = re.search(r"DB_PASSWORD=([^\s\"]+)", env_file.read_text(encoding="utf-8"))
    if m:
        os.environ["DB_PASSWORD"] = m.group(1)


_load_db_password()


async def _make_pool_or_none():
    try:
        return await make_pool(minsize=1, maxsize=2)
    except Exception:
        return None


async def _cleanup(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for tag in (TAG_A, TAG_B):
                await cur.execute(
                    "DELETE FROM legend_battle WHERE player_tag=%s OR opponent_tag=%s", (tag, tag))
                await cur.execute("DELETE FROM day WHERE player_tag=%s", (tag,))
                await cur.execute("DELETE FROM player WHERE player_tag=%s", (tag,))
        await conn.commit()


async def _fetchone(pool, query, params):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            return await cur.fetchone()


async def _fetchall(pool, query, params):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, params)
            return await cur.fetchall()


def _player_payload(tag, name, trophies, battles, last_battle_time=None):
    return {
        "tag": tag,
        "name": name,
        "trophies": trophies,
        "league_tier_id": 105000036,
        "clan": {},
        "battles": battles,
        "last_battle_time": last_battle_time,
    }


async def _run_scenario():
    pool = await _make_pool_or_none()
    if pool is None:
        return None
    try:
        await _cleanup(pool)  # in case a prior run left rows behind

        # Battle time inside the CURRENT legend day (10:00, comfortably past
        # the 05:00 UTC reset) so the mirror bump and upsert_player's own
        # day-count update land on the same `day` row.
        _, pt_day = _pt_now_and_day()
        t1 = datetime(pt_day.year, pt_day.month, pt_day.day, 10, 0, 0)

        # Step 1: A's native fetch -- one attack on B, carrying an army code.
        async with pool.acquire() as conn:
            await upsert_player(
                conn,
                _player_payload(TAG_A, "Tester A", 5300, [
                    {
                        "time": t1, "is_attack": True, "opponent_tag": "#" + TAG_B,
                        "opponent_name": "Tester B", "opponent_th": 17,
                        "stars": 3, "destruction": 100, "duration": 90,
                        "army_share_code": ARMY_CODE,
                    },
                ], last_battle_time=t1),
            )

        native_row = await _fetchone(
            pool,
            "SELECT army_share_code, source, is_attack FROM legend_battle "
            "WHERE player_tag=%s AND opponent_tag=%s AND battle_time=%s",
            (TAG_A, TAG_B, t1),
        )
        mirror_row = await _fetchone(
            pool,
            "SELECT army_share_code, source, is_attack FROM legend_battle "
            "WHERE player_tag=%s AND opponent_tag=%s",
            (TAG_B, TAG_A),
        )
        b_day = await _fetchone(
            pool, "SELECT num_attacks, num_defenses FROM day WHERE player_tag=%s", (TAG_B,))
        b_player = await _fetchone(
            pool, "SELECT name, is_active, last_battle_time FROM player WHERE player_tag=%s",
            (TAG_B,))

        # Step 2: B's own native fetch of the SAME battle, stamped 200s later
        # (cross-perspective drift) -- must replace the mirror, not duplicate.
        t1_drifted = t1 + timedelta(seconds=200)
        async with pool.acquire() as conn:
            await upsert_player(
                conn,
                _player_payload(TAG_B, "Tester B", 5100, [
                    {
                        "time": t1_drifted, "is_attack": False, "opponent_tag": "#" + TAG_A,
                        "opponent_name": "Tester A", "opponent_th": 15,
                        "stars": 3, "destruction": 100, "duration": 90,
                        "army_share_code": None,
                    },
                ], last_battle_time=t1_drifted),
            )

        b_rows_after = await _fetchall(
            pool,
            "SELECT source, battle_time FROM legend_battle "
            "WHERE player_tag=%s AND opponent_tag=%s",
            (TAG_B, TAG_A),
        )
        b_day_after = await _fetchone(
            pool, "SELECT num_attacks, num_defenses FROM day WHERE player_tag=%s", (TAG_B,))

        return {
            "native_row": native_row,
            "mirror_row": mirror_row,
            "b_day": b_day,
            "b_player": b_player,
            "b_rows_after": b_rows_after,
            "b_day_after": b_day_after,
        }
    finally:
        await _cleanup(pool)
        pool.close()
        await pool.wait_closed()


@pytest.fixture(scope="module")
def result():
    import asyncio
    r = asyncio.run(_run_scenario())
    if r is None:
        pytest.skip("no local MySQL reachable (DB_PASSWORD/run/_env.bat unavailable)")
    return r


def test_army_share_code_stored_on_native_row(result):
    code, source, is_attack = result["native_row"]
    assert code == ARMY_CODE
    assert source == "native"
    assert is_attack == 1


def test_mirror_row_created_with_carried_army_code_and_flipped_side(result):
    assert result["mirror_row"] is not None, "mirror row was not harvested"
    code, source, is_attack = result["mirror_row"]
    assert source == "mirror"
    assert is_attack == 0  # A attacked -> B's mirror row is a defense
    assert code == ARMY_CODE  # army code carried through to the flip side


def test_mirror_row_bumps_opponent_day_counts_once(result):
    num_attacks, num_defenses = result["b_day"]
    assert num_attacks == 0
    assert num_defenses == 1


def test_mirror_row_never_advances_opponent_cursor(result):
    name, is_active, last_battle_time = result["b_player"]
    assert name == "Tester B"
    assert is_active == 1
    # The cursor is the native-battlelog dedup watermark. A mirror insert
    # advancing it would make B's next real fetch skip genuine entries.
    assert last_battle_time is None


def test_native_replaces_mirror_within_300s_window_no_duplicate(result):
    rows = result["b_rows_after"]
    assert len(rows) == 1  # exactly one row for this battle, not two
    source, _battle_time = rows[0]
    assert source == "native"  # native won

    num_attacks, num_defenses = result["b_day_after"]
    assert num_attacks == 0
    assert num_defenses == 1  # replaced in place, never double-counted
