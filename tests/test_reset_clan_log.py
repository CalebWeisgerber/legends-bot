"""Integration tests for player_reset_clan_log (migration 0011) and
snapshot_reset_clan_tags. Needs a real MySQL (same convention as
test_mirror_rows.py: DB_PASSWORD from run/_env.bat, skipped if unreachable or
the migration isn't applied).

Rows live under ZZTESTRCL* tags and the snapshot is restricted to them via
`only_tags`, so real players are never touched. Dates use year 2099.
"""
from __future__ import annotations

import asyncio
import os
import re
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("aiomysql")

from legendsbot.db import (  # noqa: E402
    _reset_log_date, make_pool, snapshot_reset_clan_tags, upsert_clan)

ROOT = Path(__file__).resolve().parents[1]
P_CHANGE, P_SAME, P_NULL, P_NEW = "ZZTESTRCLP1", "ZZTESTRCLP2", "ZZTESTRCLP3", "ZZTESTRCLP4"
PLAYERS = [P_CHANGE, P_SAME, P_NULL, P_NEW]
CLAN_A, CLAN_B = "ZZTESTRCLA", "ZZTESTRCLB"


def _load_db_password() -> None:
    if os.getenv("DB_PASSWORD"):
        return
    env_file = ROOT / "run" / "_env.bat"
    if env_file.exists():
        m = re.search(r"DB_PASSWORD=([^\s\"]+)", env_file.read_text(encoding="utf-8"))
        if m:
            os.environ["DB_PASSWORD"] = m.group(1)


_load_db_password()


def test_reset_log_date_boundary():
    # Job fires 04:59 UTC just before the reset: it records the reset that opens THAT date.
    assert _reset_log_date(datetime(2099, 1, 2, 4, 59)) == date(2099, 1, 2)
    assert _reset_log_date(datetime(2099, 1, 2, 5, 0, 1)) == date(2099, 1, 2)
    assert _reset_log_date(datetime(2099, 1, 2, 23, 59)) == date(2099, 1, 2)
    assert _reset_log_date(datetime(2099, 1, 2, 0, 5)) == date(2099, 1, 1)
    assert _reset_log_date(datetime(2099, 1, 2, 4, 20)) == date(2099, 1, 1)


async def _cleanup(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for t in PLAYERS:
                await cur.execute("DELETE FROM player_reset_clan_log WHERE player_tag=%s", (t,))
                await cur.execute("DELETE FROM player WHERE player_tag=%s", (t,))
            for c in (CLAN_A, CLAN_B):
                await cur.execute("DELETE FROM clan WHERE clan_tag=%s", (c,))
        await conn.commit()


async def _scenario():
    try:
        pool = await make_pool(minsize=1, maxsize=2)
    except Exception:
        return None
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SHOW TABLES LIKE 'player_reset_clan_log'")
            if not await cur.fetchone():
                pool.close(); await pool.wait_closed()
                return None
    out = {}
    try:
        await _cleanup(pool)

        async def q(sql, args=()):
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, args)
                    rows = await cur.fetchall()
                await conn.commit()
                return rows

        async def log(tag):
            return list(await q("SELECT legend_date, clan_tag FROM player_reset_clan_log "
                           "WHERE player_tag=%s ORDER BY legend_date", (tag,)))

        async def snap(now):
            async with pool.acquire() as conn:
                await snapshot_reset_clan_tags(conn, now_utc=now, only_tags=PLAYERS)

        async with pool.acquire() as conn:
            await upsert_clan(conn, "Test A", CLAN_A)
            await upsert_clan(conn, "Test B", CLAN_B)
        # CHANGE: reset=A, now=B (changed). SAME: A/A. NULL: reset=A, now=NULL (left clan).
        # NEW: reset=B, now=B but no log row yet.
        for tag, reset, cur_ in ((P_CHANGE, CLAN_A, CLAN_B), (P_SAME, CLAN_A, CLAN_A),
                                 (P_NULL, CLAN_A, None), (P_NEW, CLAN_B, CLAN_B)):
            await q("INSERT INTO player (player_tag, current_clan_tag, reset_clan_tag, is_active) "
                    "VALUES (%s,%s,%s,1)", (tag, cur_, reset))
        # SAME already has a day-1 log row equal to its clan; others do not (except CHANGE).
        await q("INSERT INTO player_reset_clan_log VALUES (%s,'2099-01-01',%s,UTC_TIMESTAMP())",
                (P_SAME, CLAN_A))
        await q("INSERT INTO player_reset_clan_log VALUES (%s,'2099-01-01',%s,UTC_TIMESTAMP())",
                (P_CHANGE, CLAN_A))

        await snap(datetime(2099, 1, 2, 4, 59))
        out["day2"] = {t: await log(t) for t in PLAYERS}
        out["reset_after"] = await q(
            "SELECT player_tag, reset_clan_tag FROM player WHERE player_tag IN (%s,%s,%s,%s)",
            tuple(PLAYERS))

        await snap(datetime(2099, 1, 2, 4, 59))  # re-run same day
        out["rerun"] = {t: await log(t) for t in PLAYERS}

        # Next day: no changes at all -> no new rows.
        await snap(datetime(2099, 1, 3, 4, 59))
        out["day3_nochange"] = {t: await log(t) for t in PLAYERS}

        # SAME now changes clan; NULL player rejoins A.
        await q("UPDATE player SET current_clan_tag=%s WHERE player_tag=%s", (CLAN_B, P_SAME))
        await q("UPDATE player SET current_clan_tag=%s WHERE player_tag=%s", (CLAN_A, P_NULL))
        await snap(datetime(2099, 1, 4, 4, 59))
        out["day4"] = {t: await log(t) for t in PLAYERS}
        return out
    finally:
        await _cleanup(pool)
        pool.close()
        await pool.wait_closed()


@pytest.fixture(scope="module")
def result():
    r = asyncio.run(_scenario())
    if r is None:
        pytest.skip("MySQL / migration 0011 not available")
    return r


D1, D2, D3, D4 = date(2099, 1, 1), date(2099, 1, 2), date(2099, 1, 3), date(2099, 1, 4)


def test_change_inserts_row(result):
    assert result["day2"][P_CHANGE] == [(D1, CLAN_A), (D2, CLAN_B)]


def test_no_change_no_row(result):
    assert result["day2"][P_SAME] == [(D1, CLAN_A)]


def test_clanless_is_logged_as_null(result):
    # reset=A, current=NULL: `<=>` must see this as a change.
    assert result["day2"][P_NULL] == [(D2, None)]


def test_player_with_no_log_row_gets_one_even_if_unchanged(result):
    assert result["day2"][P_NEW] == [(D2, CLAN_B)]


def test_reset_clan_tag_still_refreshed(result):
    assert dict(result["reset_after"]) == {
        P_CHANGE: CLAN_B, P_SAME: CLAN_A, P_NULL: None, P_NEW: CLAN_B}


def test_rerun_same_day_idempotent(result):
    assert result["rerun"] == result["day2"]


def test_next_day_without_changes_writes_nothing(result):
    assert result["day3_nochange"] == result["day2"]


def test_later_changes_appended(result):
    assert result["day4"][P_SAME] == [(D1, CLAN_A), (D4, CLAN_B)]
    assert result["day4"][P_NULL] == [(D2, None), (D4, CLAN_A)]
    assert result["day4"][P_CHANGE] == [(D1, CLAN_A), (D2, CLAN_B)]
