# async_db.py
from datetime import datetime, timedelta, timezone
from typing import List
import warnings
import os
import aiomysql

warnings.filterwarnings("ignore", category=UserWarning, module="aiomysql")

db_name = "PlayerTracking"
DEBUG_VERBOSE = os.getenv("BOT_DEBUG", "").lower() in ("1", "true", "yes", "on")
QUIET_MODE = os.getenv("BOT_QUIET", "").lower() in ("1", "true", "yes", "on")

def _log(message: str, force: bool = False) -> None:
    if QUIET_MODE and not force:
        return
    print(message)

def _debug(message: str) -> None:
    if DEBUG_VERBOSE and not QUIET_MODE:
        print(message)

def _pt_now_and_day():
    """
    Calculates the current 'Legends Day'.
    Legends resets at 05:00 UTC.
    Before 05:00 UTC, it is still the previous day's season.
    """
    now_utc = datetime.now(timezone.utc)
    
    # If it is before 5:00 AM UTC, the 'Legends Day' is technically yesterday
    if now_utc.hour < 5:
        legend_date = (now_utc - timedelta(days=1)).date()
    else:
        legend_date = now_utc.date()
        
    return now_utc, legend_date

def normalize_tag(tag: str | None) -> str:
    if not tag:
        return ""
    return tag.replace("#", "").upper()

# -------- CLANS --------

async def upsert_clan(conn, name: str, tag: str, commit: bool = True):
    tag = normalize_tag(tag)
    query = """
    INSERT INTO clan (clan_tag, clan_name)
    VALUES (%s, %s) AS new
    ON DUPLICATE KEY UPDATE clan_name = new.clan_name
    """
    async with conn.cursor() as cur:
        try:
            await cur.execute(query, (tag, name))
        except aiomysql.MySQLError as e:
            print(f"🛑 upsert_clan error for {tag}: {e}")
    
    if commit:
        await conn.commit()

async def get_all_clan_tags(conn) -> List[str]:
    async with conn.cursor() as cur:
        await cur.execute("SELECT clan_tag FROM clan")
        rows = await cur.fetchall()
        return [row[0] for row in rows]

async def mark_clan_active(conn, clan_tag: str):
    """
    Mark a clan as active and record the last member scan time.
    Called when we see at least one Legend in this clan.
    """
    clan_tag = normalize_tag(clan_tag)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE clan
            SET is_active = 1,
                last_member_scan = UTC_TIMESTAMP()
            WHERE clan_tag = %s
            """,
            (clan_tag,),
        )
    await conn.commit()


async def mark_clan_inactive(conn, clan_tag: str):
    """
    Mark a clan as inactive and clear current_clan_tag from players
    so they don't point at an inactive clan.
    Called when we see zero Legends or the clan is 404.
    """
    clan_tag = normalize_tag(clan_tag)
    async with conn.cursor() as cur:
        # Clear current_clan_tag so no players point at an inactive clan
        await cur.execute(
            "UPDATE player SET current_clan_tag = NULL WHERE current_clan_tag = %s",
            (clan_tag,),
        )

        await cur.execute(
            """
            UPDATE clan
            SET is_active = 0,
                last_member_scan = UTC_TIMESTAMP()
            WHERE clan_tag = %s
            """,
            (clan_tag,),
        )

        _log(f"🧹 Marking clan {clan_tag} inactive (no legend members)")
    await conn.commit()


async def safe_delete_clan(conn, clan_tag: str):
    """
    Backwards-compatible wrapper.
    Old behavior: hard delete.
    New behavior: mark clan inactive & clear current_clan_tag.
    """
    await mark_clan_inactive(conn, clan_tag)
    
# -------- PLAYERS --------

async def bulk_update_seen(conn, player_tags: List[str], clan_tag: str, commit: bool = True):
    """
    FAST PATH: Updates last_seen/clan for players who haven't changed trophies.
    Executes 1 single SQL query for up to 50 players.
    """
    if not player_tags:
        return

    normalized_tags = [normalize_tag(t) for t in player_tags]
    clan_tag = normalize_tag(clan_tag)
    _, pt_day = _pt_now_and_day()

    # Dynamic SQL for IN clause: (%s, %s, %s...)
    placeholders = ','.join(['%s'] * len(normalized_tags))
    
    query = f"""
    UPDATE player 
    SET last_seen = %s, is_active = 1, current_clan_tag = %s 
    WHERE player_tag IN ({placeholders})
    """
    
    # params: [date, clan_tag, tag1, tag2, tag3...]
    params = [pt_day, clan_tag] + normalized_tags

    async with conn.cursor() as cur:
        await cur.execute(query, params)
    
    if commit:
        await conn.commit()

async def upsert_player(conn, player_data: dict, commit: bool = True):
    """
    SLOW PATH: Full logic. Checks history, inserts logs, updates day stats.
    Only run this when trophies CHANGE or for new players.
    """
    tag = normalize_tag(player_data.get("tag"))
    name = player_data.get("name")
    new_trophies = player_data.get("trophies")
    new_att = int(player_data.get("attackWins") or 0)
    new_def = int(player_data.get("defenseWins") or 0)

    clan = player_data.get("clan", {}) or {}
    raw_ctag = clan.get("tag")
    clan_tag = normalize_tag(raw_ctag) if raw_ctag else None

    pt_now, pt_day = _pt_now_and_day()

    async with conn.cursor() as cur:
        # Ensure Clan exists (Vital for process_unseen if player joined a new clan)
        if clan_tag:
            try:
                await cur.execute(
                    """
                    INSERT INTO clan (clan_tag, clan_name)
                    VALUES (%s, %s) AS new
                    ON DUPLICATE KEY UPDATE clan_name = new.clan_name
                    """,
                    (clan_tag, clan.get("name", "Unknown")),
                )
            except Exception as e:
                print(f"⚠️ Clan insert error inside player upsert: {e}")

        # Previous poll baseline (read BEFORE overwriting the player row) so we
        # can diff attack/defense win counts. memberList lacks these, so the
        # caller must have fetched /players for changed players.
        await cur.execute(
            "SELECT attack_wins, defense_wins FROM player WHERE player_tag=%s",
            (tag,),
        )
        prow = await cur.fetchone()
        prev_att, prev_def = (prow if prow else (None, None))

        # Update Player (incl. season win counters)
        await cur.execute(
            """
            INSERT INTO player (player_tag, name, trophies, attack_wins, defense_wins, current_clan_tag, last_seen, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1) AS new
            ON DUPLICATE KEY UPDATE
                name = new.name,
                trophies = new.trophies,
                attack_wins = new.attack_wins,
                defense_wins = new.defense_wins,
                current_clan_tag = new.current_clan_tag,
                last_seen = new.last_seen,
                is_active = 1
            """,
            (tag, name, new_trophies, new_att, new_def, clan_tag, pt_day),
        )

        await cur.execute(
            "SELECT start_trophies, end_trophies FROM day WHERE player_tag=%s AND date=%s",
            (tag, pt_day),
        )
        row = await cur.fetchone()

        prev_end = None

        if not row:
            # New day or new player: Look for history
            await cur.execute(
                "SELECT end_trophies FROM day WHERE player_tag=%s ORDER BY date DESC LIMIT 1",
                (tag,)
            )
            last_known = await cur.fetchone()

            if last_known:
                prev_end = last_known[0]

            await cur.execute(
                "INSERT INTO day (player_tag, date, start_trophies, end_trophies) VALUES (%s, %s, %s, %s)",
                (tag, pt_day, new_trophies, new_trophies),
            )
        else:
            _, prev_end = row

        # Counts come from win-counter deltas (exact); clamp negatives (monthly
        # season reset) and treat first-ever sighting as no baseline.
        d_att = max(0, new_att - prev_att) if prev_att is not None else 0
        d_def = max(0, new_def - prev_def) if prev_def is not None else 0

        # Magnitude comes from the trophy delta since the last poll this day.
        off_mag = 0
        def_mag = 0
        if prev_end is not None and new_trophies != prev_end:
            diff = new_trophies - prev_end
            if diff < -320:
                # Season reset likely — don't attribute as a defense.
                _debug(f"📉 Season reset detected for {tag} ({prev_end} -> {new_trophies}).")
            elif diff > 0:
                off_mag = diff
            else:
                def_mag = abs(diff)

        if off_mag > 0:
            await cur.execute(
                "INSERT INTO attack_log (player_tag, date, trophies_gained, timestamp) VALUES (%s, %s, %s, %s)",
                (tag, pt_day, off_mag, pt_now),
            )
        if def_mag > 0:
            await cur.execute(
                "INSERT INTO defense_log (player_tag, date, trophies_lost, timestamp) VALUES (%s, %s, %s, %s)",
                (tag, pt_day, def_mag, pt_now),
            )

        await cur.execute(
            """
            UPDATE day SET
                end_trophies = %s,
                offense      = offense + %s,
                defense      = defense + %s,
                num_attacks  = num_attacks + %s,
                num_defenses = num_defenses + %s
            WHERE player_tag = %s AND date = %s
            """,
            (new_trophies, off_mag, def_mag, d_att, d_def, tag, pt_day),
        )

    if commit:
        await conn.commit()

async def snapshot_reset_clan_tags(conn):
    async with conn.cursor() as cur:
        await cur.execute("UPDATE player SET reset_clan_tag = current_clan_tag WHERE is_active = 1")
    await conn.commit()

async def get_all_player_tags(conn) -> List[str]:
    """
    Return active players NOT already seen this legend-day, for the unseen pass
    to reconcile (clanless players, or players whose clan wasn't rescanned).

    Without the last_seen filter the routine update re-fetches the entire active
    playerbase every cycle (one /players call each) — after the daily full pass
    marks everyone seen, this leaves only genuine stragglers.
    """
    _, pt_day = _pt_now_and_day()
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT player_tag FROM player "
            "WHERE is_active = 1 AND (last_seen IS NULL OR last_seen < %s)",
            (pt_day,),
        )
        rows = await cur.fetchall()
        return [row[0] for row in rows]

async def delete_player_and_logs(conn, player_tag: str):
    tag = normalize_tag(player_tag)
    async with conn.cursor() as cur:
        await cur.execute("DELETE FROM day WHERE player_tag = %s", (tag,))
        await cur.execute("DELETE FROM player WHERE player_tag = %s", (tag,))
    await conn.commit()

# Database connection settings come from env vars (no credentials in source).
# DB_PASSWORD is REQUIRED; host/user/name/port have non-secret defaults.
def _db_settings() -> dict:
    password = os.getenv("DB_PASSWORD")
    if password is None:
        raise RuntimeError(
            "DB_PASSWORD environment variable is required and is not set. "
            "Set it before running, e.g. (Windows) `set DB_PASSWORD=yourpassword` "
            "or via run/_env.bat (see run/_env.bat.example)."
        )
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": password,
        "db": os.getenv("DB_NAME", db_name),
    }


async def make_pool(minsize: int = 5, maxsize: int = 75, autocommit: bool = False) -> aiomysql.Pool:
    """Create an aiomysql pool from the DB_* env settings (utf8mb4, recycle 1h)."""
    return await aiomysql.create_pool(
        **_db_settings(),
        autocommit=autocommit,
        pool_recycle=3600,
        minsize=minsize,
        maxsize=maxsize,
        charset="utf8mb4",
        use_unicode=True,
    )


async def get_pool(minsize: int = 5, maxsize: int = 75) -> aiomysql.Pool:
    return await make_pool(minsize=minsize, maxsize=maxsize, autocommit=False)
