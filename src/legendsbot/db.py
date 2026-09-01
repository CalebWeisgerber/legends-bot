# async_db.py
from datetime import datetime, timedelta, timezone
from typing import List
import warnings
import os
import aiomysql

warnings.filterwarnings("ignore", category=UserWarning, module="aiomysql")
# INSERT IGNORE on legend_battle raises a harmless "Duplicate entry" warning
# when a battle is re-seen; the dedup is intentional.
warnings.filterwarnings("ignore", message=".*Duplicate entry.*")

db_name = "PlayerTracking"

# How stale a player's reconciliation may get before the unseen pass re-fetches
# them. Bounds the window in which a player who is unreachable via clan scans
# (clanless, clan untracked/inactive, or just changed clans) goes unpolled.
# Trade-off: lower = smaller blind spot, more /players calls per pass. The clan
# pass refreshes last_seen_at every ~STALE_CLAN_MINUTES for everyone it covers,
# so this only governs genuine stragglers.
UNSEEN_RECONCILE_MINUTES = 15
DEBUG_VERBOSE = os.getenv("BOT_DEBUG", "").lower() in ("1", "true", "yes", "on")
QUIET_MODE = os.getenv("BOT_QUIET", "").lower() in ("1", "true", "yes", "on")

def _log(message: str, force: bool = False) -> None:
    if QUIET_MODE and not force:
        return
    print(message)

def _debug(message: str) -> None:
    if DEBUG_VERBOSE and not QUIET_MODE:
        print(message)

def _legend_day_for(ts: datetime):
    """Legend-day bucket for an arbitrary (naive UTC) timestamp: resets at
    05:00 UTC, so anything before that hour still belongs to the prior date."""
    if ts.hour < 5:
        return (ts - timedelta(days=1)).date()
    return ts.date()

def _pt_now_and_day():
    """
    Calculates the current 'Legends Day'.
    Legends resets at 05:00 UTC.
    Before 05:00 UTC, it is still the previous day's season.
    """
    now_utc = datetime.now(timezone.utc)
    return now_utc, _legend_day_for(now_utc)

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
            # ASCII only: this runs inside pipeline.process_clan's transaction
            # block, where a print() that raises costs the whole clan's batch.
            print(f"[ERROR] upsert_clan error for {tag}: {e!r}")
    
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

        # _debug, not _log: this fires for every zero-legend clan on every
        # pass (~1.4k/cycle, ~480 cycles/day). At _log level it was the single
        # biggest contributor to a 611 MB bot_run.log.
        _debug(f"Marking clan {clan_tag} inactive (no legend members)")
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
    
    # NOTE: currently unused (no caller as of 2026-08-23). Kept in sync with
    # upsert_player anyway -- last_seen_at is the unseen pass's staleness
    # cursor, and a caller that set last_seen without it would silently make
    # players look permanently stale (re-fetched every pass, forever).
    query = f"""
    UPDATE player
    SET last_seen = %s, last_seen_at = UTC_TIMESTAMP(),
        is_active = 1, current_clan_tag = %s
    WHERE player_tag IN ({placeholders})
    """
    
    # params: [date, clan_tag, tag1, tag2, tag3...]
    params = [pt_day, clan_tag] + normalized_tags

    async with conn.cursor() as cur:
        await cur.execute(query, params)
    
    if commit:
        await conn.commit()

# -------- MIRROR-ROW HARVESTING --------
# Every battlelog entry from player A describes one battle vs opponent B, but
# a plain fetch only ever writes A's row. The functions below additionally
# write B's flip-side row (source='mirror') so both perspectives of a battle
# land in legend_battle without waiting to fetch B's own battlelog. See
# README.md's "How the tracking actually works" for the plain-language version.

# Cross-perspective battle_time drift: the SAME battle appears in both
# players' own battlelogs, but each side stamps its own row, and those stamps
# can differ by up to ~300s. UNIQUE(player_tag, battle_time) can't dedupe
# that, so pair-matching looks up (player_tag, opponent_tag, side) within
# this window instead.
_MIRROR_MATCH_WINDOW_SECONDS = 300

async def _find_battle_pair(cur, player_tag: str, opponent_tag: str, is_attack: bool, battle_time: datetime):
    """Find an existing legend_battle row for (player_tag, opponent_tag, side)
    within the +/-300s window. Returns (id, date, source) or None."""
    lo = battle_time - timedelta(seconds=_MIRROR_MATCH_WINDOW_SECONDS)
    hi = battle_time + timedelta(seconds=_MIRROR_MATCH_WINDOW_SECONDS)
    await cur.execute(
        """
        SELECT id, date, source FROM legend_battle
        WHERE player_tag=%s AND opponent_tag=%s AND is_attack=%s
          AND battle_time BETWEEN %s AND %s
        ORDER BY ABS(TIMESTAMPDIFF(SECOND, battle_time, %s))
        LIMIT 1
        """,
        (player_tag, opponent_tag, 1 if is_attack else 0, lo, hi, battle_time),
    )
    return await cur.fetchone()

async def _bump_day_battle_count(cur, player_tag: str, date, is_attack: bool, delta: int):
    """Adjust day.num_attacks/num_defenses for `player_tag` on `date` by
    `delta` (creating the day row first if it doesn't exist yet -- a mirror
    row can land on a player who hasn't had a native poll today). Floored at 0
    so an out-of-order delete/insert pair can never go negative."""
    if delta == 0:
        return
    col = "num_attacks" if is_attack else "num_defenses"
    await cur.execute(
        "INSERT IGNORE INTO day (player_tag, date) VALUES (%s, %s)",
        (player_tag, date),
    )
    await cur.execute(
        f"UPDATE day SET {col} = GREATEST({col} + %s, 0) WHERE player_tag=%s AND date=%s",
        (delta, player_tag, date),
    )

async def _upsert_mirror_player(cur, tag: str, name):
    """Minimal player row for a mirror-row opponent we don't otherwise track.
    They're a legend by definition (battlelog is already filtered to
    battleType='legend'). Never overwrites an existing row's data and never
    triggers an API call -- purely a placeholder so FKs/queries hold."""
    await cur.execute(
        """
        INSERT INTO player (player_tag, name, trophies, is_active)
        VALUES (%s, %s, 0, 1) AS new
        ON DUPLICATE KEY UPDATE
            name = COALESCE(player.name, new.name)
        """,
        (tag, name),
    )

async def _write_mirror_battle(cur, owner_tag: str, owner_name, battle: dict) -> bool:
    """Given a battle just inserted NATIVELY for `owner_tag`, write the
    flip-side row for the opponent (source='mirror') plus its own day-count
    bump. Deliberately does NOT touch player.last_battle_time for the
    opponent -- that column is the native-battlelog dedup cursor, and
    advancing it from a mirrored stamp would let the opponent's own next
    fetch skip real battlelog entries.

    A mirror insert never overwrites an existing native or mirror row for the
    same (opponent, owner, side) pair -- if one's already there, either the
    opponent's own battlelog already covered this battle, or another mirror
    beat us to it.
    """
    opp_tag = normalize_tag(battle.get("opponent_tag"))
    battle_time = battle.get("time")
    if not opp_tag or battle_time is None:
        return False

    is_attack = bool(battle.get("is_attack"))
    mirror_is_attack = not is_attack

    existing = await _find_battle_pair(cur, opp_tag, owner_tag, mirror_is_attack, battle_time)
    if existing:
        return False

    await _upsert_mirror_player(cur, opp_tag, battle.get("opponent_name"))

    mirror_date = _legend_day_for(battle_time)
    await cur.execute(
        """
        INSERT IGNORE INTO legend_battle
            (player_tag, date, battle_time, is_attack, opponent_tag,
             opponent_name, opponent_th, stars, destruction, duration,
             army_share_code, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'mirror')
        """,
        (
            opp_tag, mirror_date, battle_time, 1 if mirror_is_attack else 0,
            owner_tag, owner_name, None,
            battle.get("stars"), battle.get("destruction"), battle.get("duration"),
            battle.get("army_share_code"),
        ),
    )
    if cur.rowcount != 1:
        return False

    await _bump_day_battle_count(cur, opp_tag, mirror_date, mirror_is_attack, 1)
    return True

async def reconcile_mirror_duplicates(conn, days_back: int = 7) -> int:
    """Post-pass sweep: delete any mirror row whose NATIVE counterpart exists
    for the same (player, opponent, side) within the +/-300s drift window,
    decrementing the day count the mirror had bumped.

    Why it's needed on top of upsert_player's inline native-wins check: the
    update pass runs clans on many concurrent connections. If players A and B
    (who just fought each other) are both polled in the same pass, A's
    transaction can write B's mirror row while B's own transaction is
    concurrently writing B's native row -- neither sees the other's
    uncommitted insert, so both land (observed on ~0.04% of inserts). This
    sweep makes "native always wins, never double-counted" hold
    deterministically. Bounded to recent rows since only fresh writes can
    race. Returns the number of mirror rows removed."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT m.id, m.player_tag, m.date, m.is_attack
            FROM legend_battle m
            JOIN legend_battle n
              ON n.player_tag = m.player_tag
             AND n.opponent_tag = m.opponent_tag
             AND n.is_attack = m.is_attack
             AND n.source = 'native'
             AND n.battle_time BETWEEN m.battle_time - INTERVAL %s SECOND
                                   AND m.battle_time + INTERVAL %s SECOND
            WHERE m.source = 'mirror'
              AND m.opponent_tag IS NOT NULL
              AND m.battle_time > UTC_TIMESTAMP() - INTERVAL %s DAY
            """,
            (_MIRROR_MATCH_WINDOW_SECONDS, _MIRROR_MATCH_WINDOW_SECONDS, days_back),
        )
        rows = await cur.fetchall()
        for mid, ptag, m_date, m_is_attack in rows:
            await cur.execute("DELETE FROM legend_battle WHERE id=%s", (mid,))
            await _bump_day_battle_count(cur, ptag, m_date, bool(m_is_attack), -1)
    await conn.commit()
    return len(rows)

async def upsert_player(conn, player_data: dict, commit: bool = True):
    """
    SLOW PATH: Full logic. Checks history, inserts logs, updates day stats.
    Only run this when trophies CHANGE or for new players.
    """
    tag = normalize_tag(player_data.get("tag"))
    name = player_data.get("name")
    new_trophies = player_data.get("trophies")

    clan = player_data.get("clan", {}) or {}
    raw_ctag = clan.get("tag")
    clan_tag = normalize_tag(raw_ctag) if raw_ctag else None

    # battlelog-derived: list of new legend battles + the new cursor (max time).
    battles = player_data.get("battles") or []
    new_cursor = player_data.get("last_battle_time")

    pt_now, pt_day = _pt_now_and_day()

    async with conn.cursor() as cur:
        # Ensure Clan exists (Vital for process_unseen if player joined a new clan)
        if clan_tag:
            try:
                # REVIVE an inactive clan on contact. Every caller of
                # upsert_player has already established that THIS player is a
                # Legend (process_clan filters on is_legend, process_unseen
                # bails out otherwise), so a clan we're told they belong to
                # provably has >=1 legend member and must be scannable again.
                #
                # Without this, a clan that momentarily showed zero legends got
                # mark_clan_inactive()'d and was then unreachable FOREVER:
                # get_clans_to_scan() only selects is_active = 1, and nothing
                # else ever set it back (only a seed run could). Its members
                # fell through to the unseen pass, which get_all_player_tags()
                # gates on last_seen < today -- so they got at most ONE
                # battlelog poll per legend day, and any battle outside that
                # single sample was lost. Measured 2026-08-23: 58,046 active
                # players (24% of the tracked base) were stranded in inactive
                # clans, and 0 of them were eligible for the unseen pass.
                #
                # last_member_scan is reset to NULL only on the revival edge so
                # the next pass picks the clan up immediately. Assignment order
                # matters: MySQL evaluates ON DUPLICATE KEY UPDATE clauses left
                # to right, so is_active must be read (and reset) BEFORE it is
                # overwritten to 1. Already-active clans keep their timestamp,
                # preserving the STALE_CLAN_MINUTES stagger.
                await cur.execute(
                    """
                    INSERT INTO clan (clan_tag, clan_name)
                    VALUES (%s, %s) AS new
                    ON DUPLICATE KEY UPDATE
                        clan_name        = new.clan_name,
                        last_member_scan = IF(clan.is_active = 0, NULL,
                                              clan.last_member_scan),
                        is_active        = 1
                    """,
                    (clan_tag, clan.get("name", "Unknown")),
                )
            except Exception as e:
                # ASCII only -- see upsert_clan's note.
                print(f"[WARN] Clan insert error inside player upsert: {e!r}")

        # Update Player
        await cur.execute(
            """
            INSERT INTO player (player_tag, name, trophies, league_tier_id,
                                current_clan_tag, last_seen, last_seen_at,
                                is_active)
            VALUES (%s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), 1) AS new
            ON DUPLICATE KEY UPDATE
                name = new.name,
                trophies = new.trophies,
                -- keep the last known tier when this payload didn't carry one
                league_tier_id = COALESCE(new.league_tier_id, player.league_tier_id),
                current_clan_tag = new.current_clan_tag,
                last_seen = new.last_seen,
                -- minute-resolution reconciliation cursor for the unseen pass;
                -- last_seen (DATE) is kept for day-level reporting.
                last_seen_at = UTC_TIMESTAMP(),
                is_active = 1
            """,
            (tag, name, new_trophies, player_data.get("league_tier_id"),
             clan_tag, pt_day),
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

        # Counts come from battlelog: insert each new legend battle (dedup on
        # uq_battle) and count only rows that were actually inserted. Each
        # native insert also harvests the opponent's flip-side (mirror) row --
        # see MIRROR-ROW HARVESTING above.
        #
        # Every row is dated by the legend day the BATTLE happened in, not the
        # legend day we happened to insert it. `pt_day` is insert-time and is
        # wrong for any battle we did not poll within the same legend day:
        # after the 2026-08-21..23 outage, date=2026-08-23 held 59,711 native
        # rows whose battle_time reached back to 08-21, while date=2026-08-22
        # had ZERO native rows. It also mis-buckets in normal operation -- a
        # battle at 04:59 UTC polled at 05:01 landed on the next legend day.
        # Mirror rows already did this correctly (_write_mirror_battle uses
        # _legend_day_for), so natives were the odd ones out.
        for b in battles:
            is_attack = bool(b.get("is_attack"))
            opp_tag = normalize_tag(b.get("opponent_tag")) or None
            battle_time = b.get("time")
            battle_date = _legend_day_for(battle_time) if battle_time else pt_day

            # Native wins: if the opponent's own fetch already wrote a mirror
            # row for this same battle, replace it and undo the day-count
            # bump it made (the native insert below re-adds it for `tag`).
            if opp_tag and battle_time is not None:
                match = await _find_battle_pair(cur, tag, opp_tag, is_attack, battle_time)
                if match and match[2] == "mirror":
                    await cur.execute("DELETE FROM legend_battle WHERE id=%s", (match[0],))
                    await _bump_day_battle_count(cur, tag, match[1], is_attack, -1)

            await cur.execute(
                """
                INSERT IGNORE INTO legend_battle
                    (player_tag, date, battle_time, is_attack, opponent_tag,
                     opponent_name, opponent_th, stars, destruction, duration,
                     army_share_code, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'native')
                """,
                (
                    tag, battle_date, battle_time, 1 if is_attack else 0,
                    opp_tag, b.get("opponent_name"), b.get("opponent_th"),
                    b.get("stars"), b.get("destruction"), b.get("duration"),
                    b.get("army_share_code"),
                ),
            )
            if cur.rowcount == 1:
                # Bump the counter on the battle's OWN legend day, not on
                # pt_day -- a catch-up poll routinely ingests battles spanning
                # several days and they must land on the day they happened.
                # _bump_day_battle_count creates the day row if it's missing.
                await _bump_day_battle_count(cur, tag, battle_date, is_attack, 1)
                if opp_tag:
                    await _write_mirror_battle(cur, tag, name, b)

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

        # Trophy state and magnitude stay on pt_day -- they describe "now", not
        # any individual battle. Battle COUNTS are bumped per-row above, on each
        # battle's own legend day, so they are deliberately absent here.
        await cur.execute(
            """
            UPDATE day SET
                end_trophies = %s,
                offense      = offense + %s,
                defense      = defense + %s
            WHERE player_tag = %s AND date = %s
            """,
            (new_trophies, off_mag, def_mag, tag, pt_day),
        )

        # Advance the battlelog cursor (only forward).
        if new_cursor is not None:
            await cur.execute(
                "UPDATE player SET last_battle_time = %s "
                "WHERE player_tag = %s AND (last_battle_time IS NULL OR last_battle_time < %s)",
                (new_cursor, tag, new_cursor),
            )

    if commit:
        await conn.commit()

async def upsert_league_season(conn, player_tag: str, items: list, commit: bool = True):
    """Store /players/{tag}/leaguehistory rows (per-league-season summary)."""
    tag = normalize_tag(player_tag)
    async with conn.cursor() as cur:
        for it in items:
            sid = it.get("leagueSeasonId")
            if sid is None:
                continue
            await cur.execute(
                """
                INSERT INTO league_season
                    (player_tag, league_season_id, league_trophies, league_tier_id, placement,
                     attack_wins, attack_losses, attack_stars,
                     defense_wins, defense_losses, defense_stars, max_battles)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
                ON DUPLICATE KEY UPDATE
                    league_trophies = new.league_trophies,
                    league_tier_id  = new.league_tier_id,
                    placement       = new.placement,
                    attack_wins     = new.attack_wins,
                    attack_losses   = new.attack_losses,
                    attack_stars    = new.attack_stars,
                    defense_wins    = new.defense_wins,
                    defense_losses  = new.defense_losses,
                    defense_stars   = new.defense_stars,
                    max_battles     = new.max_battles
                """,
                (
                    tag, sid, it.get("leagueTrophies"), it.get("leagueTierId"), it.get("placement"),
                    it.get("attackWins"), it.get("attackLosses"), it.get("attackStars"),
                    it.get("defenseWins"), it.get("defenseLosses"), it.get("defenseStars"),
                    it.get("maxBattles"),
                ),
            )
    if commit:
        await conn.commit()


async def snapshot_reset_clan_tags(conn):
    async with conn.cursor() as cur:
        await cur.execute("UPDATE player SET reset_clan_tag = current_clan_tag WHERE is_active = 1")
    await conn.commit()

async def get_all_player_tags(conn) -> List[str]:
    """
    Return active players we haven't reconciled in the last
    UNSEEN_RECONCILE_MINUTES, for the unseen pass to pick up (clanless players,
    players whose clan wasn't rescanned, and players who changed clans).

    Some staleness filter is required: without one, the routine update
    re-fetches the entire active playerbase every cycle (one /players call
    each). But this used to gate on `last_seen < today`, and last_seen is a
    DATE -- so a single reconciliation excluded a player for the REST of the
    legend day. A player who changed clans mid-day then fell through every
    path at once (old clan no longer lists them, new clan possibly untracked,
    unseen pass skipping them) and went unpolled for up to 24h. Measured
    2026-08-23: 60,053 active players (24.6%) were unreachable by the clan
    pass and zero of them were eligible here.

    A minute-resolution cursor keeps the "don't re-fetch everyone" property
    while capping the blind spot at UNSEEN_RECONCILE_MINUTES. The clan pass
    refreshes last_seen_at every ~STALE_CLAN_MINUTES for everyone it covers,
    so in steady state this returns only genuinely unreachable players.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT player_tag FROM player "
            "WHERE is_active = 1 "
            "  AND (last_seen_at IS NULL "
            "       OR last_seen_at < UTC_TIMESTAMP() - INTERVAL %s MINUTE)",
            (UNSEEN_RECONCILE_MINUTES,),
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
