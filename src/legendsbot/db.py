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

async def mark_clan_active(conn, clan_tag: str, commit: bool = True):
    """
    Mark a clan as active and record the last member scan time.
    Called when we see at least one Legend in this clan.

    `commit=False` for callers running inside their own transaction (see
    process_clan): committing here would end that transaction early, leaving
    its `except: rollback()` with nothing left to undo.
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
    if commit:
        await conn.commit()


async def mark_clan_inactive(conn, clan_tag: str, commit: bool = True):
    """
    Mark a clan as inactive and clear current_clan_tag from players
    so they don't point at an inactive clan.
    Called when we see zero Legends or the clan is 404.

    `commit=False` for callers inside their own transaction -- see
    mark_clan_active.
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
    if commit:
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
    within the +/-300s window. Returns the row and its defender clan snapshot."""
    lo = battle_time - timedelta(seconds=_MIRROR_MATCH_WINDOW_SECONDS)
    hi = battle_time + timedelta(seconds=_MIRROR_MATCH_WINDOW_SECONDS)
    await cur.execute(
        """
        SELECT id, date, source, defender_clan_name,
               defender_clan_observed_at, defender_clan_source
        FROM legend_battle
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
    # One statement, not an INSERT IGNORE + UPDATE pair. This is the highest
    # frequency write in the pipeline (once per battle, plus once per harvested
    # mirror row), so the second round trip was pure overhead.
    # GREATEST on the insert branch too: a -1 delta on a day row that does not
    # exist yet must create it at 0, never at -1.
    await cur.execute(
        f"INSERT INTO day (player_tag, date, {col}) VALUES (%s, %s, GREATEST(%s, 0)) AS new "
        f"ON DUPLICATE KEY UPDATE {col} = GREATEST(day.{col} + %s, 0)",
        (player_tag, date, delta, delta),
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

async def _write_mirror_battle(cur, owner_tag: str, owner_name, battle: dict,
                               defender_clan: tuple) -> bool:
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
        if mirror_is_attack and defender_clan[0]:
            await cur.execute(
                """UPDATE legend_battle
                   SET defender_clan_name=%s, defender_clan_observed_at=%s,
                       defender_clan_source=%s
                   WHERE id=%s AND defender_clan_name IS NULL""",
                (*defender_clan, existing[0]),
            )
        return False

    await _upsert_mirror_player(cur, opp_tag, battle.get("opponent_name"))

    mirror_date = _legend_day_for(battle_time)
    await cur.execute(
        """
        INSERT IGNORE INTO legend_battle
            (player_tag, date, battle_time, is_attack, opponent_tag,
             opponent_name, opponent_th, defender_clan_name,
             defender_clan_observed_at, defender_clan_source,
             stars, destruction, duration, army_share_code, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'mirror')
        """,
        (
            opp_tag, mirror_date, battle_time, 1 if mirror_is_attack else 0,
            owner_tag, owner_name, None,
            *(defender_clan if mirror_is_attack else (None, None, None)),
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
            SELECT m.id, m.player_tag, m.date, m.is_attack,
                   n.id, m.defender_clan_name,
                   m.defender_clan_observed_at, m.defender_clan_source
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
        for mid, ptag, m_date, m_is_attack, nid, clan_name, observed_at, clan_source in rows:
            if clan_name:
                await cur.execute(
                    """UPDATE legend_battle
                       SET defender_clan_name=%s, defender_clan_observed_at=%s,
                           defender_clan_source=%s
                       WHERE id=%s AND defender_clan_name IS NULL""",
                    (clan_name, observed_at, clan_source, nid),
                )
            await cur.execute("DELETE FROM legend_battle WHERE id=%s", (mid,))
            await _bump_day_battle_count(cur, ptag, m_date, bool(m_is_attack), -1)
    await conn.commit()
    return len(rows)

async def upsert_player(conn, player_data: dict, commit: bool = True,
                        clan_already_upserted: bool = False):
    """
    SLOW PATH: Full logic. Checks history, inserts logs, updates day stats.
    Only run this when trophies CHANGE or for new players.

    `clan_already_upserted=True` skips the clan INSERT below. process_clan
    upserts the clan once for the whole roster, so without this every legend
    member re-ran the identical statement -- 16 clan upserts for a 15-legend
    clan, every pass. process_unseen must NOT set it: a player who moved to an
    untracked clan has no other path that creates that clan row.
    """
    tag = normalize_tag(player_data.get("tag"))
    name = player_data.get("name")
    new_trophies = player_data.get("trophies")

    clan = player_data.get("clan", {}) or {}
    raw_ctag = clan.get("tag")
    clan_tag = normalize_tag(raw_ctag) if raw_ctag else None
    defender_clan = (
        clan.get("name") or None,
        player_data.get("clan_observed_at"),
        player_data.get("clan_source"),
    ) if clan_tag and player_data.get("clan_observed_at") else (None, None, None)

    # battlelog-derived: list of new legend battles + the new cursor (max time).
    battles = player_data.get("battles") or []
    new_cursor = player_data.get("last_battle_time")

    pt_now, pt_day = _pt_now_and_day()

    async with conn.cursor() as cur:
        # Ensure Clan exists (Vital for process_unseen if player joined a new clan)
        if clan_tag and not clan_already_upserted:
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
                                is_active, last_battle_time)
            VALUES (%s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), 1, %s) AS new
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
                is_active = 1,
                -- Battlelog dedup cursor, folded in here rather than issued as
                -- a second UPDATE against the row we are already writing. Both
                -- of the original guards are preserved: a NULL cursor in this
                -- payload leaves the stored value alone, and the value only
                -- ever moves forward.
                last_battle_time = IF(new.last_battle_time IS NULL,
                                      player.last_battle_time,
                                      GREATEST(COALESCE(player.last_battle_time,
                                                        new.last_battle_time),
                                               new.last_battle_time))
            """,
            (tag, name, new_trophies, player_data.get("league_tier_id"),
             clan_tag, pt_day, new_cursor),
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
            battle_defender_clan = (None, None, None) if is_attack else defender_clan

            # Native wins: if the opponent's own fetch already wrote a mirror
            # row for this same battle, replace it and undo the day-count
            # bump it made (the native insert below re-adds it for `tag`).
            if opp_tag and battle_time is not None:
                match = await _find_battle_pair(cur, tag, opp_tag, is_attack, battle_time)
                if match and match[2] == "mirror":
                    if is_attack:
                        battle_defender_clan = match[3:6]
                    await cur.execute("DELETE FROM legend_battle WHERE id=%s", (match[0],))
                    await _bump_day_battle_count(cur, tag, match[1], is_attack, -1)

            await cur.execute(
                """
                INSERT IGNORE INTO legend_battle
                    (player_tag, date, battle_time, is_attack, opponent_tag,
                     opponent_name, opponent_th, defender_clan_name,
                     defender_clan_observed_at, defender_clan_source,
                     stars, destruction, duration, army_share_code, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'native')
                """,
                (
                    tag, battle_date, battle_time, 1 if is_attack else 0,
                    opp_tag, b.get("opponent_name"), b.get("opponent_th"),
                    *battle_defender_clan,
                    b.get("stars"), b.get("destruction"), b.get("duration"),
                    b.get("army_share_code"),
                ),
            )
            inserted = cur.rowcount == 1
            if inserted:
                # Bump the counter on the battle's OWN legend day, not on
                # pt_day -- a catch-up poll routinely ingests battles spanning
                # several days and they must land on the day they happened.
                # _bump_day_battle_count creates the day row if it's missing.
                await _bump_day_battle_count(cur, tag, battle_date, is_attack, 1)
            elif not is_attack and defender_clan[0]:
                # An 'eye' row may already hold this native defense. Fill its
                # missing snapshot without counting the battle again.
                await cur.execute(
                    """UPDATE legend_battle
                       SET defender_clan_name=%s, defender_clan_observed_at=%s,
                           defender_clan_source=%s
                       WHERE player_tag=%s AND battle_time=%s
                         AND defender_clan_name IS NULL""",
                    (*defender_clan, tag, battle_time),
                )
            if opp_tag and (inserted or (not is_attack and defender_clan[0])):
                await _write_mirror_battle(cur, tag, name, b, defender_clan)

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

        # (The battlelog cursor is advanced by the player upsert above, in the
        # same statement that writes the rest of the row.)

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


def _reset_log_date(now_utc: datetime):
    """legend_date for the reset being snapshotted. The job runs at 04:59 UTC,
    i.e. just BEFORE the 05:00 reset it records, so a plain _legend_day_for(now)
    would return the previous day. Shift by 30 min so any run from 04:30 UTC
    counts as the upcoming reset (opens today's date); runs after 05:00 are in
    that same legend day."""
    return _legend_day_for(now_utc + timedelta(minutes=30))


async def snapshot_reset_clan_tags(conn, now_utc: datetime | None = None,
                                   only_tags: List[str] | None = None):
    """Log clan changes into player_reset_clan_log, then refresh
    player.reset_clan_tag -- both in one transaction. `only_tags` restricts
    both statements to those players (tests only; production passes None)."""
    now_utc = now_utc or datetime.now(timezone.utc).replace(tzinfo=None)
    legend_date = _reset_log_date(now_utc)
    tag_sql, tag_args = "", ()
    if only_tags is not None:
        tag_sql = " AND p.player_tag IN (%s)" % ",".join(["%s"] * len(only_tags))
        tag_args = tuple(only_tags)
    try:
        async with conn.cursor() as cur:
            # Changed since last snapshot, or no log row yet (reset_clan_tag may
            # already equal current for those). `<=>` handles clanless (NULL).
            await cur.execute(
                """
                INSERT INTO player_reset_clan_log (player_tag, legend_date, clan_tag, recorded_at)
                SELECT p.player_tag, %s, p.current_clan_tag, UTC_TIMESTAMP()
                FROM player p
                WHERE p.is_active = 1
                  AND (NOT (p.reset_clan_tag <=> p.current_clan_tag)
                       OR NOT EXISTS (SELECT 1 FROM player_reset_clan_log l
                                      WHERE l.player_tag = p.player_tag))
                """ + tag_sql + """
                ON DUPLICATE KEY UPDATE clan_tag = VALUES(clan_tag), recorded_at = VALUES(recorded_at)
                """,
                (legend_date,) + tag_args,
            )
            await cur.execute(
                "UPDATE player p SET p.reset_clan_tag = p.current_clan_tag WHERE p.is_active = 1" + tag_sql,
                tag_args,
            )
        await conn.commit()
    except Exception:
        await conn.rollback()
        raise

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
