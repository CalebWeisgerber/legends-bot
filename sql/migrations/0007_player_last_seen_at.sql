-- 0007: player.last_seen_at — minute-resolution "when did we last reconcile
-- this player" cursor for the unseen pass.
--
-- The unseen pass (get_all_player_tags) used to gate on `last_seen < today`.
-- last_seen is a DATE, so once a player was reconciled ONCE in a legend day
-- they were excluded from the unseen pass for the remainder of that day. A
-- player who changed clans mid-day then became completely unreachable:
--   * their OLD clan no longer lists them, so the clan pass never sees them;
--   * their NEW clan may not be tracked (or may be inactive), so it never
--     scans them either;
--   * the unseen pass skips them because last_seen is already today.
-- Result: zero battlelog polls for up to 24h, and every battle that rolled out
-- of the ~50-entry battlelog window in the meantime was lost permanently.
--
-- Worked example (2026-08-23): YVUPLGLYR moved to clan 2U0VRU9Y2, which was
-- not in the clan table at all. Its own battlelog held the defence we were
-- missing; nothing ever fetched it.
--
-- Measured the same day: 60,053 of 243,903 active players (24.6%) were
-- unreachable by the clan pass, and ZERO of them were eligible for the unseen
-- pass.
--
-- A nullable DATETIME appended at the end of the table is ALGORITHM=INSTANT on
-- MySQL 8.0.12+ (no rebuild, no lock) — stated explicitly so this FAILS LOUDLY
-- rather than silently copying ~308k rows.
--
-- Backfilled from last_seen so existing rows don't all look infinitely stale
-- on the first pass after deploy (which would queue the entire active
-- playerbase for reconciliation at once). last_seen is a DATE, so it lands at
-- midnight of that day — deliberately conservative: worst case a player is
-- reconciled once more than strictly needed.

USE PlayerTracking;

ALTER TABLE player
  ADD COLUMN last_seen_at DATETIME NULL AFTER last_seen,
  ALGORITHM=INSTANT;

UPDATE player SET last_seen_at = last_seen WHERE last_seen IS NOT NULL;

-- Supports the unseen pass's "active AND stale" scan.
ALTER TABLE player
  ADD KEY idx_player_active_seen_at (is_active, last_seen_at);
