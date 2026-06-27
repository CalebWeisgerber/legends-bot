-- Migration for the new Ranked/Legend API pipeline (promoted from Files/).
-- Adds the clan-level columns the async pipeline now reads/writes:
--   async_fetch_pipeline.get_clans_to_scan()  -> clan.is_active, clan.last_member_scan
--   async_db.mark_clan_active/mark_clan_inactive
-- Safe to run once against the PlayerTracking database.

USE PlayerTracking;

ALTER TABLE clan ADD COLUMN is_active TINYINT(1) NOT NULL DEFAULT 1;
ALTER TABLE clan ADD COLUMN last_member_scan DATETIME NULL;
CREATE INDEX idx_clan_active_scan ON clan (is_active, last_member_scan);

-- (player.is_active + idx_player_active already added previously; see server_stuff.sql)
