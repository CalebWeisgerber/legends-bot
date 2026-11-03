-- 0011: player_reset_clan_log -- change-log of each player's clan at the
-- daily legend reset.
--
-- player.reset_clan_tag is overwritten at every 05:00 UTC reset
-- (snapshot_reset_clan_tags), so past days' clans were lost. The in-game attack
-- screen shows the defender's clan AS OF the reset, so resolving a past battle's
-- defender needs that history.
--
-- Change-log, not a daily copy: a row is written only when a player's clan at
-- reset differs from their latest logged value (or they have no row yet).
--   legend_date = UTC date of the 05:00 UTC reset that opened that legend day.
--   clan_tag    = NULL means clanless at that reset.
-- Clan at reset on day D = the row with the greatest legend_date <= D. If D is
-- before the player's earliest row the answer is UNKNOWN (not the earliest row).
--
-- No FK to clan: history must survive a clan row being deleted.
-- Idempotent (CREATE IF NOT EXISTS + upsert seed).

USE PlayerTracking;

CREATE TABLE IF NOT EXISTS player_reset_clan_log (
  player_tag  VARCHAR(20) NOT NULL,
  legend_date DATE        NOT NULL,
  clan_tag    VARCHAR(20) NULL,
  recorded_at DATETIME    NOT NULL,
  PRIMARY KEY (player_tag, legend_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: current reset_clan_tag, stamped with the current legend day
-- (05:00 UTC rule).
INSERT INTO player_reset_clan_log (player_tag, legend_date, clan_tag, recorded_at)
SELECT player_tag, DATE(UTC_TIMESTAMP() - INTERVAL 5 HOUR), reset_clan_tag, UTC_TIMESTAMP()
FROM player
WHERE is_active = 1
ON DUPLICATE KEY UPDATE clan_tag = VALUES(clan_tag), recorded_at = VALUES(recorded_at);
