-- Snapshot the observed defender clan on battle rows. The observation may be
-- later than the battle; NULL means no defender clan was observed.
USE PlayerTracking;

ALTER TABLE legend_battle
  ADD COLUMN defender_clan_name VARCHAR(64) NULL AFTER opponent_th,
  ADD COLUMN defender_clan_observed_at DATETIME NULL AFTER defender_clan_name,
  ADD COLUMN defender_clan_source ENUM('clan_roster','player_profile') NULL AFTER defender_clan_observed_at,
  ALGORITHM=INSTANT;
