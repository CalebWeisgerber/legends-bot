-- migrations_v2.sql — daily offense/defense leaderboard support.
-- Run once against an existing PlayerTracking DB (new installs use schema.sql).

USE PlayerTracking;

-- Stored season win counters (diffed between polls for attack/defense counts).
-- NULL default => first poll sets the baseline without counting a bogus delta.
ALTER TABLE player ADD COLUMN attack_wins  INT NULL;
ALTER TABLE player ADD COLUMN defense_wins INT NULL;

-- Daily rollups.
ALTER TABLE day ADD COLUMN offense      INT NOT NULL DEFAULT 0;
ALTER TABLE day ADD COLUMN defense      INT NOT NULL DEFAULT 0;
ALTER TABLE day ADD COLUMN num_attacks  INT NOT NULL DEFAULT 0;
ALTER TABLE day ADD COLUMN num_defenses INT NOT NULL DEFAULT 0;

CREATE INDEX idx_day_date_offense ON day (date, offense);
CREATE INDEX idx_day_date_defense ON day (date, defense);
