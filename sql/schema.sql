-- schema.sql — full database definition for the Legends Bot.
-- Reverse-engineered from async_db.py + async_fetch_pipeline.py (promoted build).
-- Fresh-install: run this once. It already includes the clan.is_active /
-- last_member_scan columns, so migrations.sql is NOT needed on a fresh DB.
--
--   mysql -u root -p < schema.sql
--
-- Tags are stored normalized: no leading '#', uppercased (see normalize_tag()).

CREATE DATABASE IF NOT EXISTS PlayerTracking
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE PlayerTracking;

-- ---------- clan ----------
CREATE TABLE IF NOT EXISTS clan (
  clan_tag          VARCHAR(20)  NOT NULL,
  clan_name         VARCHAR(64)  NULL,
  is_active         TINYINT(1)   NOT NULL DEFAULT 1,
  last_member_scan  DATETIME     NULL,
  PRIMARY KEY (clan_tag),
  KEY idx_clan_active_scan (is_active, last_member_scan)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- player ----------
-- current_clan_tag / reset_clan_tag reference clan(clan_tag). The pipeline
-- upserts the clan before the player and nulls these out before deleting a
-- clan, so ON DELETE SET NULL keeps things consistent if that order is missed.
CREATE TABLE IF NOT EXISTS player (
  player_tag        VARCHAR(20)  NOT NULL,
  name              VARCHAR(64)  NULL,
  trophies          INT          NOT NULL DEFAULT 0,
  league_tier_id    BIGINT       NULL,  -- current leagueTier.id (105000036/35/34 = Legend I/II/III)
  -- Season win counters from /players, kept to diff attack/defense counts
  -- between polls (memberList does not carry these). NULL = no baseline yet, so
  -- the first poll sets the value without counting a bogus delta.
  attack_wins       INT          NULL,
  defense_wins      INT          NULL,
  last_battle_time  DATETIME     NULL,  -- battlelog dedup cursor (most recent ingested)
  current_clan_tag  VARCHAR(20)  NULL,
  reset_clan_tag    VARCHAR(20)  NULL,
  last_seen         DATE         NULL,
  is_active         TINYINT(1)   NOT NULL DEFAULT 1,
  PRIMARY KEY (player_tag),
  -- get_all_player_tags()'s "active AND stale" scan (added in migration 0007;
  -- this file was out of sync until 0009). is_active alone is cardinality 2
  -- with nearly every row set to 1, so it is only worth anything to the
  -- optimizer paired with last_seen_at.
  KEY idx_player_active_seen_at (is_active, last_seen_at),
  KEY idx_player_tier (league_tier_id),
  KEY idx_player_current_clan (current_clan_tag),
  KEY idx_player_reset_clan (reset_clan_tag),
  CONSTRAINT fk_player_current_clan FOREIGN KEY (current_clan_tag)
    REFERENCES clan (clan_tag) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_player_reset_clan FOREIGN KEY (reset_clan_tag)
    REFERENCES clan (clan_tag) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- day ----------
-- One row per player per Legends day (date bucket from _pt_now_and_day()).
-- Looked up / updated by (player_tag, date); history read via ORDER BY date.
-- No FK to player: deletes are managed in code (delete_player_and_logs).
CREATE TABLE IF NOT EXISTS day (
  player_tag      VARCHAR(20) NOT NULL,
  date            DATE        NOT NULL,
  start_trophies  INT         NULL,
  end_trophies    INT         NULL,
  -- Daily rollups for leaderboards (best offense / defense per day):
  offense         INT         NOT NULL DEFAULT 0,  -- total trophies gained
  defense         INT         NOT NULL DEFAULT 0,  -- total trophies lost
  num_attacks     INT         NOT NULL DEFAULT 0,  -- legend attacks (from battlelog)
  num_defenses    INT         NOT NULL DEFAULT 0,  -- legend defenses (from battlelog)
  PRIMARY KEY (player_tag, date),
  KEY idx_day_date_offense (date, offense),
  KEY idx_day_date_defense (date, defense)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- attack_log ----------
-- One row per trophy GAIN event (chunked to <=40 by upsert_player()).
CREATE TABLE IF NOT EXISTS attack_log (
  id               BIGINT      NOT NULL AUTO_INCREMENT,
  player_tag       VARCHAR(20) NOT NULL,
  date             DATE        NOT NULL,
  trophies_gained  INT         NOT NULL,
  timestamp        DATETIME    NOT NULL,
  PRIMARY KEY (id),
  KEY idx_attack_player_date (player_tag, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- defense_log ----------
-- One row per trophy LOSS event (chunked to <=40 by upsert_player()).
CREATE TABLE IF NOT EXISTS defense_log (
  id              BIGINT      NOT NULL AUTO_INCREMENT,
  player_tag      VARCHAR(20) NOT NULL,
  date            DATE        NOT NULL,
  trophies_lost   INT         NOT NULL,
  timestamp       DATETIME    NOT NULL,
  PRIMARY KEY (id),
  KEY idx_defense_player_date (player_tag, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- legend_battle ----------
-- Per-battle detail from /players/{tag}/battlelog (legend battles only). The API
-- has no per-battle trophy delta; magnitude stays in day.offense/defense.
-- opponent_th is stored raw (community reports it may be off by one).
--
-- army_share_code: the raw in-game army link off the battlelog entry, stored
-- verbatim (no parsing/archetype logic here). Present on most attack rows and
-- some defense rows; NULL when the API omits it.
--
-- source: 'native' = fetched from this player's own battlelog. 'mirror' =
-- synthesized as the flip-side of an opponent's native battle (see
-- MIRROR-ROW HARVESTING in README.md / db.py) -- lets us record both sides of
-- a battle without waiting to fetch the opponent's own battlelog. A native
-- row always wins over an existing mirror row for the same battle.
CREATE TABLE IF NOT EXISTS legend_battle (
  id              BIGINT      NOT NULL AUTO_INCREMENT,
  player_tag      VARCHAR(20) NOT NULL,
  date            DATE        NOT NULL,
  battle_time     DATETIME    NOT NULL,
  is_attack       TINYINT(1)  NOT NULL,   -- 1 = player attacked (offense), 0 = defended
  opponent_tag    VARCHAR(20) NULL,
  opponent_name   VARCHAR(64) NULL,
  opponent_th     TINYINT     NULL,
  stars           TINYINT     NULL,
  destruction     TINYINT     NULL,
  duration        INT         NULL,       -- battleTime seconds
  army_share_code VARCHAR(1024) NULL,
  source          ENUM('native','mirror') NOT NULL DEFAULT 'native',
  PRIMARY KEY (id),
  UNIQUE KEY uq_battle (player_tag, battle_time),
  KEY idx_battle_date (date),
  KEY idx_battle_player_date (player_tag, date),
  -- cross-player time-window lookups ("who attacked between T1 and T2")
  KEY idx_battle_attack_time (is_attack, battle_time),
  -- native/mirror pair-match lookup: (player_tag, opponent_tag, is_attack)
  -- within the cross-perspective battle_time drift window.
  KEY idx_battle_pair (player_tag, opponent_tag, is_attack, battle_time),
  -- reverse direction: "who fought player X". uq_battle and idx_battle_pair
  -- both lead with player_tag, so neither serves a lookup keyed on the
  -- opponent side. Single column: row counts per tag are small enough that
  -- filtering is_attack and sorting battle_time after the lookup is cheap.
  KEY idx_battle_opponent (opponent_tag),
  -- reconcile_mirror_duplicates()'s driving scan: recent mirror rows.
  KEY idx_battle_source_time (source, battle_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------- league_season ----------
-- Per-league-season summary from /players/{tag}/leaguehistory.
CREATE TABLE IF NOT EXISTS league_season (
  player_tag       VARCHAR(20) NOT NULL,
  league_season_id BIGINT      NOT NULL,
  league_trophies  INT         NULL,
  league_tier_id   BIGINT      NULL,
  placement        INT         NULL,
  attack_wins      INT         NULL,
  attack_losses    INT         NULL,
  attack_stars     INT         NULL,
  defense_wins     INT         NULL,
  defense_losses   INT         NULL,
  defense_stars    INT         NULL,
  max_battles      INT         NULL,
  PRIMARY KEY (player_tag, league_season_id),
  KEY idx_season (league_season_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
