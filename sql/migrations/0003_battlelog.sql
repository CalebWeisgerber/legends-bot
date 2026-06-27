-- migrations/0003_battlelog.sql — battlelog-based counts + per-battle detail +
-- per-season history. Run once on an existing PlayerTracking DB.

USE PlayerTracking;

-- Dedup cursor: most recent battlelog timestamp already ingested per player.
ALTER TABLE player ADD COLUMN last_battle_time DATETIME NULL;

-- Per-battle detail from /players/{tag}/battlelog (legend battles only).
-- Note: the API has no per-battle trophy delta; magnitude stays in day.offense/
-- defense. opponent_th is stored raw (community reports it may be off by one).
CREATE TABLE IF NOT EXISTS legend_battle (
  id            BIGINT      NOT NULL AUTO_INCREMENT,
  player_tag    VARCHAR(20) NOT NULL,
  date          DATE        NOT NULL,
  battle_time   DATETIME    NOT NULL,
  is_attack     TINYINT(1)  NOT NULL,   -- 1 = player attacked (offense), 0 = defended
  opponent_tag  VARCHAR(20) NULL,
  opponent_name VARCHAR(64) NULL,
  opponent_th   TINYINT     NULL,
  stars         TINYINT     NULL,
  destruction   TINYINT     NULL,
  duration      INT         NULL,       -- battleTime seconds
  PRIMARY KEY (id),
  UNIQUE KEY uq_battle (player_tag, battle_time),
  KEY idx_battle_date (date),
  KEY idx_battle_player_date (player_tag, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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
