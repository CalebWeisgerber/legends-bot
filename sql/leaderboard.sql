-- leaderboard.sql — daily best-offense / best-defense queries + views.
-- Reads the per-day rollups maintained by db.upsert_player:
--   day.offense      = trophies gained that day   (from trophy diffs between polls)
--   day.defense      = trophies lost that day      (from trophy diffs between polls)
--   day.num_attacks  = attack count               (deduped /battlelog legend battles)
--   day.num_defenses = defense count              (deduped /battlelog legend battles)

USE PlayerTracking;

-- Convenience view: today's leaderboard rows joined to player names.
CREATE OR REPLACE VIEW v_legend_day AS
SELECT
    d.date,
    d.player_tag,
    p.name,
    p.current_clan_tag,
    d.start_trophies,
    d.end_trophies,
    d.offense,
    d.defense,
    d.num_attacks,
    d.num_defenses
FROM day d
JOIN player p ON p.player_tag = d.player_tag;

-- --- Example leaderboard queries ---

-- Best OFFENSE today (most trophies gained):
-- SELECT name, offense, num_attacks, end_trophies
-- FROM v_legend_day WHERE date = CURDATE()
-- ORDER BY offense DESC LIMIT 25;

-- Best DEFENSE today (fewest trophies lost among players who were attacked):
-- SELECT name, defense, num_defenses, end_trophies
-- FROM v_legend_day WHERE date = CURDATE() AND num_defenses > 0
-- ORDER BY defense ASC LIMIT 25;

-- Most successful defender today (most defenses held):
-- SELECT name, num_defenses, defense
-- FROM v_legend_day WHERE date = CURDATE()
-- ORDER BY num_defenses DESC LIMIT 25;

-- --- Per-battle detail (legend_battle, from /battlelog) ---

-- Best 3-star attack rate today:
-- SELECT p.name, SUM(b.is_attack=1 AND b.stars=3) AS triples, SUM(b.is_attack=1) AS attacks
-- FROM legend_battle b JOIN player p ON p.player_tag = b.player_tag
-- WHERE b.date = CURDATE()
-- GROUP BY b.player_tag HAVING attacks > 0
-- ORDER BY triples DESC LIMIT 25;

-- --- Season standings (league_season, from /leaguehistory) ---

-- Latest season leaderboard by final trophies:
-- SELECT p.name, s.league_trophies, s.placement, s.attack_wins, s.defense_stars
-- FROM league_season s JOIN player p ON p.player_tag = s.player_tag
-- WHERE s.league_season_id = (SELECT MAX(league_season_id) FROM league_season)
-- ORDER BY s.league_trophies DESC LIMIT 25;
