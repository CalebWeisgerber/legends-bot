-- leaderboard.sql — daily best-offense / best-defense queries + views.
-- Reads the per-day rollups maintained by db.upsert_player:
--   day.offense      = total trophies gained that day
--   day.defense      = total trophies lost that day
--   day.num_attacks  = attack count (attackWins delta sum)
--   day.num_defenses = defense count (defenseWins delta sum)

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
