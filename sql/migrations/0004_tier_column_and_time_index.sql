-- 0004: per-player Legend tier + battle-time index.
--
-- league_tier_id: the player's current leagueTier.id (105000036 = Legend I,
-- 105000035 = II, 105000034 = III), written by the pipeline on every player
-- upsert. Lets consumers pre-filter candidates by tier (e.g. Project Eye's
-- streamer-account elimination).
--
-- idx_battle_attack_time: "which players logged an ATTACK in this time
-- window" — the elimination query — was a full scan before; uq_battle leads
-- with player_tag so it can't serve a cross-player time-range lookup.

ALTER TABLE player
  ADD COLUMN league_tier_id BIGINT NULL AFTER trophies,
  ADD KEY idx_player_tier (league_tier_id);

ALTER TABLE legend_battle
  ADD KEY idx_battle_attack_time (is_attack, battle_time);
