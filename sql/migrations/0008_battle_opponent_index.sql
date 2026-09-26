-- 0008: reverse-direction opponent lookup index.
--
-- "who fought player X" -- WHERE opponent_tag = ? -- was a full table scan.
-- uq_battle and idx_battle_pair both lead with player_tag, so neither can
-- serve a lookup keyed on the opponent side. player_tag itself needs nothing
-- new; it is already covered by those two.
--
-- Single column on purpose. is_attack and battle_time could be appended to
-- skip the post-lookup filter and the filesort on ORDER BY battle_time DESC,
-- but the row count per opponent tag is small enough that both are cheap.
--
-- NOTE: on a large table this rebuilds in place and can run for minutes.
-- InnoDB does it ONLINE (concurrent reads/writes allowed), so run it from a
-- client without a short read timeout.

USE PlayerTracking;

ALTER TABLE legend_battle
  ADD KEY idx_battle_opponent (opponent_tag);
