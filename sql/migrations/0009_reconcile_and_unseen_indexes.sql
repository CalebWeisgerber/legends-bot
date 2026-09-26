-- 0009: index the reconcile sweep; drop a redundant player key.
--
-- idx_battle_source_time: reconcile_mirror_duplicates() drives on
--   WHERE m.source = 'mirror' AND m.battle_time > UTC_TIMESTAMP() - INTERVAL n DAY
-- and no existing index leads with either column (idx_battle_attack_time leads
-- with is_attack, which is not in that WHERE). It scanned all of legend_battle
-- once per pass -- ~480 passes/day. Nothing is deleted differently as a result;
-- this only changes how the rows are found.
--
-- idx_player_active: redundant. It is (is_active) alone -- cardinality 2 with
-- nearly every row set to 1, so the optimizer ignored it -- AND an exact
-- leftmost prefix of idx_player_active_seen_at (is_active, last_seen_at),
-- which migration 0007 already added for get_all_player_tags(). Dropping it
-- loses no access path. Note that schema.sql was missing 0007's index until
-- this migration; do not re-add it here, it already exists.
--
-- NOTE: the legend_battle ALTER is ONLINE (concurrent reads/writes keep
-- working) but rebuilds in place and can run for minutes. Run it from a client
-- with no short read timeout. In MySQL Workbench raise
-- Preferences > SQL Editor > "DBMS connection read timeout interval" first,
-- then reconnect, or the client hangs up mid-build with error 2013 while the
-- server carries on building the index anyway.

USE PlayerTracking;

ALTER TABLE legend_battle
  ADD KEY idx_battle_source_time (source, battle_time);

ALTER TABLE player
  DROP KEY idx_player_active;
