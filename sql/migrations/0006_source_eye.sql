-- 0006: add 'eye' to legend_battle.source.
--
-- Project Eye already pulls FULL /players/{tag}/battlelog responses for every
-- streamer pool tag (several times per detected attack, plus a periodic live
-- sweep) and discards everything it didn't need for screenshot correlation.
-- Those responses are the same authoritative per-player battlelog the legends
-- bot's 'native' rows come from, so any legend battle in them that isn't in
-- legend_battle yet gets written by Project Eye with source='eye'.
--
-- Why this matters: measured 2026-08-23, the legends-bot scheduler had 8
-- outages >=30min in the preceding 58 days (153h dead, 88.8% uptime), the
-- worst a 99-hour hole 2026-07-21..25. The battlelog is a short sliding
-- window (~50 entries), so anything not ingested inside that window is gone
-- permanently. Project Eye stays up independently of the legends bot (it hits
-- the CoC API directly and never needed this DB to pair screenshots), so its
-- battlelog reads are a free second ingestion path across those outages.
--
-- Precedence: 'eye' rows are as authoritative as 'native' -- both come from
-- the player's OWN battlelog, only the writing process differs. The value is
-- kept distinct purely for provenance/auditing ("how much did Eye recover").
--
-- No code change is needed in the legends bot for correctness:
--   * upsert_player()'s native-wins branch only deletes rows where
--     source='mirror', so it leaves an 'eye' row alone; the native INSERT
--     IGNORE that follows then collides on uq_battle (player_tag,
--     battle_time) -- identical source data means an identical timestamp --
--     rowcount 0, so the day counter is not bumped a second time.
--   * reconcile_mirror_duplicates() only sweeps source='mirror', so it will
--     not delete 'eye' rows.
-- Project Eye's writer does the mirror-displacement itself (see
-- legend_backfill.py), matching upsert_player's behaviour.

-- 'eye' is appended at the END of the enum list on purpose: MySQL 8.0.29+ can
-- add a trailing enum value with ALGORITHM=INSTANT (no table rebuild, no lock)
-- as long as the storage size is unchanged -- an enum stays 1 byte up to 255
-- values, so 2 -> 3 qualifies. Inserting 'eye' in the middle instead would
-- renumber the existing values and force a full copy of ~9.6M rows.
-- ALGORITHM=INSTANT is stated explicitly so the alter FAILS LOUDLY rather than
-- silently degrading to an in-place rebuild on a server that can't do it.
-- Do NOT add LOCK=NONE alongside it: INSTANT already takes no lock, and MySQL
-- rejects the combination outright with
--   ERROR 1221 (HY000) Incorrect usage of ALGORITHM=INSTANT and LOCK=NONE

USE PlayerTracking;

ALTER TABLE legend_battle
  MODIFY COLUMN source ENUM('native','mirror','eye') NOT NULL DEFAULT 'native',
  ALGORITHM=INSTANT;
