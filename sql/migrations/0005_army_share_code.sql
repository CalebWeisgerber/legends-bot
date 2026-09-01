-- 0005: army share codes + native/mirror battle rows.
--
-- army_share_code: the in-game army link carried on battlelog entries. Observed
-- on ~100% of legend attack rows (proven out in the sibling stats-app project
-- against the same endpoint), and also seen on some defense rows in a live
-- probe on this project (probe_battlelog.py, July 2026 sample) -- so it's
-- captured verbatim whenever the API includes it, no is_attack filtering.
-- Sampled codes ran 105-169 chars; VARCHAR(1024) leaves generous headroom for
-- larger/more diverse army compositions. NULL when the API omits it. This is
-- the RAW code only -- no parsing/archetype logic here (that lives in the
-- sibling project by design).
--
-- source: distinguishes a legend_battle row captured from the PLAYER'S OWN
-- battlelog ('native') from one synthesized as the flip-side of an opponent's
-- battle ('mirror') -- see MIRROR-ROW HARVESTING in README.md / db.py. Native
-- rows are authoritative and always win over a mirror row for the same battle.
--
-- idx_battle_pair: supports the native/mirror pair-match lookup -- "find the
-- existing row for (player_tag, opponent_tag, is_attack) within the
-- cross-perspective battle_time drift window" -- which uq_battle (player_tag,
-- battle_time) can't serve since the two sides' own battlelogs can stamp the
-- same battle up to ~300s apart.

USE PlayerTracking;

ALTER TABLE legend_battle
  ADD COLUMN army_share_code VARCHAR(1024) NULL AFTER duration,
  ADD COLUMN source ENUM('native','mirror') NOT NULL DEFAULT 'native' AFTER army_share_code,
  ADD KEY idx_battle_pair (player_tag, opponent_tag, is_attack, battle_time);
