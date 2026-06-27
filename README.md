# Legends Bot

By [Caleb Weisgerber](https://github.com/CalebWeisgerber). [MIT](#license) —
attribution appreciated ([how](#attribution)).

Tracks Clash of Clans **Legend I + Legend II** players and records, per player per
day, their **offense** (trophies gained), **defense** (trophies lost), and exact
attack/defense counts — for daily best-offense / best-defense leaderboards.

> Legend III is deliberately excluded.

## Quick start (Windows)

1. **MySQL Server** running. Set the password the bot connects with: copy
   `run/_env.bat.example` → `run/_env.bat` (gitignored) and edit. `DB_PASSWORD` is
   **required**; `DB_HOST` `DB_PORT` `DB_USER` `DB_NAME` default to `localhost`
   `3306` `root` `PlayerTracking`.
2. **Schema:** `mysql -u root -p < sql/schema.sql`
   (upgrading an existing DB instead? run `sql/migrations/*.sql` in order.)
3. **API keys:** copy `config/keys.json.example` → `config/keys.json`, paste your
   Clash of Clans API JWT(s) from <https://developer.clashofclans.com>,
   IP-whitelisted for the machine that runs the bot.
   **`config/keys.json` is gitignored — never commit it.**
4. **Run** (first launch runs `uv sync` to build the venv):
   ```
   run\populate_db.bat     REM seed once + one update pass
   run\start_bot.bat       REM run the 24/7 scheduler
   ```

Needs **Python 3.10+** and [**uv**](https://docs.astral.sh/uv/).

Manual runs from the repo root (after `set DB_PASSWORD=...`):
```
uv run python -m legendsbot.seed             # discover clans/players
uv run python -m legendsbot.pipeline         # one update pass
uv run python -m legendsbot.main             # the scheduler
uv run python -m legendsbot.league_history   # ingest season summaries
```

## How tracking works

The API exposes **trophies** (a snapshot) and a **battlelog** (recent battles),
but no per-battle trophy delta. The bot combines both:

| Quantity | Table(s) | Source |
| --- | --- | --- |
| **Magnitude** — trophies gained/lost | `day.offense`, `day.defense`; per-event `attack_log` / `defense_log` | Trophy diff between two polls. A drop `< -320` is treated as a season reset, not a defense. |
| **Counts** — attacks / defenses | `day.num_attacks`, `day.num_defenses` | Each `/battlelog` legend battle, deduped into `legend_battle` on `(player_tag, battle_time)`. Exact — avoids the broken `attackWins`/`defenseWins` counters (can read 0 mid-season). |
| **Per-battle detail** | `legend_battle` | `/battlelog`: opponent, stars, destruction %, duration, timestamp, attack-vs-defense. |
| **Season summary** | `league_season` | `/leaguehistory`: final trophies, wins/losses/stars, placement. |

Magnitude and counts come from **different sources by necessity** — a poll sees the
net trophy change, the battlelog sees the individual battles. They reconcile but
won't always be 1:1 (multiple battles can fall between two polls).

The update pass is cheap: `/clan/{tag}` returns the whole member list (trophies for
~50 members in one call), the bot diffs against an in-memory cache, and only
players whose trophies **moved** get a follow-up `/battlelog` call. No full
player-by-player sweep. The one exception is the daily league-history job
(1 call/active player) — see schedule below.

> Battlelog `opponentTownHallLevel` is stored **raw**. The community reported it
> may be off by one (Jun 2026); adjust in queries if it matters to you.

## Schedule (UTC)

The Legend day resets ~05:00 UTC; jobs bracket that.

| Time | Job | Does |
| --- | --- | --- |
| 04:50 | Pre-Reset Seed | Deep clan/player discovery before reset. |
| 04:59 | Daily Snapshot | Baselines each player's start-of-day trophies. |
| 06:00 | League History | Ingest per-player season summaries (heavy: 1 call/active player). |
| 17:00 | Morning Sequence | Seed + full update. |
| every 3 min | Routine Update | Scan clans, diff trophies, fetch changed players. |

All jobs share one API lock, so they never overlap. Edit cadence in
`src/legendsbot/main.py`.

## Leaderboards

`sql/leaderboard.sql` creates the `v_legend_day` view and has ready example
queries (best offense/defense, 3-star rate, season standings). Best offense today:
```sql
SELECT name, offense, num_attacks FROM v_legend_day
WHERE date = CURDATE() ORDER BY offense DESC LIMIT 25;
```

## Discovery (read this)

The Clash API has **no endpoint that lists all Legend players.** The bot reaches
players only through:

1. **Clan rankings** — top clans per location → scan their members.
2. **Player rankings** — top players per location → scan their clans.
3. **`config/known_clans.txt`** — clans you register by hand.
4. **Snowball** — any Legend clan found this way is scanned thereafter.

A Legend player in a clan that **never ranks** in any location's top list is
invisible to discovery until you add their clan:
```
python tools/add_clans.py "#CLANTAG"     # register + insert into clan table
python tools/scan_clan.py "#CLANTAG"     # pull its Legend members right now
```

## Tiers

Tracked: **Legend I `105000036`**, **Legend II `105000035`**. Excluded:
Legend III `105000034`. The legacy `league` field is now `null`; detection uses
`leagueTier` (`is_legend()` in `pipeline.py`). Re-verify against the live API
anytime: `python tools/discover_tiers.py`.

## Tools (`tools/`)

| Script | Purpose |
| --- | --- |
| `add_clans.py "#TAG"` | Register a clan as a discovery root + insert now. |
| `scan_clan.py "#TAG"` | Immediately upsert one clan's Legend members. |
| `debug_player.py "#TAG"` | Dump one player's API payload + DB rows. |
| `discover_tiers.py` | Print live Legend tier ids/names from the API. |
| `probe_battlelog.py "#TAG"` | Inspect raw battlelog for a player. |
| `prune_non_legends.py` | Remove players no longer Legend I/II. |
| `import_archive_clans.py` | Bulk-load clan tags from old data dumps. |

## Layout

```
config/   keys.json (secret), location_ids.json, known_clans.txt
state/    runtime json (auto-written)
sql/      schema.sql, leaderboard.sql, migrations/
src/legendsbot/
          main.py            scheduler
          pipeline.py        update pass (scan, diff, fetch changed)
          seed.py            discovery
          db.py              MySQL access + daily rollups
          league_history.py  season-summary ingest
          scan_state.py / paths.py
tools/    operational scripts (table above)
run/      populate_db.bat, start_bot.bat
```

## Attribution

If your server uses this project, I'd appreciate:

- A link to this repository.
- Credit to the original project.
- Letting me know about your server!

## License

[MIT](LICENSE) © 2026 Caleb Weisgerber.
