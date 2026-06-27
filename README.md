# Legends Bot

By [Caleb Weisgerber](https://github.com/CalebWeisgerber). Licensed under the
[MIT License](#license) — attribution appreciated (see [Attribution](#attribution)).

Tracks Clash of Clans **Legend I + Legend II** players and records, per player per
day, their **offense** (trophies gained), **defense** (trophies lost), and attack/
defense counts — for daily best-offense / best-defense leaderboards.

## Layout

```
config/     keys.json, location_ids.json, known_clans.txt   (your settings/secrets)
state/      scan_state / last_scan / seed_state json        (runtime, auto-written)
sql/        schema.sql, leaderboard.sql, migrations/
src/legendsbot/
            main.py      scheduler entry (4 jobs + 05:00 UTC reset)
            pipeline.py  update pass: scan clans, diff trophies, fetch changed players
            seed.py      discovery: clan-points + player rankings + known_clans
            db.py        all MySQL access + daily rollups
            scan_state.py
            paths.py     resolves config/ + state/ locations
tools/      add_clans.py, scan_clan.py, prune_non_legends.py,
            debug_player.py, discover_tiers.py
run/        populate_db.bat, start_bot.bat
archive/    old data dumps + previous code (not used at runtime)
```

Requires **Python 3.10+** and [**uv**](https://docs.astral.sh/uv/) for dependency
management.

## Setup (Windows)

1. Install **MySQL Server**. Connection is read from env vars — `DB_PASSWORD` is
   **required**; `DB_HOST` `DB_PORT` `DB_USER` `DB_NAME` default to
   `localhost` `3306` `root` `PlayerTracking`. Locally, copy `run/_env.bat.example`
   to `run/_env.bat` (gitignored) and set your password there; the launchers load it.
2. Create the schema:
   ```
   mysql -u root -p < sql/schema.sql
   ```
   (Existing DB instead of fresh? run the files in `sql/migrations/` in order.)
3. Copy `config/keys.json.example` to `config/keys.json` and paste your Clash of
   Clans API JWT(s), IP-whitelisted at https://developer.clashofclans.com for the
   machine that runs the bot. **`config/keys.json` is gitignored — never commit it.**
4. Run (first launch runs `uv sync` to build the env):
   ```
   run\populate_db.bat        REM seed once + one update pass
   run\start_bot.bat          REM run the scheduler 24/7
   ```

Manual runs from the repo root (after `set DB_PASSWORD=...`):
```
uv sync
uv run python -m legendsbot.seed
uv run python -m legendsbot.pipeline
uv run python -m legendsbot.main
```

## How tracking works

- **Magnitude** (`day.offense` / `day.defense`, trophies gained/lost) comes from
  trophy diffs between polls — the API has no per-battle trophy delta.
- **Counts + per-battle detail** come from `/players/{tag}/battlelog`: for each
  legend battle we store a `legend_battle` row (opponent, stars, %, duration,
  timestamp, attack/defense), deduped by `(player_tag, battle_time)`. This is
  exact and avoids the broken `attackWins`/`defenseWins` counters (which can read
  0 mid-season).
- **Season summaries** come from `/players/{tag}/leaguehistory` → `league_season`
  (final trophies, wins/losses/stars, placement). Ingested daily at 06:00 UTC, or
  on demand: `uv run python -m legendsbot.league_history`.

> Note: the API's battlelog `opponentTownHallLevel` is stored raw; the community
> reported it may be off by one (Jun 2026) — adjust in queries if needed.

## Leaderboards

`sql/leaderboard.sql` creates `v_legend_day` and has example queries (best
offense/defense per day, 3-star rate, season standings), e.g. best offense today:
```sql
SELECT name, offense, num_attacks FROM v_legend_day
WHERE date = CURDATE() ORDER BY offense DESC LIMIT 25;
```

## Discovery (important)

The Clash API has **no endpoint that lists all Legend players**. Discovery reaches
players only through (a) clan-points rankings, (b) player rankings, and (c) clans
you register in `config/known_clans.txt`. A Legend player in a clan that never
ranks must be added manually:
```
python tools/add_clans.py "#CLANTAG"      REM register + insert now
python tools/scan_clan.py "#CLANTAG"      REM pull its members immediately
```

## Tiers

Legend I = `105000036`, Legend II = `105000035` (tracked); Legend III =
`105000034` (excluded). Verify against the live API anytime:
`python tools/discover_tiers.py`. The legacy `league` field is now `null`; the bot
uses `leagueTier` (see `is_legend()` in `pipeline.py`).

## Attribution

If your server uses this project, I'd really appreciate:

- A link to this repository.
- Credit to the original project.
- Letting me know about your server!

## License

[MIT License](LICENSE) © 2026 Caleb Weisgerber.
