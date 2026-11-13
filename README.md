# Legends Bot

A tracker for the whole Clash of Clans Legend League — every tier (**Legend I,
II and III**). It records:

- **Every legend battle event.** Each attack and defense from the battlelog
  gets its own row: opponent, stars, destruction, duration, army link, and the
  defender's clan. Both sides of each battle are written.
- **Each player's day.** Trophies gained on offense, trophies lost on defense,
  and the exact number of attacks and defenses.
- **Clan at reset.** Which clan every player was in at each daily reset.
- **Season results.** Final trophies, wins/losses and placement at the end of
  each season.

From that you can build daily leaderboards (who's hitting best, who's holding
best), look up head-to-head history, or replay any player's day battle by
battle.

I built it because the game's API won't tell you any of this. It'll show you a
player's trophy count *right now*, but nothing about their day, and there's no
"give me every Legend player" endpoint at all. So the bot polls constantly, keeps
its own database, and reconstructs the daily picture itself.

## What you need

- A machine that can stay on (an old PC, a Pi, whatever). It just needs to keep
  polling.
- **MySQL Server** for storage.
- **Python 3.10+** and [**uv**](https://docs.astral.sh/uv/) to run it.
- One or more **Clash of Clans API keys** from
  <https://developer.clashofclans.com>. Each key is locked to the IP it's used
  from, so make them for whatever box runs the bot. More keys = more requests per
  second, which means the polling loop finishes faster.

## Setup (Windows)

Four steps, and the why of each:

1. **Tell it your database password.** Copy `run/_env.bat.example` to
   `run/_env.bat` and put your MySQL password in it. The launchers read this file.
   `DB_PASSWORD` is the only required setting; host/port/user/name fall back to
   `localhost` / `3306` / `root` / `PlayerTracking`. The password is never
   hardcoded and `run/_env.bat` is gitignored, so it can't get committed by
   accident.

2. **Create the tables.**
   ```
   mysql -u root -p < sql/schema.sql
   ```
   This builds the whole schema once. If you already have an older copy of the DB,
   run the files in `sql/migrations/` in order instead. Each one is a single
   incremental change, most recently `0011_player_reset_clan_log.sql` (the
   clan-at-reset history table). `schema.sql` always includes everything, so
   fresh installs never need the migrations.

3. **Add your API keys.** Copy `config/keys.json.example` to `config/keys.json`
   and paste your key(s) in. **This file holds live credentials and is gitignored
   — don't ever commit it.**

4. **Run it.**
   ```
   run\populate_db.bat     seeds the database once, then does one update pass
   run\start_bot.bat       runs the scheduler around the clock
   ```
   The first launch runs `uv sync` to build the environment, so it'll take a
   minute. After that it's quick.

If you'd rather run things by hand from the repo root (after `set DB_PASSWORD=...`):

```
uv run python -m legendsbot.seed             find clans and players
uv run python -m legendsbot.pipeline         do one update pass
uv run python -m legendsbot.main             run the scheduler
uv run python -m legendsbot.league_history   pull end-of-season summaries
```

## How the tracking actually works

The hard part is that the API gives you a player's trophy *total* but never tells
you how a single battle went, trophy-wise. So the bot gets its numbers from two
different places and stitches them together:

| What | Where it ends up | How it's worked out |
| --- | --- | --- |
| Trophies gained / lost | `day.offense`, `day.defense` (plus a row per event in `attack_log` / `defense_log`) | The difference between two polls. If a player jumps from 5,200 to 5,230, that's +30 offense; a drop is defense. A huge drop (more than 320) is a season reset, not a defense, so it's ignored. |
| Number of attacks / defenses | `day.num_attacks`, `day.num_defenses` | Pulled straight from the player's battlelog. Every legend battle is saved once in `legend_battle` (deduped by player + battle time), so the counts are exact. |
| Battle detail | `legend_battle` | From the battlelog: opponent, stars, destruction %, how long it took, attack vs defense. |
| Defender clan | `legend_battle.defender_clan_name` | Clan name observed from the defender's clan roster or player profile, with `defender_clan_observed_at` (UTC) and `defender_clan_source`. NULL until the defender is observed; the observation may be later than the battle. No clan tag is stored on the battle row. |
| Army links | `legend_battle.army_share_code` | The battlelog also carries the in-game army link for a battle. The bot stores the raw code exactly as the API sends it — no decoding, no analysis — so anything downstream can parse it into troops/spells later. Present on nearly every attack (and some defenses); NULL when the API leaves it out. |
| Clan at reset | `player_reset_clan_log` | At every 05:00 UTC reset the bot logs each player's clan, but only when it changed (one row per player per change, `clan_tag` NULL = clanless). A player's clan on day D is the row with the latest `legend_date <= D`. |
| Season results | `league_season` | From the league-history endpoint: final trophies, wins, losses, stars, placement. |

The two sources don't always line up one-to-one (a few battles can happen between
two polls), and that's fine — the trophy diff tells you the *size* of the day, the
battlelog tells you the *count*.

### Both sides of every battle (mirror rows)

Each battlelog entry actually describes **two** players' days: if player A's log
says "A attacked B", that same event is a defense in B's day. The bot used to
only write A's row and wait until it happened to poll B to learn about B's side.
Now, whenever it saves a battle from A's log, it also writes the flip side for B
right away — B gets a defense row with the same stars, destruction and army link,
marked `source = 'mirror'` (A's own row is `source = 'native'`). If B isn't in
the database yet, a bare player row is created for them (they're a Legend by
definition — the battle came from a legend battlelog) without spending any extra
API calls.

The one wrinkle: the two players' own battlelogs stamp the *same battle* with
timestamps that can differ by up to ~5 minutes, so "same player + same time"
can't spot the duplicate when B's own log is fetched later. Instead the bot
matches on (player, opponent, attack-or-defense) within a ±300 second window: a
native row always replaces a matching mirror row (the player's own log is the
authoritative version), and a mirror is never written where a matching row
already exists. A sweep at the end of each update pass catches the rare case
where both sides land at the same instant from concurrent polls. Day counts
(`day.num_attacks` / `num_defenses`) include mirror rows — they're real
battles — but are adjusted whenever a native row replaces a mirror, so nothing
is ever counted twice.

There's a third `source` value, `'eye'`, for rows written by a separate
companion process (Project Eye) that reads the same battlelogs. Those count as
authoritative as `native`, and the bot leaves them alone.

Mirror rows deliberately do **not** advance the player's battlelog cursor
(`player.last_battle_time`) — that cursor marks how far the bot has read that
player's *own* log, and pushing it forward from a mirrored timestamp would make
the next real fetch skip entries.

Polling is cheap because of how clans work: one request for a clan returns all ~50
members' trophy counts at once. The bot keeps those in memory, and only when
someone's trophies actually move does it spend a second request to pull that one
player's battlelog. It never walks the whole player list one at a time.

> Heads up: the battlelog reports the opponent's Town Hall level, and the
> community found it can be off by one (as of June 2026). The bot stores it as-is,
> so adjust in your own queries if it matters.

## The daily schedule (UTC)

The Legend day rolls over around 05:00 UTC, so the jobs cluster around that:

| Time | Job | What it does |
| --- | --- | --- |
| 04:50 | Pre-reset seed | One last discovery sweep before the day flips. |
| 04:59 | Reset snapshot | Records every active player's clan at the reset (`player.reset_clan_tag` plus a `player_reset_clan_log` row when it changed). |
| 06:00 | League history | Saves each player's season summary. This one's heavier — a request per active player — so it runs once, well clear of the reset. |
| 17:00 | Morning sweep | A fresh discovery pass plus a full update. |
| every 3 min | Routine update | The workhorse: scan clans, spot who moved, pull their battlelogs. |

They all share one lock, so two jobs never hammer the API at the same time. Times
live in `src/legendsbot/main.py` if you want to change them.

## Getting your leaderboard

`sql/leaderboard.sql` sets up a `v_legend_day` view and has worked examples — best
offense, best defense, three-star rate, season standings. Best offense today is
just:

```sql
SELECT name, offense, num_attacks FROM v_legend_day
WHERE date = CURDATE() ORDER BY offense DESC LIMIT 25;
```

## Finding players (the catch)

There's no API call that lists every Legend player, so the bot has to discover
them. It finds players four ways:

1. The top clans in each location, then scans their members.
2. The top players in each location, then scans the clans they're in.
3. Any clan you list yourself in `config/known_clans.txt`. That file is local
   and gitignored. Copy `config/known_clans.txt.example` to start one, or let
   `tools/add_clans.py` create it for you.
4. Snowballing — once it finds a Legend clan, it keeps watching it.

The gap: a Legend player in a clan that never cracks any location's top list is
invisible to all of that. If you know someone like that, add their clan by hand:

```
python tools/add_clans.py "#CLANTAG"     register the clan as a place to look
python tools/scan_clan.py "#CLANTAG"     pull its Legend members right now
```

## Tiers

The bot tracks all three tiers — Legend I (`105000036`), Legend II
(`105000035`) and Legend III (`105000034`). It figures out who's a Legend from
the `leagueTier` field — the old `league` field is `null` now. Each player's
current tier id is stored in `player.league_tier_id`. If Supercell ever
shuffles the tier IDs, you can re-check them live with
`python tools/discover_tiers.py`.

## The tools folder

| Script | What it's for |
| --- | --- |
| `add_clans.py "#TAG"` | Register a clan as a discovery root and insert it now. |
| `scan_clan.py "#TAG"` | Pull one clan's Legend members immediately. |
| `debug_player.py "#TAG"` | Dump a player's raw API data and DB rows side by side. |
| `discover_tiers.py` | Print the live Legend tier IDs and names. |
| `probe_battlelog.py "#TAG"` | Look at a player's raw battlelog. |
| `prune_non_legends.py` | Drop players who've fallen out of Legend League (below Legend III). |
| `import_archive_clans.py` | Bulk-load clan tags from old data dumps. |
| `import_projecteye_clans.py` | Register opponent clans discovered by Project Eye as discovery roots. |

There's also a small test suite (`uv run python -m pytest`) covering the tier
detection, battlelog parsing (including army-link capture), and the mirror-row
logic — the bits most likely to break the next time the API changes. The
mirror-row tests need the local MySQL to be reachable (they use throwaway
`ZZTESTMIRROR*` tags and clean up after themselves); they skip automatically if
it isn't.

## Where things live

```
config/   keys.json (secret), known_clans.txt (local), location_ids.json, *.example
state/    runtime json, written automatically
sql/      schema.sql, leaderboard.sql, migrations/
src/legendsbot/
          main.py            the scheduler
          pipeline.py        the update pass: scan, diff, fetch
          seed.py            discovery
          db.py              all the MySQL and the daily rollups
          league_history.py  season-summary ingest
          scan_state.py / paths.py
tools/    the scripts in the table above
run/      populate_db.bat, start_bot.bat
```

## Attribution

If your server ends up using this, I'd appreciate:

- A link back to this repo.
- Credit to the original project.
- A heads-up about your server — I'd genuinely like to hear about it.

## License

[MIT](LICENSE) © 2026 Caleb Weisgerber.
