# Performance work and open issues

Audit of the ingest pipeline and database, 2026-09-03. Two halves:

- **Part 1 — Done.** Changes already applied, and why.
- **Part 2 — Open.** Issues found but not yet fixed, each with the fix.

The trigger was a `legend_battle` lookup by `opponent_tag` that never
completed — it hit MySQL Workbench's 30-second client timeout twice. After the
work in Part 1 the same query returns in **0.031s**.

---

## Part 1 — Done

### 1.1 Server configuration (the big one)

`innodb_buffer_pool_size` was still the install default of **128 MB** on a box
with 32 GB of RAM and an NVMe SSD. The buffer pool is how much of the database
MySQL can hold in memory; everything outside it is read from disk on every
access. Against a multi-GB `legend_battle` table this meant essentially nothing
was cached, which is the real reason that query took 30 seconds. Indexes cannot
compensate for a 128 MB window into a multi-GB table.

`innodb_io_capacity` at 200 is the default for a single spinning disk, not NVMe.

Applied with `SET PERSIST`, which takes effect immediately and survives
restarts — no service restart, no file edit:

```sql
SET PERSIST innodb_buffer_pool_size  = 8589934592;   -- 8G, was 128M
SET PERSIST innodb_io_capacity       = 2000;          -- NVMe, was 200
SET PERSIST innodb_io_capacity_max   = 4000;
SET PERSIST innodb_flush_neighbors   = 0;             -- SSD: no write grouping
SET PERSIST max_connections          = 300;           -- pool is 100, was 151
SET PERSIST innodb_redo_log_capacity = 2147483648;    -- 2G, was 48M
```

`innodb_redo_log_capacity` requires MySQL 8.0.30+ (this box runs 8.0.46). On
older servers the equivalent is `innodb_log_file_size`, which is **not**
dynamic and needs a my.ini edit plus a restart.

Two things to know:

- **8 GB, not the usual 50-70% of RAM.** The bot, MySQL and the desktop share
  this machine and only ~10 GB was free. 8 GB is a 64x improvement with
  headroom. It can be raised later; it resizes online either way.
- **`SET PERSIST` writes `mysqld-auto.cnf`, which overrides `my.ini`.** This
  confuses people months later when editing my.ini appears to do nothing. The
  file lives beside the data directory. To keep everything in one place
  instead, put these values in
  `C:\ProgramData\MySQL\MySQL Server 8.0\my.ini` and restart the service.

Not changed, deliberately: `innodb_flush_log_at_trx_commit` is still `1`.
Setting it to `2` would speed up this commit-heavy workload measurably, but it
trades away durability — on an OS crash or power loss you can lose up to a
second of committed transactions. That is a data-safety decision, not a
performance one.

### 1.2 Indexes

Migrations `0008` and `0009`. All three queries below were full table scans.

| Index | Table | Serves |
|---|---|---|
| `idx_battle_opponent (opponent_tag)` | `legend_battle` | "who fought player X" |
| `idx_battle_source_time (source, battle_time)` | `legend_battle` | `reconcile_mirror_duplicates()` |
| *(dropped)* `idx_player_active` | `player` | redundant — see below |

**`idx_battle_opponent`.** `uq_battle` and `idx_battle_pair` both lead with
`player_tag`, so neither can serve a lookup keyed on the opponent side. Single
column on purpose: `is_attack` and `battle_time` could be appended to skip the
post-lookup filter and the `ORDER BY battle_time DESC` filesort, but row counts
per opponent tag are small enough that both are cheap.

**`idx_battle_source_time`.** `reconcile_mirror_duplicates()` drives on
`WHERE m.source = 'mirror' AND m.battle_time > UTC_TIMESTAMP() - INTERVAL n DAY`.
No index led with either column (`idx_battle_attack_time` leads with
`is_attack`, absent from that WHERE), so the sweep scanned the entire table
**once per pass — roughly 480 times a day.**

**`idx_player_active` dropped.** It was `(is_active)` alone: cardinality 2 with
nearly every row set to 1, so the optimizer ignored it. It was also an exact
leftmost prefix of `idx_player_active_seen_at (is_active, last_seen_at)`, which
migration `0007` had already added. Dropping it loses no access path.

> **Note on `days_back`.** `reconcile_mirror_duplicates(days_back=7)` is a
> *search window*, not a retention policy. Nothing ages out of the database
> because of it. The function deletes **mirror** rows that have a native
> counterpart — synthesized rows superseded by real ones — which is what stops
> `day.num_attacks` double-counting. Lowering the window makes it delete
> *less*, not more. It stays at 7.

**Running these:** both `ALTER`s on `legend_battle` are ONLINE (concurrent
reads and writes keep working) but rebuild in place and can run for minutes.
Raise Workbench's *Preferences > SQL Editor > DBMS connection read timeout
interval* to 6000 first and reconnect, or the client hangs up at 30s with
`Error Code: 2013` while the server carries on building the index anyway.

### 1.3 Code changes

Six changes, all verified by the existing test suite (16 tests; the 5 in
`test_mirror_rows.py` run against real MySQL and exercise the rewritten SQL
directly).

**Transaction bug — `mark_clan_active` / `mark_clan_inactive` (`db.py`).**
Both committed internally. `process_clan` calls them *inside* its own
`conn.begin()` block, so the transaction ended there and the `except:
rollback()` below had nothing left to undo — the atomicity the block is written
to provide was not there. It also cost a second fsync per clan. Both now take
`commit: bool = True` and `process_clan` passes `commit=False`. The default
keeps `safe_delete_clan`'s path unchanged.

**Concurrent battlelog fetches (`pipeline.py`, `process_clan`).** Battlelogs
were fetched one member at a time. A 40-legend roster meant 40 sequential HTTP
round trips (~150ms each) before the clan's DB phase could begin — ~6 seconds
of pure idle waiting per clan. Now an `asyncio.gather` with
`return_exceptions=True`.

This does **not** raise the API request rate: every call still passes through
`limiter.throttle()`, so the `PER_KEY_RPS` ceiling is unchanged. It stops
leaving that ceiling unused.

> **Watch this one.** It raises in-flight coroutine count from ~200 (one per
> clan task) to up to 200 x roster size, most parked on the limiter semaphore.
> That cuts against the memory pressure `run_bounded` was written to avoid and
> the RSS ratcheting documented in `pipeline.py`. Watch RSS over the first few
> passes. If it climbs, cap per-clan fetch concurrency with a small semaphore
> (say 10) around the gather — that keeps most of the win with a bounded
> ceiling.

**One `player` scan instead of two (`pipeline.py`).** `load_trophy_cache` and
`load_battle_cursors` each ran `SELECT ... FROM player WHERE is_active = 1`
back to back on separate connections, reading every active row twice per pass.
Merged into `load_active_player_state`, one query returning both dicts. There
is nothing to index here — both genuinely want the whole active playerbase — so
the fix is to read it once.

**Single-statement day counter (`db.py`, `_bump_day_battle_count`).** Was
`INSERT IGNORE` followed by `UPDATE`. Now one `INSERT ... ON DUPLICATE KEY
UPDATE`. This is the highest-frequency write in the pipeline — once per battle
plus once per harvested mirror row — so the second round trip was pure
overhead. `GREATEST` is applied on the insert branch too, so a `-1` delta
against a day row that does not exist yet creates it at 0, never at -1.

**No redundant clan upsert per member (`db.py`, `pipeline.py`).**
`process_clan` upserts the clan once for the whole roster, then every
`upsert_player` call re-ran the identical statement — 16 clan upserts for a
15-legend clan, every pass. `upsert_player` now takes
`clan_already_upserted: bool = False` and `process_clan` passes `True`.

`process_unseen` must **not** set it: a player who moved to an untracked clan
has no other path that creates that clan row.

**Battlelog cursor folded into the player upsert (`db.py`).**
`last_battle_time` was a second `UPDATE` against the row `upsert_player` had
just written. It now lives in the same statement's `ON DUPLICATE KEY UPDATE`:

```sql
last_battle_time = IF(new.last_battle_time IS NULL,
                      player.last_battle_time,
                      GREATEST(COALESCE(player.last_battle_time,
                                        new.last_battle_time),
                               new.last_battle_time))
```

Both original guards are preserved: a NULL cursor leaves the stored value
alone, and the value only ever moves forward.

### 1.4 Net effect per pass

Dropped: one full scan of `legend_battle`, one full scan of `player`, one
statement per battle, one statement per mirror row, one statement per player,
N-1 clan upserts per clan, one commit per clan. Battlelog fetches within a clan
now overlap instead of queueing.

---

## Part 2 — Open issues

Found during the audit, not yet fixed. Ordered by severity.

### CRITICAL — Rate limiter delivers ~26% below its configured rate

**Where:** `pipeline.py`, `RateLimiter._refill_loop`.

```python
async def _refill_loop(self) -> None:
    delay = 1 / self.rate                      # 1/29 = 34.5ms
    while True:
        await asyncio.sleep(delay)
        if self.semaphore._value < self.burst:
            self.semaphore.release()           # one permit per tick
```

One permit is released per `asyncio.sleep(1/rate)`, so the delivered rate is
exactly `1 / actual_sleep_duration`. On Windows the default timer granularity
is ~15.6ms and sleeps round **up** to the next tick, so a requested 34.5ms
sleep actually takes 46.9ms (three ticks).

Measured on the production box (32 GB / NVMe / Python 3.14, 200 iterations):

```
requested   34.48ms -> actual   46.44ms | effective 21.54/s (nominal 29.00/s)
```

**Every API key has been delivering ~21.5 req/s instead of 29 — a 25.7%
throughput loss.** The pipeline is API-bound, so this caps the entire bot. It
is invisible because nothing compares delivered rate against nominal rate; the
limiter simply never hands out permits fast enough for anyone to notice the
ceiling is lower than configured.

This is very likely the single largest remaining performance item, and it costs
nothing in API-limit risk to fix — the corrected limiter still averages exactly
`PER_KEY_RPS`.

**Fix.** Do not assume the sleep was accurate. Derive the permit count from
elapsed clock time, which is self-correcting under any timer granularity:

```python
async def _refill_loop(self) -> None:
    # Permits are derived from ELAPSED TIME, not from an assumed-accurate
    # sleep. A per-permit sleep of 1/rate (34.5ms at rate=29) is finer than
    # the Windows timer granularity of ~15.6ms, so it actually slept 46.4ms
    # and delivered ~21.5/s instead of 29/s -- a silent 26% loss on every
    # key. Measured on this box, 2026-09-03.
    interval = 0.2          # coarse enough to be accurate, fine enough to be smooth
    carry = 0.0             # fractional permits owed, carried between ticks
    last = time.monotonic()
    try:
        while True:
            await asyncio.sleep(interval)
            now = time.monotonic()
            carry += (now - last) * self.rate
            last = now
            n, carry = int(carry), carry - int(carry)
            for _ in range(n):
                if self.semaphore._value >= self.burst:
                    carry = 0.0     # bucket full; don't bank credit while idle
                    break
                self.semaphore.release()
    except asyncio.CancelledError:
        return
```

Measured tick candidates (permits released per tick, versus nominal 29/s):

| tick | actual sleep | effective rate | error |
|---|---|---|---|
| 34.5ms (current) | 46.44ms | 21.54/s | **-25.7%** |
| 100ms | 109.23ms | 27.47/s | -5.3% |
| 200ms | 203.18ms | 29.53/s | +1.8% |
| 1000ms | 1007.94ms | 28.77/s | -0.8% |

A fixed permits-per-tick table still drifts (the 200ms row *overshoots* 29/s,
which is why `PER_KEY_RPS = 29` was chosen to sit under 30 in the first place).
The elapsed-time version above has no such drift at any interval — it grants
exactly `elapsed x rate` permits regardless of how long the sleep really took.

**Verify after fixing** by counting completed requests over a known window and
comparing against `PER_KEY_RPS * len(KEYS)`. Watch for HTTP 429s on the first
run: the bot has never actually driven these keys at their configured rate, so
this is the first time the real ceiling gets tested. `fetch_*` already handle
429 with `Retry-After` backoff, but if 429s appear, lower `PER_KEY_RPS` rather
than reverting to the broken limiter.

**Secondary issue in the same function.** `self.semaphore._value` reads a
private CPython attribute. It works today but is not API, and it is the kind of
thing that breaks silently on a Python upgrade. Track available permits in an
explicit counter instead.

### CRITICAL — Nested pool acquisition can deadlock the pipeline

**Where:** `pipeline.py`, `process_clan`, the `except` block.

```python
async with pool.acquire() as conn:          # holds connection #1
    try:
        ...
    except Exception as e:
        await conn.rollback()
        await touch_clan_timestamp(pool, clan_tag)   # acquires connection #2
```

`touch_clan_timestamp` takes its **own** connection from the pool while the
task still holds one. `POOL_MAX` is 100; `CONCURRENCY` runs up to 200.

If 100 clan tasks enter that `except` simultaneously — a DB blip, a schema
lock, a long `ALTER` — all 100 connections are held by tasks each waiting for a
101st that can never be freed. That is a permanent deadlock, not a slowdown.

**Why it matters beyond the deadlock itself.** The deadlocked task never
returns, so it never releases `API_LOCK`. `task_routine_update` then skips
every cycle. The process stays alive, the scheduler keeps ticking, and nothing
is logged. That is exactly the outage signature recorded in `main.py`: 8
stretches of 30+ minutes in 58 days, 153 hours dead, 88.8% uptime, worst single
stretch 99 hours — none producing a Windows event or a log line, each ending
only at a machine reboot.

This has not been proven to be the cause of those outages. It is a mechanism
that produces exactly them.

**Fix.** Use the connection already held instead of taking a second:

```python
        except Exception as e:
            await conn.rollback()
            try:
                print(f"[WARN] DB Transaction failed for clan {clan_tag}: {e!r}")
            except Exception:
                pass
            # Reuse `conn` -- acquiring a second connection while holding one
            # can exhaust the pool and deadlock every clan task at once.
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE clan SET last_member_scan = UTC_TIMESTAMP() "
                        "WHERE clan_tag = %s",
                        (clan_tag,),
                    )
                await conn.commit()
            except Exception:
                pass
```

Audit the rest of the file for the same shape while you are in there: any
`pool.acquire()` reached from inside another `pool.acquire()` block has this
bug.

### HIGH — Background tasks can be garbage collected

**Where:** `main.py`, in `main()`.

```python
asyncio.create_task(run_startup())
asyncio.create_task(_heartbeat())
```

`asyncio` holds only a **weak** reference to running tasks. With no strong
reference kept anywhere, either task can be collected mid-flight and silently
disappear. Losing `_heartbeat` means losing the only stall detector — the exact
instrumentation written to catch the outages above.

**Fix.** Keep strong references for the process lifetime:

```python
_BACKGROUND_TASKS: set[asyncio.Task] = set()

def _spawn(coro) -> asyncio.Task:
    """Keep a strong reference: asyncio only holds a weak one, so a bare
    create_task() can be garbage-collected mid-flight."""
    task = asyncio.create_task(coro)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    return task
```

Then `_spawn(run_startup())` and `_spawn(_heartbeat())`.

### HIGH — `process_unseen` error path retries forever

**Where:** `pipeline.py`, `process_unseen`, the `if data.get("error")` branch.

Returning `"error"` writes nothing to the database, so `last_seen_at` stays
stale and `get_all_player_tags()` re-queues that player on **every** pass,
indefinitely. A block of permanently unreachable tags becomes a fixed per-pass
tax of API calls that can never succeed, crowding out real work.

Note the contrast: `not_found` and non-Legend players are correctly archived
with `is_active = 0`. Only the transient-error path lacks an exit.

**Fix, two options.**

*Simple* — touch `last_seen_at` on error so the player re-enters the queue
after `UNSEEN_RECONCILE_MINUTES` instead of immediately:

```python
    if data.get("error"):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE player SET last_seen_at = UTC_TIMESTAMP() "
                    "WHERE player_tag = %s",
                    (player_tag,),
                )
            await conn.commit()
        return "error"
```

The cost is that a genuine transient blip delays that player by one reconcile
interval rather than retrying at once.

*Thorough* — add a `consecutive_failures` column, increment on error, reset on
success, and exclude players above a threshold from `get_all_player_tags()`
until a slower retry window elapses. More schema, but it distinguishes "the API
hiccuped" from "this tag has been dead for a week."

### HIGH — Seed timestamp is written before the work it guards

**Where:** `seed.py`, `fetch_all_clans`.

`_update_seed_timestamp()` is called **before** the clan insert loop. If that
loop throws, the timestamp claims seeding succeeded, and `_seed_recently_ran()`
suppresses retries for 12 hours — with the clans never inserted.

**Fix.** Move the call to after the insert loop, beside `record_seed_run()`.
Both should record only completed work.

### MEDIUM — `league_history` raises a false STALLED alarm every day

**Where:** `main.py` (`task_league_history`, `LOCK_STUCK_WARN_SECONDS`) and
`league_history.py`.

`task_league_history` holds `API_LOCK` for one API call per active player —
roughly 240,000 calls. `task_routine_update` skips while the lock is held, and
once the hold exceeds `LOCK_STUCK_WARN_SECONDS` (600s) the log emits:

```
[UPDATE] STALLED: skipped N run(s); API_LOCK held by 'league_history' ...
         Ingestion is STOPPED until that task returns.
```

Every day, as normal operation. The statement is technically true, but an alarm
that cries wolf daily is one you learn to ignore — which defeats the purpose of
instrumentation added specifically to catch silent stalls.

**Fix.** Give known-long tasks their own threshold, so the warning keeps
meaning "something is wrong":

```python
# Tasks legitimately expected to hold API_LOCK past the normal warn window.
LONG_TASK_WARN_SECONDS = {"league_history": 3600, "morning_seed": 1800}
```

and in `task_routine_update`, compare `held` against
`LONG_TASK_WARN_SECONDS.get(_lock_holder, LOCK_STUCK_WARN_SECONDS)`.

Worth deciding separately whether league history needs `API_LOCK` at all — it
is rate-limited independently, and blocking ingestion for its duration may not
be necessary.

### MEDIUM — Two independent seed-timestamp mechanisms

`seed.py` writes `seed_state.json` via `_update_seed_timestamp()` **and** calls
`record_seed_run()`, which writes `last_scan.json`. Two files, two readers, one
concept — they can disagree.

Related: `state/scan_state.json` exists on disk but `scan_state.py` only ever
reads and writes `last_scan.json`. The file appears orphaned.

**Fix.** Pick `scan_state.py` as the single owner of run timestamps, delete
`_update_seed_timestamp()` and `SEED_STATE_PATH`, and remove the stale files.

### LOW — Remaining performance items

**Commit per clan during seeding.** `seed.py` calls `upsert_clan(conn, name,
tag)` in a loop with the default `commit=True` — one commit per clan, thousands
per seed run. Pass `commit=False` and commit once after the loop.

**Connection and commit per player in league history.** `league_history.py`
acquires a pool connection and commits per player, ~240k times daily. Batch
into chunks (a few hundred players per transaction).

**`attack_log` / `defense_log` have no reader.** Nothing in `src/`, `sql/`, or
`tools/` reads either table, but both take an indexed insert per trophy change
per poll, forever. Left alone deliberately — "no reader in this repo" is not
"no value," and an external consumer may exist. Worth confirming either way,
since the write cost is real.

**Three tag-normalization idioms.** `normalize_tag()` in `db.py`,
`.replace("#", "").upper()` in `load_active_player_state`, and
`.strip("#").upper()` in `process_clan`. `strip("#")` also removes a *trailing*
`#`. Same intent, three implementations; `normalize_tag()` should be the only
one.

**Unbounded gather over locations in seeding.** `seed.py`, `fetch_all_clans`
builds one task per location for both ranking endpoints and awaits them with
two bare `asyncio.gather` calls — roughly 2 x `len(LOCATION_IDS)` coroutines
live at once, bypassing `run_bounded` entirely. The rate limiter still caps
actual request throughput, so this is memory shape rather than API risk, but it
is the same pattern `run_bounded` was written to replace. Route both through
`run_bounded` for consistency.

**`fetch_all_clans(processes: int = 10)` — dead parameter.** Never referenced
in the body. Remove it, or wire it to the `run_bounded` concurrency above.

**Full result buffering on the 240k-row player scan.** `load_active_player_state`
calls `cur.fetchall()`, which materialises every active player row as a list of
tuples before building the two dicts. With ~240k rows that is a transient spike
on every pass. `aiomysql`'s `SSCursor` streams instead, trading a held
connection for a flat memory profile. Only worth doing if RSS proves to be a
problem — noted for completeness, not recommended blindly.

---

## What is verified clean

- **Secrets.** `config/keys.json` and `run/_env.bat` are both gitignored,
  `DB_PASSWORD` is required from the environment with no fallback, and there
  are no credentials in source.
- **Error isolation.** `run_bounded` correctly isolates per-item exceptions so
  one failure cannot orphan a batch, and the reporting path is itself wrapped
  so a bad log line cannot escalate into a lost batch.
- **Encoding discipline.** ASCII-only logging inside transaction handlers is
  well-reasoned and documented — a cp1252 stdout genuinely did turn an emoji
  into a lost clan batch.
- **Legend-day bucketing.** `_legend_day_for()` is applied consistently to both
  native and mirror rows, so battles land on the day they happened rather than
  the day they were ingested.

---

## Suggested order

1. Rate limiter refill (CRITICAL — measured ~26% throughput recovery, and the
   largest single performance item left)
2. Nested pool acquisition (CRITICAL — likely cause of the silent outages)
3. Background task references (HIGH — restores stall detection)
4. `process_unseen` retry loop (HIGH)
5. Seed timestamp ordering (HIGH)
6. STALLED alarm threshold (MEDIUM — stops alarm fatigue)
7. Seed state consolidation (MEDIUM)
8. Low-priority performance and consistency items

Item 1 is the biggest remaining speed win and is self-contained. Items 2 and 3
both bear directly on the unexplained downtime and are each a few lines.

Do item 1 on its own and watch for 429s before moving on — it is the first
time these API keys will actually run at their configured rate.
