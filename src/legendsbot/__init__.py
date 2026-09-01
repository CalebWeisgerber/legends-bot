"""legendsbot — Clash of Clans Legend player tracker (Legend I/II/III)."""

import sys as _sys

# Force UTF-8 on stdout/stderr, with errors='replace' as a belt-and-braces
# fallback.
#
# Why this is not cosmetic: start_bot.bat redirects stdout to state\bot_run.log,
# so Python picks the ANSI codepage (cp1252) instead of a console's UTF-8. Every
# log line that starts with an emoji -- db.mark_clan_inactive's "🧹", the "⚠️ DB
# Transaction failed" handler in pipeline.process_clan, upsert_clan's "🛑" --
# then raised UnicodeEncodeError from the print() ITSELF.
#
# That turned a log line into a control-flow bug. In process_clan the emoji
# print sits inside the DB-transaction `except` handler, so the handler blew up
# while handling, the exception escaped the clan task, and the pass moved on
# having rolled back that clan's entire batch of battle inserts. The run to
# 2026-09-01 logged 5,993,722 "Clan task failed: 'charmap' codec can't encode"
# lines over 9 days and grew bot_run.log to 611 MB.
for _stream in (_sys.stdout, _sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        # Not a TextIOWrapper (pytest capture, embedded interpreter, ...).
        pass
