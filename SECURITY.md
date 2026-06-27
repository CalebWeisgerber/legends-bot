# Security

## Reporting

Found a vulnerability, or a leaked secret in this repo's history? Open a
[GitHub security advisory](https://github.com/CalebWeisgerber/legends-bot/security/advisories/new)
(private) rather than a public issue.

## Secrets in this project

- **`config/keys.json`** — Clash of Clans API JWTs. Gitignored. Never commit.
- **`run/_env.bat`** — holds `DB_PASSWORD`. Gitignored. Never commit.
- `DB_PASSWORD` is read from the environment with **no hardcoded default**; the bot
  refuses to start without it.

If you ever commit a key by accident: revoke it at
<https://developer.clashofclans.com>, rotate the DB password, and rewrite history
before pushing.
