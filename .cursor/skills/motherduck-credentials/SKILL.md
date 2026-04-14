---
name: motherduck-credentials
description: Where MotherDuck read/write and read-scaling tokens live in THYROID_2026, resolution order, and safe usage. Use whenever running --md scripts, MotherDuckClient, or answering "where is the API key".
---

# MotherDuck credentials (THYROID_2026)

## Canonical code

- **`motherduck_client.py`** — `get_token()` (RW), `get_read_scaling_token()` (read-scaling only). Prefer importing these instead of reading files ad hoc.

## Read/write token resolution order (`get_token`)

Matches `docs/motherduck_database_contract_v1.md` §8 and the docstring in `motherduck_client.py`:

1. Environment: `MD_SA_TOKEN`
2. Environment: `MOTHERDUCK_TOKEN`
3. Environment: `motherduck_token` (alias)
4. `LOCAL_DB_PATH` — only if the value looks like a JWT / `md_` PAT (misconfig guard)
5. **Repo-root TOML (gitignored):** `motherduck.local.toml` — keys in order: `MD_SA_TOKEN`, `MOTHERDUCK_TOKEN`, `motherduck_token`
6. **Streamlit secrets:** `.streamlit/secrets.toml` — same key order as (5)

**Bootstrap:** Copy `motherduck.local.toml.example` → `motherduck.local.toml` at repo root and set keys there (file stays gitignored).

**Optional env files:** At import, `motherduck_client` may load repo-root `.env` and `.env.motherduck` via python-dotenv (`override=False`). See `.env.motherduck.example` if present.

## Read-scaling token (`get_read_scaling_token`)

Use **only** for read-only / dashboard attach patterns — **never** as RW for promotion or `connect_rw()`.

Order: `MD_READ_SCALING_TOKEN` → `MOTHERDUCK_READ_SCALING_TOKEN` → `motherduck.local.toml` → `.streamlit/secrets.toml`.

## Security (mandatory)

- **Never** print, commit, or paste token values into chat, logs, or markdown. Logs should report SET / MISSING / length only (see project `AGENTS.md`).
- CI: inject via GitHub Actions secrets / secret manager, not repo files.

## Running scripts against MotherDuck

- Most scripts use `--md`; some support `--md-sa` to prefer `MD_SA_TOKEN` where documented.
- Session / affinity: optional `MOTHERDUCK_SESSION_HINT` (see smoke test and runbooks under `docs/motherduck_*.md`).

## Quick reference

| Purpose | Primary keys                                      | Typical file / source |
|----------------|---------------------------------------------------|------------------------------|
| RW (dev/CI)    | `MD_SA_TOKEN`, `MOTHERDUCK_TOKEN`                 | Env → `motherduck.local.toml` → `.streamlit/secrets.toml` |
| Read-scaling   | `MD_READ_SCALING_TOKEN`, `MOTHERDUCK_READ_SCALING_TOKEN` | Same TOML files after env    |

When the user says "use the token in the TOML file," they mean **repo-root `motherduck.local.toml`** (from the example), not a committed path with real secrets.
