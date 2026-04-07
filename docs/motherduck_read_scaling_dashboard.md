# MotherDuck read scaling for THYROID_2026 dashboards

This note complements [`motherduck_database_contract_v1.md`](motherduck_database_contract_v1.md) §8 and the implementation in [`motherduck_client.py`](../motherduck_client.py).

## Token split

| Role | Environment variables | Use |
|------|----------------------|-----|
| **Writer / admin** | `MOTHERDUCK_TOKEN`, `MD_SA_TOKEN`, or Streamlit `secrets.toml` keys | ETL, `116_*` loaders, promotion gate, `115_release_snapshot.py`, validators with `--md`, `CREATE SNAPSHOT` |
| **Reader / dashboard** | `MD_READ_SCALING_TOKEN` or `MOTHERDUCK_READ_SCALING_TOKEN` | High-concurrency read load, Streamlit, analyst notebooks using `connect_read_scaling()` or RO share |

Read-scaling tokens are **not** wired into `get_token()` / `connect_rw()` so CI and promotion scripts cannot accidentally pick them up.

## Environment variable contract

**Core (unchanged)**

- `MOTHERDUCK_TOKEN` — personal read/write API token  
- `MD_SA_TOKEN` — service account read/write (prefer in automation)  
- `MOTHERDUCK_DATABASE` / `MOTHERDUCK_DB` — optional catalog name override  
- `MOTHERDUCK_ENV` — `dev` | `qa` | `prod` (selects row in `config/motherduck_environments.yml`)  
- `MOTHERDUCK_CUSTOM_USER_AGENT` — query-history attribution  

**Read scaling**

- `MD_READ_SCALING_TOKEN` — primary read-scaling secret  
- `MOTHERDUCK_READ_SCALING_TOKEN` — alias  
- `MD_READ_SCALING_SESSION_HINT` / `MOTHERDUCK_READ_SCALING_SESSION_HINT` — optional; passed as MotherDuck `session_hint` (connection string + `SET motherduck_session_hint`) for steadier duckling affinity on readers  
- `MOTHERDUCK_SESSION_HINT` — used for **read/write** and for **RW** RO-share attempts in the dashboard  

**Streamlit dashboard flags (default off)**

- `MOTHERDUCK_DASHBOARD_PREFER_READ_SCALING_TOKEN=1` (or `THYROID_DASHBOARD_PREFER_READ_SCALING_TOKEN=1`) — try the read-scaling token **before** the RW token when attaching the configured RO share  
- `MOTHERDUCK_DASHBOARD_ALLOW_READ_SCALING_ATTACH=1` (or `THYROID_DASHBOARD_ALLOW_READ_SCALING_ATTACH=1`) — allow fallback `connect_read_scaling()` to the **primary** database catalog when share + RW paths fail (Business-tier attach)  

Without the attach flag, the dashboard never uses primary-catalog read-scaling attach automatically (safer default).

## Connection examples

**Read/write (Python / DuckDB URI)**

```text
md:?motherduck_token=<URL_ENCODED_RW_TOKEN>&custom_user_agent=<UA>&session_hint=<HINT>
-- then USE "Thyroid 2026"
```

Or, when the database name has no spaces:

```text
md:Thyroid%202026?motherduck_token=...
```

Use `MotherDuckClient.for_env("prod").connect_rw()` in this repo instead of hand-building URIs.

**Read-scaling reader**

```python
from motherduck_client import MotherDuckClient
con = MotherDuckClient.for_env("prod").connect_read_scaling()
```

**RO share (dashboard default path)**

The Streamlit app uses `MotherDuckClient.connect_ro_share(token=...)` with `share_path` from `dashboard.py`, then `USE "thyroid_research_ro_v2"`.

**Secrets file**

The same keys may be placed in `.streamlit/secrets.toml` (see `motherduck_client.get_read_scaling_token()`).

## Snapshot and refresh (freshness)

MotherDuck recommends:

1. **Writer:** `CREATE SNAPSHOT OF <database>;` (optionally `CREATE SNAPSHOT <name> OF <database>;`)  
2. **Reader:** `REFRESH DATABASE <database>;` or `REFRESH DATABASES` on read-scaling connections  

Helper SQL builders and runners live in [`utils/md_read_scaling_refresh.py`](../utils/md_read_scaling_refresh.py). CLI:

```bash
# Writer (RW token) — after ETL / release
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod

# Reader (read-scaling token)
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod

# All attached reader DBs
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --all
```

`--dry-run` prints SQL without connecting.

**Warning:** Creating a snapshot can interrupt in-flight queries on that database ([MotherDuck docs](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-snapshot)).

## Eventual consistency and freshness

Read-scaling replicas **lag** the writer by default (on the order of **about a minute** for automatic sync). Queries against a reader may therefore return slightly older snapshots than the primary. That is normal eventual consistency, not a bug.

If you need readers to see writes immediately after a release:

1. Run `CREATE SNAPSHOT` on the **writer** (or wait for automatic snapshot behavior per MotherDuck policy).  
2. Run `REFRESH DATABASE` / `REFRESH DATABASES` on **readers** you control, **or** accept automatic sync latency.  

`session_hint` does not fix staleness; it influences routing/caching affinity for a connection. Combine snapshot + refresh for strict freshness bounds.
