# Next steps attempt — read-scaling token still absent

**Checked:** `.streamlit/secrets.toml` and process env.

| Source | `MD_READ_SCALING_TOKEN` | `MOTHERDUCK_READ_SCALING_TOKEN` |
|--------|-------------------------|----------------------------------|
| secrets.toml | empty / missing | empty / missing |
| `$MD_READ_SCALING_TOKEN` | unset | — |
| `$MOTHERDUCK_READ_SCALING_TOKEN` | — | unset |

`read_scaling_token_mode()` = **`none`**. Live `136 reader` and `connect_read_scaling()` were **not run** (would fail immediately).

## What you run locally (one time)

1. Paste your MotherDuck **Business read-scaling** (reader) token into secrets — **not** the same string as `MOTHERDUCK_TOKEN`:

   ```bash
   cd /path/to/THYROID_2026
   printf '%s\n' 'YOUR_READER_TOKEN_HERE' | .venv/bin/python scripts/merge_streamlit_read_scaling_token.py
   ```

   Or edit `.streamlit/secrets.toml` and add:

   ```toml
   MD_READ_SCALING_TOKEN = "md_...."
   ```

2. Refresh readers after a writer snapshot:

   ```bash
   export MOTHERDUCK_ENV=prod   # if not already
   .venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod
   ```

3. Optional connectivity check:

   ```bash
   .venv/bin/python -c "from motherduck_client import MotherDuckClient; c=MotherDuckClient.for_env('prod').connect_read_scaling(); print(c.execute('SELECT current_database()').fetchone())"
   ```

Re-run this doc’s checks after step 1; then an agent can execute reader + probe and commit evidence if you want.
