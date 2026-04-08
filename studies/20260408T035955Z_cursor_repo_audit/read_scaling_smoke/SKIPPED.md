# Read-scaling smoke — skipped

`get_read_scaling_token()` did not resolve (`MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` / secrets.toml keys absent).

Per `docs/motherduck_read_scaling_dashboard.md`, read-scaling tests require a dedicated reader token; RW token must not be used for `connect_read_scaling()`.
