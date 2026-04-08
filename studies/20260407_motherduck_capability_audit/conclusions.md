# MotherDuck capability audit — conclusions

Verification used **locally saved** credentials (`.streamlit/secrets.toml` RW token per `token_mode(): secrets.toml:MOTHERDUCK_TOKEN`), not user-supplied pasted secrets.

| Question | Result |
|----------|--------|
| RW token available? | **Yes** (source label above) |
| Read-scaling token available? | **No** (`read_scaling_token_mode(): none`) |
| `connect_rw` / fail-closed smoke validated? | **Yes** — `smoke_test_md_connection.py --md` exit **0** |
| `connect_read_scaling` validated live? | **No** — no read-scaling token; `RuntimeError` as expected |
| Reader refresh helper path (`136 reader --dry-run`)? | **Yes** — prints `REFRESH DATABASE "Thyroid 2026"`, exit **0** |
| Writer snapshot helper (`136 writer --dry-run`)? | **Yes** — prints `CREATE SNAPSHOT OF "Thyroid 2026"`, exit **0** |
| Current prod appears DuckLake? | **Yes** — `MD_INFORMATION_SCHEMA` type `DUCKLAKE` |
| Business-style read-scaling available **on this machine**? | **Not demonstrated** — no read-scaling credential configured; cannot attach or refresh readers with a reader token |
| Credential separation (RW vs RS-only)? | **PASS** — subprocess RS-only placeholder → smoke `--md` exit **1** with explicit FATAL (see `fail_closed_separation_test.txt`) |
| Make `md-smoke` | **PASS** (exit 0) |
| Make `md-v2-gate-md-dryrun` | **PASS** (exit 0); embedded 119 summary text included WARN/FAIL counts — treat as **formalization report content**, not Make failure |
| Make `md-live-release-dryrun` | **Interrupted** during first run (buffered output); **re-run `124 -u` completed** with exit **0** (`124_direct_exit=0` in `make_targets_audit.txt`) |
| Pytest subset | **33 passed** |

## Exact evidence

- Exit codes and stdout are in: `md_smoke_output.txt`, `md_inspect_output.txt`, `prepromote_capability_probe.txt`, `read_scaling_validation.txt`, `fail_closed_separation_test.txt`, `make_targets_audit.txt`, `pytest_md_audit.txt`.
- Catalog snapshot listing: `md_inspect_output.txt` (`DATABASE_SNAPSHOTS` section).
