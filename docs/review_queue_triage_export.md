# Manual review queue triage export (script 120)

Read-only bundle for **`qa.manual_review_queue`**: CSVs plus **`summary.md`**, under a **timestamped** folder `exports/review_queue_triage_<UTC_YYYYMMDD_HHMMSS>/`.

- **No writes** to MotherDuck or local DuckDB (SELECT only).
- **No raw clinical note text**: `review_reason` is **omitted** from reviewer worklists; `entity_value_norm`, `reviewer_comment`, and `reviewer_evidence_span` are **truncated** in worklists (see `scripts/120_review_queue_triage.py`).
- **Tokens**: Same resolution as other `--md` scripts — `MOTHERDUCK_TOKEN`, `MD_SA_TOKEN` (with `--md-sa`), or `.streamlit/secrets.toml` / `.env.motherduck`. See [`docs/motherduck_database_contract_v1.md`](motherduck_database_contract_v1.md) §8.

## Outputs (per run)

| Artifact | Purpose |
|----------|---------|
| `summary.md` | Headline counts, file manifest, top domains |
| `counts_*.csv` | Domain / verification / blocking breakdown |
| `oldest_pending_rows.csv` | Stale pending rows (no free-text note fields) |
| `worklists/worklist__<domain>__tier_<tier>.csv` | Pending rows only, split for reviewers |

## Commands

**MotherDuck (requires read/write attach token — not read-scaling-only):**

```bash
export MOTHERDUCK_TOKEN='md_…'   # or use MD_SA_TOKEN with --md-sa
.venv/bin/python scripts/120_review_queue_triage.py --md
```

**MotherDuck + service-account token precedence:**

```bash
export MD_SA_TOKEN='md_…'
.venv/bin/python scripts/120_review_queue_triage.py --md --md-sa
```

**Optional filters:**

```bash
.venv/bin/python scripts/120_review_queue_triage.py --md --run-label formalization_20260406_v3
```

**Local DuckDB file (offline / smoke):**

```bash
.venv/bin/python scripts/120_review_queue_triage.py --db-path thyroid_master.duckdb
```

**Custom export parent directory:**

```bash
.venv/bin/python scripts/120_review_queue_triage.py --md --output-root exports
```

Default `--output-root` is the repo `exports/` directory.

## Tests

Offline smoke: `python -m pytest tests/test_120_review_queue_triage.py` (in-memory and CLI `main()` path).

## Related

- Release gate: pending rows (`verification_status` NULL) block `scripts/119_md_formalization_validate.py --release-mode`.
- DB contract: [`docs/motherduck_database_contract_v1.md`](motherduck_database_contract_v1.md) (script inventory §9).
