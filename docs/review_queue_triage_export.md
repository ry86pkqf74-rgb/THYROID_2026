# Manual review queue triage export (script 120)

Read-only bundle for **`qa.manual_review_queue`**: CSVs plus **`summary.md`**, under a **timestamped** folder `exports/review_queue_triage_<UTC_YYYYMMDD_HHMMSS>/`.

- **No writes** to MotherDuck or local DuckDB (SELECT only).
- **No raw clinical note text**: `review_reason` is **omitted** from reviewer worklists; `entity_value_norm`, `reviewer_comment`, and `reviewer_evidence_span` are **truncated** in worklists (see `scripts/120_review_queue_triage.py`).
- **Tokens**: Same resolution as other `--md` scripts — `MOTHERDUCK_TOKEN`, `MD_SA_TOKEN` (with `--md-sa`), repo-root **`motherduck.local.toml`** (gitignored; copy from [`motherduck.local.toml.example`](../motherduck.local.toml.example)), `.streamlit/secrets.toml`, or `.env.motherduck`. Precedence: env vars → `LOCAL_DB_PATH` JWT guard → TOML → secrets. See [`docs/motherduck_database_contract_v1.md`](motherduck_database_contract_v1.md) §8.

## Outputs (per run)

| Artifact | Purpose |
|----------|---------|
| `summary.md` | Headline counts, file manifest, top domains |
| `counts_*.csv` | Domain / verification / blocking breakdown |
| `counts_manuscript_quality_tiers.csv` | Pending vs **synthetic placeholder** vs `auto_accepted*` vs human reviewer identity vs other reviewed |
| `counts_mrq_three_bucket_signoff.csv` | **Governance rollup:** `unresolved_pending` / `synthetic_automation_only` / `true_human_reviewed` / `automation_tier_or_incomplete_non_human` |
| `oldest_pending_rows.csv` | Stale pending rows (no free-text note fields) |
| `worklists/worklist__<domain>__tier_<tier>.csv` | Pending rows only, split for reviewers |

## Commands

**MotherDuck (read/write attach — promotion/staging identity; not read-scaling-only):**

```bash
export MOTHERDUCK_TOKEN='md_…'   # or use MD_SA_TOKEN with --md-sa
.venv/bin/python scripts/120_review_queue_triage.py --md
```

**MotherDuck read-scaling (least privilege — `MD_READ_SCALING_TOKEN` only; run `136 … reader` after writer snapshot):**

```bash
export MD_READ_SCALING_TOKEN='md_…'
# optional: MD_READ_SCALING_SESSION_HINT=reviewer_bundle_01
.venv/bin/python scripts/120_review_queue_triage.py --read-scaling --output-root exports
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

## Human-reviewed MRQ hydration (publication path)

**Do not** use rehearsal CSVs with `SYNTHETIC_AUTOMATION_ONLY_*` statuses for a publication hydrate.

1. Complete **`manual_review_queue.csv`** in a gate directory (same column names as promotion gate output; `114` accepts aliases per [`scripts/114_qa_schema_setup.py`](../scripts/114_qa_schema_setup.py) `col_map`).
2. **Replace / hydrate into MotherDuck** (destructive to `qa.manual_review_queue` for that batch — prefer **dev/qa catalog** first per [`docs/motherduck_sandbox_clone_runbook.md`](motherduck_sandbox_clone_runbook.md)):
   - [`scripts/114_qa_schema_setup.py`](../scripts/114_qa_schema_setup.py) `--md --hydrate-from <gate_dir>` — merges scorecard + **`hydrate_manual_review_queue`** (DELETE by `run_label` then INSERT).
   - [`scripts/126_final_master_release.py`](../scripts/126_final_master_release.py) `--md --hydrate-mrq-from <reviewed_dir>` — orchestrated final-master path; calls CSV preflight `assert_mrq_csv_fully_reviewed` (blocks synthetic placeholders when `--release-mode`).
3. Re-run **`scripts/119_md_formalization_validate.py --md --release-mode`** to confirm CHECK 5 / 5b and downstream gates.

Provenance: `reviewer`, `reviewed_at`, `verification_status` columns on `qa.manual_review_queue` are loaded from CSV; `114` does not overwrite unrelated columns. Full-catalog **126** replace clears the table before hydrate — see `126` docstring.

## Specimen / genomic review burden (script 151)

Read-only export for **`qa.specimen_genomic_link_review_v1`** and **`qa.v_diag_specimen_review_burden_v1`** (no raw note text; truncates long text fields). Batched CSVs by linkage tier × `review_status` × `source_table` × age bucket.

**Connection modes:** same three-way split as script **120** and **141**: `--md` (RW token, fail-closed), `--read-scaling` (`MD_READ_SCALING_TOKEN` only), or neither (local `--db-path`). **Do not** pass `--md` and `--read-scaling` together.

**Env (read-scaling):** `MD_READ_SCALING_TOKEN` (required); optional `MD_READ_SCALING_SESSION_HINT` or `MOTHERDUCK_SESSION_HINT` / `--session-hint`; `MOTHERDUCK_CUSTOM_USER_AGENT` overrides the default exporter UA. Tokens resolve via `motherduck_client.get_read_scaling_token()` and gitignored `motherduck.local.toml` / `.streamlit/secrets.toml` per [`docs/motherduck_database_contract_v1.md`](motherduck_database_contract_v1.md) §8.

**After a writer snapshot:** reviewers must run **`REFRESH DATABASE`** on the read-scaling connection (or `scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod`) before `--read-scaling` exports see fresh data.

```bash
.venv/bin/python scripts/151_specimen_genomic_review_queue_export.py --md --output-root exports
export MD_READ_SCALING_TOKEN='md_…'   # read-scaling only
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod
.venv/bin/python scripts/151_specimen_genomic_review_queue_export.py --read-scaling --output-root exports
```

Output: `exports/specimen_genomic_review_<UTC_YYYYMMDD_HHMMSS>/` with `summary.md`, burden CSV, full detail CSV, and `worklists/*.csv`. Tests: `tests/test_151_specimen_genomic_review_export.py` (offline; CI job **`llm-extraction-gold`**).

## Related

- Release gate: pending rows (`verification_status` NULL) block `scripts/119_md_formalization_validate.py --release-mode`.
- Publication governance (synthetic vs manuscript): [`docs/publication_governance_gate.md`](publication_governance_gate.md).
- DB contract: [`docs/motherduck_database_contract_v1.md`](motherduck_database_contract_v1.md) (script inventory §9).
