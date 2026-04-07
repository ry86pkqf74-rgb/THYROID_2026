# Final master database contract (manuscript-safe)

This document defines the **analyst-facing** presentation layer and export rules for the post-review **final master** release on MotherDuck. It complements [`motherduck_database_contract_v1.md`](motherduck_database_contract_v1.md).

## Preconditions

1. Every row in `qa.manual_review_queue` for the promotion run has a non-null `verification_status` (enforced by `scripts/119_md_formalization_validate.py --release-mode`).
2. Optional append-only batch in `qa.promotion_review_decisions` records adjudication history (`decision_batch_id`, `source_object_id`, `evidence_ref`).
3. Final institutional lab file ingested with `scripts/127_analyst_institutional_lab_append.py` **after** `scripts/117_md_contract_views.py` so baseline parquets do not overwrite the lab wave.

## Orchestrator

Run (fail-closed, no silent local fallback):

```bash
.venv/bin/python scripts/126_final_master_release.py --md --release-date YYYYMMDD \
  --hydrate-mrq-from studies/<gate_run_dir> \
  [--decisions-csv studies/<date>_final_master_release/promotion_review_decisions.csv] \
  [--lab-csv exports/incoming/final_lab_YYYYMMDD.csv --ingestion-wave final_institutional_YYYYMMDD]
```

## Core objects (re-materialized)

| Object | Role |
|--------|------|
| `main.canonical_extracted_fact_long_v2` | One row per promoted extracted fact; includes `research_id`, `note_row_id`, domain, entity fields, linkage, provenance |
| `main.canonical_fact_quarantine_v2` | Quarantined / non-promoted facts |
| `main.note_extraction_runs` | Extraction run registry (`run_id`, model, timestamps) |
| `main.longitudinal_lab_canonical_v1` | Append-only long-format labs (multiple `ingestion_wave` values) |
| `main.longitudinal_lab_deduped_v` | Deterministic dedup across waves for analysis |
| `main.master_fact_long_verified_v1` | Analyst surface: facts + latest reviewer status + `release_tag` |
| `main.master_patient_rollup_verified_v1` | Per-patient rollup over verified facts |
| `main.master_source_lineage_v1` | Provenance chain: extraction run → fact → reviewer → release |

### Traceability

Every row in `master_fact_long_verified_v1` must be joinable to:

- `research_id`
- `source_object_id` (= `note_row_id` in canonical facts)
- `extraction_run_id` (from `note_extraction_runs`)

## Snapshot and exports

- **Immutable schema copy:** `scripts/115_release_snapshot.py --md --tag <YYYYMMDD> --final-master` creates `release_<tag>` with tables copied from `main` (including materialized snapshot of the three `master_*` views).
- **Curated parquet:** `scripts/118_parquet_release_bundle.py --md --tag <YYYYMMDD> --final-master` writes `exports/final_master_release_<tag>/` with facts, labs, lineage, and QA tables — **no raw note text columns**.

## Cloud artifacts

- Do not export clinical note bodies or PHI to cloud bundles.
- MotherDuck **named database snapshots** follow your team runbook (UI or API); record snapshot id in `studies/<date>_final_master_release/evidence_pack.json` when available.

## Evidence pack

After a successful run, `scripts/126_final_master_release.py` writes:

- `studies/<date>_final_master_release/EVIDENCE_PACK.md` — row counts, lineage summary, review queue status, source-limited caveats
- `studies/<date>_final_master_release/SAFE_TO_START_STATS_MEMO.md` — concise go/no-go for stats/manuscript work
- `studies/<date>_final_master_release/evidence_pack.json` — machine-readable metadata including `git_sha` and `release_tag`

## Related scripts

| Script | Purpose |
|--------|---------|
| `126_final_master_release.py` | End-to-end orchestration |
| `127_analyst_institutional_lab_append.py` | Deterministic institutional lab append + dedup view refresh |
| `125_master_verified_views.py` | Builds `master_*_verified_v1` views |
| `119_md_formalization_validate.py --release-mode` | Final promotion gate |
