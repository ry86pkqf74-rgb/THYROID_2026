# Extraction run ID hardening — traceability metrics

**Database:** MotherDuck `Thyroid 2026` (fail-closed). **No note text.**

## Root cause (summary)

1. **Fleet expansion:** `scripts/103_fact_lineage_materialize.py` exploded `result_json` into entity rows without copying `extraction_run_id` from the parent note row → ~31k `llm_v2_fleet` facts blank on canonical.
2. **Temporal join gap:** Facts with `extracted_at` before the first successful `note_extraction_runs.started_at` (~24.4k rows) could not resolve a run via `started_at <= extracted_at`, leaving canonical blank and master view partially blank.

## Resolution rules (code)

- **Materialization (`103`):** After concat, `utils/extraction_run_id_resolve.backfill_extraction_run_id_column` using `note_extraction_runs.parquet` (successful runs only when present; else all runs). Latest successful run with `started_at <= extracted_at`; if none, **earliest successful run** (pre-telemetry attribution).
- **MotherDuck backfill (`128`):** Same resolver over `main.canonical_extracted_fact_long_v2` + `main.note_extraction_runs` when cloud table must be fixed without a full local `103` run.
- **Master views (`125`):** `fact_core` CTE resolves `extraction_run_id` with the same two-step SQL; `release_tag` from `qa.release_manifest` ordered by `TRY_CAST(release_tag AS BIGINT) DESC` then `created_at`.

## Before / after (row aggregates)

| Metric | Before (2026-04-07 RC audit) | After |
|--------|-------------------------------|-------|
| Canonical NULL/blank `extraction_run_id` | 55,500 / 123,577 | 0 / 123,577 |
| `master_fact_long_verified_v1` NULL/blank `extraction_run_id` | 24,421 / 123,577 | 0 / 123,577 |
| Distinct `release_tag` on master facts | `20260406` | `20260409` |

## Validation

- `scripts/119_md_formalization_validate.py --md --release-mode` → **0 FAIL** (includes new Check 10).
- Evidence pack: this folder (`126` with `--out-dir`), plus `grain_note_row_id.md`.

## Operator order of execution

1. Keep `main.note_extraction_runs` current.
2. **Preferred (reproducible):** Run `103_fact_lineage_materialize.py` (with `note_extraction_runs.parquet` present) → upload/replace canonical; then `125_master_verified_views.py --md`.
3. **Cloud-only patch:** `128_canonical_extraction_run_id_backfill.py --md --apply` → `125_master_verified_views.py --md`.
4. Gate: `119_md_formalization_validate.py --md --release-mode`.
