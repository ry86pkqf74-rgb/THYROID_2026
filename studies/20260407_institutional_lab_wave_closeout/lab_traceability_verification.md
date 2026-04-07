# Traceability — `final_institutional_20260407`

Verified on MotherDuck after `127_analyst_institutional_lab_append.py` commit path.

## Wave filter

`WHERE ingestion_wave = 'final_institutional_20260407'` → **989** rows, **629** distinct patients.

## Governed columns

| Check | Result |
|-------|--------|
| `research_id` non-null | 989 / 989 |
| `lab_date` non-null | 989 / 989 |
| `lab_name_standardized` non-null / non-blank | 989 / 989 |
| `source_table` non-null / non-blank | 989 / 989 (values from CSV, e.g. `canonical_extracted_fact_long_v2`, `extracted_postop_labs_expanded_v1`) |
| `ingestion_wave` recorded | 989 / 989 = `final_institutional_20260407` |
| `source_script` | `127_analyst_institutional_lab_append.py` (set by script) |

## `source_lineage_key` / provenance

- CSV `source_lineage_key` is required unique per row.
- Script 127 encodes it into `provenance_note` as `lineage_key=<key>` (and preserves CSV `provenance_note` text after `|` when present).
- Post-ingest: **989 / 989** rows have `provenance_note` starting with `lineage_key=`.

This preserves deterministic identity `(research_id, lab_date, lab_name_standardized, source_lineage_key)` at the CSV level; the table stores lineage in `provenance_note` plus stable `source_lineage_key` semantics via that note (governed append path).

## Manual review queue (MRQ) posture

Live `qa.manual_review_queue` (prior to final-master rerun) showed **no** `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` statuses; only `auto_accepted_*`, `auto_accepted_critical_sample_ok`, `auto_accepted_informational`, and `confirmed_correct`, with **0** NULL `verification_status`. This satisfies the “MRQ non-synthetic / non-placeholder” gate for proceeding to evidence refresh.

## Formalization gate

`scripts/119_md_formalization_validate.py --md --md-sa --release-mode` as executed from `126_final_master_release.py`:** 26 PASS / 1 WARN / 0 FAIL** (specimen-adjacent review burden WARN only). Log: `studies/20260411_final_master_release/validation_run/validation_report.md`.
