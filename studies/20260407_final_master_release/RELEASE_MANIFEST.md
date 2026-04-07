# Governed release manifest — final master (2026-04-07)

## Authoritative snapshot

| Field | Value |
|-------|-------|
| **MotherDuck database** | `Thyroid 2026` |
| **Database engine (md_information_schema)** | `DUCKLAKE` |
| **Release tag (this cut)** | `20260407_final2` |
| **Immutable schema** | `release_20260407_final2` |
| **Snapshot script** | `scripts/115_release_snapshot.py --final-master` |
| **Parquet bundle (curated export)** | `exports/parquet_release_20260407_final2/` |
| **Git SHA at bundle time** | See `parquet_bundle_manifest.json` / `qa.release_manifest` |

## Tables copied into `release_20260407_final2`

All rows include column `release_tag = '20260407_final2'`.

- `canonical_extracted_fact_long_v1`, `canonical_extracted_fact_long_v2`
- `canonical_fact_quarantine_v1`, `canonical_fact_quarantine_v2`
- `thyroglobulin_lab_canonical_v1`
- `note_extraction_runs`
- `longitudinal_lab_canonical_v1`
- `master_fact_long_verified_v1`
- `master_patient_rollup_verified_v1`
- `master_source_lineage_v1`

Row counts are recorded in `qa.release_manifest` for tag `20260407_final2` and duplicated in `metrics_snapshot.json` in this folder.

## Superseded / informational snapshots

- `release_20260407_final` was created during an orchestrator pass where `qa.manual_review_queue` was briefly inconsistent with the RC sign-off bundle. **Do not use** that schema for manuscript sign-off; use `release_20260407_final2`.

## Prior releases

Earlier `release_*` schemas (`release_20260406`, `release_20260407`, `release_20260408`, `release_20260409`, …) remain in the database per append-only policy.

## Lab extract provenance

- **Structured EHR file:** `raw/Thyroid_Thyroglobulin_Lab_20251120.csv`
- **Ingestion:** `scripts/113_tg_lab_ingestion.py --md` (PII stripped; deterministic dedupe keys; parquet + MotherDuck)
- **QC artifact:** `processed/tg_lab_ingestion_qc_v1.json`
- **No raw note text** written to MotherDuck or parquet release bundles.
