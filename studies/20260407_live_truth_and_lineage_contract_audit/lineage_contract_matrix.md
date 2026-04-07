# Lineage contract matrix (machine-checkable)

**Catalog:** `Thyroid 2026` only (other attached MotherDuck databases ignored).  
**Generated:** see `audit_run_meta.json` and `run_lineage_audit.py`.  
**Full matrix:** [`lineage_contract_matrix.csv`](lineage_contract_matrix.csv) (208 objects incl. header row 209 lines in `wc`; use a CSV parser — fields may contain commas).

## Contract-critical surfaces (main)

Subset of rows from the CSV for objects named in `qa.release_manifest` for tag **20260409**, plus canonical spine and run registry.

| schema | object | class | rows (live) | overall | patient | origin | time | linkage | build/release |
|--------|--------|-------|------------:|:--------|:--------|:-------|:-----|:--------|:---------------|
| main | canonical_extracted_fact_long_v2 | row_level_fact_event | 123577 | PARTIAL | PASS | PASS (`note_row_id` + domain in wide canonical) | PARTIAL (sparse `entity_date`) | PARTIAL (join spine) | PASS (`extraction_run_id`) |
| main | canonical_fact_quarantine_v2 | row_level_fact_event | 199 | PARTIAL | PASS | PASS | PASS | PARTIAL | PASS |
| main | master_fact_long_verified_v1 | row_level_fact_event | 123577 | PARTIAL | PASS | PASS | PARTIAL | PARTIAL | PASS |
| main | master_source_lineage_v1 | row_level_fact_event | 123577 | PARTIAL | PASS | PASS | PARTIAL | PARTIAL | PASS |
| main | master_patient_rollup_verified_v1 | aggregate_rollup | 5574 | N/A | — | — | — | — | — |
| main | longitudinal_lab_canonical_v1 | row_level_lab | (see CSV) | PASS | PASS | PASS | PASS | PASS (lab exception) | PASS (`ingestion_wave` / ingest cols) |
| main | thyroglobulin_lab_canonical_v1 | row_level_lab | 76971 | PASS | PASS | PASS (`ingestion_script`) | PASS | PASS (lab exception) | PASS |
| main | note_extraction_runs | row_level_fact_event | 3 | PASS (exception) | run grain | git/domain metadata | PASS | N/A | PASS |
| main | canonical_extracted_fact_long_v1 | row_level_fact_event | (see CSV) | PARTIAL | PASS | PASS | PARTIAL | PARTIAL | PASS |

**Interpretation:** “PARTIAL” on the canonical/presentation path is dominated by **population** (nullable `entity_date`) and **episodes not denormalized on every fact row** (substitute: episode/linkage tables + `note_row_id`). This matches `scripts/119_md_formalization_validate.py --release-mode` expectations for core non-null presentation columns (see `119_release_validation/validation_report.md`).

## Rollup / control objects

- **Aggregates** (`*_summary_v`, `master_patient_rollup_verified_v1`, `longitudinal_lab_deduped_v`, etc.): **N/A** row-grain audit; traceability is defined **relative to underlying fact/lab tables**.
- **`qa.release_manifest`**: control metadata; `release_tag` + `created_at` + `tables_included` (not patient-row grain).

## FHIR export tables (`main.fhir_*` except de-id map)

Overall **PARTIAL (exception: FHIR export row)**: patient join via `patient_fhir_id` → `fhir_patient_deid_map_v1` / specimen spine; origin/linkage via `specimen_id` + JSON; time via `built_at`.

## Row-grain verdict histogram (all audited row-level objects)

Derived from `lineage_contract_matrix.csv`:

- **PASS:** 9
- **PASS (exception: run-registry grain):** 2 (`note_extraction_runs` in `main` + `release_20260409`)
- **PARTIAL:** 60
- **PARTIAL (exception: FHIR export row):** 4
- **FAIL:** 103 — overwhelmingly **legacy `main` analytic/staging tables** outside the release manifest projection (see CSV `remediation` / `gap_kind`).

For **release sign-off**, gate on the **manifest-named surfaces** + explicit exceptions above, not on every historical `main` table.

## Verdict (single sentence)

**PARTIALLY TRUE ONLY** — Full row-level traceability is **contract-true** for promoted canonical + presentation + lab surfaces **with documented join/substitute semantics**; it is **not** true that **every** row-grain object in `main` satisfies all five key families without exception or out-of-scope classification.
