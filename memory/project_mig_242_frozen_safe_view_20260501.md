# mig_242 — `semantic_publication.vw_frozen_section_safe_VIEW_v1` (2026-05-01)

**Batch:** `mig_242_frozen_safe` · **verified_by:** `cursor_composer_mig_242`  
**Migration file:** `qc_framework_v1/migrations/242_vw_frozen_section_safe_VIEW_v1_20260501.sql`

## Rationale

`main.canonical_frozen_section_patient_rollup_v1` has **188 columns** (12 wide slots × detailed fields + aggregates). Analysts and manuscript workflows need a **compact patient-level summary** in `semantic_publication`, not a repeat of every `frozen_N_date` / `frozen_N_result_*` column.

## Columns included (10)

| # | Output column | Source / definition |
|---|---------------|---------------------|
| 1 | `release_id` | `CROSS JOIN semantic_publication.release_manifest_v1` |
| 2 | `research_id` | `CAST(research_id AS VARCHAR)` |
| 3 | `any_frozen_section_performed` | `frozen_section_any_performed_flag` |
| 4 | `any_frozen_malignant` | `frozen_section_any_malignant_flag` |
| 5 | `any_frozen_deferred` | `frozen_section_any_deferred_flag` |
| 6 | `n_frozen_events` | `frozen_section_count` |
| 7 | `frozen_section_first_date` | aggregate first date (DATE post mig_160) |
| 8 | `frozen_section_last_date` | aggregate last date (DATE post mig_160) |
| 9 | `any_frozen_suspected` | `frozen_section_any_suspected_flag` |
| 10 | `any_frozen_excel_corroborated` | OR of `frozen_1..12_excel_corroborated_flag` |

## Excluded (178 rollup columns)

- All **12 slot blocks**: `frozen_N_yn`, `frozen_N_date`, `frozen_N_location`, `frozen_N_result_*`, `frozen_N_was_*`, `frozen_N_source_of_data`, `frozen_N_excel_*`, `frozen_N_surgery_n`.
- Rationale: slot-level stays in the canonical rollup; safe view is for cohort/tabulation and Methods-facing aggregates only.

## Acceptance

- View row count **4,116** (matches rollup).
- Registry: `canonical_table_signoff_registry_v1` + `canonical_column_verification_registry_v1` updated; `gate1_verified_tables` +1; gates 2–5 unchanged; `cohort_parity_ok` TRUE.
