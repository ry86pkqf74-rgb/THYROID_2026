# mig_241 Closeout — LN Safe-View Promotion to semantic_publication

**Date:** 2026-05-01  
**Agent:** Cline Sonnet 4.6  
**Migration file:** `qc_framework_v1/migrations/241_ln_safe_view_promotion_to_semantic_publication_20260501.sql`  
**Batch ID:** `mig_241_ln_safe_promotion`

---

## What was done

Promoted 3 LN-domain publication-safe views from `manuscript_workspace` (prototyping home, mig_224–229) into `semantic_publication` (analyst SSOT).  Each target view adds:
- `release_id` via `CROSS JOIN semantic_publication.release_manifest_v1`
- `research_id` cast to `VARCHAR` (mig_239 convention)

| Target view (semantic_publication) | Cols | Rows | Source (manuscript_workspace) |
|---|---|---|---|
| `vw_ln_patient_safe_VIEW_v1` | 10 | 4 008 | `vw_ln_patient_publication_safe_VIEW_v1` |
| `vw_ln_surgery_safe_VIEW_v1` | 11 | 4 008 | `vw_ln_surgery_publication_safe_VIEW_v1` |
| `vw_ln_histology_attribution_safe_VIEW_v1` | 75 | 5 918 | `vw_ln_histology_attribution_VIEW_v1` |

Source views in `manuscript_workspace` were **left in place** (do not drop — still consumed by Lane M Methods SQL).

Column registrations: 10 + 11 + 75 = **96 rows** inserted into `main.canonical_column_verification_registry_v1`.  
Signoff registrations: **3 rows** inserted into `main.canonical_table_signoff_registry_v1`.

---

## Verification results

| Check | Result |
|---|---|
| `vw_ln_patient_safe_VIEW_v1` rows | ✅ 4 008 |
| `vw_ln_surgery_safe_VIEW_v1` rows | ✅ 4 008 |
| `vw_ln_histology_attribution_safe_VIEW_v1` rows | ✅ 5 918 |
| gate1_verified_tables | ✅ **217** (was 214, +3) |
| gate2_missing_signoff | ✅ 0 |
| gate3_count_mismatch | ✅ 0 |
| gate4_verified_cols_missing_metadata | ✅ 0 |
| gate5_clinical_date_violations | ✅ 0 |
| cohort_parity_ok | ✅ True |

---

## Notes / carry-forwards

- `gate1_distinct_objects` also equals 217 (matches `gate1_verified_tables`).
- No changes to `vw_publication_qc_status_VIEW_v1` definition needed — gate1 query already counts the new semantic_publication views automatically via `duckdb_views()` filter.
- **Next migration in the v17 round:** mig_242 (if queued) or whatever the dispatcher sends next.
