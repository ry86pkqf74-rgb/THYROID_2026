# Final Operative NLP Propagation Sync — 20260314

**Script**: `scripts/86_operative_nlp_final_sync.py`  
**Fix Script**: `scripts/_tmp_fix_episode_update.py` (composite-join episode UPDATE)  
**Date**: 2026-03-15  
**Status**: COMPLETE

---

## Root Cause

`scripts/48_build_analysis_resolved_layer.py` was authored before `scripts/71_enrich_operative_v2.py` 
enriched `operative_episode_detail_v2` with 7 new NLP fields. The `EPISODE_RESOLVED_SQL` CTE 
only propagated 5 of 12 operative fields. The 7 missing fields were never written to 
`episode_analysis_resolved_v1`, and their absence cascaded to the patient and manuscript layers.

### Secondary Bugs Discovered and Fixed

| Bug | Symptom | Fix |
|-----|---------|-----|
| `safe_exec` treated DDL rowcount=-1 as failure | ALTER TABLE "FAILED" but columns actually added | `return 0 if rc == -1 else rc` |
| Episode UPDATE used single-field join on `surgery_episode_id` | 89.7M cartesian product, no rows matched | Composite key: `(research_id, surgery_episode_id)` |
| UPDATE SET used `COALESCE(o.col, e.col, FALSE)` (self-reference) | DuckDB error on self-referencing target table | `COALESCE(o.col, FALSE)` only |
| `build_patient_update_sql` used `p.col = ...` in SET clause | DuckDB binder error on qualified column in SET | Bare column names: `col = agg.col` |

---

## Source-Limited Field

**`esophageal_involvement_flag`** = 0 (confirmed source-limited, not a bug)

Checked 10 op notes matching `esophag%invaded%` — all are anatomical references 
(tracheoesophageal groove, dilator placement, intact/preserved language). Zero entity 
records in `note_entities_procedures` for esophageal invasion type. No re-engineering 
of the extractor warranted given 0% signal.

---

## Before / After Coverage

### episode_analysis_resolved_v1 (9,575 rows)

| Field | Before | After | Status |
|-------|--------|-------|--------|
| parathyroid_autograft_flag | 0 | 40 | FIXED |
| local_invasion_flag | 0 | 25 | FIXED |
| tracheal_involvement_flag | 0 | 9 | FIXED |
| esophageal_involvement_flag | 0 | 0 | SOURCE_LIMITED |
| strap_muscle_involvement_flag | 0 | 186 | FIXED |
| reoperative_field_flag | 0 | 49 | FIXED |
| operative_findings_raw | 0 | 594 | FIXED |

### patient_analysis_resolved_v1 (10,871 rows)

| Field | After | Status |
|-------|-------|--------|
| op_rln_monitoring_any | 1701 | NEW |
| op_drain_placed_any | 169 | NEW |
| op_strap_muscle_any | 186 | NEW |
| op_reoperative_any | 46 | NEW |
| op_parathyroid_autograft_any | 40 | NEW |
| op_local_invasion_any | 25 | NEW |
| op_tracheal_inv_any | 9 | NEW |
| op_esophageal_inv_any | 0 | SOURCE_LIMITED |
| op_intraop_gross_ete_any | 22 | NEW |
| op_n_surgeries_with_findings | 587 | NEW |

### manuscript_cohort_v1 (10,871 rows)

| Field | After | Status |
|-------|-------|--------|
| op_rln_monitoring_any | 1701 | NEW |
| op_strap_muscle_any | 186 | NEW |
| op_parathyroid_autograft_any | 40 | NEW |
| … (8 more op_ fields) | see val table | NEW |

---

## Tables Modified / Created

| Table | Change |
|-------|--------|
| `episode_analysis_resolved_v1` | +7 columns, backfilled via composite join |
| `episode_analysis_resolved_v1_dedup` | Rebuilt (9,368 rows, 0 dup groups) |
| `patient_analysis_resolved_v1` | +11 op_* columns, backfilled |
| `manuscript_cohort_v1` | +11 op_* columns, backfilled |
| `md_episode_analysis_resolved_v1_dedup` | Mirror rebuilt |
| `md_patient_analysis_resolved_v1` | Mirror rebuilt |
| `md_manuscript_cohort_v1` | Mirror rebuilt |
| `val_operative_nlp_final_sync_v1` | Created (28 rows) |

---

## Manuscript Impact

No manuscript-facing counts changed. All new fields are additive operative NLP columns 
that were previously absent (BEFORE count = 0, columns did not exist). The dedup table 
row count remains 9,368 (unchanged). The manuscript cohort row count remains 10,871 (unchanged).

---

## Validation

`val_operative_nlp_final_sync_v1` table on MotherDuck (28 rows):
- All FIXED/NEW fields show before=0, after>0
- SOURCE_LIMITED fields show before=0, after=0 with documented justification
- Export: `exports/final_operative_nlp_sync_20260314/val_operative_nlp_final_sync_v1.csv`
