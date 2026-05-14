# CPM rebuild — Phase 1 BigQuery validation (read-only)

**Date:** 2026-05-14  
**Project:** `thyroid-canonical-pub-2026`  
**Purpose:** Evidence for Phase 1.5 lineage pinning (`studies/cpm_bq_native_rebuild_phase1_plan_20260514.md`).

## 1. Row invariants

| Table | Rows |
|-------|-----:|
| `pub_workspace.patient_analysis_resolved_v1` | 10,871 |
| `pub_archive.canonical_patient_master_base_archived_20260514` | 10,871 |
| `pub_archive.canonical_patient_master_v1_8_archived_20260514` | 10,871 |
| `pub_canonical.canonical_patient_master` (live) | 10,871 (per integration invariants) |

## 2. Column counts

| Object | Columns |
|--------|--------:|
| `pub_workspace.patient_analysis_resolved_v1` | 146 |
| `pub_canonical.canonical_patient_master` | 2,314 |
| `pub_archive.canonical_patient_master_base_archived_20260514` | 1,663 |
| `pub_archive.canonical_patient_master_v1_2_archived_20260514` | 1,749 |
| `pub_archive.canonical_patient_master_v1_3_archived_20260514` | 1,967 |
| `pub_archive.canonical_patient_master_v1_4_archived_20260514` | 2,110 |
| `pub_archive.canonical_patient_master_v1_5_archived_20260514` | 2,153 |
| `pub_archive.canonical_patient_master_v1_6_archived_20260514` | 2,233 |
| `pub_archive.canonical_patient_master_v1_7_archived_20260514` | 2,237 |
| `pub_archive.canonical_patient_master_v1_8_archived_20260514` | 2,314 |

**Side snapshot (same width as v1_8, not a ladder step):**  
`pub_archive.canonical_patient_master_pre_workup_census_merge_20260514` — 2,314 cols (merge QA / census context; do not confuse with assembly base).

## 3. PAR vs CPM — exact name parity

**Method:** `INFORMATION_SCHEMA.COLUMNS` set difference on `column_name`.

- **Columns only in `patient_analysis_resolved_v1` (not in live CPM):** **6**  
  `imaging_tirads_best`, `imaging_tirads_worst`, `imaging_tirads_category`, `imaging_tirads_source`, `path_multifocal_flag`, `path_n_tumors`
- **Implication:** **140 / 146** PAR columns share **identical names** with CPM (`146 - 6 = 140`).

**Reproduction (BigQuery SQL):**

```sql
WITH par AS (
  SELECT column_name
  FROM `thyroid-canonical-pub-2026.pub_workspace.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'patient_analysis_resolved_v1' AND table_schema = 'pub_workspace'
),
cpm AS (
  SELECT column_name
  FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'canonical_patient_master' AND table_schema = 'pub_canonical'
)
SELECT column_name
FROM par
EXCEPT DISTINCT
SELECT column_name FROM cpm
ORDER BY 1;
```

## 4. Assembly gap (no intermediate archive)

The first **wide** parity checkpoint in `pub_archive` after the resolved spine is **`canonical_patient_master_base_archived_20260514`** (1,663 columns).  
There is **no** archived table for the intermediate MotherDuck steps **204–217** individually — Phase 2 should materialize **`cpm_stage_asm_*`** scratch tables (or new dated `pub_archive` snapshots) per `studies/cpm_bq_native_rebuild_phase1_dag_20260514.json`.

## 5. Ladder deltas (column-additive)

| Step | Archive table | Δ vs prior |
|------|---------------|-----------:|
| PAR (S2) | `pub_workspace.patient_analysis_resolved_v1` | — (146) |
| Assembly cap | `pub_archive.canonical_patient_master_base_archived_20260514` | **+1,517** vs PAR (1,663 − 146) |
| v1_2 | `..._v1_2_archived_20260514` | +86 |
| v1_3 | `..._v1_3_archived_20260514` | +218 |
| v1_4 | `..._v1_4_archived_20260514` | +143 |
| v1_5 | `..._v1_5_archived_20260514` | +43 |
| v1_6 | `..._v1_6_archived_20260514` | +80 |
| v1_7 | `..._v1_7_archived_20260514` | +4 |
| v1_8 / live | `..._v1_8_archived_20260514` | +77 |

---

*Machine-readable DAG: `studies/cpm_bq_native_rebuild_phase1_dag_20260514.json`.*
