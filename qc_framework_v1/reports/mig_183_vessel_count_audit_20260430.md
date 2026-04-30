# mig_183 PM vessel_count last not_started audit

**Date:** 2026-04-30  
**Batch:** `mig_183_pm_vessel_count_verify_apply_20260430`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Posture:** read-only audit + SQL authoring only; no MotherDuck DDL/DML executed by this handoff.  
**Deliverable SQL:** `qc_framework_v1/migrations/183_pm_vessel_count_verify_apply_20260430.sql`

## 1. Lineage discovery

Target column: `main.canonical_patient_master.vessel_count`.

Narrow repo grep across `scripts/`, `qc_framework_v1/migrations/`, and `cursor_prompts/` found the original canonical master assembly/consolidation lineage:

- `scripts/frozen/204_canonical_master_assembly.py` maps `g.vascular_vessel_count AS vessel_count` in the pathology tumor feature section.
- `scripts/frozen/205_canonical_consolidation.py` repeats the same mapping: `g.vascular_vessel_count AS vessel_count`.
- `scripts/230_path_synoptic_rollup.sql` derives the parallel vascular scalar as `MAX(tn.vi_count_vessels) AS vi_vessels_max`.
- `scripts/231_update_canonical_master.sql` carries `r.vi_vessels_max` into the canonical master update lane.
- `qc_framework_v1/migrations/154_patient_master_pathology_invasion_cluster_signoff_20260429.sql` verified the related vascular family (`vasc_vessel_count_v13`, `vascular_vessel_count`, `vi_vessels_max`) against the canonical invasion/pathology rollup lineage but did not include bare `vessel_count` in its 38-column allow-list.

Conclusion: `vessel_count` is a legacy/bare alias of the same vascular vessel-count measurement spine, not an independent source and not a boolean/presence helper.

## 2. Live read-only probe results

Read-only probes were run through `scripts._md_connect.connect_locked()` against `thyroid_canonical_publication_v1_0`. CPM invariants were enforced by the connection helper before queries resolved.

### 2.1 Registry pre-state

| column_name | data_type | ordinal_position | verification_status | batch_id | verification_method | verified_by | notes |
|---|---:|---:|---|---|---|---|---|
| `vessel_count` | DOUBLE | 965 | not_started | NULL | NULL | NULL | NULL |

### 2.2 PM registry status counts

| verification_status | n_cols |
|---|---:|
| na | 24 |
| not_started | 1 |
| verified | 1590 |

### 2.3 `vessel_count` distribution

| vessel_count | n_patients |
|---:|---:|
| NULL | 10,825 |
| 1.0 | 20 |
| 2.0 | 14 |
| 3.0 | 7 |
| 4.0 | 2 |
| 5.0 | 1 |
| 6.0 | 2 |

Total populated: 46 / 10,871 patients (0.42%).

## 3. Pairwise correspondence with parallel VI columns

### 3.1 Non-nullness pattern

| vessel_count non-null | vasc_vessel_count_v13 non-null | vascular_vessel_count non-null | vi_vessels_max non-null | n_patients |
|---|---|---|---|---:|
| FALSE | FALSE | FALSE | FALSE | 10,825 |
| TRUE | TRUE | TRUE | TRUE | 46 |

### 3.2 Equality on all populated rows

| n_all_nonnull | matches vasc_vessel_count_v13 | matches vascular_vessel_count | matches vi_vessels_max | diffs v13 | diffs vascular_vessel_count | diffs vi_vessels_max |
|---:|---:|---:|---:|---:|---:|---:|
| 46 | 46 | 46 | 46 | 0 | 0 | 0 |

### 3.3 Deterministic spot-check

| research_id | vessel_count | vasc_vessel_count_v13 | vascular_vessel_count | vi_vessels_max | vi_any_present_path | vasc_grade | vasc_grade_final_v13 | vascular_who_2022_grade |
|---:|---:|---:|---:|---:|---|---|---|---|
| 10273 | 4.0 | 4.0 | 4.0 | 4.0 | TRUE | extensive | extensive | extensive (>=4 vessels) |
| 10306 | 1.0 | 1.0 | 1.0 | 1.0 | TRUE | focal | focal | focal (<4 vessels) |
| 10360 | 2.0 | 2.0 | 2.0 | 2.0 | TRUE | focal | focal | focal (<4 vessels) |
| 10503 | 2.0 | 2.0 | 2.0 | 2.0 | TRUE | focal | focal | focal (<4 vessels) |
| 10554 | 2.0 | 2.0 | 2.0 | 2.0 | TRUE | focal | focal | focal (<4 vessels) |
| 10646 | 1.0 | 1.0 | 1.0 | 1.0 | TRUE | focal | focal | focal (<4 vessels) |
| 11107 | 3.0 | 3.0 | 3.0 | 3.0 | TRUE | focal | focal | focal (<4 vessels) |
| 11142 | 4.0 | 4.0 | 4.0 | 4.0 | TRUE | extensive | extensive | extensive (>=4 vessels) |
| 11170 | 3.0 | 3.0 | 3.0 | 3.0 | TRUE | focal | focal | focal (<4 vessels) |
| 11450 | 1.0 | 1.0 | 1.0 | 1.0 | TRUE | focal | focal | focal (<4 vessels) |

## 4. Cohort-uniformity classification

`vessel_count` is neither:

- Type-A near-uniform TRUE/FALSE flag, nor
- Type-B placeholder / degenerate helper.

It is a sparse multi-valued numeric measurement with 6 observed positive values and exact equality to the already-verified vascular vessel-count spine. Its shape and values match `vasc_vessel_count_v13`, `vascular_vessel_count`, and `vi_vessels_max` exactly.

## 5. Disposition and rationale

Disposition: **verified**.

Chosen verification method: `derivation_vs_vascular_vessel_count`.

Rationale:

1. Build lineage explicitly maps `g.vascular_vessel_count AS vessel_count` in both frozen canonical master assembly/consolidation scripts.
2. Live CPM data shows identical non-nullness and exact equality between `vessel_count` and `vascular_vessel_count` on all 46 populated rows.
3. The parallel verified columns `vasc_vessel_count_v13` and `vi_vessels_max` also match exactly on all populated rows.
4. The column is not a superseded empty placeholder; it carries a real sparse vascular vessel-count measurement.

## 6. Expected post-state after Cowork Path-C apply

Expected PM registry state after executing `qc_framework_v1/migrations/183_pm_vessel_count_verify_apply_20260430.sql`:

| verification_status | expected n_cols |
|---|---:|
| na | 24 |
| not_started | 0 |
| verified | 1591 |

Expected `canonical_table_signoff_registry_v1` row for `main.canonical_patient_master`:

| table_status | n_columns_total | n_verified | n_not_started | n_failed | n_na | signoff_migration |
|---|---:|---:|---:|---:|---:|---|
| verified | 1615 | 1591 | 0 | 0 | 24 | `qc_framework_v1/migrations/183_pm_vessel_count_verify_apply_20260430.sql` |

This drops the final PM `not_started` count from 1 to 0 and makes PM eligible for the next-priority `mig_162` PM finalization + lakehouse coverage report lane.

## 7. Governance boundary

No MotherDuck mutation was executed in this session. The authored SQL is a Path-C apply artifact for Cowork/governed execution.