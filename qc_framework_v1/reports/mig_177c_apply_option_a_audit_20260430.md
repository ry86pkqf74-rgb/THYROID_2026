# mig_177c_apply — Option A clear-only authoring for LVI/VI derivative flippers

**Date:** 2026-04-30  
**Batch:** `mig_177c_apply_option_a_clear_only_20260430`  
**Migration artifact:** `qc_framework_v1/migrations/177c_apply_option_a_clear_only_20260430.sql`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Posture:** Path-C apply SQL authored; no MotherDuck DDL/DML executed by this authoring session.  
**Predecessor:** mig_177c read-only scoping at commit `7210f80`.

## 1. Flipper re-confirmation against pre-snapshot

Read-only live probes confirmed the mig_177b pre-snapshot exists as:

`"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_20260429`

| family | pre true | post true | TRUE→FALSE flippers | FALSE/NULL→TRUE flippers |
|---|---:|---:|---:|---:|
| LVI (`lvi_any_present_path`) | 3,392 | 989 | 2,502 | 99 |
| VI (`vi_any_present_path`) | 3,698 | 1,178 | 2,580 | 60 |

CPM pre-flight invariants from the same read-only probe: 10,871 rows, 10,871 distinct `research_id`, and 0 null `cpm_built_at` values.

## 2. Pre-snapshot plan

The migration creates this mandatory pre-snapshot before any update:

`"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_derivatives_pre_mig177c_apply_20260430`

The snapshot is `CREATE TABLE IF NOT EXISTS` and captures the LVI/VI boolean flags, all 15 derivative fields, current `cpm_built_at`, and a snapshot timestamp for every patient with any non-null/non-zero derivative value in the scoped derivative families.

The table did not exist during the read-only authoring probe (`snapshot_exists=0`), so the Path-C apply will preserve the true pre-apply state on first execution.

## 3. Per-column cells to clear

Expected Option A clear-only impact from mig_177c scoping:

| family | column | action | non-null/non-zero cells on TRUE→FALSE flippers |
|---|---|---|---:|
| LVI | `lvi_grade` | `NULL` | 2,460 |
| LVI | `lvi_ordinal_worst` | `NULL` | 2,502 |
| LVI | `n_tumors_lvi_present` | `0` | 2,502 |
| VI | `vasc_grade` | `NULL` | 2,580 |
| VI | `vasc_grade_final_v13` | `NULL` | 2,579 |
| VI | `vascular_invasion_final` | `NULL` | 2,579 |
| VI | `vascular_invasion_grade` | `NULL` | 2,579 |
| VI | `vascular_who_2022_grade` | `NULL` | 0 |
| VI | `vi_ordinal_worst` | `NULL` | 2,580 |
| VI | `vasc_vessel_count_v13` | `NULL` | 0 |
| VI | `vascular_vessel_count` | `NULL` | 0 |
| VI | `vi_vessels_max` | `NULL` | 0 |
| VI | `vasc_confidence_final_v13` | `NULL` | 2,579 |
| VI | `vasc_source_final_v13` | `NULL` | 2,579 |
| VI | `n_tumors_vi_present` | `0` | 2,580 |

Expected totals:

| family | flippers | derivative columns | cells cleared |
|---|---:|---:|---:|
| LVI | 2,502 | 3 | 7,464 |
| VI | 2,580 | 12 | 20,635 |
| **Total** | **5,082** | **15** | **28,099** |

The update also refreshes `cpm_built_at` for touched rows, consistent with the CPM build-provenance convention.

## 4. Post-state probes embedded in SQL

The SQL artifact includes post-state verification probes that must pass after Path-C execution:

- `pm_total = 10,871`
- `pm_distinct_rids = 10,871`
- `null_cpm_built_at = 0`
- all 15 flipper-scope residual derivative counts = `0`
- one provenance row for `canonical_cleanup_mig177c_apply_option_a_clear_only_20260430`

This authoring session did not execute the write SQL, per the prompt governance boundary. The expected post-state is therefore encoded as executable checks rather than reported as live post-mutation results.

## 5. Registry and carry-forward notes

The SQL appends an idempotent note to the 15 scoped PM registry rows:

- Closes `CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN` via Option A clear-only on 5,082 flippers.
- Opens `CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS` for 99 LVI + 60 VI FALSE/NULL→TRUE patients that now lack derivatives.

The note update is appendix-only; it does not flip verification statuses.

## 6. Provenance row

The SQL inserts a single idempotent provenance row:

| field | value |
|---|---|
| `run_id` | `canonical_cleanup_mig177c_apply_option_a_clear_only_20260430` |
| `phases_applied` | `pre_snapshot_lvi_clear_vi_clear_post_state_probe_registry_notes` |
| `critical_findings_cleared` | `CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN` |
| `held_for_adjudication` | `CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS` |

## 7. Next lane

`CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS` should move to a future Option B lane after `canonical_invasion_events_v1` is extended with ratified grade/count lineage columns. Until then, the 99 LVI + 60 VI new TRUE flippers are intentionally left unmodified by this clear-only migration.

## Acceptance summary

**Status:** SQL authored for Path-C apply; read-only flipper reconfirmation PASS.

The migration artifact implements the ratified minimal-blast-radius cleanup and keeps the broader derivative rederive explicitly out of scope.