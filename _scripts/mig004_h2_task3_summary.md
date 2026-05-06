# MIG-004 (H2 Task 3) — closeout summary

**Date:** 2026-05-06  
**migration_id:** `mig_082_mig004_vc_finding_source_20260506`  
**DFL:** `DFL-20260506-082` (Airtable Data Feedback Log)  
**Linear:** [THY-16](https://linear.app/rostemp/issue/THY-16) → **In Review** + label **`auto-close:pending`**

## BigQuery

| Artifact | Location |
|----------|----------|
| Staging table | `pub_workspace.canonical_patient_master_vc_source_v1` |
| New columns | `pub_canonical.canonical_patient_master`: `vc_finding_source_first`, `vc_finding_source_set`, `vc_finding_source_concordance` |

**SQL (committed):** `bq_migrations/mig_082_mig004_vc_finding_source_20260506.sql` (repo root copy mirrors Desktop migration folder).

## Dry-run (upper bound bytes processed)

| Step | Bytes |
|------|-------|
| Aggregate QC (`denom` / `any_signal` / `multi_source`) | 28,676 |
| `CREATE OR REPLACE` staging (CTAS) | 105,295 |
| `UPDATE` CPM ← staging (after columns exist) | 46,349,661 (~44 MB) |

## Validation counts (CPM-wide)

| Metric | Value |
|--------|-------|
| `denom` | 10,871 |
| `any_vc_signal` (≥1 source) | **2,914** |
| `multi_source` (≥2 sources) | **402** |

**`vc_finding_source_first` distribution:** none 7,957; operative_rln 2,062; mri_vocal_cords 438; laryngoscopy 338; nsqip_attribution 76.

**`vc_finding_source_concordance`:** none 7,957; single_source 2,512; concordant_multi 402.

## Schema adaptations (vs prompt prototype)

- `nsqip_rln_injury` in BQ is **STRING** (Yes-/No/Unknown text), not numeric: `has_nsqip` = `(nsqip_rln_injury_flag = 1) OR REGEXP_CONTAINS(LOWER(TRIM(injury)), r'^yes')`.
- `syn_io_rln_monitoring` is **BOOL** (not STRING): rolled into `has_op_rln` with `COALESCE(..., FALSE)`.
- `vc_finding_source_first` uses literal **`'none'`** when no source (COALESCE around scalar subquery) so the field matches the documented enum.

## Snowflake

`CALL VALIDATE_ALL_COHORTS()` **not run** in this session — Snowflake CLI returned **250001 PAT invalid**. Re-run after rotating a generic-scope PAT in `~/.snowflake/config.toml` (per `memory/skill_snowflake_cortex_2026_05_04.md`).

## Manuscript

`studies/hypothesis2_goiter_sdoh/H2_manuscript_v2_20260506.md` — **Limitations item 10** updated to describe landed columns and v3 optional stratification.

## Governance

- `pub_signoff.bq_migration_log_v1`: row inserted for `mig_082_mig004_vc_finding_source_20260506`.

**Status line:** MIG-004 done. mig_082. any_vc_signal=2914, multi_source=402.
