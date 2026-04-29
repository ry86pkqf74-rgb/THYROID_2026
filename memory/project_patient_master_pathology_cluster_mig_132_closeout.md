# `canonical_patient_master` pathology cluster — mig_132 close-out (Lane 23)

**Date:** 2026-04-29  
**Migration:** `qc_framework_v1/migrations/132_patient_master_pathology_cluster_signoff_20260429.sql`  
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)

## Probe vs flip count

- **Predicate** (`information_schema`): `path_%` OR `%histology%` OR `%tumor%` OR `%stage_%` OR `bethesda_%` OR `%synoptic%` OR `%t_stage%` OR `%n_stage%` OR `%m_stage%`.
- **110** columns matched on live CPM; **2** were already **verified** under mig_130 (**ops_io_tumor_appearance**, **ops_tumor_side** — operative-sheet fields swept by `%tumor%`).
- **106** columns flipped **`not_started` → `verified`** in this lane.
- **Excluded** from Lane 23 (recurrence lane / deferred): **recurrence_histology**, **recurrence_histology_v2** — remain **`not_started`** until post–Script 203 / mig_123 alignment.

## Bucket summary

| Bucket | n | verification_method (abbrev) |
|--------|---|------------------------------|
| path_* + gm_path_* | 23 | derivation_vs_canonical_path_events_and_gm_raw_feed |
| syn_* + ene_path_synoptic | 7 | synoptic_surface_upstream_preservation_no_row_number |
| Tumor / histology counts / sizes | 23 | tumor_histology_counts_and_size_rollups_path_family |
| bethesda_* | 15 | derivation_vs_canonical_fna_events_bethesda_cluster |
| AJCC + dominant + completion | 29 | patient_level_ajcc_overlay_dominant_tumor_mig266b_family |
| Staging notes + NLP synoptic | 9 | staging_notes_heterogeneity_flags_and_nlp_synoptic_cluster |

## Upstream SSOT referenced

- `canonical_path_malignant_events_v1` (mig_89 family), `canonical_path_benign_events_v1` (mig_97b), `canonical_pathology_clinical_events_v1` (mig_110).
- `canonical_fna_events_v1` for Bethesda lineage (mig_78 / mig_96); table has **no** `build_ts` column (noted in migration header).

## Drift / lineage notes

- Naive BOOL_OR replay vs **`canonical_path_malignant_events_v1`** can diverge from patient-level flags (e.g. **path_gross_ete_flag** sample drift **125** rows vs naive aggregate) — consistent with multi-source rollup semantics downstream of canonical events; **CF stale rollup probe**, non-blocking per Lane prompt.
- AJCC **patient-level** columns may disagree with **per-event** staging rows where **ete_subgrade_events** / mig_266b adjudication applies — documented as acceptable overlay semantics.

## TIMESTAMP carry-forward

- **CF-mig132-PM-PATH-STAGE-DERIVED-AT-RETYPE** — `path_stage_raw_derived_at`, `gm_path_stage_raw_derived_at` remain **TIMESTAMP**; calendar-safe joins use **CAST AS DATE** / **DATE_TRUNC** vs DATE SSOTs; umbrella **CF-100-DATE-RETYPE**.

## Registry state (post-apply)

- **canonical_column_verification_registry_v1:** **339 verified**, **1255 not_started**, **4 na** on `canonical_patient_master`.
- **canonical_table_signoff_registry_v1:** **`table_status = in_progress`** (partial thematic verification only).

## 5-gate audit

- **Gate 4** (verified cols missing metadata on flipped rows): **0** gaps (`verified_by`, `verification_method`, `batch_id` populated).

## Next slices

Lymph node (Lane 24), labs (Lane 25), remaining CPM thematic clusters per mig_130/MEMORY index.
