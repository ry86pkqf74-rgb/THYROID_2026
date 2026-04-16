# Script 235 — Parathyroid & Calcium/PTH Data Quality Fix Report

Generated: 2026-04-16T15:14:20-04:00
Database: `thyroid_canonical_publication_v1_0` on MotherDuck

## Pre-run state
- canonical_patient_master columns: 1492
- Pre-fix lab_calcium_min: n=165 gt20=21 gt50=21 mean=97.96 max=2000.0
- Pre-fix postop_calcium_min_value: gt20=20 mean=35.41 max=2500.0

## Phase 1 — Calcium unit normalization
- Audit: 29 patient rows with calcium > 20 on either column (scripts/output/235_calcium_contamination_audit.csv)
- extracted_postop_labs calcium corrections by rule:
  - `no_change`: n=557
  - `nulled_pg_ml`: n=22
  - `divided_by_100`: n=13
  - `nulled_unrecoverable`: n=3
- longitudinal_lab_canonical_v1 calcium corrections by rule:
  - `no_change`: n=100
  - `source_null`: n=63
  - `divided_by_100`: n=19
  - `nulled_unrecoverable`: n=5
  - `divided_by_10`: n=1
- extracted post-fix calcium (min/max/avg/n_nonnull/out_of_range): (1.9, 14.8, 9.39, 570, 2)
- longitudinal post-fix calcium (min/max/avg/n_nonnull/out_of_range): (1.9, 20.0, 9.195, 120, 4)
- Canonical contaminated patients: 0 (recoverable: 0; dispositions: {})
- Canonical post-fix: lab_ca_min_gt20=0 lab_ca_min_mean=9.175 has_low_ca_flag_true=39

## Phase 2 — Flag re-derivation
- has_low_calcium_flag: [(None, 10247), ('false', 585), ('true', 39)]
- has_low_pth_flag: [(None, 10178), ('false', 594), ('true', 99)]
- postop_low_calcium_flag: [(None, 10327), ('false', 512), ('true', 32)]
- postop_low_pth_flag: [(None, 10198), ('false', 576), ('true', 97)]

## Phase 3 — NSQIP hypocalcemia recovery
- NSQIP Yes with canonical match: 82
- Recovery candidates: 0
- Patients promoted to comp_hypocalcemia_confirmed = TRUE via NSQIP: 80
- comp_hypocalcemia_confirmed distribution: [(None, 8944), ('false', 1829), ('true', 98)]
- Hypocalcemia rate by procedure:
  - ('total_thyroidectomy', 4561, 58, 1.27)
  - ('hemithyroidectomy', 3809, 6, 0.16)
  - ('other', 354, 0, 0.0)
  - ('unknown', 9, 0, 0.0)

## Phase 4 — Hypoparathyroidism assessment
- Pre-fix diagnostics (confirmed cohort): (34, 28, 4, 5, 22, 1, 18)
- Permanent candidates (biochem + FU > 6mo): 18
- Post-fix: confirmed=34 transient=11 permanent=18
- Known limitation: 1.5% PTH lab coverage and short follow-up significantly under-ascertain permanent hypoparathyroidism (see comp_hypoparathyroidism_permanent dictionary entry).

## Phase 5 — Complication table rebuilds
- complication_phenotype_v1 confirmed_flag x entity: [('chyle_leak', 'false', 1568), ('chyle_leak', 'true', 20), ('hematoma', 'false', 215), ('hematoma', 'true', 38), ('hypocalcemia', 'false', 1829), ('hypocalcemia', 'true', 98), ('hypoparathyroidism', 'false', 396), ('hypoparathyroidism', 'true', 34), ('rln_injury', 'false', 672), ('rln_injury', 'true', 59), ('seroma', 'false', 846), ('seroma', 'true', 28), ('vocal_cord_paralysis', 'false', 88), ('vocal_cord_paresis', 'false', 71), ('wound_infection', 'false', 14), ('wound_infection', 'true', 2)]
- biochemical flag counts: (23, 5905, 50, 17, 5911, 50)
- NSQIP-recovered patients without prior hypocalcemia phenotype row (inserted): 50
- complication_patient_summary_v1 summary: (2938, 98, 34, 240)

## Phase 6 — Registry + __readme
- detail_table_registry_v1 issues: 0
- data_dictionary_v235 entries: 1492

## Phase 7 — Final validation
- Invariants: 10,871 rows / 10,871 distinct RIDs / 0 NULL / 0 NULL FNA — PASS
- lab_calcium_min > 20: 0 (must be 0)
- postop_calcium_min_value > 20: 0 (must be 0)
- has_low_calcium_flag = TRUE: 39
- has_low_pth_flag = TRUE: 99
- comp_hypocalcemia_confirmed = TRUE: 98 (of which 80 NSQIP-recovered)
- comp_hypoparathyroidism: confirmed=34 transient=11 permanent=18
- NSQIP concordance:
  - ('No', 857, 0, 0.0)
  - ('Unknown', 4, 0, 0.0)
  - ('Yes', 82, 82, 100.0)
- Complication rates by procedure:
  - ('total_thyroidectomy', 4561, 1.3, 0.7)
  - ('hemithyroidectomy', 3809, 0.2, 0.0)
  - ('other', 354, 0.0, 0.0)
  - ('unknown', 9, 0.0, 0.0)

## Backups retained
- canonical_patient_master_pre235_backup
- complication_phenotype_v1_pre235_backup
- complication_patient_summary_v1_pre235_backup
- extracted_postop_labs_expanded_v1_pre235_backup
