# Script 240 — LN/Staging Data Quality Remediation Report

Generated: 2026-04-16T18:27:01Z
Database: `thyroid_canonical_publication_v1_0` on MotherDuck

## Pre-run state
- canonical_patient_master: 10,871 × 1479 columns
- Pre-existing fix columns: ['ajcc8_stage_group_v2', 'ajcc8_t_stage_v2']

## Issue 1 — Microscopic ETE T3b correction
- cohort: 949 patients
- corrected rows: 906
- corrected T-stage distribution: `[('T1b', 243), ('T2', 241), ('T3a', 216), ('T1a', 206)]`
- stage-group migrations (original -> corrected): `[('II', 'I', 49), ('III', 'II', 6)]`
- New columns: `ajcc8_t_stage_corrected`, `ajcc8_stage_group_corrected`, `microscopic_ete_t3b_corrected`

## Issue 2 — ln_positive_flag reconciliation
- flag<>total mismatches: 51
- gap-fill candidates (flag set, total NULL): 457
- `ln_positive_binary` filled: 5113 (TRUE=2637, FALSE=2476)
- `ln_count_reconciled` filled: 5162
- `ln_positive_count_raw` filled: 4060
- New columns: `ln_positive_binary`, `ln_positive_count_raw`, `ln_count_reconciled`

## Issue 3 — ENE/LN concordance
- concordance distribution among ENE-positive:
  `[('concordant', 1109), ('discordant_unresolved', 122), ('ene_positive_ln_unknown', 21)]`
- New column: `ene_ln_concordance_status`

## Issue 4 — Stage discordance (N1b + age>=55 at Stage III)
- candidates: 8
- note distribution: `[('n1b_incorrectly_upstaged_to_III_corrected_to_II', 8)]`
- New column: `stage_discordance_note`

## Issue 5 — Level-specific impossible values
- pre-run counts by level: `{'i': 0, 'ii': 1, 'iii': 0, 'iv': 0, 'v': 0, 'vi': 0, 'vii': 0}`
- ln_data_quality_flag distribution: `[('positive_without_exam_count', 20), ('level_ii_transposition_corrected', 1)]`
- New column: `ln_data_quality_flag`

## Data dictionary
- data_dictionary_v240 entries: 1488

## Registry + cohort views
- cohort views tested: 64
- broken views: 0
- registry issues: 0
- canonical_detail_pointer_v1 rows: 1488

## Stale objects
- canonical_patient_master_v1 in publication DB: False

## Final state
- canonical_patient_master: 10,871 × 1488 columns
- All invariants pass
- mic-ETE still T3b (DTC, corrected col): 0
- New-column coverage:
  - ajcc8_t_stage_corrected: 4083 (37.6%)
  - ajcc8_stage_group_corrected: 4083 (37.6%)
  - microscopic_ete_t3b_corrected: 10871 (100.0%)
  - ln_positive_binary: 5113 (47.0%)
  - ln_positive_count_raw: 4060 (37.3%)
  - ln_count_reconciled: 5162 (47.5%)
  - ene_ln_concordance_status: 1252 (11.5%)
  - stage_discordance_note: 8 (0.1%)
  - ln_data_quality_flag: 21 (0.2%)
