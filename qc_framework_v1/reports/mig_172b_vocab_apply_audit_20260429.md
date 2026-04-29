# mig_172b — vocab normalization apply audit (post-mig_178)

**Date:** 2026-04-29  
**Batch:** `mig_172b_vocabulary_normalization_apply_recurrence_completion_20260429`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Posture:** SQL authored only. **No MotherDuck apply was executed in this lane.**

## Executive summary

- Verified the ratified SSOT CSV is present and tracked: `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_draft_ratified.csv`.
- Generated the post-mig_178 CSV for the four remaining recurrence/completion histology columns only.
- Rejected `mtc_ptc_mixed` is absent from the rewritten CSV; the single affected raw value is mapped to `MTC | PTC` per mig_178 convention.
- Read-only MotherDuck probes show zero unmapped raw values for the four target columns after adding two exact-match newline aliases.
- Authored apply SQL only: `qc_framework_v1/migrations/172b_vocabulary_normalization_apply_recurrence_completion_20260429.sql`.

## CSV rewrite log

| metric | value |
| --- | --- |
| exact_match_alias_rows_added | 2 |
| input_csv | exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_draft_ratified.csv |
| input_rows | 94 |
| mtc_ptc_mixed_code_rows_remaining | 0 |
| mtc_ptc_mixed_code_rows_rewritten | 1 |
| output_csv | exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_post_mig178_v1.csv |
| output_sha256 | cba52fb45342f9431f48bed1f0dbf8bf4abf43adc555c15e1e1ecc9e63861b41 |
| rows_filtered_to_other_cols | 0 |
| rows_out | 96 |

Scoped columns: `completion_histology_type, completion_prior_histology, recurrence_histology, recurrence_histology_v2`.

Notes:
- `mtc_ptc_mixed_code_rows_rewritten=1`: `completion_prior_histology` raw value `MTC PTC mixed composit` now maps to `MTC | PTC`.
- `exact_match_alias_rows_added=2`: the live database stores two raw values with embedded newlines while the ratified CSV stored space-normalized display text; alias rows preserve exact-match update coverage.

## Pre/post distinct-value audit

Post counts are simulated read-only by applying the rewritten CSV mapping to live CPM values; no database mutation was performed.

| column | live pre distinct values | simulated post distinct values |
| --- | --- | --- |
| recurrence_histology | 42 | 11 |
| recurrence_histology_v2 | 26 | 8 |
| completion_prior_histology | 15 | 10 |
| completion_histology_type | 11 | 5 |

## Pre-flight invariants

| probe | value |
| --- | --- |
| CPM rows | 10871 |
| CPM distinct research_id | 10871 |
| histologic_types_all mtc_ptc_mixed count | 0 |
| histologic_variants_all mtc_ptc_mixed count | 0 |

## Unmapped raw-value audit

| column | unmapped count | unmapped raw values |
| --- | --- | --- |
| recurrence_histology | 0 | NONE |
| recurrence_histology_v2 | 0 | NONE |
| completion_prior_histology | 0 | NONE |
| completion_histology_type | 0 | NONE |

## Mapping spot-checks

### recurrence_histology

| research_id | raw_value | canonical_code |
| --- | --- | --- |
| 1889 | PTC | ptc |
| 4661 | PTC | ptc |
| 11898 | MTC | mtc |
| 10360 | Follicular carcinoma | ftc |
| 10700 | metastatic PTC classical | ptc |
### recurrence_histology_v2

| research_id | raw_value | canonical_code |
| --- | --- | --- |
| 10700 | metastatic PTC classical | ptc |
| 3037 | metastatic PTC | ptc |
| 550 | metastatic PTC | ptc |
| 7168 | metastatic PTC | ptc |
| 8894 | metastatic PTC tall cell variant | ptc |
### completion_prior_histology

| research_id | raw_value | canonical_code |
| --- | --- | --- |
| 1889 | PTC | ptc |
| 768 | PTC | ptc |
| 1460 | PTC | ptc |
| 1535 | PTC | ptc |
| 4661 | PTC | ptc |
### completion_histology_type

| research_id | raw_value | canonical_code |
| --- | --- | --- |
| 1889 | PTC | ptc |
| 1779 | PTC | ptc |
| 1941 | PTC | ptc |
| 4661 | PTC | ptc |
| 11898 | MTC | mtc |

## Live registry CF scan

| column | verification_status | batch_id | CF tokens observed in notes |
| --- | --- | --- | --- |
| completion_histology_type | verified | mig_132_patient_master_pathology_cluster_20260429 | none in registry row |
| completion_prior_histology | verified | mig_132_patient_master_pathology_cluster_20260429 | none in registry row |
| recurrence_histology | verified | mig_138_patient_master_recurrence_response_cluster_20260429 | CF-mig123-RECURRENCE-DATE-RETYPE;, CF-mig138-CPM-RECURRENCE-SPINE-RESYNC-PENDING |
| recurrence_histology_v2 | verified | mig_138_patient_master_recurrence_response_cluster_20260429 | none in registry row |

Interpretation:
- The four registry rows are already verified from prior migration lanes.
- The apply SQL appends a guarded mig_172b note to each row; it does not downgrade verification status.
- The recurrence row still documents the prior `CF-mig138-CPM-RECURRENCE-SPINE-RESYNC-PENDING` closure; no live registry row contained a dedicated `CF-mig168-VOCAB-DRIFT-*` token at read-only probe time.

## Carry-forward disposition

| Carry-forward | Disposition |
|---|---|
| `CF-mig172-MTC-PTC-MIXED-REJECT` | Addressed in authored CSV/SQL: rejected code removed, mixed label represented as `MTC | PTC`. |
| `CF-mig168-VOCAB-DRIFT-RECURRENCE-HISTOLOGY-42-VALUES` | Apply SQL prepared; expected `recurrence_histology` distinct values drop from 42 to 11 after Path C execution. |
| `CF-mig172b-RAW-UNMAPPED-recurrence_histology` | Not opened; read-only audit found 0 unmapped values. |
| `CF-mig172b-RAW-UNMAPPED-recurrence_histology_v2` | Not opened; read-only audit found 0 unmapped values. |
| `CF-mig172b-RAW-UNMAPPED-completion_prior_histology` | Not opened; read-only audit found 0 unmapped values. |
| `CF-mig172b-RAW-UNMAPPED-completion_histology_type` | Not opened; read-only audit found 0 unmapped values. |

## Authored artifacts

1. `scripts/_mig172b_csv_rewrite.py`
2. `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_post_mig178_v1.csv`
3. `qc_framework_v1/migrations/172b_vocabulary_normalization_apply_recurrence_completion_20260429.sql`
4. `qc_framework_v1/reports/mig_172b_vocab_apply_audit_20260429.md`

## Execution boundary

This lane intentionally stopped before MotherDuck mutation. Cowork should execute the SQL through governed Path C with pre-snapshot verification, one-statement logging, CPM invariant checks, and post-state distinct-value confirmation.
