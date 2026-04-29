# mig_168 — PM controlled-vocabulary standardization audit

**Date:** 2026-04-29  
**Posture:** read-only MotherDuck catalog + drift audit; no database writes.  
**Target:** `main.canonical_patient_master` verified `VARCHAR` columns.  
**Export directory:** `exports/mig168_pm_vocab_audit_20260429_175417`

## Executive summary

| Metric | Value |
|---|---:|
| Verified `VARCHAR` PM columns audited | 461 |
| Likely controlled-vocabulary columns | 367 |
| High-cardinality/free-text/identifier columns excluded from enum draft | 73 |
| Columns with casing/whitespace/raw-variant drift | 123 |
| Enum-candidate columns with rare/possible rogue values | 113 |
| Draft enum dictionary rows | 2,128 |

## Column-class histogram

| column_class                     |   n_columns |
|:---------------------------------|------------:|
| controlled_vocab_candidate       |         254 |
| degenerate_single_value          |          57 |
| possible_enum_review             |          54 |
| high_cardinality_text            |          51 |
| free_text_or_identifier          |          22 |
| date_or_timestamp_text           |          16 |
| empty_verified_varchar           |           5 |
| controlled_vocab_candidate_large |           2 |

## Enum dictionary semantic-family histogram

| semantic_family     |   n_enum_values |
|:--------------------|----------------:|
| other               |            1353 |
| stage               |             196 |
| histology           |             164 |
| provenance          |             123 |
| complication        |              53 |
| recurrence_survival |              51 |
| molecular           |              47 |
| treatment           |              44 |
| laterality          |              42 |
| risk_scoring        |              38 |
| demographics        |              17 |

## Interpretation

This is a **standardization audit**, not a verification-status flip. A value is treated as a controlled-vocabulary candidate when its normalized distinct count is low enough to be safely dictionary-managed. High-cardinality text, dates stored as text, source/provenance strings, identifiers, and list payloads are catalogued but excluded from the SSOT enum draft unless they have low-cardinality enum behavior.

Drift classes:

- `raw_variant_drift`: multiple raw spellings/casing/spacing collapse to the same normalized value.
- `leading_trailing_whitespace`: at least one row has leading/trailing spaces.
- `repeated_internal_whitespace`: at least one row has repeated internal whitespace.
- `rare_value_review`: low-frequency normalized value in an enum-candidate column; this is the best available read-only proxy for rogue values until a clinically approved dictionary exists.

## Drift class counts

| issue_type                   |   n_findings |   n_columns |
|:-----------------------------|-------------:|------------:|
| rare_value_review            |          641 |         113 |
| raw_variant_drift            |           61 |          25 |
| leading_trailing_whitespace  |           12 |           8 |
| repeated_internal_whitespace |            5 |           4 |

## Highest-yield column-level findings

### Raw-variant drift groups

| column_name                  | column_class               | semantic_family   |   non_null |   distinct_raw |   distinct_norm |   norm_groups_with_raw_variants |
|:-----------------------------|:---------------------------|:------------------|-----------:|---------------:|----------------:|--------------------------------:|
| syn_right_lobe_size_cm       | high_cardinality_text      | other             |       7058 |           6599 |            6031 |                             457 |
| syn_left_lobe_size_cm        | high_cardinality_text      | other             |       7204 |           6715 |            6149 |                             452 |
| syn_isthmus_size_cm          | high_cardinality_text      | other             |       3981 |           3500 |            3102 |                             333 |
| nlp_synoptic_key_finding     | high_cardinality_text      | other             |       4835 |            956 |             824 |                              99 |
| nlp_path_histology_mentioned | high_cardinality_text      | histology         |       1994 |            995 |             926 |                              64 |
| ct_indication_first          | high_cardinality_text      | other             |       3034 |           2504 |            2417 |                              60 |
| ct_indication_last           | high_cardinality_text      | other             |       3034 |           2506 |            2440 |                              46 |
| syn_frozen_section_result    | high_cardinality_text      | other             |       4061 |           3860 |            3838 |                              21 |
| histologic_types_all         | high_cardinality_text      | other             |       4137 |            177 |             145 |                              21 |
| histologic_variants_all      | high_cardinality_text      | histology         |       3310 |            275 |             247 |                              18 |
| ops_preop_diagnosis          | high_cardinality_text      | other             |       2083 |            321 |             299 |                              17 |
| path_histology_variant_raw   | free_text_or_identifier    | histology         |       3317 |            167 |             141 |                              17 |
| ops_prior_neck_operation     | high_cardinality_text      | other             |        456 |            128 |             104 |                              16 |
| path_histology_raw           | free_text_or_identifier    | histology         |       4137 |             59 |              44 |                              12 |
| mri_indication_first         | high_cardinality_text      | other             |        404 |            373 |             365 |                               8 |
| recurrence_histology         | controlled_vocab_candidate | histology         |        440 |             42 |              33 |                               8 |
| ops_max_diameter_cm          | high_cardinality_text      | other             |       1868 |            153 |             147 |                               6 |
| syn_architecture             | high_cardinality_text      | other             |       1402 |             85 |              79 |                               6 |
| mri_exam_type_first          | high_cardinality_text      | other             |        411 |            116 |             110 |                               5 |
| ops_io_tumor_appearance      | high_cardinality_text      | other             |       2025 |             65 |              61 |                               4 |

### Whitespace drift columns

| column_name                | column_class               | semantic_family     |   non_null |   leading_trailing_rows |   repeated_ws_rows |   distinct_norm |
|:---------------------------|:---------------------------|:--------------------|-----------:|------------------------:|-------------------:|----------------:|
| syn_right_lobe_size_cm     | high_cardinality_text      | other               |       7058 |                    3183 |                924 |            6031 |
| syn_left_lobe_size_cm      | high_cardinality_text      | other               |       7204 |                    3091 |                882 |            6149 |
| syn_isthmus_size_cm        | high_cardinality_text      | other               |       3981 |                    1734 |                432 |            3102 |
| syn_frozen_section_result  | high_cardinality_text      | other               |       4061 |                    1245 |               2343 |            3838 |
| ops_surgeon                | high_cardinality_text      | other               |       8591 |                    1162 |                  0 |             142 |
| path_histology_raw         | free_text_or_identifier    | histology           |       4137 |                      77 |                  2 |              44 |
| ops_intraop_appearance     | high_cardinality_text      | other               |        340 |                      73 |                  2 |             326 |
| histologic_types_all       | high_cardinality_text      | other               |       4137 |                      70 |                 30 |             145 |
| ct_thyroid_details_last    | free_text_or_identifier    | other               |       2246 |                      39 |                 24 |            2240 |
| recurrence_site_primary    | high_cardinality_text      | recurrence_survival |        100 |                      38 |                 17 |              79 |
| recurrence_site_v2         | high_cardinality_text      | recurrence_survival |        100 |                      38 |                 17 |              79 |
| recurrence_site_raw        | free_text_or_identifier    | recurrence_survival |         77 |                      37 |                  9 |              67 |
| histologic_variants_all    | high_cardinality_text      | histology           |       3310 |                      13 |                 13 |             247 |
| ct_ln_details_last         | high_cardinality_text      | other               |       2268 |                      11 |                 19 |            1348 |
| path_histology_variant_raw | free_text_or_identifier    | histology           |       3317 |                      11 |                  7 |             141 |
| syn_tumor2_histologic_type | controlled_vocab_candidate | other               |       1346 |                       8 |                  1 |              29 |
| recurrence_histology       | controlled_vocab_candidate | histology           |        440 |                       7 |                  0 |              33 |
| completion_histology_type  | controlled_vocab_candidate | histology           |        188 |                       6 |                  0 |               8 |
| completion_prior_histology | controlled_vocab_candidate | histology           |        385 |                       6 |                  0 |              12 |
| ops_max_diameter_cm        | high_cardinality_text      | other               |       1868 |                       5 |                  0 |             147 |

### Rare / possible rogue-value review columns

| column_name                 | column_class                     | semantic_family   |   non_null |   distinct_norm |   rare_value_count |
|:----------------------------|:---------------------------------|:------------------|-----------:|----------------:|-------------------:|
| proc_nlp_note_types         | controlled_vocab_candidate_large | other             |       4711 |              62 |                 45 |
| pmhx_nlp_note_types         | controlled_vocab_candidate_large | other             |       3895 |              51 |                 33 |
| histology_final             | controlled_vocab_candidate       | histology         |       4137 |              38 |                 28 |
| ops_preop_symptoms          | controlled_vocab_candidate       | other             |        159 |              34 |                 25 |
| ops_preop_imaging_performed | controlled_vocab_candidate       | other             |        143 |              28 |                 25 |
| ct_exam_type_first          | controlled_vocab_candidate       | other             |       3034 |              33 |                 25 |
| recurrence_histology        | controlled_vocab_candidate       | histology         |        440 |              33 |                 23 |
| ops_para_ag_performed       | controlled_vocab_candidate       | other             |       2082 |              25 |                 22 |
| syn_tumor2_histologic_type  | controlled_vocab_candidate       | other             |       1346 |              29 |                 21 |
| cnln_img_laterality         | controlled_vocab_candidate       | laterality        |        272 |              31 |                 20 |
| ops_intraop_nodule_count    | controlled_vocab_candidate       | other             |        650 |              25 |                 20 |
| pet_distant_met_sites       | controlled_vocab_candidate       | other             |        130 |              30 |                 17 |
| path_ete_raw                | possible_enum_review             | other             |       4075 |              28 |                 16 |
| cnln_modalities_present     | controlled_vocab_candidate       | other             |       1436 |              36 |                 16 |
| gm_path_ete_raw             | possible_enum_review             | other             |       4075 |              28 |                 16 |
| recurrence_histology_v2     | controlled_vocab_candidate       | histology         |        118 |              22 |                 16 |
| ops_prior_neck_irradiation  | controlled_vocab_candidate       | other             |        147 |              19 |                 15 |
| mol_genes_list              | controlled_vocab_candidate       | other             |        316 |              29 |                 14 |
| gm_path_vascular_inv_raw    | possible_enum_review             | other             |       3682 |              17 |                 11 |
| path_vascular_invasion_raw  | possible_enum_review             | other             |       3682 |              17 |                 11 |

## High-signal observations

- The PM vocabulary surface is larger than the prompt estimate: **461** verified `VARCHAR` columns were in scope after joining the live information schema to the verification registry.
- **367** columns behave like controlled-vocabulary candidates or low-cardinality review candidates; **73** high-cardinality/free-text/identifier fields should not be normalized by enum policy.
- Raw-variant drift is concentrated in pathology/recurrence/histology fields such as `recurrence_histology`, `completion_prior_histology`, `completion_histology_type`, `path_ete_raw`, and `gm_path_ete_raw`.
- Whitespace drift is common in size/detail text fields (`syn_right_lobe_size_cm`, `syn_left_lobe_size_cm`, `syn_isthmus_size_cm`, `syn_frozen_section_result`) and should be treated as a text-cleaning concern rather than a controlled-vocabulary mutation.
- Multi-label laterality fields such as `cnln_img_laterality` and level-list fields such as `lateral_levels_v10` / `ene_levels_v9` need list-token normalization rules; coercing the whole semicolon-delimited string to a single enum would preserve drift rather than fix it.

## Top drift / review findings

| table_name               | column_name                 | semantic_family   | norm_value                                       |   n_rows |   pct_of_nonnull | issue_types       |   raw_variant_count | top_raw_variants                                 | suggested_canonical_code                         | suggested_display_label                          |
|:-------------------------|:----------------------------|:------------------|:-------------------------------------------------|---------:|-----------------:|:------------------|--------------------:|:-------------------------------------------------|:-------------------------------------------------|:-------------------------------------------------|
| canonical_patient_master | ajcc7_missing_components    | stage             | n                                                |        3 |            0.044 | rare_value_review |                   1 | N                                                | n                                                | N                                                |
| canonical_patient_master | ajcc8_n_stage_note          | stage             | derived from ln laterality (central-only)        |        2 |            0.048 | rare_value_review |                   1 | derived from LN laterality (central-only)        | derived from ln laterality (central-only)        | derived from LN laterality (central-only)        |
| canonical_patient_master | ajcc8_n_stage_note          | stage             | derived from ln laterality (lateral involvement) |        2 |            0.048 | rare_value_review |                   1 | derived from LN laterality (lateral involvement) | derived from ln laterality (lateral involvement) | derived from LN laterality (lateral involvement) |
| canonical_patient_master | ajcc8_stage_group_v2        | stage             | iva                                              |        4 |            0.097 | rare_value_review |                   1 | IVA                                              | iva                                              | IVA                                              |
| canonical_patient_master | ajcc8_t_stage               | stage             | t1                                               |        2 |            0.048 | rare_value_review |                   1 | T1                                               | t1                                               | T1                                               |
| canonical_patient_master | ajcc8_t_stage_v2            | stage             | t1                                               |        2 |            0.048 | rare_value_review |                   1 | T1                                               | t1                                               | T1                                               |
| canonical_patient_master | best_ene_grade              | other             | microscopic                                      |        2 |            0.125 | rare_value_review |                   1 | microscopic                                      | microscopic                                      | microscopic                                      |
| canonical_patient_master | bethesda_derivation_methods | provenance        | none\|rules                                      |        1 |            0.019 | rare_value_review |                   1 | none\|rules                                      | none\|rules                                      | none\|rules                                      |
| canonical_patient_master | cnln_img_laterality         | laterality        | lateral                                          |        2 |            0.735 | rare_value_review |                   1 | lateral                                          | lateral                                          | lateral                                          |
| canonical_patient_master | cnln_img_laterality         | laterality        | left; bilateral                                  |        2 |            0.735 | rare_value_review |                   1 | left; bilateral                                  | left; bilateral                                  | left; bilateral                                  |
| canonical_patient_master | cnln_img_laterality         | laterality        | left; right; bilateral                           |        2 |            0.735 | rare_value_review |                   1 | left; right; bilateral                           | left; right; bilateral                           | left; right; bilateral                           |
| canonical_patient_master | cnln_img_laterality         | laterality        | null; bilateral                                  |        2 |            0.735 | rare_value_review |                   1 | null; bilateral                                  | null; bilateral                                  | null; bilateral                                  |
| canonical_patient_master | cnln_img_laterality         | laterality        | bilateral; central                               |        1 |            0.368 | rare_value_review |                   1 | bilateral; central                               | bilateral; central                               | bilateral; central                               |
| canonical_patient_master | cnln_img_laterality         | laterality        | bilateral; lateral neck                          |        1 |            0.368 | rare_value_review |                   1 | bilateral; lateral neck                          | bilateral; lateral neck                          | bilateral; lateral neck                          |
| canonical_patient_master | cnln_img_laterality         | laterality        | bilateral; left                                  |        1 |            0.368 | rare_value_review |                   1 | bilateral; left                                  | bilateral; left                                  | bilateral; left                                  |
| canonical_patient_master | cnln_img_laterality         | laterality        | bilateral; right                                 |        1 |            0.368 | rare_value_review |                   1 | bilateral; right                                 | bilateral; right                                 | bilateral; right                                 |
| canonical_patient_master | cnln_img_laterality         | laterality        | central; left                                    |        1 |            0.368 | rare_value_review |                   1 | central; left                                    | central; left                                    | central; left                                    |
| canonical_patient_master | cnln_img_laterality         | laterality        | central; left; bilateral                         |        1 |            0.368 | rare_value_review |                   1 | central; left; bilateral                         | central; left; bilateral                         | central; left; bilateral                         |
| canonical_patient_master | cnln_img_laterality         | laterality        | central; right; left                             |        1 |            0.368 | rare_value_review |                   1 | central; right; left                             | central; right; left                             | central; right; left                             |
| canonical_patient_master | cnln_img_laterality         | laterality        | lateral neck; bilateral                          |        1 |            0.368 | rare_value_review |                   1 | lateral neck; bilateral                          | lateral neck; bilateral                          | lateral neck; bilateral                          |
| canonical_patient_master | cnln_img_laterality         | laterality        | lateral; central                                 |        1 |            0.368 | rare_value_review |                   1 | lateral; central                                 | lateral; central                                 | lateral; central                                 |
| canonical_patient_master | cnln_img_laterality         | laterality        | left; central; bilateral                         |        1 |            0.368 | rare_value_review |                   1 | left; central; bilateral                         | left; central; bilateral                         | left; central; bilateral                         |
| canonical_patient_master | cnln_img_laterality         | laterality        | null; lateral; bilateral                         |        1 |            0.368 | rare_value_review |                   1 | null; lateral; bilateral                         | null; lateral; bilateral                         | null; lateral; bilateral                         |
| canonical_patient_master | cnln_img_laterality         | laterality        | null; right; bilateral                           |        1 |            0.368 | rare_value_review |                   1 | null; right; bilateral                           | null; right; bilateral                           | null; right; bilateral                           |
| canonical_patient_master | cnln_img_laterality         | laterality        | right; bilateral; left                           |        1 |            0.368 | rare_value_review |                   1 | right; bilateral; left                           | right; bilateral; left                           | right; bilateral; left                           |

## Recommended fixes / changes

1. **Adopt the emitted dictionary as a review draft, not an immediate mutation source.** Use `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_draft.csv` as the first SSOT enum dictionary draft and route any `rare_value_review` entries through clinical/data-owner review before normalizing.
2. **Implement normalization at build time, not by ad hoc CPM updates.** For each accepted enum column, add a deterministic `CASE`/mapping layer in the CPM builder or upstream feeder, then rebuild CPM and update `cpm_built_at` + `cpm_reconciliation_provenance_v1` per repo policy.
3. **Separate display labels from stored codes.** Store stable lowercase snake/canonical codes where possible, and keep human-readable labels in the dictionary. This prevents future casing drift while preserving publication-friendly display values.
4. **Do not coerce high-cardinality text columns into enums.** Columns classified as `free_text_or_identifier`, `date_or_timestamp_text`, or `high_cardinality_text` should get separate type/lineage audits rather than enum standardization.
5. **Open a follow-up migration for accepted changes only.** Suggested lane name: `mig_168b_pm_vocab_normalization_apply`, with pre-snapshot archive, per-column mapping table, drift-count pre/post gates, CPM invariants, and no value changes outside the reviewed dictionary.

## Artifacts

| Artifact | Purpose |
|---|---|
| `exports/mig168_pm_vocab_audit_20260429_175417/pm_verified_varchar_column_catalog.csv` | One row per verified PM `VARCHAR` column with nullness/cardinality/drift metrics. |
| `exports/mig168_pm_vocab_audit_20260429_175417/pm_vocab_value_catalog.csv` | Normalized value counts for enum-candidate columns. |
| `exports/mig168_pm_vocab_audit_20260429_175417/pm_vocab_drift_findings.csv` | Column/value-level casing, whitespace, raw-variant, and rare-value review queue. |
| `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_draft.csv` | Draft SSOT enum dictionary with canonical codes and suggested display labels. |
| `exports/mig168_pm_vocab_audit_20260429_175417/manifest.json` | Machine-readable run manifest and thresholds. |

## Run manifest

```json
{
  "migration": "mig_168",
  "run_timestamp_utc": "20260429_175826",
  "posture": "read_only_motherduck_audit_no_db_writes",
  "target_table": "main.canonical_patient_master",
  "verified_varchar_columns_audited": 461,
  "enum_candidate_columns": 367,
  "drift_columns": 123,
  "drift_findings": 702,
  "enum_dictionary_rows": 2128,
  "thresholds": {
    "enum_max_distinct": 40,
    "large_enum_max_distinct": 100,
    "rare_min_count": 2,
    "rare_pct_threshold": "0.1% of non-null rows, minimum rare_min_count"
  },
  "artifacts": [
    "pm_verified_varchar_column_catalog.csv",
    "pm_vocab_value_catalog.csv",
    "pm_vocab_drift_findings.csv",
    "pm_ssot_enum_dictionary_draft.csv"
  ]
}
```
