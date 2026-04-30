# mig_177c — LVI+VI derivative reclean scoping only

**Run ID:** `mig_177c_lvi_vi_derivative_reclean_scope_20260429`  
**Run timestamp (UTC):** `2026-04-30T00:30:29.930759+00:00`  
**Posture:** read-only MotherDuck investigation; no production DDL/DML.  
**Scope:** Option A clear vs Option B rederive for the mig_177b TRUE→FALSE derivative flippers.  

## Executive summary

- LVI flippers confirmed: **2,502** TRUE→FALSE after mig_177b (expected 2,502).
- VI flippers confirmed: **2,580** TRUE→FALSE after mig_177b (expected 2,580).
- Option A is the minimal consistency cleanup: clear derivative fields only on those flippers (`n_tumors_*_present` → 0; other derivatives → NULL). It would affect **7,464** non-null LVI derivative cells and **20,635** non-null VI derivative cells.
- Option B is a family-wide rederive. It is clinically cleaner long-term, but current `canonical_invasion_events_v1` lacks grade/ordinal/vessel-count columns, so it needs a ratified grade/count lineage before apply.
- Recommendation for Logan: ratify **Option A now** if the objective is internal consistency after mig_177b; ratify **Option B** only with a separate source-lineage specification for grade/count fields and new TRUE flippers.

## Flipper summary

| family   | event_type            |   pre_true |   post_true |   true_to_false_flippers |   expected_true_to_false |   false_or_null_to_true_flippers |   stable_true |   stable_false_or_null |   derivative_columns_scoped |   option_a_clear_cells_non_null_on_true_to_false |   option_a_clear_cells_non_zero_or_non_blank_on_true_to_false | option_b_requires_grade_count_lineage   |
|:---------|:----------------------|-----------:|------------:|-------------------------:|-------------------------:|---------------------------------:|--------------:|-----------------------:|----------------------------:|-------------------------------------------------:|--------------------------------------------------------------:|:----------------------------------------|
| lvi      | lymphatic_microscopic |       3392 |         989 |                     2502 |                     2502 |                               99 |           890 |                   7380 |                           3 |                                             7464 |                                                          7464 | True                                    |
| vi       | vascular_microscopic  |       3698 |        1178 |                     2580 |                     2580 |                               60 |          1118 |                   7113 |                          12 |                                            20635 |                                                         20634 | True                                    |

## Option matrix

| family   | option          | scope                                                    |   rows_impacted | columns_impacted                                                                                                                                                                                                                                                   | proposed_rule                                                                                                                                                     | pros                                                                                                                   | cons                                                                                                                                          | logan_decision_needed   |
|:---------|:----------------|:---------------------------------------------------------|----------------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------|:------------------------|
| lvi      | A_clear_only    | Only TRUE→FALSE flippers from mig_177b                   |            2502 | lvi_grade, lvi_ordinal_worst, n_tumors_lvi_present                                                                                                                                                                                                                 | Set derivative strings/ordinals/vessel/confidence/source columns to NULL and n_tumors_*_present to 0 where the corresponding strict-present boolean is now FALSE. | Smallest blast radius; immediately removes internal inconsistency for flippers; no need to invent grade/count lineage. | Does not improve missing derivative values among FALSE/NULL→TRUE patients; leaves broader grade/count rederive for later.                     | True                    |
| lvi      | B_full_rederive | All patients for this family after refreshed event truth |           10871 | lvi_grade, lvi_ordinal_worst, n_tumors_lvi_present                                                                                                                                                                                                                 | Rebuild derivative fields from strict event-present patients plus a ratified source-linked grade/count lineage; clear false patients as a byproduct.              | Creates coherent family-wide derivative semantics and can backfill new TRUE flippers.                                  | Current canonical_invasion_events_v1 has no ordinal grade or vessel-count columns; requires separate source-lineage design before safe apply. | True                    |
| vi       | A_clear_only    | Only TRUE→FALSE flippers from mig_177b                   |            2580 | vasc_grade, vasc_grade_final_v13, vascular_invasion_final, vascular_invasion_grade, vascular_who_2022_grade, vi_ordinal_worst, vasc_vessel_count_v13, vascular_vessel_count, vi_vessels_max, vasc_confidence_final_v13, vasc_source_final_v13, n_tumors_vi_present | Set derivative strings/ordinals/vessel/confidence/source columns to NULL and n_tumors_*_present to 0 where the corresponding strict-present boolean is now FALSE. | Smallest blast radius; immediately removes internal inconsistency for flippers; no need to invent grade/count lineage. | Does not improve missing derivative values among FALSE/NULL→TRUE patients; leaves broader grade/count rederive for later.                     | True                    |
| vi       | B_full_rederive | All patients for this family after refreshed event truth |           10871 | vasc_grade, vasc_grade_final_v13, vascular_invasion_final, vascular_invasion_grade, vascular_who_2022_grade, vi_ordinal_worst, vasc_vessel_count_v13, vascular_vessel_count, vi_vessels_max, vasc_confidence_final_v13, vasc_source_final_v13, n_tumors_vi_present | Rebuild derivative fields from strict event-present patients plus a ratified source-linked grade/count lineage; clear false patients as a byproduct.              | Creates coherent family-wide derivative semantics and can backfill new TRUE flippers.                                  | Current canonical_invasion_events_v1 has no ordinal grade or vessel-count columns; requires separate source-lineage design before safe apply. | True                    |

## Derivative-column impact on TRUE→FALSE flippers

| family   | column_name               |   true_to_false_flippers |   current_non_null_on_flippers |   current_non_zero_or_non_blank_on_flippers | option_a_clear_target   | top_current_values_on_flippers                   |
|:---------|:--------------------------|-------------------------:|-------------------------------:|--------------------------------------------:|:------------------------|:-------------------------------------------------|
| lvi      | lvi_grade                 |                     2502 |                           2460 |                                        2460 | set_to_null             | x=2453; <NULL>=42; indeterminate=5; c/a=2        |
| lvi      | lvi_ordinal_worst         |                     2502 |                           2502 |                                        2502 | set_to_null             | 2=2502                                           |
| lvi      | n_tumors_lvi_present      |                     2502 |                           2502 |                                        2502 | set_to_zero             | 1=2130; 2=311; 3=42; 4=17; 5=2                   |
| vi       | vasc_grade                |                     2580 |                           2580 |                                        2579 | set_to_null             | present_ungraded=2575; indeterminate=4; =1       |
| vi       | vasc_grade_final_v13      |                     2580 |                           2579 |                                        2579 | set_to_null             | present_ungraded=2575; indeterminate=4; <NULL>=1 |
| vi       | vascular_invasion_final   |                     2580 |                           2579 |                                        2579 | set_to_null             | present_ungraded=2575; indeterminate=4; <NULL>=1 |
| vi       | vascular_invasion_grade   |                     2580 |                           2579 |                                        2579 | set_to_null             | present_ungraded=2575; indeterminate=4; <NULL>=1 |
| vi       | vascular_who_2022_grade   |                     2580 |                              0 |                                           0 | set_to_null             | <NULL>=2580                                      |
| vi       | vi_ordinal_worst          |                     2580 |                           2580 |                                        2580 | set_to_null             | 2=2580                                           |
| vi       | vasc_vessel_count_v13     |                     2580 |                              0 |                                           0 | set_to_null             | <NULL>=2580                                      |
| vi       | vascular_vessel_count     |                     2580 |                              0 |                                           0 | set_to_null             | <NULL>=2580                                      |
| vi       | vi_vessels_max            |                     2580 |                              0 |                                           0 | set_to_null             | <NULL>=2580                                      |
| vi       | vasc_confidence_final_v13 |                     2580 |                           2579 |                                        2579 | set_to_null             | 0.75=2575; 0.5=4; <NULL>=1                       |
| vi       | vasc_source_final_v13     |                     2580 |                           2579 |                                        2579 | set_to_null             | path_synoptic_text=2579; <NULL>=1                |
| vi       | n_tumors_vi_present       |                     2580 |                           2580 |                                        2580 | set_to_zero             | 1=2202; 2=314; 3=47; 4=14; 5=3                   |

## Event context for TRUE→FALSE flippers

| family   | event_type            | finding_status   |   n_patients |   n_events | source_kinds_seen   | source_tables_seen                                                                 |
|:---------|:----------------------|:-----------------|-------------:|-----------:|:--------------------|:-----------------------------------------------------------------------------------|
| lvi      | lymphatic_microscopic | <NO_EVENT>       |            2 |          0 | <NULL>              | <NULL>                                                                             |
| lvi      | lymphatic_microscopic | absent           |         2500 |       3102 | structured          | main.canonical_path_malignant_events_v1                                            |
| lvi      | lymphatic_microscopic | indeterminate    |           10 |         20 | structured          | main.canonical_path_malignant_events_v1                                            |
| lvi      | lymphatic_microscopic | suspected        |            1 |          1 | structured          | main.canonical_path_malignant_events_v1                                            |
| vi       | vascular_microscopic  | absent           |         2580 |       7277 | llm | structured    | main.canonical_path_malignant_events_v1 | main.note_entities_llm_vascular_invasion |
| vi       | vascular_microscopic  | indeterminate    |           12 |         22 | llm | structured    | main.canonical_path_malignant_events_v1 | main.note_entities_llm_vascular_invasion |
| vi       | vascular_microscopic  | suspected        |            9 |         10 | llm                 | main.note_entities_llm_vascular_invasion                                           |

## Current derivative coverage by cohort

| family   | cohort                | column_name               |   n_patients |   non_null |   non_zero_or_non_blank |
|:---------|:----------------------|:--------------------------|-------------:|-----------:|------------------------:|
| lvi      | stable_true           | lvi_grade                 |          890 |        852 |                     852 |
| lvi      | stable_true           | lvi_ordinal_worst         |          890 |        890 |                     890 |
| lvi      | stable_true           | n_tumors_lvi_present      |          890 |        890 |                     890 |
| lvi      | false_or_null_to_true | lvi_grade                 |           99 |          5 |                       5 |
| lvi      | false_or_null_to_true | lvi_ordinal_worst         |           99 |          0 |                       0 |
| lvi      | false_or_null_to_true | n_tumors_lvi_present      |           99 |         92 |                       0 |
| lvi      | true_to_false         | lvi_grade                 |         2502 |       2460 |                    2460 |
| lvi      | true_to_false         | lvi_ordinal_worst         |         2502 |       2502 |                    2502 |
| lvi      | true_to_false         | n_tumors_lvi_present      |         2502 |       2502 |                    2502 |
| vi       | stable_true           | vasc_grade                |         1118 |       1118 |                    1117 |
| vi       | stable_true           | vasc_grade_final_v13      |         1118 |       1117 |                    1117 |
| vi       | stable_true           | vascular_invasion_final   |         1118 |       1117 |                    1117 |
| vi       | stable_true           | vascular_invasion_grade   |         1118 |       1117 |                    1117 |
| vi       | stable_true           | vascular_who_2022_grade   |         1118 |        392 |                     392 |
| vi       | stable_true           | vi_ordinal_worst          |         1118 |       1118 |                    1118 |
| vi       | stable_true           | vasc_vessel_count_v13     |         1118 |         46 |                      46 |
| vi       | stable_true           | vascular_vessel_count     |         1118 |         46 |                      46 |
| vi       | stable_true           | vi_vessels_max            |         1118 |         46 |                      46 |
| vi       | stable_true           | vasc_confidence_final_v13 |         1118 |       1117 |                    1117 |
| vi       | stable_true           | vasc_source_final_v13     |         1118 |       1117 |                    1117 |
| vi       | stable_true           | n_tumors_vi_present       |         1118 |       1118 |                    1118 |
| vi       | false_or_null_to_true | vasc_grade                |           60 |         60 |                      28 |
| vi       | false_or_null_to_true | vasc_grade_final_v13      |           60 |         28 |                      28 |
| vi       | false_or_null_to_true | vascular_invasion_final   |           60 |         28 |                      28 |
| vi       | false_or_null_to_true | vascular_invasion_grade   |           60 |         28 |                      28 |
| vi       | false_or_null_to_true | vascular_who_2022_grade   |           60 |          8 |                       8 |
| vi       | false_or_null_to_true | vi_ordinal_worst          |           60 |          0 |                       0 |
| vi       | false_or_null_to_true | vasc_vessel_count_v13     |           60 |          0 |                       0 |
| vi       | false_or_null_to_true | vascular_vessel_count     |           60 |          0 |                       0 |
| vi       | false_or_null_to_true | vi_vessels_max            |           60 |          0 |                       0 |
| vi       | false_or_null_to_true | vasc_confidence_final_v13 |           60 |         28 |                      28 |
| vi       | false_or_null_to_true | vasc_source_final_v13     |           60 |         28 |                      28 |
| vi       | false_or_null_to_true | n_tumors_vi_present       |           60 |         48 |                       0 |
| vi       | true_to_false         | vasc_grade                |         2580 |       2580 |                    2579 |
| vi       | true_to_false         | vasc_grade_final_v13      |         2580 |       2579 |                    2579 |
| vi       | true_to_false         | vascular_invasion_final   |         2580 |       2579 |                    2579 |
| vi       | true_to_false         | vascular_invasion_grade   |         2580 |       2579 |                    2579 |
| vi       | true_to_false         | vascular_who_2022_grade   |         2580 |          0 |                       0 |
| vi       | true_to_false         | vi_ordinal_worst          |         2580 |       2580 |                    2580 |
| vi       | true_to_false         | vasc_vessel_count_v13     |         2580 |          0 |                       0 |
| vi       | true_to_false         | vascular_vessel_count     |         2580 |          0 |                       0 |
| vi       | true_to_false         | vi_vessels_max            |         2580 |          0 |                       0 |
| vi       | true_to_false         | vasc_confidence_final_v13 |         2580 |       2579 |                    2579 |
| vi       | true_to_false         | vasc_source_final_v13     |         2580 |       2579 |                    2579 |
| vi       | true_to_false         | n_tumors_vi_present       |         2580 |       2580 |                    2580 |

## Governance boundary

This run did not execute any `UPDATE`, `CREATE`, `ALTER`, `DROP`, or registry mutation in MotherDuck. The generated SQL artifact is read-only probe SQL. Any apply lane must wait for Logan ratification of Option A vs Option B.
