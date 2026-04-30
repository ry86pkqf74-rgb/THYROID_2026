# mig_182 — CF-87-AJCC investigation

**Run ID:** `mig182_cf87_ajcc_investigation_20260429`  
**Run timestamp (UTC):** `2026-04-30T00:37:38.816327+00:00`  
**Posture:** read-only MotherDuck investigation; no production DDL/DML.  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Carry-forward:** `CF-87-AJCC` (36-column original path-malignant col-impact; 45 current registry mentions including downstream ETE-event replay columns).  

## Executive summary

- Registry lookup found **45** current `CF-87-AJCC`/AJCC-drift mentions across **2** tables.
- The original mig_87 path-malignant scope is **36** columns on `canonical_path_malignant_events_v1`; this is the 36-column col-impact referenced in the handoff.
- Original mig_87 proved faithful-copy equivalence to CTC pre-361; it did **not** prove that the copied AJCC values are derivationally correct from findings.
- The highest-risk manuscript question is patient-level AJCC8 source choice: canonical CPM, dominant-tumor/heterogeneity layer, path-event rollup, or fresh findings-derived stage.
- Recommended follow-up is **R2 now** for patient-level manuscript stability plus **R1 later** for full per-tumor derivation closure; no apply SQL is included in this lane.

## 1. CF-87-AJCC inventory

| table_name                         |   n_columns_tagged |   ajcc_stage_columns |   finding_or_input_columns | columns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|:-----------------------------------|-------------------:|---------------------:|---------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| canonical_ete_event_resolved_v1    |                  9 |                    4 |                          7 | ajcc_overall_stage, derived_t_stage_ajcc8, histology_variant, laterality, multifocal_flag, primary_histology, reported_t_stage_ajcc8, size_greatest_dimension_cm, t_stage_discordance_flag                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| canonical_path_malignant_events_v1 |                 36 |                   13 |                         10 | ajcc7_stage_calculable_flag, ajcc8_stage_calculable_flag, angioinvasion_quantify, capsular_invasion, data_completeness_pct, extranodal_extension, extrathyroidal_extension, gross_ete, histology_variant, laterality, ln_examined, ln_involved, lymphatic_invasion, m_stage_ajcc7, m_stage_ajcc8, margin_status, multifocality_flag, n_stage_ajcc7, n_stage_ajcc8, nodal_disease_positive_count, nodal_disease_total_count, number_of_tumors, overall_stage_ajcc7, overall_stage_ajcc8, perineural_invasion, primary_histology, site, size_greatest_dimension_cm, stage_group_ajcc7, stage_group_ajcc8, stage_migration_7_to_8, staging_source_note, t_stage_ajcc7, t_stage_ajcc8, tumor_size_cm_per_surgery, vascular_invasion |

### Complete registry rows

| schema_name   | table_name                         | column_name                  | batch_id                                  | verification_status   | notes_excerpt                                                                                                                                                                                                                                                                                                                                                                                          |
|:--------------|:-----------------------------------|:-----------------------------|:------------------------------------------|:----------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| main          | canonical_ete_event_resolved_v1    | ajcc_overall_stage           | mig121_ete_event_resolved_family_20260429 | verified              | | mig121: Staging column block replays trusted layer on path-malignant spine; reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). Derived/discordance/overall stage per manuscript rules in mig_61 family.                                                                                                                                                   |
| main          | canonical_ete_event_resolved_v1    | derived_t_stage_ajcc8        | mig121_ete_event_resolved_family_20260429 | verified              | | mig121: Staging column block replays trusted layer on path-malignant spine; reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). Derived/discordance/overall stage per manuscript rules in mig_61 family.                                                                                                                                                   |
| main          | canonical_ete_event_resolved_v1    | histology_variant            | mig121_ete_event_resolved_family_20260429 | verified              | | mig121: Staging column block replays trusted layer on path-malignant spine; reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). Derived/discordance/overall stage per manuscript rules in mig_61 family.                                                                                                                                                   |
| main          | canonical_ete_event_resolved_v1    | laterality                   | mig121_ete_event_resolved_family_20260429 | verified              | | mig121: Staging column block replays trusted layer on path-malignant spine; reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). Derived/discordance/overall stage per manuscript rules in mig_61 family.                                                                                                                                                   |
| main          | canonical_ete_event_resolved_v1    | multifocal_flag              | mig121_ete_event_resolved_family_20260429 | verified              | | mig121: Staging column block replays trusted layer on path-malignant spine; reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). Derived/discordance/overall stage per manuscript rules in mig_61 family.                                                                                                                                                   |
| main          | canonical_ete_event_resolved_v1    | primary_histology            | mig121_ete_event_resolved_family_20260429 | verified              | | mig121: Staging column block replays trusted layer on path-malignant spine; reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). Derived/discordance/overall stage per manuscript rules in mig_61 family.                                                                                                                                                   |
| main          | canonical_ete_event_resolved_v1    | reported_t_stage_ajcc8       | mig121_ete_event_resolved_family_20260429 | verified              | | mig121: Staging column block replays trusted layer on path-malignant spine; reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). Derived/discordance/overall stage per manuscript rules in mig_61 family.                                                                                                                                                   |
| main          | canonical_ete_event_resolved_v1    | size_greatest_dimension_cm   | mig121_ete_event_resolved_family_20260429 | verified              | | mig121: Staging column block replays trusted layer on path-malignant spine; reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). Derived/discordance/overall stage per manuscript rules in mig_61 family.                                                                                                                                                   |
| main          | canonical_ete_event_resolved_v1    | t_stage_discordance_flag     | mig121_ete_event_resolved_family_20260429 | verified              | | mig121: Staging column block replays trusted layer on path-malignant spine; reported_t_stage_ajcc8 matches pm.t_stage_ajcc8 on raw IS-NOT-DISTINCT join subset (0 drift). Derived/discordance/overall stage per manuscript rules in mig_61 family.                                                                                                                                                   |
| main          | canonical_path_malignant_events_v1 | ajcc7_stage_calculable_flag  | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | ajcc8_stage_calculable_flag  | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | angioinvasion_quantify       | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | capsular_invasion            | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | data_completeness_pct        | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | extranodal_extension         | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | extrathyroidal_extension     | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | gross_ete                    | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | histology_variant            | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | laterality                   | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | ln_examined                  | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | ln_involved                  | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | lymphatic_invasion           | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | m_stage_ajcc7                | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | m_stage_ajcc8                | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | margin_status                | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | multifocality_flag           | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | n_stage_ajcc7                | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | n_stage_ajcc8                | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | nodal_disease_positive_count | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | nodal_disease_total_count    | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | number_of_tumors             | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | overall_stage_ajcc7          | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | overall_stage_ajcc8          | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | perineural_invasion          | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | primary_histology            | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | site                         | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | size_greatest_dimension_cm   | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | stage_group_ajcc7            | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | stage_group_ajcc8            | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | stage_migration_7_to_8       | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | staging_source_note          | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | t_stage_ajcc7                | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | t_stage_ajcc8                | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | tumor_size_cm_per_surgery    | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |
| main          | canonical_path_malignant_events_v1 | vascular_invasion            | mig_87_path_malignant_ctc_equivalence     | verified              | | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- 6 join-duplicate artifact) under faithful-copy equivalence join on (rid, sid, ordinal, surgery_date, synoptic_row_ix). CTC pre361 archive is the immediate upstream that produced canonical via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging derivation correctness deferred (upstream concern in CTC build). |

## 2. Original mig_87 context (verbatim)

> CF-87-AJCC: AJCC7/8 staging values inherited from CTC pre361 are verified
> --     as faithful copies. The findings-vs-staging derivation correctness
> --     (Logan's airway-invasion rule extended to ETE/multifocality/nodal) is a
> --     separate validation question that operates UPSTREAM of canonical (in
> --     CTC's build pipeline, scripts 251/266). Defer to a future round that
> --     either (a) restores CTC and validates its staging derivation against
> --     findings, or (b) re-derives staging post-canonical from the verified
> --     finding columns and audits diff vs current values.

Interpretation: mig_87 verified copied values, not clinical derivation correctness. AJCC7 and AJCC8 columns involved in the original 36-column batch are `t/n/m/overall/stage_group_ajcc7` and `t/n/m/overall/stage_group_ajcc8`, plus the calculability flags and staging-source metadata.

## 3. Drift quantification on live data

### Path-event internal findings-vs-stored-stage probes

| dimension              |   rows_total |   stored_non_null |   inferred_non_null |   paired_non_null |   paired_mismatches |   paired_mismatch_pct |
|:-----------------------|-------------:|------------------:|--------------------:|------------------:|--------------------:|----------------------:|
| ajcc8_t_stage          |         6689 |              6443 |                6195 |              6182 |                1781 |                 28.81 |
| ajcc8_n_positive_group |         6689 |              6298 |                2542 |              2525 |                  92 |                  3.64 |

Top stored-vs-inferred AJCC8 T distributions are exported in CSV; top rows are shown here:

| stored_t   | inferred_t   |   n_rows |
|:-----------|:-------------|---------:|
| T1A        | T1A          |     2250 |
| T1B        | T1B          |      954 |
| T2         | T2           |      794 |
| T1A        | T3B          |      648 |
| T3A        | T3A          |      369 |
| T1B        | T3B          |      330 |
| T2         | T3B          |      280 |
| T3A        | T3B          |      263 |
| nan        | nan          |      233 |
| T2         | nan          |       93 |
| T1B        | nan          |       68 |
| T1A        | nan          |       50 |
| T3B        | T1B          |       47 |
| T3B        | T2           |       45 |
| T3A        | nan          |       43 |
| T3B        | T3A          |       38 |
| T3B        | T3B          |       34 |
| T3B        | T1A          |       34 |
| T1B        | T1A          |       28 |
| T2         | T1A          |       24 |
| T3A        | T1A          |       17 |
| nan        | T3B          |       13 |
| T3B        | nan          |        6 |
| T2         | T1B          |        5 |
| T4A        | T1A          |        5 |
| T3A        | T1B          |        4 |
| T4A        | T3B          |        4 |
| T3A        | T2           |        3 |
| T1A        | T1B          |        2 |
| T4A        | T1B          |        2 |

### Patient-level cross-source stage comparisons

| comparison                                                            | available   |   n_rows |   left_non_null |   right_non_null |   paired_non_null |   mismatches |   mismatch_pct_of_paired |
|:----------------------------------------------------------------------|:------------|---------:|----------------:|-----------------:|------------------:|-------------:|-------------------------:|
| ajcc8_t_stage vs dominant_tumor_ajcc8_t_stage                         | True        |     4137 |            4128 |             3994 |              3992 |         1141 |                    28.58 |
| ajcc8_t_stage vs ajcc8_t_stage_with_microete_t3b_DEPRECATED           | True        |     4137 |            4128 |             4091 |              4091 |            1 |                     0.02 |
| ajcc8_t_stage vs ajcc8_t_stage_v2                                     | True        |     4137 |            4128 |             4130 |              4128 |          363 |                     8.79 |
| ajcc8_stage_group vs dominant_tumor_ajcc8_stage_group                 | True        |     4137 |            4128 |             3605 |              3600 |          123 |                     3.42 |
| ajcc8_stage_group vs ajcc8_stage_group_corrected                      | True        |     4137 |            4128 |             4131 |              4125 |           14 |                     0.34 |
| ajcc8_stage_group vs ajcc8_stage_group_v2                             | True        |     4137 |            4128 |             4127 |              4122 |         2160 |                    52.4  |
| ajcc8_t_stage vs path_worst_ajcc8_t_stage                             | True        |     4137 |            4128 |             3994 |              3992 |         1141 |                    28.58 |
| dominant_tumor_ajcc8_t_stage vs path_worst_ajcc8_t_stage              | True        |     4137 |            3994 |             3994 |              3994 |            0 |                     0    |
| ajcc8_t_stage vs path_worst_inferred_ajcc8_t_from_findings            | True        |     4137 |            4128 |             3993 |              3991 |          428 |                    10.72 |
| path_worst_ajcc8_t_stage vs path_worst_inferred_ajcc8_t_from_findings | True        |     4137 |            3994 |             3993 |              3993 |         1030 |                    25.8  |
| ajcc8_n_stage vs path_worst_ajcc8_n_stage                             | True        |     4137 |            4077 |             4083 |              4077 |         2064 |                    50.63 |
| ajcc8_m_stage vs path_worst_ajcc8_m_stage                             | True        |     4137 |            4137 |             4137 |              4137 |            0 |                     0    |
| ajcc7_t_stage vs path_worst_ajcc7_t_stage                             | True        |     4137 |            4127 |             3994 |              3994 |          926 |                    23.18 |
| ajcc7_stage_group vs path_worst_ajcc7_stage_group                     | True        |     4137 |            3882 |             3863 |              3863 |         2028 |                    52.5  |

## 4. Cross-source reconciliation coverage

### Path-event coverage

| source_table                       | column_name                |   non_null_rows |   total_rows |   non_null_pct |
|:-----------------------------------|:---------------------------|----------------:|-------------:|---------------:|
| canonical_path_malignant_events_v1 | t_stage_ajcc7              |            6443 |         6689 |          96.32 |
| canonical_path_malignant_events_v1 | n_stage_ajcc7              |            6632 |         6689 |          99.15 |
| canonical_path_malignant_events_v1 | m_stage_ajcc7              |            6689 |         6689 |         100    |
| canonical_path_malignant_events_v1 | overall_stage_ajcc7        |            6224 |         6689 |          93.05 |
| canonical_path_malignant_events_v1 | stage_group_ajcc7          |            6224 |         6689 |          93.05 |
| canonical_path_malignant_events_v1 | t_stage_ajcc8              |            6443 |         6689 |          96.32 |
| canonical_path_malignant_events_v1 | n_stage_ajcc8              |            6632 |         6689 |          99.15 |
| canonical_path_malignant_events_v1 | m_stage_ajcc8              |            6689 |         6689 |         100    |
| canonical_path_malignant_events_v1 | overall_stage_ajcc8        |            5851 |         6689 |          87.47 |
| canonical_path_malignant_events_v1 | stage_group_ajcc8          |            5851 |         6689 |          87.47 |
| canonical_path_malignant_events_v1 | size_greatest_dimension_cm |            6088 |         6689 |          91.02 |
| canonical_path_malignant_events_v1 | extrathyroidal_extension   |            6244 |         6689 |          93.35 |
| canonical_path_malignant_events_v1 | gross_ete                  |            1571 |         6689 |          23.49 |
| canonical_path_malignant_events_v1 | ln_examined                |            4421 |         6689 |          66.09 |
| canonical_path_malignant_events_v1 | ln_involved                |            2576 |         6689 |          38.51 |
| canonical_path_malignant_events_v1 | extranodal_extension       |            1706 |         6689 |          25.5  |
| canonical_path_malignant_events_v1 | multifocality_flag         |               0 |         6689 |           0    |

### Canonical patient master coverage

| source_table             | column_name                                  |   non_null_rows |   total_rows |   non_null_pct |
|:-------------------------|:---------------------------------------------|----------------:|-------------:|---------------:|
| canonical_patient_master | ajcc7_t_stage                                |            4130 |        10871 |          37.99 |
| canonical_patient_master | ajcc7_n_stage                                |            4137 |        10871 |          38.06 |
| canonical_patient_master | ajcc7_m_stage                                |           10871 |        10871 |         100    |
| canonical_patient_master | ajcc7_stage_group                            |            3882 |        10871 |          35.71 |
| canonical_patient_master | ajcc8_t_stage                                |            4175 |        10871 |          38.4  |
| canonical_patient_master | ajcc8_t_stage_with_microete_t3b_DEPRECATED   |            4138 |        10871 |          38.06 |
| canonical_patient_master | ajcc8_t_stage_v2                             |            4133 |        10871 |          38.02 |
| canonical_patient_master | ajcc8_n_stage                                |            5490 |        10871 |          50.5  |
| canonical_patient_master | ajcc8_n_stage_v2                             |            4137 |        10871 |          38.06 |
| canonical_patient_master | ajcc8_m_stage                                |           10871 |        10871 |         100    |
| canonical_patient_master | ajcc8_m_stage_v2                             |           10871 |        10871 |         100    |
| canonical_patient_master | ajcc8_stage_group                            |            4131 |        10871 |          38    |
| canonical_patient_master | ajcc8_stage_group_corrected                  |            7816 |        10871 |          71.9  |
| canonical_patient_master | ajcc8_stage_group_v2                         |            4130 |        10871 |          37.99 |
| canonical_patient_master | dominant_tumor_ajcc8_t_stage                 |            3999 |        10871 |          36.79 |
| canonical_patient_master | dominant_tumor_ajcc8_n_stage                 |            5496 |        10871 |          50.56 |
| canonical_patient_master | dominant_tumor_ajcc8_m_stage                 |            8422 |        10871 |          77.47 |
| canonical_patient_master | dominant_tumor_ajcc8_stage_group             |            3610 |        10871 |          33.21 |
| canonical_patient_master | n_tumors_ajcc8_staged                        |            8422 |        10871 |          77.47 |
| canonical_patient_master | tumor_stage_heterogeneous_t_ajcc8_flag       |            8422 |        10871 |          77.47 |
| canonical_patient_master | tumor_stage_heterogeneous_overall_ajcc8_flag |            8422 |        10871 |          77.47 |

## 5. Manuscript-impact assessment

| metric                                                        |     n | denominator        |   pct_of_denominator |
|:--------------------------------------------------------------|------:|:-------------------|---------------------:|
| cpm_total_rows                                                | 10871 | all_cpm_rows       |               100    |
| cpm_malignant_rows                                            |  4137 | all_cpm_rows       |                38.06 |
| malignant_with_cpm_ajcc8_t_stage                              |  4128 | cpm_malignant_rows |                99.78 |
| malignant_with_dominant_tumor_ajcc8_t_stage                   |  3994 | cpm_malignant_rows |                96.54 |
| malignant_with_path_event_ajcc8_t_stage                       |  3994 | cpm_malignant_rows |                96.54 |
| malignant_cpm_vs_dominant_ajcc8_t_stage_diff                  |  1141 | cpm_malignant_rows |                27.58 |
| malignant_cpm_vs_dominant_ajcc8_stage_group_shift             |   123 | cpm_malignant_rows |                 2.97 |
| malignant_path_stored_vs_findings_inferred_ajcc8_t_stage_diff |  1030 | cpm_malignant_rows |                24.9  |
| malignant_with_any_scoped_cross_source_stage_discordance      |  3275 | cpm_malignant_rows |                79.16 |

The denominator is CPM malignant patients where `is_malignant` is TRUE when available. Cross-source discordance here should be interpreted as a scoping queue, not an automatic correction list: some differences are expected because per-tumor path-event rollups, dominant-tumor patient-level staging, and legacy CPM fields operate at different grains.

## 6. R1/R2/R3 fix-plan options

| option                             | scope                                                                                                                                                               | pros                                                                                                                                   | cons                                                                                                                                                                        | recommendation                                                                               |
|:-----------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------|
| R1_rederive_from_verified_findings | Recompute AJCC staging from verified finding columns into new *_resolved columns; keep legacy cols unchanged until sign-off.                                        | Directly addresses CF-87's findings-vs-staging question; auditable row-level diff; avoids overwriting ratified legacy copies.          | Requires formal AJCC7/8 derivation spec for T/N/M/stage group, including nodal location and M-stage rules; current path-event findings cannot fully distinguish N1a vs N1b. | Best long-term manuscript-grade closure after Logan ratifies exact rules.                    |
| R2_priority_resolved_stage         | Create patient-level resolved AJCC columns with priority dominant-tumor/heterogeneity layer > canonical patient master > path-event rollup, plus discordance flags. | Lowest practical manuscript disruption; leverages 266b/266c dominant-tumor layer already aligned to canonical CPM at high concordance. | Does not prove every per-tumor path-event stage is derivationally correct; it is a patient-level resolution strategy.                                                       | Recommended immediate follow-up if the manuscript needs a stable patient-level stage source. |
| R3_flag_only_no_stage_mutation     | Leave all stage columns unchanged; add/export discordance and manuscript-impact review queues only.                                                                 | Safest governance posture; no production staging mutation; preserves CF as transparent caveat.                                         | Does not close the semantic CF; downstream analysts must choose among stage columns manually.                                                                               | Acceptable only if Logan wants no apply lane before manuscript lock.                         |

## 7. Generated local artifacts

- `exports/mig182_cf87_ajcc_investigation_20260429/mig182_registry_inventory.csv`
- `exports/mig182_cf87_ajcc_investigation_20260429/mig182_registry_family_summary.csv`
- `exports/mig182_cf87_ajcc_investigation_20260429/mig182_path_internal_drift.csv`
- `exports/mig182_cf87_ajcc_investigation_20260429/mig182_path_t_stage_distribution.csv`
- `exports/mig182_cf87_ajcc_investigation_20260429/mig182_path_t_stage_mismatch_examples.csv`
- `exports/mig182_cf87_ajcc_investigation_20260429/mig182_patient_cross_source_drift.csv`
- `exports/mig182_cf87_ajcc_investigation_20260429/mig182_manuscript_impact.csv`
- `exports/mig182_cf87_ajcc_investigation_20260429/mig182_patient_discordance_examples.csv`
- `exports/mig182_cf87_ajcc_investigation_20260429/manifest.json`

## Governance boundary

This migration lane did not execute any `UPDATE`, `CREATE`, `ALTER`, `DROP`, or registry mutation in MotherDuck. The R1/R2/R3 plan requires Logan ratification before any apply lane is authored.
