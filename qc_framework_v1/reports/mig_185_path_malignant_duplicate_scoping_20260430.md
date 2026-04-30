# mig_185 — canonical_path_malignant_events_v1 duplicate scoping

**Date:** 2026-04-30  
**Posture:** READ-ONLY scoping. No MotherDuck DDL/DML executed.  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Trigger:** Logan rid 2480 duplicate finding during R1 size CSV review.  

## Executive summary

Read-only probes confirm `main.canonical_path_malignant_events_v1` has **6,689 rows** and **6,156 distinct `(research_id, surgery_episode_id, tumor_ordinal)` grains**, for **533 excess duplicate rows** across **491 duplicate grains**.

The duplicate signal is already present in the Script 361 upstream archive `canonical_tumor_characteristics_v1_pre361_20260422_002245`, with the same **6,689 / 6,156 / 533** row/distinct/excess pattern. This supports the lineage finding that Script 361 faithfully copied pre-existing CTC duplicate grain rows rather than introducing them with later date-retype or mig178 passes.

## §1 Duplicate-pattern classification

### Rows per duplicate grain

|   n_rows |   n_grains_with_this_count |   total_event_rows |
|---------:|---------------------------:|-------------------:|
|        2 |                        457 |                914 |
|        3 |                         27 |                 81 |
|        4 |                          6 |                 24 |
|        5 |                          1 |                  5 |

### Bucket definitions used

- `A_fully_identical`: all 56 columns are identical within the duplicate grain.
- `B_differs_in_audit_only`: only `build_script`, `build_ts`, `consolidation_source`, or `extracted_at` differ.
- `C_differs_in_synoptic_or_source_only`: clinical fields are identical, but source/provenance fields differ (`synoptic_row_ix`, `specimen_id`, `path_surgery_id`, `source_tables`, linkage fields, etc.).
- `D_differs_clinically`: at least one clinical/finding field differs (size, histology, ETE, invasion, margin, LN, stage, multifocality, completeness, etc.).

### Classification summary

| bucket                               |   n_grains |   duplicate_excess_rows |   total_event_rows |
|:-------------------------------------|-----------:|------------------------:|-------------------:|
| C_differs_in_synoptic_or_source_only |          1 |                       1 |                  2 |
| D_differs_clinically                 |        490 |                     532 |               1022 |

### NULL surgery-episode contribution

| bucket                               | surgery_episode_id_is_null   |   n_grains |   duplicate_excess_rows |
|:-------------------------------------|:-----------------------------|-----------:|------------------------:|
| C_differs_in_synoptic_or_source_only | False                        |          1 |                       1 |
| D_differs_clinically                 | False                        |        429 |                     463 |
| D_differs_clinically                 | True                         |         61 |                      69 |

**Important interpretation:** no duplicate grains are fully identical across all columns. The rid 2480 finding is identical on the selected review fields in the prompt, but the full-row comparison shows differences in source identifiers plus `site` / `data_completeness_pct`. In other words, the current 533 excess rows are mostly **source-distinct or clinically distinct rows sharing an under-specified logical key**, not simple byte-identical row repeats.

## §2 Script 361 lineage trace

Script 361 Step 1 (`scripts/361_op_path_consolidation.py`) builds `canonical_path_malignant_events_v1` with a faithful filtered copy:

```sql
CREATE OR REPLACE TABLE main.canonical_path_malignant_events_v1 AS
SELECT *
FROM main.canonical_tumor_characteristics_v1
WHERE primary_histology IS NOT NULL
  AND TRIM(CAST(primary_histology AS VARCHAR)) <> '';
```

The script then adds discordance/linkage/provenance columns. There is **no `ROW_NUMBER()` / `QUALIFY` dedupe gate** on `(research_id, surgery_episode_id, tumor_ordinal)` in Step 1.

### Lineage duplicate counts

| table                                                                                                 |   total_rows |   distinct_grains |   duplicate_excess | error   |
|:------------------------------------------------------------------------------------------------------|-------------:|------------------:|-------------------:|:--------|
| main.canonical_path_malignant_events_v1                                                               |         6689 |              6156 |                533 |         |
| "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_tumor_characteristics_v1_pre361_20260422_002245     |         6689 |              6156 |                533 |         |
| "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_date_retype_20260428   |         6689 |              6156 |                533 |         |
| "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig178_20260429_205720 |         6689 |              6156 |                533 |         |
| "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig178_20260429_205801 |         6689 |              6156 |                533 |         |
| "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_events_v1_pre_mig178_20260429_205813 |         6689 |              6156 |                533 |         |

**Conclusion:** the 533 excess duplicate rows trace back to `canonical_tumor_characteristics_v1_pre361_20260422_002245`. Later archives preserve the same duplicate count, so later date-retype / mig178 operations did not create the duplicate grain problem.

## §3 `synoptic_row_ix` tiebreaker analysis

| bucket                               | has_multi_syn   |   n_grains |   duplicate_excess_rows |
|:-------------------------------------|:----------------|-----------:|------------------------:|
| C_differs_in_synoptic_or_source_only | True            |          1 |                       1 |
| D_differs_clinically                 | False           |          1 |                       1 |
| D_differs_clinically                 | True            |        489 |                     531 |

`canonical_path_malignant_events_v1.synoptic_row_ix` is populated on current rows. For duplicate grains, multi-`synoptic_row_ix` patterns are concentrated in the source/provenance-only and clinical-difference buckets. This indicates most duplicate grains represent multiple source synoptic rows mapped to the same logical `(rid, surgery_episode_id, tumor_ordinal)` event. There is no meaningful fully-identical cleanup opportunity in the live table.

Sample rows for Logan spot-check are exported to `exports/mig185_dedupe_scoping_20260430/duplicate_examples.csv`. Full grain classification is exported to `exports/mig185_dedupe_scoping_20260430/duplicate_grain_classification.csv`.

## §4 Dedupe rule recommendation for Logan ratification

| Rule | Result from scoping | Recommendation |
|---|---:|---|
| R-A: drop fully-identical duplicates only | 0 excess rows | Safe but **no-op** on current live data; useful as a guardrail, not as the mig_185 fix. |
| R-B: dedupe by `(rid, surg_ep, tumor_ord, synoptic_row_ix)` keeping max `build_ts` | 3 excess rows | Also nearly no-op; confirms `synoptic_row_ix` is usually source-distinct and should be preserved rather than collapsed. |
| R-C: dedupe by `(rid, surg_ep, tumor_ord)` keeping maximum completeness | Aggressive | Not recommended without manual review of Bucket D because it can discard clinically distinct multi-row evidence. |

**Recommended Logan decision:** do **not** ratify a blind delete on the current key. Ratify a follow-up design lane that either (1) expands the path-malignant event grain to include a stable source-row/focus discriminator such as `synoptic_row_ix` / `specimen_id`, or (2) preserves all Bucket C/D rows in a review/staging table before any R-C-style one-row-per-key collapse. If an immediate patient-rollup correction is needed, recompute `n_tumors_total` from distinct `(research_id, surgery_episode_id, tumor_ordinal)` while leaving event rows untouched.

## §5 Logan spot-check samples

See:

- `exports/mig185_dedupe_scoping_20260430/duplicate_examples.csv`
- `exports/mig185_dedupe_scoping_20260430/duplicate_grain_classification.csv`
- `exports/mig185_dedupe_scoping_20260430/lineage_duplicate_counts.csv`
- `exports/mig185_dedupe_scoping_20260430/downstream_impact_metrics.csv`

## §6 Downstream impact

| domain                                     | metric                                             | value                                                                                                                                                                                                                                                                                                                                                | note                                                                 |
|:-------------------------------------------|:---------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:---------------------------------------------------------------------|
| path_malignant_events                      | total_rows                                         | 6689                                                                                                                                                                                                                                                                                                                                                 |                                                                      |
| path_malignant_events                      | distinct_key_grains                                | 6156                                                                                                                                                                                                                                                                                                                                                 |                                                                      |
| path_malignant_events                      | duplicate_excess_rows                              | 533                                                                                                                                                                                                                                                                                                                                                  |                                                                      |
| path_malignant_events                      | duplicate_grains                                   | 491                                                                                                                                                                                                                                                                                                                                                  |                                                                      |
| canonical_path_malignant_patient_rollup_v1 | patients_compared                                  | 4137                                                                                                                                                                                                                                                                                                                                                 | n_tumors_total uses COUNT(*) over event rows                         |
| canonical_path_malignant_patient_rollup_v1 | patients_inflated                                  | 466                                                                                                                                                                                                                                                                                                                                                  | n_tumors_total uses COUNT(*) over event rows                         |
| canonical_path_malignant_patient_rollup_v1 | tumor_count_excess                                 | 533                                                                                                                                                                                                                                                                                                                                                  | n_tumors_total uses COUNT(*) over event rows                         |
| canonical_invasion_events_v1               | columns                                            | invasion_event_id;research_id;invasion_type;finding_status;source_modality;source_kind;source_table;source_row_id;finding_date;linked_surgery_episode_id;linked_path_malignant_event_id;linkage_method;n_candidate_episodes;linkage_ambiguous_multi_finding;confidence;evidence_span_hash;evidence_qualifier;extraction_run_id;build_script;build_ts | schema inventory                                                     |
| canonical_invasion_events_v1               | invasion_rows_total                                | 58582                                                                                                                                                                                                                                                                                                                                                | Joinable at patient+surgery only; no tumor_ordinal in invasion table |
| canonical_invasion_events_v1               | invasion_rows_on_duplicate_surgery_grains          | 0                                                                                                                                                                                                                                                                                                                                                    | Joinable at patient+surgery only; no tumor_ordinal in invasion table |
| canonical_invasion_events_v1               | patients_with_invasion_on_duplicate_surgery_grains | 0                                                                                                                                                                                                                                                                                                                                                    | Joinable at patient+surgery only; no tumor_ordinal in invasion table |
| canonical_us_lymph_node_patient_rollup_v2  | rows                                               | 10871                                                                                                                                                                                                                                                                                                                                                | patient-grain rollup; not joined at path-event key                   |
| canonical_us_lymph_node_patient_rollup_v2  | distinct_patients                                  | 10871                                                                                                                                                                                                                                                                                                                                                | patient-grain rollup; not joined at path-event key                   |
| canonical_us_lymph_node_patient_rollup_v2  | duplicate_patient_rows                             | 0                                                                                                                                                                                                                                                                                                                                                    | patient-grain rollup; not joined at path-event key                   |

Key impact interpretation:

1. `canonical_path_malignant_patient_rollup_v1.n_tumors_total` is inflated because Script 361 uses `COUNT(*)` over malignant events. The excess equals the duplicate-row excess where duplicate event rows pass through the rollup.
2. Boolean/max patient rollup fields are less sensitive, but mode/COUNT-derived fields can be affected.
3. `canonical_invasion_events_v1` is not keyed to `tumor_ordinal`; it can only be compared at patient+surgery grain, so path-event duplicate cleanup should be validated against invasion linkage before any aggressive collapse.
4. `canonical_us_lymph_node_patient_rollup_v2` is patient-grain and not directly duplicated by the path malignant event key; it should be treated as a downstream contextual rollup rather than an event-grain victim.

## Governance

This report and companion SQL are read-only scoping deliverables. No writes were made to MotherDuck. Any dedupe apply must be ratified by Logan before execution.
