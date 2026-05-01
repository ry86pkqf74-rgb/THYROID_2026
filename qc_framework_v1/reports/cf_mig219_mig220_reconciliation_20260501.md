# CF-mig219 + CF-mig220 Reconciliation Report

**Run ID:** `cf_mig219_mig220_reconciliation_20260501`  
**Generated:** 2026-05-01T04:13:21.856051+00:00  
**Git HEAD:** `358cf7b658212a8ccb5205f119710c38e82cc0e5`  
**Mode:** read-only MotherDuck SELECT probes; no DDL/DML executed.

## Executive Conclusion

| Carry-forward | Status | Conclusion |
|---|---|---|
| CF-mig220-QUEUE-CURRENT-V2-DRIFT | closed | All 2506 distinct high-priority queue keys now map to `main.canonical_us_nodule_v2`; missing keys = 0. No remediation migration needed. |
| CF-mig219-NOT-FULLY-PARSED-COUNT-DRIFT | closed_with_methods_clarification | Live view count remains 24,371, not the planning expectation of 8,243. The delta is explained by the mig221 semantic clarification: the live view uses descriptor completeness (`acr2017_feature_points_complete=FALSE`), not derived-point/category missingness. |

## CF-mig220 Probe

The Cowork preflight probe was re-run exactly at current MotherDuck state.

|   distinct_high_pri_keys |   keys_in_v2 |   keys_missing_from_v2 |
|-------------------------:|-------------:|-----------------------:|
|                     2506 |         2506 |                      0 |

Field-level reconciliation confirms the same high-priority scope is represented in the canonical row tags.

| field_name         |   queue_rows |   nonblank_value_rows |   key_mapped_rows |   nonblank_mapped_rows |   canonical_rows_tagged |
|:-------------------|-------------:|----------------------:|------------------:|-----------------------:|------------------------:|
| tirads_category_v2 |          123 |                   123 |               123 |                    123 |                     120 |
| tirads_reported    |         2494 |                  2494 |              2494 |                   2494 |                    2491 |
| tirads_score_2017  |           23 |                    23 |                23 |                     23 |                      23 |

**Decision:** close CF-mig220. The v13 carry-forward was a transient queue/current-table drift that is no longer present after the current `canonical_us_nodule_v2` state.

## CF-mig219 Probe

The live `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` count is still 24,371, while the ChatGPT planning note expected 8,243. The DDL filter in mig219 matches the live direct filter exactly.

|   direct_filter_rows |   view_rows |   direct_minus_view |
|---------------------:|------------:|--------------------:|
|                24371 |       24371 |                   0 |

Key count decomposition:

| metric                                                   |     n |
|:---------------------------------------------------------|------:|
| any_reported_and_derived_points_or_cat_missing           |  7317 |
| any_reported_and_descriptor_not_complete                 | 24371 |
| any_reported_descriptor_complete_but_not_strict          |    13 |
| any_reported_descriptor_incomplete_and_derived_missing   |  7304 |
| any_reported_descriptor_not_complete_but_derived_present | 17067 |
| any_reported_has_reported_text_only_no_acr_no_updated    |  5098 |
| base_nonaggregate_nonshell                               | 34371 |
| current_minus_doc_expected                               | 16128 |
| doc_expected_reported_not_fully_parsed                   |  8243 |
| view_any_reported                                        | 29504 |
| view_reported_not_fully_parsed_current                   | 24371 |
| view_strict_acr2017                                      |  5120 |

Cross-tab of descriptor completeness versus derived ACR availability:

| descriptor_state      | derived_state                       | has_reported_text   | has_acr_category   | has_updated_category   |    n |
|:----------------------|:------------------------------------|:--------------------|:-------------------|:-----------------------|-----:|
| descriptor_complete   | derived_points_and_category_present | False               | True               | False                  |   57 |
| descriptor_complete   | derived_points_and_category_present | False               | True               | True                   |   75 |
| descriptor_complete   | derived_points_and_category_present | True                | True               | False                  | 2898 |
| descriptor_complete   | derived_points_and_category_present | True                | True               | True                   | 2090 |
| descriptor_complete   | derived_points_or_category_missing  | False               | False              | True                   |   12 |
| descriptor_complete   | derived_points_or_category_missing  | True                | False              | True                   |    1 |
| descriptor_incomplete | derived_points_and_category_present | False               | True               | False                  |  549 |
| descriptor_incomplete | derived_points_and_category_present | False               | True               | True                   |  289 |
| descriptor_incomplete | derived_points_and_category_present | True                | True               | False                  | 8926 |
| descriptor_incomplete | derived_points_and_category_present | True                | True               | True                   | 7303 |
| descriptor_incomplete | derived_points_or_category_missing  | False               | False              | True                   |  782 |
| descriptor_incomplete | derived_points_or_category_missing  | True                | False              | False                  | 5098 |
| descriptor_incomplete | derived_points_or_category_missing  | True                | False              | True                   | 1424 |

### Diagnosis

The 16,128-row apparent drift is not evidence that the view is malformed. It is a definition mismatch:

- The mig219 view defines "reported not fully parsed" as any reported TIRADS signal with `acr2017_feature_points_complete=FALSE`.
- The mig221 clarification states that `acr2017_feature_points_complete` means all five upstream ACR descriptor fields were present in the legacy CUNC source. It is not equivalent to "derived ACR points/category are missing" after later normalized-feature backfills.
- Current live data show only 7,317 any-reported rows with missing derived points or category, but 17,067 rows whose descriptors are incomplete while derived points and category are present.

Therefore the 24,371-row live view is internally consistent with the applied mig219 DDL and mig221 semantics. The 8,243 planning expectation should not be used as a manuscript count unless the manuscript explicitly intends the narrower derived-missing definition, which would be a different view/filter.

## Recommendation

1. Mark CF-mig220 closed with `closed_in_mig=mig_222/current_v2_absorption` and no follow-up migration.
2. Mark CF-mig219 closed as a semantics reconciliation: retain the live mig219 view count (24,371) for descriptor-incomplete TIRADS reporting.
3. If manuscript Methods need a smaller "derived ACR unavailable" denominator, create a separately named view in a future migration; do not reinterpret `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1`.

## Evidence Artifacts

CSV evidence and manifest are in `exports/cf_mig219_mig220_reconciliation_20260501/`.
