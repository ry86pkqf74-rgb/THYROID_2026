# M044 data freeze: MotherDuck synchronization pass

- **Generated (UTC):** 2026-05-01T14:45:55Z
- **Database:** `thyroid_canonical_publication_v1_0.main` via `scripts/_md_connect.py::connect_locked()`
- **Scope:** Data and presentation synchronization only; literature/citation verification intentionally not performed.
- **Primary endpoint:** `recurrence_path_proven IS TRUE AND NOT COALESCE(is_implausible_date_quarantine, FALSE)`.
- **Secondary imaging-only endpoint:** `recurrence_status_final = 'imaging_only_unconfirmed'`.
- **Composite endpoint:** primary path-proven OR imaging-only unconfirmed.
- **Person-year rule:** numerator and denominator both restrict to `followup_years > 0`.

## Cohort, Date, and Follow-up Audit

| metric                 | value      |
|:-----------------------|:-----------|
| cohort_total           | 4128       |
| distinct_research_id   | 4128       |
| duplicate_research_ids | 0          |
| followup_zero          | 1400       |
| followup_positive      | 2728       |
| surg_date_nonmissing   | 4128       |
| surg_date_missing      | 0          |
| min_surg_first_date    | 1945-07-13 |
| max_surg_first_date    | 2025-08-01 |
| surg_date_pre1999      | 3          |
| surg_date_1999_2024    | 4090       |
| surg_date_after2024    | 35         |

## Recurrence Endpoint by ETE Group

| ete_group        |    n |   zero_fu_n |   positive_fu_n |   path_proven_n |   path_proven_pct |   imaging_only_n |   imaging_only_pct |   composite_n |   composite_pct |   imaging_then_path_n |   person_years_posfu |   path_per_100py_posfu |   composite_per_100py_posfu |
|:-----------------|-----:|------------:|----------------:|----------------:|------------------:|-----------------:|-------------------:|--------------:|----------------:|----------------------:|---------------------:|-----------------------:|----------------------------:|
| Microscopic ETE  | 2576 |         992 |            1584 |              80 |              3.11 |               13 |               0.5  |            93 |            3.61 |                     2 |               8137.3 |                   0.96 |                        1.09 |
| Gross ETE        | 1266 |         294 |             972 |             105 |              8.29 |                8 |               0.63 |           113 |            8.93 |                     3 |               4138.3 |                   2.49 |                        2.68 |
| No/negative ETE  |  192 |          74 |             118 |              18 |              9.38 |                1 |               0.52 |            19 |            9.9  |                     2 |                700.8 |                   2.43 |                        2.57 |
| Present ungraded |   29 |          11 |              18 |               1 |              3.45 |                0 |               0    |             1 |            3.45 |                     0 |                 86.5 |                   1.16 |                        1.16 |
| Missing/other    |   65 |          29 |              36 |               0 |              0    |                2 |               3.08 |             2 |            3.08 |                     0 |                 99.3 |                   0    |                        2.01 |

## Implausible-Date Quarantine Summary

| recurrence_path_proven_source   | is_implausible_date_quarantine   |   n |   path_n |   path_before_first_surg_n | min_path_date       | max_path_date       |   min_days_to_path |   max_days_to_path |
|:--------------------------------|:---------------------------------|----:|---------:|---------------------------:|:--------------------|:--------------------|-------------------:|-------------------:|
| structural_confirmed            | True                             |  14 |       14 |                         14 | 0202-12-30 00:00:00 | 2023-12-19 00:00:00 |            -665306 |                -24 |
| llm_path_keyword                | True                             |  10 |       10 |                         10 | 2019-08-29 00:00:00 | 2024-03-01 00:00:00 |              -1186 |                -18 |
| post_op_fna_b56                 | False                            |  99 |       99 |                          0 | 2001-01-29 00:00:00 | 2025-01-21 00:00:00 |                 91 |               6685 |
| multi_malignant_surgery         | False                            |  57 |       57 |                          0 | 2020-12-07 00:00:00 | 2025-02-06 00:00:00 |                 13 |               3588 |
| structural_confirmed            | False                            |  34 |       34 |                          0 | 2015-04-08 00:00:00 | 2025-08-01 00:00:00 |                  0 |               3136 |
| llm_path_keyword                | False                            |  14 |       14 |                          0 | 2012-05-10 00:00:00 | 2024-10-04 00:00:00 |                  0 |               4071 |

## Legacy Recurrence Flag Audit

|   m044_cohort_n |   legacy_any_recurrence_true_n |   legacy_any_true_canonical_status_none_n |   legacy_structural_recurrence_true_n |   legacy_structural_true_canonical_status_none_n |   canonical_recurrence_row_missing_n |
|----------------:|-------------------------------:|------------------------------------------:|--------------------------------------:|-------------------------------------------------:|-------------------------------------:|
|            4128 |                            503 |                                       316 |                                  1817 |                                             1659 |                                    0 |

## Strict-DTC Model Subset Audit

| metric                                                    |   value |
|:----------------------------------------------------------|--------:|
| strict_dtc_n                                              |    3789 |
| strict_3level_n                                           |    3756 |
| strict_3level_missing_tumor_size                          |       6 |
| strict_3level_model_complete_n                            |    3750 |
| strict_3level_path_including_quarantine                   |     215 |
| strict_3level_path_excluding_quarantine                   |     193 |
| strict_3level_positive_fu_model_complete_n                |    2521 |
| strict_3level_positive_fu_model_complete_path_excl_q      |     188 |
| strict_3level_date_1999_2024_model_complete_n             |    3717 |
| strict_3level_positive_fu_date_1999_2024_model_complete_n |    2500 |

## Validation Gates

| gate                               | status   | detail                                                                          |
|:-----------------------------------|:---------|:--------------------------------------------------------------------------------|
| G1 cohort_rows_distinct            | PASS     | 4128 rows / 4128 distinct                                                       |
| G2 duplicate_extract_ids           | PASS     | 0                                                                               |
| G3 no_primary_quarantined          | PASS     | 0                                                                               |
| G4 no_negative_primary_days        | PASS     | 0                                                                               |
| G5 py_numerator_subset_positive_fu | PASS     | 199 positive-FU primary events; 5 zero-FU primary events excluded from PY rates |
| G6 imaging_status_boolean_mismatch | PASS     | 0                                                                               |

## Quarantined Row Listing

The 24 path-proven rows quarantined for implausible pre-surgery dates are exported to `studies/m044_validation/m044_quarantined_path_proven_rows.csv`.

## Generated Audit Artifacts

- `studies/m044_validation/m044_live_motherduck_sync_audit.json`
- `studies/m044_validation/m044_quarantined_path_proven_rows.csv`
