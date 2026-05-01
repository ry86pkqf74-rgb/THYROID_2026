# mig_253 surgical procedure type dry-run

Run timestamp UTC: `20260501T103539Z`
Migration path: `qc_framework_v1/migrations/253_surg_procedure_type_fill_20260501.sql`

No `main.*` tables were mutated. All derivations were computed in session-scoped TEMP tables.

## Baseline gap
| n_total | null_proc_type | null_all_three |
| --- | --- | --- |
| 10871 | 2138 | 2138 |

## Source coverage among all-three-NULL patients
| null_patients | has_first_surgery_date | has_n_surgeries | has_gland_weight | has_histology | has_nsqip_cpt |
| --- | --- | --- | --- | --- | --- |
| 2138 | 2138 | 2138 | 1595 | 916 | 348 |

## Proposed resolution by source
| resolution_source | proposed_surg_procedure_type | proposed_surg_total_thyroidectomy | proposed_surg_hemithyroidectomy | n_patients |
| --- | --- | --- | --- | --- |
| canonical_operative_procedure_codes_v1 | total_thyroidectomy | True | False | 1066 |
| canonical_operative_procedure_codes_v1 | hemithyroidectomy | False | True | 586 |
| nsqip_cpt | total_thyroidectomy | True | False | 348 |
| path_synoptics | other | False | False | 46 |
| path_synoptics | hemithyroidectomy | False | True | 37 |
| path_synoptics | total_thyroidectomy | True | False | 24 |
| canonical_operative_procedure_codes_v1 | other | False | False | 22 |
| path_synoptics | isthmusectomy | False | False | 7 |
| unresolved | None | None | None | 2 |

## Proposed procedure distribution
| proposed_surg_procedure_type | n_pts_resolved |
| --- | --- |
| total_thyroidectomy | 1438 |
| hemithyroidectomy | 623 |
| other | 68 |
| isthmusectomy | 7 |
| None | 2 |

## Simulated post-mig_253 CPM gap
| cpm_rows | simulated_null_proc_type | simulated_null_all_three |
| --- | --- | --- |
| 10871 | 2 | 2 |

## Simulated M038 >=200g distribution
| surg_procedure_type | surg_total_thyroidectomy | surg_hemithyroidectomy | n |
| --- | --- | --- | --- |
| total_thyroidectomy | True | False | 400 |
| hemithyroidectomy | False | True | 74 |
| other | False | False | 1 |

## Consistency checks
| total_type_but_total_flag_not_true | hemi_type_but_hemi_flag_not_true | both_total_and_hemi_true | resolved_type_null_total_flag | resolved_type_null_hemi_flag |
| --- | --- | --- | --- | --- |
| 0 | 0 | 0 | 0 | 0 |

## Pre-apply QC gate state
| gate1_verified_tables | gate2_missing_signoff | gate3_count_mismatch | gate4_verified_cols_missing_metadata | gate5_clinical_date_violations | cohort_parity_ok |
| --- | --- | --- | --- | --- | --- |
| 218 | 0 | 0 | 0 | 0 | True |

## Residual follow-up
Residual unresolved rows are exported to `manuscript_outputs/audit/mig253_residual_surg_proc_type_review.csv`.
