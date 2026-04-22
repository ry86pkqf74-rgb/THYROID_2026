# CPM TIRADS column inventory

- Total CPM columns: **1585**
- TIRADS-related columns (regex match): **58**
- → Audit-target columns (excluding NLP_*): **53**
- → NLP-coverage columns (excluded from audit): **5**

## Audit columns

| pos | column_name | data_type | n_populated |
|---:|---|---|---:|
| 251 | `imaging_tirads_source` | VARCHAR | 3474 |
| 333 | `max_tirads_ever` | BIGINT | 3439 |
| 729 | `preop_tirads_best` | BIGINT | 3474 |
| 730 | `preop_tirads_category` | VARCHAR | 3474 |
| 731 | `preop_tirads_worst` | BIGINT | 3474 |
| 944 | `tirads_best_category_v12` | VARCHAR | 3474 |
| 945 | `tirads_best_combined` | INTEGER | 3439 |
| 946 | `tirads_best_score_v12` | BIGINT | 3474 |
| 947 | `tirads_concordant_count_v12` | BIGINT | 3474 |
| 948 | `tirads_has_acr_recalc_v12` | BOOLEAN | 3474 |
| 949 | `tirads_mismatch_count_v12` | BIGINT | 3474 |
| 950 | `tirads_n_nodule_records_v12` | BIGINT | 3474 |
| 951 | `tirads_n_sources_v12` | BIGINT | 3474 |
| 952 | `tirads_nodule_size_max_mm_v12` | DOUBLE | 3439 |
| 953 | `tirads_nodules_scored_combined` | BIGINT | 3439 |
| 954 | `tirads_reliability_v12` | DOUBLE | 3474 |
| 955 | `tirads_source_v12` | VARCHAR | 3474 |
| 956 | `tirads_worst_category_v12` | VARCHAR | 3474 |
| 957 | `tirads_worst_combined` | INTEGER | 3439 |
| 958 | `tirads_worst_score_v12` | BIGINT | 3474 |
| 992 | `worst_tirads_category` | VARCHAR | 3439 |
| 1249 | `imaging_tirads_best` | BIGINT | 3474 |
| 1250 | `imaging_updated_tirads_category_cpm_v1` | VARCHAR | 3474 |
| 1251 | `imaging_tirads_worst` | BIGINT | 3474 |
| 1514 | `tirads_worst_points_v271` | DOUBLE | 1326 |
| 1515 | `tirads_best_points_v271` | DOUBLE | 1326 |
| 1516 | `tirads_source_system_v271` | VARCHAR | 1326 |
| 1517 | `imaging_laterality_rollup` | VARCHAR | 3439 |
| 1519 | `pathology_vs_imaging_laterality_concordant` | BOOLEAN | 3364 |
| 1520 | `tumor_pathology_laterality_v271b` | VARCHAR | 3986 |
| 1525 | `imaging_laterality_rollup_v271b` | VARCHAR | 3439 |
| 1526 | `pathology_vs_imaging_laterality_concordant_v271b` | VARCHAR | 10871 |
| 1527 | `tirads_v2_n_nodules_scored` | BIGINT | 2465 |
| 1528 | `tirads_v2_worst_category` | VARCHAR | 2819 |
| 1529 | `tirads_v2_max_points` | DOUBLE | 1357 |
| 1530 | `tirads_v2_largest_nodule_cm` | DOUBLE | 2440 |
| 1531 | `tirads_v2_any_ete_on_us` | BOOLEAN | 2465 |
| 1532 | `tirads_v2_any_interval_growth` | BOOLEAN | 2465 |
| 1533 | `tirads_v2_any_fna_recommended` | BOOLEAN | 2465 |
| 1534 | `tirads_v2_n_reports` | BIGINT | 4073 |
| 1535 | `tirads_v2_any_suspicious_ln_on_us` | BOOLEAN | 1498 |
| 1536 | `tirads_v2_shortest_followup_months` | DOUBLE | 806 |
| 1551 | `tirads_v2_worst_rank` | INTEGER | 2465 |
| 1552 | `tirads_v2_worst_rank_source` | VARCHAR | 2465 |
| 1553 | `tirads_v2_any_fna_recommended_report` | BOOLEAN | 4073 |
| 1554 | `tirads_v2_any_fna_recommended_report_source` | VARCHAR | 4073 |
| 1575 | `imaging_tirads_best_v2` | VARCHAR | 1226 |
| 1576 | `imaging_tirads_worst_v2` | VARCHAR | 1226 |
| 1577 | `imaging_updated_tirads_category_cpm_v2` | VARCHAR | 1226 |
| 1578 | `imaging_laterality_rollup_v2` | VARCHAR | 3439 |
| 1579 | `max_tirads_ever_v2` | DOUBLE | 1300 |
| 1580 | `preop_tirads_best_v2` | VARCHAR | 1043 |
| 1581 | `preop_tirads_category_v2` | VARCHAR | 1043 |

## NLP-coverage columns (excluded from audit)

| pos | column_name | data_type | n_populated |
|---:|---|---|---:|
| 505 | `nlp_tirads_has_component_detail` | BOOLEAN | 1714 |
| 506 | `nlp_tirads_has_data` | BOOLEAN | 1715 |
| 507 | `nlp_tirads_max_category` | VARCHAR | 1714 |
| 508 | `nlp_tirads_n_entities` | BIGINT | 1715 |
| 509 | `nlp_tirads_n_notes` | BIGINT | 1715 |
