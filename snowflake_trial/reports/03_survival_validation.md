# Snowflake Cortex Validation — Prompt 3: Survival/Recurrence Integrity
**Generated:** 2026-05-01 10:47:14
**Source:** CANONICAL_PATIENT_MASTER_FLAT

---
## Vital status × death_occurred

| VITAL_STATUS | DEATH_OCCURRED | N |
| --- | --- | --- |
| alive | False | 10679 |
| deceased | True | 192 |

## Malignancy × recurrence flag

| IS_MALIGNANT | ANY_RECURRENCE_FLAG | N | MEAN_DAYS_TO_RECUR |
| --- | --- | --- | --- |
| False | False | 6728 | 1119.0 |
| False | True | 6 | 1263.0 |
| True | False | 3629 | 408.0 |
| True | True | 508 | 1470.0 |

## Deterministic contradiction probes

| probe | n_flagged |
| --- | --- |
| alive but death_occurred=TRUE | 0 |
| deceased but death_occurred=FALSE | 0 |
| any_recurrence_flag=TRUE but time_to_recurrence_days NULL | 0 |
| any_recurrence_flag=FALSE but time_to_recurrence_days NOT NULL | 740 |
| benign but recurrence flagged | 6 |
| followup_years > overall_survival_years (deceased pts) | 100 |

## AI_CLASSIFY: 50-sample consistency grading (malignant cohort)

- **Consistent:** 46
- **Minor discrepancy:** 4
- **Major contradiction:** 0
- **Insufficient data:** 0
- **Other:** 0
