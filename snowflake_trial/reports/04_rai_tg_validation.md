# Snowflake Cortex Validation — Prompt 4: RAI / Tg Kinetics
**Generated:** 2026-05-01 11:28:18
**Sources:** CANONICAL_PATIENT_MASTER_FLAT + CANONICAL_LABS_THYROGLOBULIN_V1_FLAT

---
## Tg availability × RAI receipt (malignant cohort)

| RAI_STATUS | N_PTS | N_WITH_TG | MEAN_TG_RESULTS |
| --- | --- | --- | --- |
| RAI_no | 3654 | 1889 | 8.5 |
| RAI_yes | 483 | 420 | 21.9 |

## Per-patient Tg longitudinal coverage

| N_PTS | N_TOTAL_RESULTS | MEAN_RESULTS_PER_PT | MAX_RESULTS_PER_PT |
| --- | --- | --- | --- |
| 3124 | 3124 | 17.0 | 259 |

## Tg value distribution (`VALUE_NUMERIC`, parsed via TRY_TO_DOUBLE)

| N_TOTAL | N_NON_NUMERIC | N_CENSORED | N_NEGATIVE | N_OVER_5000 | N_ZERO | MEAN_VAL | MEDIAN_VAL | MIN_VAL | MAX_VAL |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 51080 | 0 | 31005 | 0 | 57 | 1 | 32.24 | 0.9 | 0.0 | 25600.0 |

## Top-5 most longitudinally tracked patients

| RESEARCH_ID | N_RESULTS |
| --- | --- |
| 1350.0 | 259 |
| 625.0 | 162 |
| 3545.0 | 160 |
| 3080.0 | 140 |
| 3081.0 | 140 |

## AI_CLASSIFY: RAI×Tg concordance (30-pt sample)

- **Concordant:** 29
- **Discordant - RAI without followup:** 0
- **Discordant - Tg without RAI:** 1
- **Insufficient data:** 0
- **Other:** 0

