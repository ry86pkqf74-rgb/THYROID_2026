# SMOKING full-scale — SF AI_CLASSIFY
**Generated:** 2026-05-03T23:44:20.534302
**Cohort:** 3,541 notes from `main.clinical_notes_long` (full keyword-positive corpus)
**AI_CLASSIFY elapsed:** 92.5s
**Distinct patients with ≥1 actionable extraction:** 3,013

## Yield
| Total notes | Actionable | NULL |
|---:|---:|---:|
| 3,541 | 3,280 (92.6%) | 3 |

## Distribution
| smoking_status | n |
|---|---:|
| never_smoker | 2,501 |
| former_smoker | 552 |
| unknown_or_not_mentioned | 258 |
| current_smoker | 227 |
| None | 3 |

## Tables in Snowflake
- `THYROID_VALIDATION.PUBLIC.NLP_SMOKING_FULL_NOTES` (PHI text)
- `THYROID_VALIDATION.PUBLIC.NLP_SMOKING_FULL_RESULTS_v1` ← consumed by Cursor mig_281