# FAMILY_HX_THYROID full-scale — SF AI_CLASSIFY
**Generated:** 2026-05-03T23:46:03.961102
**Cohort:** 3,534 notes from `main.clinical_notes_long` (full keyword-positive corpus)
**AI_CLASSIFY elapsed:** 97.0s
**Distinct patients with ≥1 actionable extraction:** 2,794

## Yield
| Total notes | Actionable | NULL |
|---:|---:|---:|
| 3,534 | 2,968 (84.0%) | 3 |

## Distribution
| family_hx_status | n |
|---|---:|
| family_hx_thyroid_cancer_absent | 2,587 |
| family_hx_unknown_or_not_mentioned | 563 |
| family_hx_thyroid_cancer_present | 381 |
| None | 3 |

## Tables in Snowflake
- `THYROID_VALIDATION.PUBLIC.NLP_FAMILY_HX_THYROID_FULL_NOTES` (PHI text)
- `THYROID_VALIDATION.PUBLIC.NLP_FAMILY_HX_THYROID_FULL_RESULTS_v1` ← consumed by Cursor mig_281