# FAMILY_HX_THYROID pilot — SF AI_CLASSIFY
**Generated:** 2026-05-03T23:31:12.240523
**Cohort:** 100 random notes matching slice filter from `main.clinical_notes_long`
**AI_CLASSIFY elapsed:** 3.1s

## Yield

| Total | Actionable | NULL |
|---:|---:|---:|
| 100 | 84 (84.0%) | 0 |

## Distribution

| family_hx_status | n |
|---|---:|
| family_hx_thyroid_cancer_absent | 76 |
| family_hx_unknown_or_not_mentioned | 16 |
| family_hx_thyroid_cancer_present | 8 |

## Tables created in Snowflake

- `THYROID_VALIDATION.PUBLIC.NLP_FAMILY_HX_THYROID_PILOT_NOTES` (PHI text — keep in SF)
- `THYROID_VALIDATION.PUBLIC.NLP_FAMILY_HX_THYROID_PILOT_RESULTS_v1` (research_id + classification only)

PHI policy: no note text in this report.