# VASC_INVASION pilot — SF AI_CLASSIFY
**Generated:** 2026-05-03T23:31:18.100367
**Cohort:** 100 random notes matching slice filter from `main.clinical_notes_long`
**AI_CLASSIFY elapsed:** 2.8s

## Yield

| Total | Actionable | NULL |
|---:|---:|---:|
| 100 | 49 (49.0%) | 0 |

## Distribution

| vasc_invasion_status | n |
|---|---:|
| vascular_invasion_unknown_or_not_mentioned | 51 |
| vascular_invasion_absent | 23 |
| vascular_invasion_present | 20 |
| vascular_invasion_extensive | 4 |
| vascular_invasion_focal | 2 |

## Tables created in Snowflake

- `THYROID_VALIDATION.PUBLIC.NLP_VASC_INVASION_PILOT_NOTES` (PHI text — keep in SF)
- `THYROID_VALIDATION.PUBLIC.NLP_VASC_INVASION_PILOT_RESULTS_v1` (research_id + classification only)

PHI policy: no note text in this report.