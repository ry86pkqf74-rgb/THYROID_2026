# VASC_INVASION full-scale — SF AI_CLASSIFY
**Generated:** 2026-05-03T23:46:23.994373
**Cohort:** 806 notes from `main.clinical_notes_long` (full keyword-positive corpus)
**AI_CLASSIFY elapsed:** 16.6s
**Distinct patients with ≥1 actionable extraction:** 375

## Yield
| Total notes | Actionable | NULL |
|---:|---:|---:|
| 806 | 421 (52.2%) | 2 |

## Distribution
| vasc_invasion_status | n |
|---|---:|
| vascular_invasion_unknown_or_not_mentioned | 383 |
| vascular_invasion_absent | 221 |
| vascular_invasion_present | 167 |
| vascular_invasion_focal | 20 |
| vascular_invasion_extensive | 13 |
| None | 2 |

## Tables in Snowflake
- `THYROID_VALIDATION.PUBLIC.NLP_VASC_INVASION_FULL_NOTES` (PHI text)
- `THYROID_VALIDATION.PUBLIC.NLP_VASC_INVASION_FULL_RESULTS_v1` ← consumed by Cursor mig_281