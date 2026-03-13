# RAI Structural Gap Maximization Audit — 20260313

## Summary

This document captures the end-state of RAI data coverage after all extraction,
refinement, and hardening phases. It identifies source limitations that cannot
be resolved without additional institutional data feeds.

## Source Limitation: Nuclear Medicine Reports

**Status: FIRST-CLASS STRUCTURAL LIMITATION**

Zero nuclear medicine notes exist in the `clinical_notes_long` corpus.
RAI dose/scan data relies entirely on endocrine clinic notes and discharge
summaries. This constrains dose coverage to approximately 41% of confirmed
RAI episodes. This limitation is encoded in `val_rai_source_limitation_v1`
as `nuclear_medicine_reports_absent`.

## Coverage Metrics

All metrics are sourced from `val_rai_structural_coverage_v1`.

| Metric | Value | Category |
|--------|-------|----------|
| total_surgical_cohort | 10,871 | denominator |
| dose_available | 761 | dose_missingness |
| dose_linkage_failed | 1,096 | dose_missingness |
| dose_no_source_report | 0 | dose_missingness |
| dose_source_no_value | 0 | dose_missingness |
| avg_dose_mci | 152.1 | dose_summary |
| median_dose_mci | 153 | dose_summary |
| confirmed_episodes | 49 | event_level |
| episodes_with_date | 1,272 | event_level |
| episodes_with_dose | 761 | event_level |
| exact_date_episodes | 434 | event_level |
| inferred_date_episodes | 838 | event_level |
| total_rai_episodes | 1,857 | event_level |
| unresolved_date_episodes | 585 | event_level |
| pts_with_dc_sum_notes | 169 | note_coverage |
| pts_with_endocrine_notes | 519 | note_coverage |
| pts_with_nuclear_med_notes | 0 | note_coverage |
| pts_with_op_notes | 4,439 | note_coverage |
| confirmed_patients | 35 | patient_level |
| patients_with_dose | 249 | patient_level |
| total_rai_patients | 862 | patient_level |
| tier_confirmed_no_dose | 0 | validation_tier |
| tier_confirmed_with_dose | 35 | validation_tier |
| tier_no_rai | 0 | validation_tier |
| tier_unconfirmed_no_dose | 821 | validation_tier |
| tier_unconfirmed_with_dose | 6 | validation_tier |
| validated_patients_total | 862 | validation_tier |

## RAI Assertion Status Taxonomy

| Status | Meaning | Manuscript Use |
|--------|---------|----------------|
| definite_received | Explicit documentation of RAI administration | Primary analysis |
| likely_received | Strong evidence but not explicitly stated | Primary with caveat |
| planned | RAI planned but completion not confirmed | Exclude from primary |
| historical | Reference to prior RAI in older notes | Exclude unless corroborated |
| negated | RAI explicitly not given / refused | Negative control |
| ambiguous | Unclear whether RAI was administered | Manual review queue |

## Dose Missingness Classification

| Category | Description |
|----------|-------------|
| dose_available | Dose value present in episode record |
| source_present_no_dose_stated | Endocrine/DC note exists but dose not mentioned |
| linkage_failed | NLP dose found but could not link to RAI episode |
| no_source_report_available | No endocrine, DC, or nuclear med notes for patient |

## Source Domain Availability

| Source | Status | Impact |
|--------|--------|--------|
| Nuclear medicine reports | ABSENT | Cannot improve dose coverage beyond ~41% |
| Endocrine clinic notes | PARTIAL | Primary RAI data source; coverage patient-dependent |
| Structured RAI orders | ABSENT | No pharmacy/order system integration |
| Discharge summaries | PARTIAL (1.6%) | Low yield for RAI-specific data |

## Validation Tables Created

- `val_rai_structural_coverage_v1` — 27 coverage metrics
- `val_rai_source_limitation_v1` — 5 source domain limitation entries

Generated: 2026-03-13T14:11:41.735178