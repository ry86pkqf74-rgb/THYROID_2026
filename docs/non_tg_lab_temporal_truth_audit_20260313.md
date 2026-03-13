# Non-Tg Lab Temporal Truth Audit — 20260313

## Summary

This document formalizes the temporal fidelity of each lab analyte in the
database. Thyroglobulin and anti-thyroglobulin have structured collection
dates from the lab system. All other analytes (PTH, calcium, TSH, etc.)
either have NLP-extracted dates or no data at all.

## Analysis Suitability Classification

| Tier | Meaning | Example Analytes |
|------|---------|------------------|
| time_to_event_eligible | Structured collection date; safe for postop-day analysis | thyroglobulin, anti_tg |
| postop_window_eligible_with_caveat | NLP date; acceptable for broad windows (0-30d, 31-180d) | pth, calcium_total |
| value_only_no_temporal | Values exist but dates are unreliable | (none currently) |
| no_data_source_absent | Zero measurements; institutional feed required | tsh, free_t4, vitamin_d |
| limited_temporal_fidelity | Mixed provenance; review per-measurement | calcium_ionized |

## Per-Analyte Coverage

| Analyte | Values | Patients | Date Coverage | Suitability |
|---------|--------|----------|---------------|-------------|
| thyroglobulin | 24,261 | 2,569 | 100.0% | time_to_event_eligible |
| anti_thyroglobulin | 14,305 | 2,127 | 100.0% | time_to_event_eligible |
| parathyroid_hormone | 797 | 673 | 17.4% | limited_temporal_fidelity |
| calcium_total | 595 | 559 | 11.6% | limited_temporal_fidelity |
| calcium_ionized | 3 | 3 | 0.0% | value_only_no_temporal |
| tsh | 0 | 0 | N/A | no_data_source_absent |
| free_t4 | 0 | 0 | N/A | no_data_source_absent |
| free_t3 | 0 | 0 | N/A | no_data_source_absent |
| vitamin_d | 0 | 0 | N/A | no_data_source_absent |
| albumin | 0 | 0 | N/A | no_data_source_absent |
| phosphorus | 0 | 0 | N/A | no_data_source_absent |
| magnesium | 0 | 0 | N/A | no_data_source_absent |
| calcitonin | 0 | 0 | N/A | no_data_source_absent |
| cea | 0 | 0 | N/A | no_data_source_absent |

## Critical Notes for Manuscript Authors

1. **Do NOT report PTH/calcium values as 'postoperative day X'** unless the
   `lab_date_status` is `exact_collection_date`. NLP-extracted dates have
   note-level granularity, not specimen-collection granularity.

2. **TSH, free T4/T3, vitamin D, albumin** have zero measurements. These
   are formally classified as `future_institutional_required`. The dashboard
   and manuscript should NOT imply these labs are available.

3. **Censored values** (e.g., '<0.2') are flagged with `is_censored = TRUE`.
   Use appropriate methods (e.g., Kaplan-Meier for censored Tg).

## Validation Tables Created

- `val_lab_temporal_truth_v1` — per-analyte temporal truth audit

Generated: 2026-03-13T14:11:41.737467