# Reviewer Defense: How Was Recurrence Defined and Dated?

## Recurrence Source

Recurrence is defined from the **structured institutional recurrence registry** via `recurrence_risk_features_mv`, not from NLP extraction. The source flag is a boolean (`recurrence_flag`) that was populated by clinical abstractors and cross-validated against two independent data streams.

## Detection Categories

| Category | N Patients | Date Precision | Method |
|----------|-----------|----------------|--------|
| Structural confirmed | 54 | Exact source date | Imaging/biopsy-confirmed with documented date |
| Biochemical only | 168 | Inferred inflection point | Tg trajectory analysis (see criteria below) |
| Structural, date unknown | 1,764 | Flag only, no day-level date | Registry flag without event date |
| **Total recurrence** | **1,986** | — | **18.3% of 10,871** |

Among analysis-eligible cancer patients (N=4,136): 1,933 (46.7%) flagged — the high rate reflects the cancer-enriched surgical denominator.

## Biochemical Recurrence Criteria

Thyroglobulin (Tg) trajectory from `thyroglobulin_labs` (30,245 measurements across 2,569 patients):

1. Last Tg value > **1.0 ng/mL**
2. Last Tg > **2× Tg nadir** for that patient
3. No concurrent structural evidence of disease

This identifies patients with rising tumor markers in the absence of radiographic or pathologic confirmation — classified as `biochemical_only` with the date set to the Tg inflection point.

## Date Availability for Time-to-Event Analysis

| Date Tier | N | % of Recurrences | TTE Suitability |
|-----------|---|-------------------|-----------------|
| Exact source date | 54 | 2.7% | Yes |
| Biochemical inflection (inferred) | 168 | 8.5% | Conditional |
| Unresolved (flag only) | 1,764 | 88.8% | No |
| Not applicable (no recurrence) | 8,885 | — | Censored |

Survival analyses use the 222 patients with usable event dates plus all censored patients. The 88.8% unresolved-date rate is a **source limitation**: the institutional recurrence registry provides a boolean flag without day-level date for most patients. Without a structured recurrence date registry, these cannot be recovered.

## Multi-Source Corroboration

`extracted_recurrence_refined_v1` cross-links each recurrence event to up to 3 source tables:

- `recurrence_risk_features_mv` (structured flag)
- `thyroglobulin_labs` (Tg trajectory)
- `extracted_clinical_events_v4` (clinical event timeline)

**54% of recurrence events** (1,072/1,986) have multi-source corroboration.

## What Recurrence Is NOT

- NOT from NLP entity extraction (raw NLP `recurrence` events in `extracted_clinical_events_v4` are contaminated — 6,405 events from single words "recurrence/recurrent" in H&P notes)
- NOT from single Tg measurement alone (requires trajectory pattern)
- NOT from imaging reports alone (requires registry flag confirmation)

## Key References

- **Structured source**: `recurrence_risk_features_mv` (MotherDuck, 4,976 rows, 3,986 unique patients)
- **Refined recurrence table**: `extracted_recurrence_refined_v1` (MotherDuck, 10,871 rows)
- **Tg labs**: `thyroglobulin_labs` (MotherDuck, 30,245 values, 2,569 patients)
- **Date readiness audit**: `val_recurrence_readiness_v1` (MotherDuck, 10 rows)
- **Recurrence detection view**: `vw_recurrence_by_detection_method` (4 rows)
- **Phase 8 report**: `notes_extraction/phase8_final_report.md`
- **Structural gap doc**: `docs/recurrence_structural_gap_maximization_20260313.md`
