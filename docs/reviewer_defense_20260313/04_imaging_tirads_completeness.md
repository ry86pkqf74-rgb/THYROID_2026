# Reviewer Defense: How Complete Were Imaging/TIRADS Data?

## TIRADS Coverage Summary

| Metric | Value |
|--------|-------|
| Patients with TIRADS | 3,474 of 10,871 (32.0%) |
| Baseline before enrichment | 4.19% |
| Improvement factor | 7.7x |

## Data Sources

| Source | Records | Patients | Content |
|--------|---------|----------|---------|
| `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` | 6,793 reports | 4,074 | Per-nodule ACR criteria (composition, echogenicity, shape, margins, calcifications, dimensions) |
| `US Nodules TIRADS 12_1_25.xlsx` | 14 sheets | ~10,862 each | Radiologist-assigned TIRADS scores only |
| NLP from clinical notes | 417 extractions | 417 | Regex pattern `TI-?RADS\s*[1-5]` from h_p and op notes |

## ACR TI-RADS Recalculation

Points computed per Tessler et al. 2017 JACR:
- Composition (0–2) + Echogenicity (0–3) + Shape (0/3) + Margin (0–3) + Echogenic foci (0–3)
- Total: 0 = TR1, 1–2 = TR2, 3 = TR3, 4–6 = TR4, ≥7 = TR5

**Concordance with radiologist scores: 80.1%** (15,671 of 19,572 evaluable records). Systematic mismatch: radiologists tend to score **1 tier lower** than ACR recalculation (mean difference −1.0), consistent with published observation that clinical judgment leads to borderline nodule downgrading.

## TIRADS Distribution

| Category | N | % |
|----------|---|---|
| TR1 Not Suspicious | 68 | 2.0% |
| TR2 Minimally Suspicious | 156 | 4.5% |
| TR3 Mildly Suspicious | 882 | 25.4% |
| TR4 Moderately Suspicious | 1,530 | 44.0% |
| TR5 Highly Suspicious | 838 | 24.1% |

Combined TR4+TR5 = 68.1% — consistent with a surgical cohort enriched for suspicious nodules warranting intervention.

## Nodule Size Data

| Metric | Value |
|--------|-------|
| Patients with nodule size | 3,051 (23.7%) |
| Structured Excel source | 3,439 patients (per-nodule measurements in mm) |
| NLP source | 1 patient |
| Median size | 3.2 cm |
| Range | 0.2–12.5 cm |
| Plausibility guard | 0.1–15.0 cm |

## Known Limitations

1. **68% of patients lack TIRADS data** — ultrasound was performed at external facilities, pre-dates the institutional reporting period, or was not documented in extracted data sources
2. **`imaging_nodule_long_v2`** canonical V2 table has ALL size/TIRADS columns NULL on MotherDuck (schema exists, data not populated from Excel sources); the separately built `imaging_nodule_master_v1` (19,891 per-nodule records) and `extracted_tirads_validated_v1` (3,474 rows) have full dimensional data
3. **CT/MRI/PET imaging**: exists only in clinical note free-text; no structured radiology report tables available. ENE imaging assessment (CT 369 patients, US 465, PET 63) was extracted from notes separately

## Key References

- **Validated TIRADS**: `extracted_tirads_validated_v1` (MotherDuck, 3,474 rows)
- **Per-nodule master**: `imaging_nodule_master_v1` (MotherDuck, 19,891 rows)
- **ACR concordance**: `val_phase12_tirads_validation` (MotherDuck, 4 rows)
- **TIRADS summary view**: `vw_us_nodule_tirads_validated` (MotherDuck, 5 rows)
- **Phase 12 engine**: `notes_extraction/extraction_audit_engine_v10.py`
- **Excel ingestion**: Phase 12 `ingest_complete_us_excel()` and `ingest_tirads_scored_excel()`
