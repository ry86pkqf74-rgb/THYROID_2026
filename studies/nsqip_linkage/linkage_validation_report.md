# NSQIP Case Details Linkage Validation Report

**Generated:** 2026-03-10
**Source:** Case_Details_and_Custom_Fields_Report-14-Dec-2025-1204.xlsx (1,281 rows)
**Scope:** All rows are thyroidectomy cases (CPT 60220–60271) with Custom Fields module data

## 1. Match Statistics

| Metric | Value |
|--------|-------|
| Total NSQIP Case Details rows | 1,281 |
| Perfect deterministic matches | 1,275 |
| Unmatched | 6 |
| **Match rate** | **99.5%** |
| Unique patients linked | 1,261 |
| Patients with 2 NSQIP surgeries | 14 |

## 2. Match Methods

| Method | Rows | Description |
|--------|------|-------------|
| EXISTING_LINKAGE | 1,086 | Reused from verified `nsqip_thyroid_linkage_final.csv` (prior round) |
| MRN_DATE_EXACT | 185 | NSQIP IDN = EUH_MRN + exact surgery date in master_cohort |
| MRN_DATE_DISAMBIG | 2 | MRN mapped to multiple patients, disambiguated by exact surgery date |
| MRN_DOB_MULTISURGERY | 2 | MRN + DOB + sex + age all confirmed; master_cohort records a different surgery date (multi-surgery patient with NSQIP capturing an earlier procedure) |

## 3. Relationship to Prior Linkage

This Case Details report overlaps with but differs from the prior `Thyroid NSQIP dataset 2010-2023.xlsx`:

| Metric | Prior linkage | This file |
|--------|--------------|-----------|
| Source rows | 2,280 (1,813 thyroid CPT) | 1,281 (all thyroid) |
| Case Numbers in both | 1,089 | 1,089 |
| Cases only in prior | 724 | — |
| Cases only here | — | 192 |
| Custom Fields columns | 285 (same) | 285 |

The 192 new cases include 2024 operations not in the prior 2010–2023 extract.

## 4. Verification Protocol

All matches verified by at least two independent criteria:

**EXISTING_LINKAGE (1,086):** Previously verified via MRN + sex concordance + age concordance + surgery date confirmation across 4 source files.

**MRN_DATE_EXACT (185):** IDN matched to EUH_MRN in ≥1 of 4 source files (synoptic, op_sheet, notes, complications), AND surgery date matched exactly to master_cohort. Sex verified where available.

**MRN_DATE_DISAMBIG (2):** IDN mapped to multiple patients via MRN; disambiguated by exact surgery date yielding a unique 1:1 match. Sex verified.

**MRN_DOB_MULTISURGERY (2):** MRN confirmed in 3 independent source files, DOB matched, sex matched, age concordant. Surgery date differs because master_cohort records a later procedure for the same patient.

## 5. Verification Samples (first 10 matched rows)

| research_id | Case# | IDN | Op Date | CPT | Age | Sex | MC Sex | MC Age | MC Date | Method |
|------------|-------|-----|---------|-----|-----|-----|--------|--------|---------|--------|
| 1618 | 589 | 1027369 | 01/05/2010 | 60240 | 26.0 | — | female | 25 | 2010-01-05 | EXISTING_LINKAGE |
| 1460 | 594 | 1015180 | 01/05/2010 | 60260 | 50.0 | — | female | 49 | 2010-01-05 | EXISTING_LINKAGE |
| 1899 | 610 | 1066664 | 01/06/2010 | 60240 | 41.0 | — | female | 40 | 2010-01-06 | EXISTING_LINKAGE |
| 1934 | 674 | 1072224 | 01/19/2010 | 60240 | 35.0 | — | female | 34 | 2010-01-19 | EXISTING_LINKAGE |
| 3259 | 675 | 1056469 | 01/19/2010 | 60240 | 59.0 | — | female | 58 | 2010-01-19 | EXISTING_LINKAGE |
| 3261 | 680 | 1072857 | 01/20/2010 | 60240 | 79.0 | — | male | 78 | 2010-01-20 | EXISTING_LINKAGE |
| 3262 | 683 | 1072080 | 01/20/2010 | 60260 | 51.0 | — | female | 50 | 2010-01-20 | EXISTING_LINKAGE |
| 3265 | 714 | 1064341 | 01/26/2010 | 60240 | 39.1 | — | female | 38 | 2010-01-26 | EXISTING_LINKAGE |
| 1958 | 730 | 1043348 | 01/27/2010 | 60240 | 30.1 | — | female | 29 | 2010-01-27 | EXISTING_LINKAGE |
| 1423 | 734 | 1066984 | 01/27/2010 | 60260 | 59.1 | — | female | 58 | 2010-01-27 | EXISTING_LINKAGE |

## 6. Unmatched Rows (6)

| IDN | Case# | Date | Age | Sex | CPT | Reason |
|-----|-------|------|-----|-----|-----|--------|
| 2108711 | 112133 | 03/18/2014 | 77.2 | — | 60240 | IDN not in any source file; DOB not found; only date-match (RID=4344) is different patient (age diff 16yr) |
| 2628518 | 138509 | 08/19/2022 | 87.6 | Male | 60240 | MRN collision: IDN maps to RID=9738 (female, age 37) — confirmed different patient |
| 12017569 | 142049 | 03/04/2024 | 75.8 | Male | 60260 | IDN/LMRN/LCN all absent from sources; DOB matches RID=9787 in timeline but no MRN confirmation — insufficient for deterministic match |
| 19003467 | 142221 | 04/02/2024 | 36.1 | Female | 60252 | IDN/LMRN/LCN all absent from sources; DOB not found; no viable candidate |
| 7828861 | 143386 | 08/28/2024 | 27.7 | Female | 60260 | IDN/LMRN/LCN all absent from sources; DOB not found; no viable candidate |
| 1386262 | 144346 | 11/20/2024 | 38.0 | Female | 60240 | IDN/LMRN/LCN all absent from sources; no surgery on that date in cohort; DOB not found; patient likely not yet in data extract |

## 7. Enrichment Outputs

| File | Rows | Columns | Description |
|------|------|---------|-------------|
| `exports/nsqip/nsqip_enrichment.parquet` | 1,275 | 95 | Surgery-level (one row per matched NSQIP case) |
| `exports/nsqip/nsqip_patient_summary.parquet` | 1,261 | 95 | Patient-level (first surgery per patient) |
| `studies/nsqip_linkage/case_details_linkage_results.csv` | 1,281 | Full linkage with match status |

## 8. Manuscript Content

Manuscript-ready statistics, limitations paragraph, and methods text are in:
**`studies/nsqip_pth_protocol_manuscript/`**

## 9. Usage (LEFT JOIN pattern)

```sql
SELECT mc.*, ns.*
FROM master_cohort mc
LEFT JOIN read_parquet('exports/nsqip/nsqip_patient_summary.parquet') ns
  ON mc.research_id = CAST(ns.research_id AS VARCHAR);
```

No existing tables were modified. All enrichment columns are prefixed with `nsqip_`.
