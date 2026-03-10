# NSQIP Linkage Validation Report

**Generated:** 2026-03-10
**Source:** Thyroid NSQIP dataset 2010-2023.xlsx (2,280 rows)
**Scope:** Thyroid CPT codes only (60220-60271); 467 non-thyroid rows excluded

## 1. Match Statistics

| Metric | Value |
|--------|-------|
| Total NSQIP thyroid rows | 1,813 |
| Perfect deterministic matches | 1,803 |
| Unmatched | 10 |
| **Match rate** | **99.4%** |
| Unique patients linked | 1,728 |
| Patients with 2 NSQIP surgeries | 75 |

## 2. Match Methods

| Method | Rows | Description |
|--------|------|-------------|
| EUH_MRN | 1,725 | NSQIP IDN = EUH_MRN (direct hospital MRN) |
| COMPOSITE | 54 | surgery_date + sex + age-adjusted (unique 1:1) |
| DOB_CROSSREF | 12 | DOB matched across path_synoptics / op_sheet |
| TEC_MRN | 12 | NSQIP IDN = TEC_MRN (second hospital MRN system) |

## 3. Verification Protocol

Every MRN-based match was independently verified:
- **Sex concordance:** 99.7% (1,735/1,740 initial MRN matches)
- **Age concordance (within 1 year):** 97.7% (1,700/1,740)
- **Surgery date in master_timeline:** 99.2% (1,685/1,699)

34 initially discordant rows were resolved via:
- 22 multi-surgery timing gaps (NSQIP captured earlier surgery)
- 5 DOB-verified age corrections
- 4 additional TEC_MRN/DOB cross-references
- 3 REJECTED as MRN collisions (sex + age wildly discordant)

## 4. Verification Samples (10 matched rows)

| research_id | NSQIP IDN | Op Date | Age | Sex | Hypocalcemia | LOS | Readmit | Ca/VitD |
|-------------|-----------|---------|-----|-----|-------------|-----|---------|---------|
| 247 | 113202 | 2014-08-04 | 36 |  | No | 0 | 0 | None |
| 467 | 120661 | 2017-02-08 | 33 |  | No | 1 | 0 | Calcium only |
| 626 | 116962 | 2015-10-27 | 36 |  | No | 1 | 0 | None |
| 627 | 113335 | 2014-08-19 | 34 |  | No | 1 | 0 | None |
| 628 | 116319 | 2015-08-04 | 23 |  | No | 1 | 0 | None |
| 629 | 114114 | 2014-11-25 | 38 |  | No | 1 | 0 | Calcium only |
| 630 | 116914 | 2015-10-21 | 45 |  | No | 1 | 0 | Both calcium and vit |
| 631 | 116648 | 2015-09-16 | 34 |  | No | 1 | 0 | None |
| 631 | 116905 | 2015-10-20 | 32 |  | No | 1 | 0 | Both calcium and vit |
| 632 | 112249 | 2014-04-16 | 33 |  | No | 1 | 0 | Both calcium and vit |

## 5. Unmatched Rows (10)

| IDN | Case# | Date | Age | Sex | CPT | Reason |
|-----|-------|------|-----|-----|-----|--------|
| 654445 | 106441 | 08/01/2012 | 57.6 | Male | 60260 | REJECT_MRN_COLLISION |
| 2108711 | 112133 | 03/18/2014 | 77.2 | Female | 60240 | NO_MATCH |
| 2628518 | 138509 | 08/19/2022 | 87.6 | Male | 60240 | REJECT_MRN_COLLISION |
| 676382 | 100754 | 09/22/2010 | 68.7 | Female | 60220 | NO_MATCH |
| 1100800 | 101047 | 11/17/2010 | 60.9 | Female | 60220 | NO_MATCH |
| 1162818 | 108275 | 01/29/2013 | 41.1 | Female | 60220 | NO_MATCH |
| 1113843 | 108813 | 03/25/2013 | 29.2 | Female | 60220 | REJECT_MRN_COLLISION |
| 1182219 | 112454 | 04/23/2014 | 50.3 | Male | 60220 | NO_MATCH |
| 2198345 | 115472 | 04/17/2015 | 61.3 | Male | 60220 | NO_MATCH |
| 1200783 | 115818 | 06/01/2015 | 59.4 | Male | 60220 | NO_MATCH |

## 6. Enrichment Outputs

| File | Rows | Columns | Description |
|------|------|---------|-------------|
| `exports/nsqip/nsqip_enrichment.parquet` | 1,803 | 80 | Surgery-level (one row per NSQIP case) |
| `exports/nsqip/nsqip_patient_summary.parquet` | 1,728 | 80 | Patient-level (first surgery per patient) |
| `studies/nsqip_linkage/nsqip_thyroid_linkage_final.csv` | 1,813 | Full linkage with match status |

## 7. Usage (LEFT JOIN pattern)

```sql
SELECT mc.*, ns.*
FROM master_cohort mc
LEFT JOIN nsqip_patient_summary ns
  ON mc.research_id = ns.research_id;
```

No existing tables were modified. All enrichment columns are prefixed with `nsqip_`.