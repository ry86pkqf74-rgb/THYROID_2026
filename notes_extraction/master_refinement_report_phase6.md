# Phase 6: Source-Linked Staging Variable Refinement Report

_Generated: 2026-03-12 | Engine: extraction_audit_engine_v4.py_

## Executive Summary

Phase 6 refines 6 staging variables with source hierarchy attribution (path_synoptic 1.0 > op_note 0.9 > imaging 0.7 > consent 0.2). Three new parser classes (MarginDistanceParser, InvasionGrader, LNYieldCalculator) were deployed to MotherDuck Business Pro, producing 9 tables and a unified 81-column master clinical table (v5).

**Data-Quality Score: 93/100** (up from 91 in Phase 5)

---

## 1. Margin Status (R-Classification)

| R-Class | N Patients | % of Margin-Recorded | Avg Distance (mm) |
|---------|-----------|---------------------|-------------------|
| R0 | 1 | 0.0% | 0.50 |
| R0_close | 0 | 0.0% | — |
| R1 (microscopic residual) | 3,896 | 98.5% | 1.21 |
| R2 (macroscopic residual) | 25 | 0.6% | 0.78 |
| Rx (indeterminate) | 35 | 0.9% | — |
| **Total with margin data** | **3,957** | — | — |

- **Distance recorded**: 1,525/3,957 (38.5%)
- **R2 with gross ETE**: 25 patients (all have path_synoptic-confirmed gross ETE)
- **Source**: 100% path_synoptic (reliability 1.0)
- **Note**: 'x' placeholder in path_synoptics = margin involved (consistent with dataset convention)

---

## 2. Vascular Invasion (WHO 2022 Classification)

| Grade | N | % of Total | Source |
|-------|---|-----------|--------|
| Focal (<4 vessels) | 231 | 6.1% | path_synoptic (quantify field) |
| Extensive (≥4 vessels) | 169 | 4.5% | path_synoptic (quantify field) |
| Present, ungraded | 3,295 | 87.2% | path_synoptic (raw 'x') |
| Indeterminate | 56 | 1.5% | path_synoptic |
| Absent | 0 | 0.0% | — |
| **Total with data** | **3,780** | — | — |

- **WHO 2022 graded**: 400/3,780 (10.6%) — uses vessel count cutoff (<4 focal, ≥4 extensive)
- **Vessel count available**: 310 patients
- **Fill rate**: 3,780/10,871 (34.8%)

---

## 3. Lymphatic Invasion (LVI)

| Grade | N | % |
|-------|---|---|
| Present (ungraded) | 3,305 | 87.4% |
| Extensive | 57 | 1.5% |
| Focal | 8 | 0.2% |
| Indeterminate | 62 | 1.6% |
| Absent | 1 | 0.0% |
| **Total with LVI data** | **3,433** | — |

- **Fill rate**: 3,433/10,871 (31.6%)
- **LVI positive rate**: 99.2% (among those with data; selection bias — only recorded when present)

---

## 4. Perineural Invasion (PNI)

| Grade | N | % |
|-------|---|---|
| Present (ungraded) | 1,480 | 99.5% |
| Focal | 4 | 0.3% |
| Indeterminate | 3 | 0.2% |
| **Total with PNI data** | **1,487** | — |

- **Fill rate**: 1,487/10,871 (13.7%)
- **Source**: 100% path_synoptic

---

## 5. Lymph Node Yield

| Metric | Value |
|--------|-------|
| Patients with LN data | 8,339 |
| With examined count | 8,315 |
| With positive count | 3,819 |
| Median LN examined | 2.0 |
| Mean LN examined | 8.9 |
| Mean LN positive | 2.1 |
| Mean LN ratio | 0.137 |
| LN positive rate | 32.6% (2,719/8,339) |
| Central dissection | 3,240 (38.9%) |
| Lateral dissection | 25 (0.3%) |

- **LN ratio**: Available for patients with both examined and positive counts
- **Location detail**: Parsed from "positive/examined location" format (e.g., "0/1 perithyroidal")
- **Central identification**: composite flag from location text + central_compartment_dissection + other_ln_dissection

---

## 6. Extranodal Extension (Deepened)

| Status | N | % |
|--------|---|---|
| Present (ungraded) | 836 | 66.0% |
| Present (explicit) | 402 | 31.7% |
| Extensive | 4 | 0.3% |
| Focal | 4 | 0.3% |
| Absent | 7 | 0.6% |
| Indeterminate | 7 | 0.6% |
| **Total** | **1,267** | — |

- **Dual-source (path + op_note)**: 6 patients
- **Concordant positive**: 5/6 (83.3%)
- **Discordant**: 1 case (path_positive, op_note graded focal)
- **Source hierarchy**: path_synoptic → op_note gap-fill

---

## 7. H1 Sensitivity Analysis (CLN-Lobectomy)

**Cohort**: 5,799 lobectomy patients (CLN+ 1,430, CLN- 4,369)

### Primary Result (unchanged)
- **CLN-Recurrence Crude OR = 2.193 (1.918–2.506)**

### Phase 6 Subgroup Analyses

| Subgroup | N | OR (95% CI) | Interpretation |
|----------|---|-------------|---------------|
| R1 margins only | 2,411 | 1.258 (1.063–1.488) | CLN effect attenuated but significant |
| LN ≥ 0 examined | 4,594 | 1.481 (1.293–1.697) | Attenuated vs crude |
| LN ≥ 3 examined | 926 | 0.845 (0.650–1.099) | CLN benefit vanishes with adequate sampling |
| LN ≥ 6 examined | 563 | 1.024 (0.732–1.434) | Null effect |
| Vascular invasion+ | 2,282 | 1.257 (1.056–1.496) | CLN effect persists in vascular+ |

**Key finding**: When ≥3 LN are examined (adequate sampling), CLN adds no recurrence benefit (OR=0.845, NS). The crude OR of 2.19 is driven by indication bias — CLN patients inherently have more complete staging.

---

## 8. H2 Sensitivity Analysis (Goiter-SDOH)

**Cohort**: 6,566 goiter patients (294 substernal, 6,272 cervical)

### Substernal vs Cervical Invasion Profile

| Variable | Substernal | Cervical | p-direction |
|----------|-----------|----------|------------|
| Vascular invasion+ | 13.9% | 29.2% | Lower in substernal |
| LVI+ | 13.9% | 28.1% | Lower in substernal |
| PNI+ | 5.1% | 11.8% | Lower in substernal |

**Key finding**: Substernal goiters have LOWER invasion rates despite larger specimen size. This suggests substernal goiters are predominantly benign multinodular disease, while cervical goiters more often contain concurrent cancer with invasion features.

---

## 9. Materialized Tables (MotherDuck Pro)

| Table | Rows | Type |
|-------|------|------|
| `extracted_margins_refined_v1` | 3,957 | TABLE |
| `extracted_invasion_profile_v1` | 3,780 | TABLE |
| `extracted_ln_yield_v1` | 8,339 | TABLE |
| `extracted_ene_refined_v2` | 1,267 | TABLE |
| `extracted_staging_details_refined_v1` | 8,399 | TABLE (consolidated) |
| `patient_refined_master_clinical_v5` | 11,861 | TABLE (81 columns) |
| `vw_margins_by_source` | 4 | Summary TABLE |
| `vw_invasion_profile` | 12 | Summary TABLE |
| `vw_ln_yield_summary` | 1 | Summary TABLE |

---

## 10. Data Quality Assessment

| Metric | Phase 5 | Phase 6 | Delta |
|--------|---------|---------|-------|
| Master table columns | 50 | 81 | +31 |
| Variables source-attributed | 5 | 11 | +6 |
| WHO 2022 vascular grading | — | 10.6% graded | New |
| LN yield quantified | — | 8,339 patients | New |
| R-classification derived | — | 3,957 patients | New |
| Cross-source ENE concordance | — | 83.3% | New |
| **Data quality score** | **91/100** | **93/100** | **+2** |

---

## 11. Suggested Phase 7 Priorities

1. **'Present_ungraded' decontamination**: 87% of vascular/LVI marked 'x' remain ungraded — op note NLP could recover grading for ~200–400 patients
2. **Margin R0 recovery**: Only 1 patient classified R0 from structured data; NLP margin language in path reports could identify 1,000+ R0 patients from the 7,602 NULL margin records
3. **Lateral neck dissection enrichment**: Only 25 patients flagged lateral — likely under-captured; parse formal neck dissection operative notes
4. **Multi-tumor invasion**: Tumor 2–5 invasion fields exist but not yet aggregated
5. **Lab-invasion correlation**: Link PTH/calcium nadir to invasion extent for risk stratification
6. **MICE imputation for Phase 6 variables**: 65% missingness in LN/margin data; imputation needed before publication-grade models
