# Phase 4 Variable Inventory and Prioritization Matrix

_Generated: 2026-03-12 19:33 UTC_

## Executive Summary

This inventory covers all clinically meaningful variables in the THYROID_2026 pipeline that require source-specific attribution for publication-quality analyses. Variables are scored on three axes: **clinical impact** (AJCC staging / ATA guideline criticality), **current precision** (estimated reliability of existing extraction), and **source diversity** (number of distinct note types contributing mentions).

### Source Reliability Hierarchy

| Source Category | Reliability | Description |
|----------------|-------------|-------------|
| `path_report` | 1.0 | Formal synoptic pathology report |
| `structured_db` | 1.0 | Structured database table (complications, labs) |
| `op_note` | 0.9 | Operative note — direct surgical observation |
| `endocrine` | 0.8 | Endocrine clinic follow-up note |
| `discharge` | 0.7 | Discharge summary |
| `imaging` | 0.7 | CT/US radiology report |
| `h_p_consent` | 0.2 | H&P / consent template — boilerplate contamination |
| `other` | 0.5 | Other notes |

---

## Prioritization Matrix

| Rank | Variable | Clinical Impact | Fill Rate | Current Precision | Source Diversity | Source-Split Needed |
|------|----------|----------------|-----------|-------------------|------------------|---------------------|
| 1 | **Extrathyroidal Extension (ETE)** | 10/10 | 35.7% | 85% | 4 | YES |
| 2 | **Tumor Size / T Stage** | 9/10 | 37.1% | 85% | 3 | YES |
| 3 | **Surgical Margin Status (R0/R1/R2)** | 9/10 | 36.4% | 85% | 1 | — |
| 4 | **Vascular / Angioinvasion** | 8/10 | 34.5% | 85% | 1 | — |
| 5 | **Perineural Invasion (PNI)** | 7/10 | 13.7% | 85% | 1 | — |
| 6 | **Lymphovascular Invasion (LVI)** | 7/10 | 31.6% | 85% | 1 | — |
| 7 | **BRAF / Molecular Markers** | 9/10 | N/A | 45% | 3 | YES |
| 8 | **Capsular Invasion** | 6/10 | 11.3% | 85% | 1 | — |
| 9 | **Recurrence Site and Detection Method** | 8/10 | N/A | 30% | 2 | YES |
| 10 | **Post-op Calcium / PTH Nadir** | 6/10 | N/A | 65% | 3 | YES |
| 11 | **Completion Thyroidectomy Indication** | 5/10 | N/A | 70% | 2 | YES |
| 12 | **Voice / Laryngoscopy Findings** | 7/10 | N/A | 85% | 3 | — |
| 13 | **Extranodal Extension (ENE)** | 6/10 | N/A | 70% | 1 | — |
| 14 | **Aggressive Histologic Variant** | 7/10 | N/A | 70% | 1 | — |
| 15 | **TERT Promoter Mutation** | 8/10 | N/A | 45% | 2 | YES |

---

## Detailed Variable Profiles

### 1. Extrathyroidal Extension (ETE) (`ete_overall`)

**Clinical Impact:** 10/10  
**Phase 4 Priority:** 1  
**Fill Rate:** 35.7% of patients
**Current Precision Estimate:** 85% — _Structured but raw text normalization needed_  
**Source Diversity:** 4 distinct note type categories  
**Requires Source Split:** YES

**Structured Sources:**
- `path_synoptics.tumor_1_extrathyroidal_extension`
- `tumor_episode_master_v2.extrathyroidal_extension`
- `recurrence_risk_features_mv.ete`
- `operative_episode_detail_v2.gross_ete_flag`

**NLP Sources:**
- note_entities_staging (extrathyroidal_extension_detail)
- extract_histology_v2 -> gross/microscopic ETE from path notes
- extract_operative_v2 -> gross_ete/ete_present from op notes
- extract_imaging_v2 -> imaging_ete from CT/US

**Notes:** MUST source-split: path (gold) vs op note vs imaging vs consent. Gross vs microscopic distinction is AJCC T-stage critical. 35.7% fill rate in path_synoptics. Raw values include 'x','present','minimal','microscopic','extensive'.

**Recommended New Columns:**
- `ete_path_confirmed`
- `ete_op_note_observed`
- `ete_imaging_suspected`
- `ete_overall_confirmed`
- `ete_grade`

### 2. Tumor Size / T Stage (`tumor_size`)

**Clinical Impact:** 9/10  
**Phase 4 Priority:** 2  
**Fill Rate:** 37.1% of patients
**Current Precision Estimate:** 85% — _Structured but raw text normalization needed_  
**Source Diversity:** 3 distinct note type categories  
**Requires Source Split:** YES

**Structured Sources:**
- `path_synoptics.tumor_1_size_greatest_dimension_cm`
- `tumor_episode_master_v2.tumor_size_cm`
- `imaging_nodule_long_v2.size_cm_max`

**NLP Sources:**
- extract_histology_v2 (implicit in histology notes)
- extract_imaging_v2 (size_cm_max from US/CT)

**Notes:** Path size is canonical; imaging pre-op size may differ. 37.1% fill rate in path_synoptics. Path > imaging hierarchy for AJCC T-staging.

**Recommended New Columns:**
- `tumor_size_path_cm`
- `tumor_size_imaging_cm`
- `tumor_size_source`

### 3. Surgical Margin Status (R0/R1/R2) (`margin_status`)

**Clinical Impact:** 9/10  
**Phase 4 Priority:** 3  
**Fill Rate:** 36.4% of patients
**Current Precision Estimate:** 85% — _Structured but raw text normalization needed_  
**Source Diversity:** 1 distinct note type categories  
**Requires Source Split:** No

**Structured Sources:**
- `path_synoptics.tumor_1_margin_status`
- `path_synoptics.tumor_1_distance_to_closest_margin_mm`
- `tumor_episode_master_v2.margin_status`

**NLP Sources:**
- extract_histology_v2 (margin_status from path reports)

**Notes:** Path report only; 36.4% fill rate in path_synoptics. Raw values: 'involved', 'c/a', 'present', 'indeterminate', 'Involved'. Closest margin distance: 14.2% fill rate. Current normalization in tumor_episode_master_v2 is not clean.

**Recommended New Columns:**
- `margin_status_refined`
- `closest_margin_mm`
- `margin_site`

### 4. Vascular / Angioinvasion (`vascular_invasion`)

**Clinical Impact:** 8/10  
**Phase 4 Priority:** 4  
**Fill Rate:** 34.5% of patients
**Current Precision Estimate:** 85% — _Structured but raw text normalization needed_  
**Source Diversity:** 1 distinct note type categories  
**Requires Source Split:** No

**Structured Sources:**
- `path_synoptics.tumor_1_angioinvasion`
- `path_synoptics.tumor_1_angioinvasion_quantify`
- `tumor_episode_master_v2.vascular_invasion`

**NLP Sources:**
- extract_histology_v2 (vascular_invasion_detail)

**Notes:** Path report only; 34.5% fill rate. Raw values in tumor_episode_master_v2: 'x','present','focal','extensive'. focal vs extensive distinction matters for AJCC 8th Ed.

**Recommended New Columns:**
- `vascular_invasion_refined`
- `vascular_invasion_grade`

### 5. Perineural Invasion (PNI) (`perineural_invasion`)

**Clinical Impact:** 7/10  
**Phase 4 Priority:** 5  
**Fill Rate:** 13.7% of patients
**Current Precision Estimate:** 85% — _Structured but raw text normalization needed_  
**Source Diversity:** 1 distinct note type categories  
**Requires Source Split:** No

**Structured Sources:**
- `path_synoptics.tumor_1_perineural_invasion`
- `tumor_episode_master_v2.perineural_invasion`

**NLP Sources:**
- extract_histology_v2 (perineural_invasion)

**Notes:** Path report only; 13.7% fill rate (sparse). Binary: present vs absent.

**Recommended New Columns:**
- `perineural_invasion_refined`

### 6. Lymphovascular Invasion (LVI) (`lymphovascular_invasion`)

**Clinical Impact:** 7/10  
**Phase 4 Priority:** 6  
**Fill Rate:** 31.6% of patients
**Current Precision Estimate:** 85% — _Structured but raw text normalization needed_  
**Source Diversity:** 1 distinct note type categories  
**Requires Source Split:** No

**Structured Sources:**
- `path_synoptics.tumor_1_lymphatic_invasion`
- `tumor_episode_master_v2.lymphatic_invasion`

**NLP Sources:**
- extract_histology_v2 (lymphatic_invasion_detail)

**Notes:** Path report only; 31.6% fill rate. Key intermediate-risk factor in ATA guidelines.

**Recommended New Columns:**
- `lvi_refined`

### 7. BRAF / Molecular Markers (`braf_molecular`)

**Clinical Impact:** 9/10  
**Phase 4 Priority:** 7  
**Fill Rate:** N/A  
**Current Precision Estimate:** 45% — _NLP heavily consent-contaminated; structured sources needed_  
**Source Diversity:** 3 distinct note type categories  
**Requires Source Split:** YES

**Structured Sources:**
- `molecular_test_episode_v2 (platform, result)`
- `thyroseq_molecular_enrichment (BRAF, RAS, TERT)`
- `recurrence_risk_features_mv (braf_positive, ras_positive)`

**NLP Sources:**
- note_entities_genetics (BRAF=344 h_p + 83 op_note + 39 other_history)
- extract_molecular_v2

**Notes:** BRAF NLP in h_p/op_note is heavily consent/risk-list contaminated. Structured molecular_test_episode_v2 is the gold standard. Need: tested_flag vs positive_flag, platform, date.

**Recommended New Columns:**
- `braf_tested`
- `braf_positive_refined`
- `molecular_platform`
- `molecular_test_date`

### 8. Capsular Invasion (`capsular_invasion`)

**Clinical Impact:** 6/10  
**Phase 4 Priority:** 8  
**Fill Rate:** 11.3% of patients
**Current Precision Estimate:** 85% — _Structured but raw text normalization needed_  
**Source Diversity:** 1 distinct note type categories  
**Requires Source Split:** No

**Structured Sources:**
- `path_synoptics.tumor_1_capsular_invasion`
- `tumor_episode_master_v2.capsular_invasion`

**NLP Sources:**
- extract_histology_v2 (capsular_invasion)

**Notes:** Path report only. Key for FTC vs follicular adenoma distinction. Binary: present vs absent.

**Recommended New Columns:**
- `capsular_invasion_refined`

### 9. Recurrence Site and Detection Method (`recurrence_site`)

**Clinical Impact:** 8/10  
**Phase 4 Priority:** 9  
**Fill Rate:** N/A  
**Current Precision Estimate:** 30% — _NLP events contaminated; structured recurrence_flag reliable_  
**Source Diversity:** 2 distinct note type categories  
**Requires Source Split:** YES

**Structured Sources:**
- `recurrence_risk_features_mv (recurrence_flag, first_recurrence_date)`

**NLP Sources:**
- extracted_clinical_events_v4 (recurrence NLP - heavily contaminated)
- note_entities_problem_list

**Notes:** recurrence_flag in recurrence_risk_features_mv is structured (reliable). NLP clinical events are contaminated (6,405 false positives from single words). Need: recurrence_site (local/regional/distant), detection_method (imaging/Tg/biopsy).

**Recommended New Columns:**
- `recurrence_site_refined`
- `recurrence_detection_method`
- `recurrence_confirmed`

### 10. Post-op Calcium / PTH Nadir (`calcium_pth_nadir`)

**Clinical Impact:** 6/10  
**Phase 4 Priority:** 10  
**Fill Rate:** N/A  
**Current Precision Estimate:** 65% — _No dedicated lab table; NLP post Phase 3 refinement_  
**Source Diversity:** 3 distinct note type categories  
**Requires Source Split:** YES

**Structured Sources:**
- `thyroglobulin_labs (tg only)`
- `thyroseq_followup_labs`

**NLP Sources:**
- note_entities_medications (calcitriol, calcium_supplement)
- note_entities_problem_list (hypocalcemia, hypoparathyroidism)

**Notes:** No dedicated calcium/PTH lab table found. Note_entities hypocalcemia/hypoparathyroidism refined in Phase 3. Need: post-op PTH nadir value + timing, calcium nadir + timing.

**Recommended New Columns:**
- `pth_nadir_pg_ml`
- `pth_nadir_days_post_op`
- `calcium_nadir_mg_dl`
- `hypoparathyroidism_confirmed`

### 11. Completion Thyroidectomy Indication (`completion_reason`)

**Clinical Impact:** 5/10  
**Phase 4 Priority:** 11  
**Fill Rate:** N/A  
**Current Precision Estimate:** 70% — _Moderate; source split would improve_  
**Source Diversity:** 2 distinct note type categories  
**Requires Source Split:** YES

**Structured Sources:**
- `path_synoptics.completion (yes/no flag only)`

**NLP Sources:**
- note_entities_procedures (completion_thyroidectomy: 465 h_p + 344 op_note)
- note_entities_problem_list

**Notes:** Completion flag exists but reason (cancer found, patient preference, etc.) is NLP-only. Op note is most reliable source for surgical indication.

**Recommended New Columns:**
- `completion_reason_refined`
- `completion_indication_source`

### 12. Voice / Laryngoscopy Findings (`voice_laryngoscopy`)

**Clinical Impact:** 7/10  
**Phase 4 Priority:** 12  
**Fill Rate:** N/A  
**Current Precision Estimate:** 85% — _Already refined in Phase 2/3_  
**Source Diversity:** 3 distinct note type categories  
**Requires Source Split:** No

**Structured Sources:**
- `complications.vocal_cord_status`
- `complications.laryngoscopy_date`
- `extracted_rln_injury_refined_v2 (already refined)`

**NLP Sources:**
- note_entities_complications (vocal_cord_paralysis, vocal_cord_paresis)

**Notes:** Already refined in Phase 2/3 (extracted_rln_injury_refined_v2). Need: bilateral vs unilateral extension, laryngoscopy scope findings, hoarseness severity. Build on existing refined tables.

**Recommended New Columns:**
- `rln_injury_grade`
- `laryngoscopy_finding`
- `voice_outcome`

### 13. Extranodal Extension (ENE) (`extranodal_extension`)

**Clinical Impact:** 6/10  
**Phase 4 Priority:** 13  
**Fill Rate:** N/A  
**Current Precision Estimate:** 70% — _Moderate; source split would improve_  
**Source Diversity:** 1 distinct note type categories  
**Requires Source Split:** No

**Structured Sources:**
- `path_synoptics.tumor_1_extranodal_extension`
- `tumor_episode_master_v2.extranodal_extension`

**NLP Sources:**
- extract_histology_v2 (extranodal_extension_detail)

**Notes:** Path report only. N2b staging determinant.

**Recommended New Columns:**
- `ene_refined`

### 14. Aggressive Histologic Variant (`histology_variant`)

**Clinical Impact:** 7/10  
**Phase 4 Priority:** 14  
**Fill Rate:** N/A  
**Current Precision Estimate:** 70% — _Moderate; source split would improve_  
**Source Diversity:** 1 distinct note type categories  
**Requires Source Split:** No

**Structured Sources:**
- `tumor_episode_master_v2.histology_variant`
- `path_synoptics.tumor_1_variant`

**NLP Sources:**
- extract_histology_v2 (aggressive_features, histology_subtype)

**Notes:** Tall cell, hobnail, diffuse sclerosing - high-risk variants. Path report only. Relatively well captured.

**Recommended New Columns:**
- `aggressive_variant_confirmed`

### 15. TERT Promoter Mutation (`tert_status`)

**Clinical Impact:** 8/10  
**Phase 4 Priority:** 15  
**Fill Rate:** N/A  
**Current Precision Estimate:** 45% — _NLP heavily consent-contaminated; structured sources needed_  
**Source Diversity:** 2 distinct note type categories  
**Requires Source Split:** YES

**Structured Sources:**
- `recurrence_risk_features_mv (tert_positive)`

**NLP Sources:**
- note_entities_genetics (TERT: 72 h_p + 39 op_note)

**Notes:** High-risk molecular marker. Structured source reliable. NLP likely consent/risk contaminated.

**Recommended New Columns:**
- `tert_tested`
- `tert_positive_refined`

---

## ETE Source Distribution (from path_synoptics)

| Raw Value | Count | Normalized Category |
|-----------|-------|---------------------|
| `x` | 3,382 | microscopic (placeholder) — needs audit |
| `present` | 252 | present — needs grade sub-classification |
| `minimal` | 174 | microscopic |
| `microscopic` | 65 | microscopic |
| `c/a` | 29 | ambiguous — needs review |
| `extensive` | 24 | gross |
| `yes` | 19 | present — ambiguous grade |
| `focal` | 13 | microscopic |
| `indeterminate` | 9 | ambiguous |
| `Yes;` | 7 | present |
| _long free text_ | ~20 | mixed |
| `None` | 7,691 | absent / no data |

**Key insight**: The 'x' placeholder (3,382 cases) is the largest category and means 'present but grade unspecified' — these require sub-classification by parsing the accompanying free-text comment fields.

---

## Source Contamination Summary

| Entity | h_p Mentions | op_note Mentions | True Event Rate Est. |
|--------|-------------|------------------|----------------------|
| BRAF (genetics) | 344 | 83 | ~10-20% (consent/risk lists) |
| ETE (staging) | — | — | NLP not deployed to note_entities_staging |
| chyle_leak | 645 | 2,316 | ~3.3% (Phase 2 confirmed) |
| hypocalcemia | 1,803 | 651 | ~3.3% (Phase 2 confirmed) |
| rln_injury | 952 | 20 | ~0.85% (Phase 2 refined) |

---

## Phase 4 Execution Order

1. **ETE** (priority 1) — source-split with gross/microscopic/suspected classification
2. **Tumor Size** (priority 2) — path vs imaging concordance
3. **Margin Status** (priority 3) — R0/R1 normalization + closest margin mm
4. **Vascular Invasion** (priority 4) — focal vs extensive normalization
5. **Perineural Invasion** (priority 5) — binary, path-only
6. **LVI** (priority 6) — binary, path-only
7. **BRAF/Molecular** (priority 7) — tested vs positive, platform attribution
8. **Recurrence Site** (priority 9) — detection method attribution

---

## Recommended Next 5 Variables (Post-Phase 4)

| Variable | Rationale |
|----------|-----------|
| **TERT promoter mutation** | High-risk molecular marker; NLP contamination needs audit |
| **Extranodal extension (ENE)** | N2b staging determinant; path-only, sparse |
| **Aggressive variant sub-type** | Tall cell / hobnail affect prognosis; needs validation |
| **Post-op TSH suppression** | RAI eligibility surrogate; lab + note sources |
| **RAI dose and avidity** | Already in rai_episode_v3 but needs source attribution |