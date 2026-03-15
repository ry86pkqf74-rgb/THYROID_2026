# Repo Truth-Sync Report — 20260314

**Generated:** 20260314_2213 UTC  
**Source DB:** `md:thyroid_research_2026` (live MotherDuck)  
**Script:** `scripts/97_repo_truth_sync.py`  
**Purpose:** Deterministic reconciliation of all repo documentation claims against live data.

---

## 1. Core Cohort Metrics (Live)

| Metric | Canonical Value | Source Table |
|--------|----------------|--------------|
| Total patients (manuscript cohort) | **10871** | `manuscript_cohort_v1` |
| Surgical cohort unique patients | **10871** | `path_synoptics` |
| Patient analysis resolved total | **10871** | `patient_analysis_resolved_v1` |
| Analysis-eligible (flag) | **4136** | `patient_analysis_resolved_v1` |
| Analysis-eligible cancer cohort | **4136** | `analysis_cancer_cohort_v1` |
| Episode dedup rows | **9368** | `episode_analysis_resolved_v1_dedup` |
| Episode dedup removed | **207** | raw minus dedup |

---

## 2. Recurrence Metrics (Live)

| Tier | Count | % of recurrence-flagged | Notes |
|------|-------|------------------------|-------|
| Any recurrence flagged | **1986** | 100% | boolean from `extracted_recurrence_refined_v1` |
| Exact source date | **54** | — | Day-level date from structured registry |
| Biochemical inferred date | **168** | — | Rising Tg trajectory; proxy date |
| Unresolved date | **1764** | **88.8%** | Boolean flag only; no date available |

> **Canon:** 88.8% unresolved is a source limitation (no structured recurrence registry with dates).
> Only 54 patients have manuscript-quality time-to-event dates.

---

## 3. RAI Metrics (Live)

| Metric | Value | Notes |
|--------|-------|-------|
| RAI episodes total | **1857** | `rai_treatment_episode_v2` |
| RAI dose available | **761** (41.0%) | Non-zero `dose_mci` |
| Nuclear medicine notes in corpus | **0** | `clinical_notes_long` LIKE '%nuclear%' |

> Nuclear medicine reports = 0 is a **first-class structural limitation** confirmed by live query.
> RAI dose coverage cap of ~41% is architecturally bounded by this absence.

---

## 4. Vascular Invasion (Live)

**path_synoptics level** (vascular-positive rows, N=3,846):

| Grade | Count | % |
|-------|-------|---|
| present_ungraded / 'x' | **3,389** | **88.1%** |
| focal | ~231 | ~6.0% |
| extensive | ~162 | ~4.2% |
| other/indeterminate | ~64 | ~1.7% |
| **Total vascular-positive rows** | **3,846** | 100% |

**Patient level** (`patient_refined_master_clinical_v12`, vascular-positive patients, N=5,570):

| Grade | Count | % |
|-------|-------|---|
| present_ungraded | **4,652** | **83.5%** |
| graded (focal + extensive) | **819** | **14.7%** |
| other | ~99 | ~1.8% |
| **Total** | **5,570** | 100% |

> **Denominator note:** The 'x' placeholder in `path_synoptics.tumor_1_angioinvasion`
> without a vessel count in `tumor_1_angioinvasion_quantify` IS the primary source of
> present_ungraded; this is a synoptic template limitation, not a code quality gap.
> Prior docs cited "87%" — live canonical value is **88.1% (path)** / **83.5% (patient)**.

---

## 5. Operative NLP Boolean Fields (Live)

Total operative episodes: **9371**

| Field | TRUE count | % | Status |
|-------|-----------|---|--------|
| `rln_monitoring_flag` | 1702 | 18.2% |  |
| `parathyroid_autograft_flag` | 40 | 0.4% |  |
| `gross_ete_flag` | 22 | 0.2% |  |
| `local_invasion_flag` | 25 | 0.3% |  |
| `tracheal_involvement_flag` | 9 | 0.1% |  |
| `esophageal_involvement_flag` | 0 | 0.0% | **ZERO** |
| `strap_muscle_involvement_flag` | 186 | 2.0% |  |
| `reoperative_field_flag` | 46 | 0.5% |  |
| `drain_flag` | 169 | 1.8% |  |
| `parathyroid_resection_flag` | 0 | 0.0% | **ZERO** |

> Fields marked **ZERO** remain NOT_PARSED (not confirmed-negative). The V2 extractor
> codebase exists at `notes_extraction/extract_operative_v2.py` but outputs were never
> materialized to MotherDuck. `FALSE` = UNKNOWN, not confirmed-absent.

**Zero-materialized count: 5** (per `val_operative_nlp_propagation_v1`)  
**Fields:** esophageal_involvement_flag, berry_ligament_flag, ebl_ml_nlp,
frozen_section_flag, parathyroid_identified_count  
(Prior docs cited "8 fields at 0%" — corrected to 5 after Script 86 propagation.)

---

## 6. Imaging / TIRADS (Live)

| Metric | Value | Notes |
|--------|-------|-------|
| TIRADS patients | **3474** (31.96%) | `extracted_tirads_validated_v1` |
| imaging_fna_linkage_v2 rows | **N/A** | v2 linkage |
| imaging_fna_linkage_v3 rows | **9024** | v3 linkage |

> Imaging-FNA linkage remains 0 because `imaging_nodule_long_v2.linked_fna_episode_id`
> was not populated (imaging size columns were NULL upstream). TIRADS can be linked to
> patients but NOT to specific FNA episodes without spatial/temporal nodule matching.

---

## 7. Lab / Date Completeness (Live)

Total lab_canonical rows: **39,961** (`longitudinal_lab_canonical_v1`)

| Analyte | Patients | Date Coverage | Analysis Suitability |
|---------|----------|---------------|---------------------|
| thyroglobulin | 2,569 | **100.0%** | time_to_event_eligible |
| anti_thyroglobulin | 2,127 | **100.0%** | time_to_event_eligible |
| pth | 162 | **17.4%** | limited_temporal_fidelity |
| calcium_total | 559 | **11.6%** | limited_temporal_fidelity |
| calcium_ionized | 7 | 0% | value_only_no_temporal |
| TSH / free_T4 / free_T3 | — | **0%** | no_data_source_absent |
| vitamin_D / albumin / phosphorus | — | **0%** | no_data_source_absent |

> Source: `val_lab_temporal_truth_v1`. Thyroglobulin/anti-Tg = structured `specimen_collect_dt`
> from `thyroglobulin_labs` (gold). PTH/calcium = NLP-extracted dates only (partial).
> All other analytes = future institutional data feed required.

---

## 8. Adjudication

| Metric | Value |
|--------|-------|
| Active adjudication decisions | **0** |

---

## 9. MATERIALIZATION_MAP Stats

| Metric | Value |
|--------|-------|
| Total MAP entries | **220** |
| Duplicate MD keys | **0** |
| Duplicate source table keys | **0** |
| Non-conventional aliases | **3** (not blocking) |

> Source: `scripts/94_map_dedup_validator.py` — PASS. Regex parsing of script 26 tuple list.

---

## 10. val_* Table Inventory

| Table | Present | Rows |
|-------|---------|------|
| `val_scoring_systems` | ✓ | 1 |
| `val_analysis_resolved_v1` | ✓ | 29 |
| `val_rai_structural_coverage_v1` | ✓ | 27 |
| `val_rai_source_limitation_v1` | ✓ | 5 |
| `val_recurrence_readiness_v1` | ✓ | 10 |
| `val_lab_temporal_truth_v1` | ✓ | 14 |
| `val_operative_field_semantics_v1` | ✓ | 17 |
| `val_provenance_traceability` | ✓ | 6801 |
| `val_complication_refinement` | ✓ | 9 |
| `val_phase5_refinement` | ✓ | 5 |
| `val_phase6_staging_refinement` | ✓ | 6 |
| `val_phase7_preop_molecular` | ✗ MISSING | None |
| `val_phase8_final_outcomes` | ✗ MISSING | None |
| `val_phase9_targeted_refinement` | ✓ | 5 |
| `val_phase10_staging_recovery` | ✓ | 5 |
| `val_phase11_imaging_molecular` | ✗ MISSING | None |
| `val_phase12_tirads_validation` | ✓ | 4 |
| `val_phase13_final_gaps` | ✓ | 4 |

Present: **15** / 18 expected  
Missing: val_phase7_preop_molecular, val_phase8_final_outcomes, val_phase11_imaging_molecular

---

## 11. Discrepancy Table

| ID | Old Claim | New Canonical Value | Reason | Status |
|----|-----------|---------------------|--------|--------|
| OP_NLP_ZERO_FIELDS | "8 fields at 0%" (prior docs) | **5 fields at 0%** (berry_ligament_flag, ebl_ml_nlp, esophageal_involvement_flag, frozen_section_flag, parathyroid_identified_count) | Script 86 propagated 3 more fields post-March-13; `val_operative_nlp_propagation_v1` is canonical | ⚠ CORRECTED |
| VASCULAR_UNGRADED_PCT | "87% vascular ungraded" (prior docs) | **88.1% (path_synoptics)** / **83.5% (patient-level)** | Different denominators; path_synoptics row-level vs patient_refined_master_clinical_v12 patient-level | ⚠ CORRECTED |
| IMAGING_FNA_LINKAGE | "imaging_fna_linkage v3=0 rows" | **9,024 rows** (2,072 patients; 652 high_confidence; 3,048 analysis_eligible) | imaging_nodule_master_v1 populated from TIRADS Excel; v3 linkage built on top | ⚠ CORRECTED |
| MATERIALIZATION_MAP | "131 entries" (Phase 13 AGENTS.md) | **220 entries** | Scripts 82–92 added 89 new entries not reflected in older docs | ⚠ CORRECTED |
| RECURRENCE_DATES | exact=54, biochem=168, unresolved=1764 | **exact=54, biochem=168, unresolved=1764** | — | ✓ MATCH |


---

## 12. Pre-2019 Operative Note Coverage

| Period | Count |
|--------|-------|
| op_notes pre-2019 | **136** |
| op_notes post-2019 | **3138** |

---

## Summary

This report is generated deterministically from live MotherDuck data.
All metrics above supersede any earlier documentation for the same date.
See `exports/repo_truth_sync_20260314_2213/` for raw CSV and JSON outputs.
