# Source-Limited Field Registry — 2026-03-14

**Companion narrative to** `exports/final_release_hardening_20260314/source_limited_field_registry.csv`

---

## Purpose

This document explains the four-tier field classification scheme applied to all
analytical variables in `patient_refined_master_clinical_v12` and its downstream
views. Understanding the classification of a field is critical for correct
manuscript Methods language and for avoiding over-interpreting sparse or
default-value data.

---

## Status Tier Definitions

### `CANONICAL`
The field is derived from structured, source-linked data with high coverage and
explicit positive-qualifier confirmation. Use freely in regression models, Table 1,
and without caveat in the manuscript body.

**Key CANONICAL fields:**
- `braf_positive_final`, `ras_positive_final`, `tert_positive_final` — confirmed with
  positive-qualifier NLP gate; BRAF 546 confirmed positive (canonical from `patient_refined_master_clinical_v12`)
- `ete_grade_v9` — 98.6% of ungraded ETE resolved via Phase 9 microscopic rule
- `ajcc8_stage_group`, `t_stage_final`, `n_stage_final` — structured from path_synoptics/tumor_pathology
- `ata_risk_initial`, `macis_score`, `ages_score`, `ames_risk` — computed from CANONICAL inputs
- `age_at_surgery`, `sex_harmonized`, `race_harmonized` — demographics harmonized from 6+ sources
- `recurrence_flag_structured`, `recurrence_any` — boolean, source-linked to structured registries
- `ln_positive_v6`, `ln_examined`, `ln_ratio` — from path_synoptics + multi-tumor aggregation
- `margin_status_refined`, `margin_r_classification` — Phase 6 structured + NLP
- `specimen_weight_g` — from path_synoptics.weight_total (35% coverage; acceptable denominator)

---

### `SOURCE_LIMITED`
The field exists in the database but its completeness is architecturally bounded by
the absence of a required institutional data feed. This is **not a bug** or code
quality gap; it reflects the known data landscape of the study period.

Manuscript usage: **allowed** but **requires caveat**. Provide coverage percentage
and explain which institutional source is absent.

**Critical SOURCE_LIMITED fields and their primary limitations:**

| Field | Coverage | Missing Source |
|-------|----------|----------------|
| `rai_dose_mci` | ~41% | Nuclear medicine order system not connected |
| `tsh_result` | ~0% | General lab system not connected |
| `free_t4_result` | ~0% | General lab system not connected |
| `vitamin_d_result` | ~0% | General lab system not connected |
| `non_tg_lab_date` | ~0% | Specimen collect date not in NLP-derived labs |
| `tirads_score_v12` | ~32% | Ultrasound radiology PACS not fully integrated |
| `recurrence_date_exact` | ~2.7% | Structured recurrence registry not available |
| `bmi` | ~1.3% | Vitals/flowsheet data not connected |
| `esophageal_involvement_flag` | 0% | See "Operative Defaults" note below |
| `ebl_ml_nlp` | ~0% | V2 extractor output never materialized |

**Operative Boolean Defaults (CRITICAL misinterpretation risk):**

The flags `rln_monitoring_flag`, `parathyroid_autograft_flag`,
`gross_ete_flag`, `local_invasion_flag`, `tracheal_involvement_flag`,
`esophageal_involvement_flag`, `strap_muscle_involvement_flag`,
`reoperative_field_flag`, `drain_flag`, `parathyroid_resection_flag`
default to `FALSE` in `operative_episode_detail_v2` when the V2 NLP extractor
did not fire.

> **FALSE in these fields means UNKNOWN/NOT_PARSED — not confirmed-negative.**

Only use these flags when treating `FALSE` as unknown (e.g., logistic regression
where missingness is analyzed separately) or after the V2 extractor outputs are
materialized. Exception: `rln_monitoring_flag` = TRUE is reliable (1,701 episodes).

---

### `DERIVED_APPROXIMATE`
The field is computed from proxy sources or heuristic rules. Values are directionally
correct but may not match precision-grade structured data. Use in exploratory analysis
and sensitivity models, not as primary outcomes.

**DERIVED_APPROXIMATE fields:**
- `recurrence_date_biochemical_inferred` — inferred from rising Tg trajectory; 168 patients;
  not a confirmed structural recurrence date
- `ata_response_status` — computed from Tg thresholds (0.2, 1.0 ng/mL); biochemical
  completeness only; structural response requires imaging
- `lab_completeness_score` — heuristic 0–100 score; not externally validated
- `follow_up_completeness_score` — heuristic aggregate; useful for sensitivity stratification

---

### `MANUAL_REVIEW_ONLY`
The field was populated for a subset of patients via manual chart review or
external data linkage. Values are high-quality but not representative of the full
cohort. **Do not use in population-level denominators.**

**MANUAL_REVIEW_ONLY fields:**
- `nsqip_linked_research_id` — 1,314 NSQIP-linked patients only; ACS-NSQIP linkage
- `thyroseq_excel_matched` — 48 ThyroSeq-matched patients from external workbook

---

## Full Field Table

The complete machine-readable registry is in:
`exports/final_release_hardening_20260314/source_limited_field_registry.csv`

Column reference:
- `field_name` — Column name in master table or episode/patient views
- `domain` — Clinical domain (Operative, Labs, Imaging, etc.)
- `status` — One of: CANONICAL, SOURCE_LIMITED, DERIVED_APPROXIMATE, MANUAL_REVIEW_ONLY
- `rationale` — One-line reason for the classification
- `primary_source_tables` — DuckDB table(s) used for the field
- `expected_missingness_behavior` — How NULLs or FALSE should be interpreted
- `manuscript_allowed` — YES / YES_WITH_CAVEAT / EXPLORATORY_ONLY
- `notes` — Additional guidance

---

## Recommended Manuscript Methods Language

### For SOURCE_LIMITED fields

> "Due to the absence of a structured [nuclear medicine / general lab / PACS]
> data feed in the study database, {field} was available for {N} of {D} patients
> ({pct}%). Analyses using this variable are restricted to the subset with
> available data; missingness was confirmed to be non-systematic by [comparison
> with {proxy_source}]."

### For Operative Boolean Defaults

> "Operative detail flags (including parathyroid autograft, tracheal involvement,
> and reoperative field status) were extracted via NLP from operative notes.
> A flag value of FALSE indicates that the entity was not identified in the parsed
> text and should be interpreted as 'not documented' rather than confirmed absent."

### For recurrence_date_exact sparsity (2.7%)

> "Exact day-level recurrence dates were available for {N} patients ({pct}%);
> the majority of structural recurrences were identified via boolean flag in the
> structured registry without a linked calendar date. Time-to-event analyses were
> therefore restricted to patients with confirmed date information (N={N_tte})
> and supplemented with biochemical recurrence trajectory analysis."

---

## BRAF Prevalence Context

The overall BRAF positivity rate in this cohort (546 of 10,871 surgical patients,
5.0%) is lower than the published 40–45% prevalence in PTC because:
1. The denominator includes all thyroid surgeries (including benign/completion)
2. Among molecularly-tested patients only (N=~800), BRAF positivity is ~68%

Always specify the denominator when reporting molecular positivity rates.

---

## Provenance Chain Quick-Reference

For any field where the source is questioned, the standard audit chain is:

```
patient_refined_master_clinical_v12
  └── patient_refined_staging_flags_v3  (Phase 4 structured)
  └── extracted_*_v1 (Phase 5–11 NLP/structured)
  └── lineage_audit_v1  (provenance traceability)
  └── val_provenance_traceability  (validation gate)
```

Run `scripts/46_provenance_audit.py --md` to regenerate provenance reports.
