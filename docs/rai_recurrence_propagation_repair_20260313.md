# RAI & Recurrence Date Propagation Report

**Date:** 2026-03-13

---

## RAI Treatment Episodes

### Current State (Verified)

| Metric | Value |
|--------|-------|
| Total RAI episodes | 1,857 |
| Episodes with dose_mci | 761 (41.0%) |
| Episodes with dose_source | 761 (100% of dosed) |
| Episodes with dose_confidence | 761 |
| Episodes with surgery_link_score_v3 | 70 |
| Episodes with dose_missingness_reason | 1,857 (100%) |

### Dose Missingness Classification

All 1,857 episodes have `dose_missingness_reason` populated (by script 78 Phase C):
- `dose_available`: 761 — dose confirmed from structured or NLP source
- `source_present_no_dose`: ~variable — RAI event noted but dose not recorded
- `linkage_failed`: ~1,096 — Could not link to dose source

### Propagation to Resolved Layer

RAI data propagates through:
1. `rai_treatment_episode_v2` → `pathology_rai_linkage_v3` (23 linked)
2. `rai_treatment_episode_v2` → `patient_analysis_resolved_v1` (via script 48)
3. `rai_treatment_episode_v2` → `manuscript_cohort_v1` (via frozen cohort)

**Source limitation:** Only 23 pathology→RAI linkages exist due to the temporal matching
requirements. Most RAI episodes cannot be confidently linked to a specific pathology episode.

---

## Recurrence Events

### Current State (Verified)

| Date Status | Count | Percentage |
|-------------|-------|-----------|
| not_applicable | 8,885 | 81.7% |
| unresolved_date | 1,764 | 16.2% |
| biochemical_inflection_inferred | 168 | 1.5% |
| exact_source_date | 54 | 0.5% |
| **Total with recurrence** | **1,986** | **18.3%** |

### Date Quality Breakdown for Recurrences

Of the 1,986 patients with recurrence:
- 54 (2.7%) have an exact recurrence date from structured sources
- 168 (8.5%) have biochemical recurrence inferred from rising Tg trajectory
- 1,764 (88.8%) have recurrence flag but no precise date (structural_date_unknown)

### Propagation to Resolved Layer

Recurrence data propagates through:
1. `extracted_recurrence_refined_v1` → `recurrence_event_clean_v1` (1,946 events)
2. `recurrence_risk_features_mv` → `patient_analysis_resolved_v1`
3. Script 76 Phase E added `recurrence_date_best`, `recurrence_date_status`,
   `recurrence_date_confidence` to the refined table

### Why 88.8% Are Unresolved

The recurrence flag comes from `recurrence_risk_features_mv.recurrence_flag` (a structured
boolean). The recurrence *date* requires either:
- An explicit date in `first_recurrence_date` (available for 54 patients)
- A biochemical inflection point in Tg trajectory (identified for 168 patients)
- Chart review (needed for the remaining 1,764)

This is a **source limitation**, not a pipeline gap. The manual review queue
(`recurrence_manual_review_queue_v1`) surfaces unresolved cases prioritized by
clinical importance, but resolution requires human chart review.

---

## Acceptance Criteria

- [x] Resolved/manuscript-facing tables show truthful propagation
- [x] Unresolved cases remain unresolved rather than guessed
- [x] All propagation logic documented
- [x] Date taxonomy preserved (exact_source_date / biochemical_inflection / unresolved)
