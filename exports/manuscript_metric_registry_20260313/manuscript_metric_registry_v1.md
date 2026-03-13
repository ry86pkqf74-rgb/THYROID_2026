# Manuscript Metric Registry — Single Source of Truth

**Frozen Date:** 2026-03-13
**Git SHA:** e1e8897
**Registry Version:** v1
**Total Metrics:** 25

---

## Denominator Definitions

| Denominator Label | N | Definition |
|---|---|---|
| full_surgical_cohort | 10,871 | All patients in `path_synoptics` with a surgical episode |
| analysis_eligible_cancer | 4,136 | Confirmed malignancy + complete staging from `patient_analysis_resolved_v1` |
| molecular_tested_patients | 10,025 | Patients with any molecular panel result from `extracted_molecular_panel_v1` |
| survival_cohort | 3,201 | Analysis-eligible cancer patients with positive follow-up time and recurrence endpoint |
| vascular_positive_patients | 3,846 | Patients with any vascular invasion (positive, focal, extensive, or ungraded) |

---

## Metric Registry

| # | metric_id | metric_name | Value | Num | Denom | Denom Label | Source Table | Source Script | SQL Fragment | Population | Table | Figure | Caveats |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | total_surgical_patients | Total surgical patients | 10,871 | 10,871 | 10,871 | full_surgical_cohort | path_synoptics | 57_freeze_manuscript_cohort.py | `COUNT(DISTINCT research_id) FROM path_synoptics` | Full surgical cohort | Table 1 | Fig 1 | — |
| 2 | analysis_eligible_cancer | Analysis-eligible cancer patients | 4,136 | 4,136 | 10,871 | all_resolved_patients | patient_analysis_resolved_v1 | 57_freeze_manuscript_cohort.py | `WHERE analysis_eligible_flag IS TRUE` | Analysis-eligible cancer patients | Tables 1-3 | Figs 2-5 | Confirmed malignancy + complete staging required |
| 3 | recurrence_any | Any recurrence (structural or biochemical) | 1,986 | 1,986 | 10,871 | full_surgical_cohort | extracted_recurrence_refined_v1 | Phase 8 engine v6 | `WHERE recurrence_any IS TRUE` | Any recurrence | Table 3 | — | 88.8% lack day-level dates |
| 4 | recurrence_structural | Structural recurrence | 1,818 | 1,818 | 10,871 | full_surgical_cohort | extracted_recurrence_refined_v1 | Phase 8 engine v6 | `detection_category IN (structural_confirmed, structural_date_unknown)` | Structural recurrence | Table 3 | — | — |
| 5 | recurrence_biochemical | Biochemical recurrence (Tg trajectory) | 115 | 115 | 10,871 | full_surgical_cohort | extracted_recurrence_refined_v1 | Phase 8 engine v6 | `detection_category = biochemical_only` | Biochemical recurrence | Table 3 | — | Rising Tg >1.0 and >2x nadir |
| 6 | braf_positive | BRAF mutation positive | 376 | 376 | 10,025 | molecular_tested_patients | extracted_braf_recovery_v1 | Phase 11 engine v9 | `WHERE braf_status = positive` | BRAF positive | Table 2 | Fig 4 | NLP requires explicit positive qualifier |
| 7 | ras_positive | RAS mutation positive (NRAS+HRAS+KRAS) | 292 | 292 | 10,025 | molecular_tested_patients | extracted_ras_patient_summary_v1 | Phase 11 engine v9 | `WHERE ras_positive IS TRUE` | RAS positive | Table 2 | Fig 4 | ras_flag in molecular_test_episode_v2 is FALSE (bug) |
| 8 | tert_positive | TERT promoter mutation positive | 108 | 108 | 10,025 | molecular_tested_patients | patient_refined_master_clinical_v12 | Phase 9 engine v7 | `tert_positive column` | TERT positive | Table 2 | Fig 4 | Structured molecular sources only |
| 9 | molecular_tested | Patients with any molecular panel result | 10,025 | 10,025 | 10,871 | full_surgical_cohort | extracted_molecular_panel_v1 | Phase 7 engine v5 | `COUNT(DISTINCT research_id)` | Molecular tested | Table 2 | Fig 4 | Includes ThyroSeq Afirma IHC PCR FISH |
| 10 | rai_treated_strict | RAI receipt confirmed with dose | 35 | 35 | 10,871 | full_surgical_cohort | extracted_rai_validated_v1 | Phase 8 engine v6 | `rai_validation_tier = confirmed_with_dose` | RAI confirmed | Table 2 | — | 0 definite_received; all likely_received |
| 11 | rai_episodes_total | Total RAI episodes (all certainty) | 1,857 | 1,857 | 10,871 | full_surgical_cohort | rai_treatment_episode_v2 | script 22 | `COUNT(*)` | RAI episodes | Supplement | — | Dose for 761 (41%); 0 nuclear med notes |
| 12 | rln_injury_confirmed | Confirmed RLN injury (3-tier) | 59 | 59 | 10,871 | full_surgical_cohort | extracted_rln_injury_refined_v2 | RLN refined pipeline | `WHERE rln_injury_is_confirmed IS TRUE` | RLN confirmed | Table 3 | Fig 5 | T1=6 T2=19 T3_confirmed=34 |
| 13 | complication_any | Any confirmed post-op complication | 287 | 287 | 10,871 | full_surgical_cohort | patient_refined_complication_flags_v2 | Complications refined pipeline | `COUNT(DISTINCT research_id)` | Any complication | Table 3 | Fig 5 | See per-entity breakdown |
| 14 | tirads_coverage | Pre-operative TIRADS available | 3,474 | 3,474 | 10,871 | full_surgical_cohort | extracted_tirads_validated_v1 | Phase 12 engine v10 | `COUNT(DISTINCT research_id)` | TIRADS available | Supplement | — | ACR concordance 80.1% |
| 15 | survival_cohort | Survival analysis cohort | 3,201 | 3,201 | 4,136 | analysis_eligible_cancer | manuscript_cohort_v1 | script 64 | `analysis_eligible + positive time + recurrence endpoint` | Survival cohort | — | Fig 2 | 965 events; median 7.4y follow-up |
| 16 | tg_available | Patients with thyroglobulin labs | 2,559 | 2,559 | 10,871 | full_surgical_cohort | longitudinal_lab_canonical_v1 | script 77 | `DISTINCT research_id WHERE analyte_group=thyroglobulin` | Tg available | Supplement | — | 99.5% date accuracy |
| 17 | dedup_episodes | Deduplicated surgery episodes | 9,368 | 9,368 | 10,871 | full_surgical_cohort | episode_analysis_resolved_v1_dedup | script 48 | `COUNT(*)` | Dedup episodes | Table 2 | — | 146 duplicates removed |
| 18 | scoring_ajcc8 | AJCC 8th Edition calculable | 4,083 | 4,083 | 10,871 | full_surgical_cohort | thyroid_scoring_py_v1 | script 51b | `WHERE ajcc8_calculable_flag IS TRUE` | AJCC8 calculable | Table 2 | Fig 3 | 37.6% of full cohort |
| 19 | scoring_ata | ATA 2015 risk calculable | 3,144 | 3,144 | 10,871 | full_surgical_cohort | thyroid_scoring_py_v1 | script 51b | `WHERE ata_calculable_flag IS TRUE` | ATA calculable | Table 2 | Fig 3 | 28.9% of full cohort |
| 20 | vascular_invasion_graded | WHO 2022 vascular invasion graded | 819 | 819 | 3,846 | vascular_positive_patients | extracted_vascular_grading_v13 | Phase 13 engine v11 | `vasc_grade_final_v13 IN (focal, extensive)` | Vascular graded | Supplement | — | 4,652 remain present_ungraded |
| 21 | hypocalcemia_confirmed | Confirmed hypocalcemia | 18 | 18 | 10,871 | full_surgical_cohort | patient_refined_complication_flags_v2 | Complications refined pipeline | `confirmed_hypocalcemia flag` | Hypocalcemia | Table 3 | Fig 5 | — |
| 22 | hematoma_confirmed | Confirmed hematoma | 38 | 38 | 10,871 | full_surgical_cohort | patient_refined_complication_flags_v2 | Complications refined pipeline | `confirmed_hematoma flag` | Hematoma | Table 3 | Fig 5 | — |
| 23 | cox_concordance | Cox PH concordance index | 0.853 | NA | NA | survival_cohort | manuscript_analysis/cox_ph_results.csv | script 64 | `Multivariable Cox PH` | Cox C-index | — | — | Schoenfeld non-proportionality flags |
| 24 | km_5y_stage_i_ii | 5-year survival Stage I/II | 0.823 | NA | NA | survival_cohort | manuscript_analysis/km_summary.csv | script 64 | `KM estimator at 5y` | KM Stage I/II | — | Fig 2 | — |
| 25 | km_5y_stage_iii_iv | 5-year survival Stage III/IV | 0.161 | NA | NA | survival_cohort | manuscript_analysis/km_summary.csv | script 64 | `KM estimator at 5y` | KM Stage III/IV | — | Fig 2 | — |

---

## Caveats

| metric_id | Caveat |
|---|---|
| analysis_eligible_cancer | Confirmed malignancy + complete staging required |
| recurrence_any | 88.8% lack day-level dates; structural_confirmed=54, biochemical=168, date_unknown=1,764 |
| recurrence_biochemical | Rising Tg >1.0 ng/mL and >2x nadir without structural disease |
| braf_positive | Structured + NLP-confirmed; NLP requires explicit positive qualifier (positive/detected/V600E) |
| ras_positive | `ras_flag` in `molecular_test_episode_v2` is FALSE (known bug); use `extracted_ras_patient_summary_v1` exclusively |
| tert_positive | Structured molecular sources only; 23 promoter_unspecified (ThyroSeq Excel lacks variant position) |
| molecular_tested | Includes ThyroSeq, Afirma, IHC, PCR, FISH |
| rai_treated_strict | All 35 are `likely_received` with dose verification; 0 `definite_received` |
| rai_episodes_total | Dose available for 761/1,857 (41%); 0 nuclear medicine notes in clinical_notes_long |
| rln_injury_confirmed | Tier 1 = 6 (laryngoscopy), Tier 2 = 19 (chart-documented), Tier 3 confirmed = 34 |
| complication_any | Per-entity breakdown: RLN=59, hematoma=38, hypoparathyroidism=34, seroma=28, chyle_leak=20, hypocalcemia=18, wound_infection=2 |
| tirads_coverage | ACR concordance 80.1%; systematic -1.0 tier mismatch (radiologist downgrading) |
| survival_cohort | 965 events; median 7.4 years follow-up |
| tg_available | 99.5% date accuracy via `specimen_collect_dt` from `thyroglobulin_labs` |
| dedup_episodes | 146 duplicates removed (multi-pathology per surgery); documented as true edge cases |
| scoring_ajcc8 | 37.6% of full cohort; higher among analysis-eligible patients |
| scoring_ata | 28.9% of full cohort |
| vascular_invasion_graded | 4,652 remain `present_ungraded` (synoptic 'x' placeholder limitation); 310 have vessel count |
| cox_concordance | Schoenfeld test flags non-proportionality for age, stage_III_IV, ata_high, ln_positive |

---

## Usage Instructions

This registry is the canonical source for all manuscript metrics. Any metric cited in the manuscript **MUST** match a row in this registry. If a metric changes due to data updates, the registry must be re-frozen with a new `frozen_date` and the prior version archived.

- **CSV** (`manuscript_metric_registry_v1.csv`): Machine-readable for automated reconciliation scripts.
- **JSON** (`manuscript_metric_registry_v1.json`): Programmatic access with denominator metadata.
- **Markdown** (this file): Human-readable reference for authors and reviewers.

To verify a manuscript number, locate the `metric_id` in this registry and confirm:
1. The `canonical_value` matches the manuscript text.
2. The `denominator` and `denominator_label` match the reported population.
3. All `caveats` are addressed in the manuscript limitations section.
