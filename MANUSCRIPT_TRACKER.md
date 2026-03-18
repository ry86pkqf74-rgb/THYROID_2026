# THYROID_2026 — Manuscript Tracker

> **Last updated:** 2026-03-18

## Active Manuscripts

| # | Short Title | Status | Study Dir | Lead |
|---|-------------|--------|-----------|------|
| 1 | Multimodal Prediction of Thyroid Cancer Recurrence | Modeling complete | `studies/proposal_multimodal_prediction_20260318/` | — |
| 2 | Molecular + Imaging Discordance in Bethesda III/IV | Planned | `studies/proposal_mol_imaging_discordance/` | — |
| 3 | Surgeon-Level Variability & Outcomes (Hierarchical Modeling) | Planned | `studies/proposal_surgeon_variability/` | — |

---

## Paper 1 — Multimodal Prediction of Thyroid Cancer Recurrence

**Status:** Modeling complete — results under review

**Aim:** Evaluate incremental predictive value of imaging (TIRADS) and notes-derived features (molecular markers, labs, FNA) over structured clinical/pathology data for thyroid cancer recurrence prediction.

| Milestone | Status | Date |
|-----------|--------|------|
| Feasibility analysis | Done | 2026-03-18 |
| Candidate dataset built | Done | 2026-03-18 |
| Feature sets A/B/C defined | Done | 2026-03-18 |
| Model training (LR + XGBoost) | Done | 2026-03-18 |
| AUC / calibration / importance | Done | 2026-03-18 |
| Manuscript draft | Not started | — |
| Internal review | Not started | — |
| Submission | Not started | — |

**Key results:** Set C (structured + imaging + notes) XGBoost AUC = 0.999; largest gain from notes-derived features (A→C Δ AUC +0.019).

**Artifacts:**
- `studies/proposal_multimodal_prediction_20260318/model_results/`
- `studies/proposal_multimodal_prediction_20260318/model_results/model_results_summary.md`

---

## Paper 2 — Molecular + Imaging Discordance in Bethesda III/IV

**Status:** Planned

**Aim:** Characterize the frequency and clinical significance of discordance between molecular testing results (ThyroSeq, Afirma) and ultrasound imaging features (ACR TI-RADS) among patients with indeterminate FNA cytology (Bethesda III AUS/FLUS and Bethesda IV FN/SFN). Evaluate whether combined molecular–imaging concordance/discordance patterns predict malignancy on final surgical pathology and inform management decisions.

**Rationale:**
- Bethesda III/IV nodules represent a clinical gray zone where molecular testing and imaging each contribute independent risk information
- Discordant cases (e.g., suspicious molecular + low TIRADS, or benign molecular + high TIRADS) create management dilemmas
- This cohort has strong overlap of both modalities: 1,577 patients with validated TIRADS + 3,854 with molecular data; FNA Bethesda available for 2,521

**Candidate cohort:**
- Bethesda III (AUS/FLUS): N ≈ 692
- Bethesda IV (FN/SFN): N ≈ 649
- Subset with both TIRADS + molecular: to be quantified in feasibility

**Key tables/views:**
- `extracted_fna_bethesda_v1` — FNA Bethesda by source
- `extracted_tirads_validated_v1` — multi-source TIRADS
- `molecular_test_episode_v2` — ThyroSeq / Afirma results
- `patient_refined_master_clinical_v12` — unified patient-level flags
- `manuscript_cohort_v1` — analysis-ready cohort

**Planned analyses:**
- Concordance/discordance matrix (molecular result × TIRADS category)
- Malignancy rate by concordance group
- Logistic regression: malignancy ~ molecular + TIRADS + interaction
- Subgroup analysis by molecular platform (ThyroSeq vs Afirma)
- Diagnostic performance metrics (sensitivity, specificity, NPV, PPV) per strategy

| Milestone | Status | Date |
|-----------|--------|------|
| Feasibility / cohort definition | Not started | — |
| Concordance matrix | Not started | — |
| Statistical analysis | Not started | — |
| Manuscript draft | Not started | — |
| Internal review | Not started | — |
| Submission | Not started | — |

**Artifacts:** `studies/proposal_mol_imaging_discordance/` (to be created)

---

## Paper 3 — Surgeon-Level Variability & Outcomes (Hierarchical Modeling)

**Status:** Planned

**Aim:** Quantify surgeon-level variability in operative technique, complication rates, and oncologic outcomes using hierarchical (mixed-effects) models that account for patient case-mix. Identify institutional factors and surgeon practice patterns associated with differential outcomes.

**Rationale:**
- 10,871 surgical patients across multiple surgeons provide sufficient volume for hierarchical modeling
- Complication rates (RLN injury, hypocalcemia, hypoparathyroidism) and oncologic outcomes (recurrence, completeness of resection) may vary by surgeon even after risk adjustment
- Surgeon-level random effects capture unmeasured provider-level confounding not addressed by patient-level covariates
- Results inform quality benchmarking, training, and credentialing

**Candidate cohort:**
- Full surgical cohort: N = 10,871
- Surgeon identification via operative details (surgeon field availability to be confirmed)
- Minimum surgeon volume threshold for inclusion (e.g., ≥ 20 cases)

**Key tables/views:**
- `operative_episode_detail_v2` — surgery details, procedure type
- `complication_phenotype_v1` — structured complication classification
- `complication_patient_summary_v1` — per-patient complication flags
- `extracted_rln_injury_refined_v2` — refined RLN injury
- `extracted_complications_refined_v5` — all refined complications
- `patient_analysis_resolved_v1` — full patient-level resolved data
- `thyroid_scoring_py_v1` — AJCC8, ATA, MACIS risk scores

**Planned analyses:**
- Surgeon-level case volume and case-mix profiling
- Random-intercept logistic models: complication ~ patient covariates + (1|surgeon)
- Random-intercept Cox models: recurrence ~ risk factors + (1|surgeon)
- Intraclass correlation coefficient (ICC) for surgeon-level variance
- Funnel plots: observed vs expected complication rates by surgeon
- Caterpillar plots: surgeon random effects with 95% CI
- Sensitivity analysis: minimum volume thresholds (20, 50, 100 cases)

| Milestone | Status | Date |
|-----------|--------|------|
| Surgeon ID feasibility check | Not started | — |
| Cohort definition + case-mix | Not started | — |
| Hierarchical model fitting | Not started | — |
| Funnel / caterpillar plots | Not started | — |
| Manuscript draft | Not started | — |
| Internal review | Not started | — |
| Submission | Not started | — |

**Artifacts:** `studies/proposal_surgeon_variability/` (to be created)

---

## Completed / Submitted Manuscripts

| # | Title | Status | Study Dir |
|---|-------|--------|-----------|
| — | ETE Staging & Recurrence (PSM) | Submitted | `studies/proposal2_ete_staging/` |
| H1 | Central LN Dissection in Lobectomy | Analysis complete | `studies/hypothesis1_cln_lobectomy/` |
| H2 | Goiter, Race & SDOH Disparities | Analysis complete | `studies/hypothesis2_goiter_sdoh/` |
