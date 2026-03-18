# Multimodal Thyroid Cancer Prediction — Feasibility Analysis Summary

Generated: 20260318_0604

## 1. Cohort Size

| Metric | Count |
|--------|-------|
| Full surgical cohort (master_cohort) | 11,673 |
| Manuscript cohort (manuscript_cohort_v1) | 10,871 |
| **Cancer analytic cohort (analysis_cancer_cohort_v1)** | **4,136** |
| Patient analysis resolved | 10,871 |
| Dedup episodes | 9,368 |

## 2. Multimodal Data Availability (Cancer Cohort, N=4,136)

| Question | Count |
|----------|-------|
| Structured clinical data only | 267 |
| Imaging-linked data (TIRADS) | 1,577 |
| Note-derived / NLP-linked data (labs, NLP events) | 2,522 |
| All three modalities | 1,040 |

## 3. Modality Group Breakdown

                   modality  n_patients  pct
            group:all_three        1040 25.1
group:imaging_and_molecular         522 12.6
         group:imaging_only          15  0.4
       group:molecular_only        2292 55.4
      group:structured_only         267  6.5

## 4. Usable Endpoints (Manuscript-Safe)

                  endpoint  n_events  pct                                      manuscript_safe
            recurrence_any      1933 46.7    YES – boolean; date sparse (see analysis_summary)
             braf_positive       287  6.9                                                  YES
              ras_positive       198  4.8                                                  YES
             tert_positive        62  1.5                                                  YES
           ete_microscopic      3639 88.0                                                  YES
                 ete_gross       188  4.5                                                  YES
     vascular_invasion_any      3748 90.6 YES (87% are present_ungraded — synoptic limitation)
any_complication_confirmed        42  1.0                   YES – boolean; timing windows vary
     structural_recurrence      1818 44.0               YES – binary; date NOT manuscript-safe
    biochemical_recurrence       115  2.8               YES – binary; date NOT manuscript-safe

## 5. Endpoints NOT Manuscript-Safe

- time_to_recurrence (88.8% unresolved dates – not manuscript-safe for TTE analysis)
- time_to_death (no death events in clinical_events – augmented only with synthetic proxy)
- RAI dose (41% coverage – usable as covariate, not as primary endpoint)
- voice outcomes (0.23% coverage – too sparse)

## 6. Top 10 Candidate Predictors (by Completeness)

```
                feature  n_available  pct_available
         age_at_surgery         4136          100.0
                    sex         4136          100.0
        histology_final         4136          100.0
          braf_positive         4136          100.0
        ames_risk_group         4136          100.0
          tert_positive         4136          100.0
           ras_positive         4136          100.0
             ages_score         4136          100.0
        recurrence_flag         4136          100.0
has_complication_record         4136          100.0
```

## 7. Executive Summary

### Recommended Primary Endpoint
**Recurrence (binary)**: 1,986/10,871 overall; precise rate in cancer cohort captured in `recurrence_flag`.
For multimodal prediction, binary recurrence is the most defensible primary endpoint given current data maturity.
Secondary: adverse pathology composite (ETE + vascular invasion + positive margins).

### Recommended Cohort
**analysis_cancer_cohort_v1** (N=4,136): analysis-eligible patients with confirmed thyroid cancer, complete staging, and eligibility flags.

### Expected Manuscript-Safe Sample Size
- Full cancer cohort: **4,136**
- With TIRADS imaging: **1,577**
- With molecular + imaging + labs: **1,040**
- Minimum viable multimodal subset for prediction: **1,040** (if imaging required); **4,136** (if imaging optional/imputed)

### Top Blockers
1. **TIRADS coverage**: Only ~38.1% of cancer cohort has validated TIRADS data — limits mandatory imaging arm
2. **Recurrence date sparsity**: Binary recurrence is available; precise time-to-event is not manuscript-safe (88.8% unresolved dates)
3. **Nuclear medicine absence**: RAI dose coverage capped at 41% — usable as covariate, not endpoint
4. **Vascular invasion grading**: 87% remain 'present_ungraded' — synoptic template limitation
5. **Molecular testing coverage**: only 3854 cancer patients with molecular episode data; platform heterogeneity (ThyroSeq vs Afirma)

### Next 5 Concrete Steps
1. Finalize endpoint definition: binary recurrence (primary) + adverse pathology composite (secondary)
2. Run multiple imputation (MICE) for imaging/molecular missingness in full cancer cohort
3. Build multimodal feature matrix: structured (demographics+staging) + imaging (TIRADS+nodule features) + NLP (lab trajectories+complication flags)
4. Fit and internally validate prediction models (logistic + XGBoost + Cox PH on binary recurrence)
5. Draft TRIPOD-compliant Methods section and register study protocol
