# M085 — Aims and Hypotheses

## Primary aim

Compare diagnostic performance (sensitivity, specificity, PPV, NPV, AUC) of eleven thyroid ultrasound risk-stratification systems against final-pathology malignancy outcomes in a 25-year American surgical thyroid cancer cohort (n=37,579 nodules from ~10,871 patients):

1. ACR TI-RADS 2017 — strict (feature-complete nodules only)
2. ACR TI-RADS 2017 — imputed (missing features filled by mode imputation)
3. Kwak 2011 (Korean single-institution)
4. K-TIRADS 2021 (Korean Thyroid Association)
5. C-TIRADS 2020 (Chinese guidelines)
6. EU-TIRADS 2017 (European Thyroid Association)
7. ATA 2015 ultrasound pattern guidelines
8. BTA U1–U5 2014 (British Thyroid Association)
9. AACE/ACE/AME 2016 (tri-society American/European guidelines)
10. Horvath / Chilean 2009
11. Park / T-US 2009 — original Korean coefficients (three coefficient sets from secondary literature)
12. Park / T-US 2009 — cohort-refit GLM (re-trained on this cohort, holdout AUC 0.69)
13. SRU 2005 (Society of Radiologists in Ultrasound)

## Secondary aims

1. Characterize cross-system concordance and disagreement patterns (Cochran's Q for multi-system agreement; pairwise kappa for binary suspicious/not).
2. Evaluate per-system performance in subgroups defined by:
   - Composition class (cystic vs spongiform vs solid vs mixed)
   - Size band (≤1 cm, 1–2 cm, 2–4 cm, >4 cm)
   - Reported-versus-computed score agreement
3. Assess generalization of internationally-trained models (Park 2009 Korean cohort, K-TIRADS Korean cohort, C-TIRADS Chinese cohort) when applied to this American surgical cohort.

---

## Pre-specified hypotheses

**H1 (Coverage hypothesis):** ACR TI-RADS 2017 imputed will achieve ≥0.03 higher AUC than ACR TI-RADS 2017 strict, due to increased eligible-nodule coverage without meaningful accuracy penalty.

- Rationale: strict scoring excludes ~33% of nodules with missing composition/echogenicity features; imputation reduces denominator bias.

**H2 (Generalization failure — Park 2009):** Park 2009 original Korean coefficients will show ≥0.10 lower AUC than the same-feature-set cohort-refit GLM, consistent with non-portability of logistic-regression risk models across markedly different baseline malignancy prevalences (~8% Korean population vs ~50% American surgical cohort).

- Preliminary evidence: Park AUC 0.5365 vs cohort-refit AUC 0.6914 (Phase B.6, commit 778a61b). H2 is already confirmed in preliminary data; manuscript will formally test it and explore mechanisms.

**H3 (Pattern-based vs feature-count disagreement):** Pattern-based systems (ATA 2015, AACE/ACE/AME, BTA) will show wider inter-system disagreement rates (>15 pp difference in suspicious-classification rate) on nodules with ≥2 indeterminate features, compared to point-count systems (ACR TI-RADS, Kwak, Park) whose arithmetic scoring constrains the classification boundary.

**H4 (American-cohort advantage):** No single internationally-trained system will match the cohort-refit logistic regression on overall AUC, supporting the conclusion that cohort-specific calibration is required for optimal performance in surgical cohorts.

---

## Out-of-scope for M085

- Longitudinal surveillance intervals (see M025 and M075 for FNA-selection analysis).
- Molecular testing integration (see M045, M030).
- Cost-effectiveness analysis.
