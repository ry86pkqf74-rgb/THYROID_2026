# M085 — Analysis Plan

## 1. Primary diagnostic performance (per system)

For each of the 11+ systems, on the labeled-nodule subset:

- **AUC (ROC):** DeLong method with 95% CI
- **Sensitivity / Specificity / PPV / NPV** at the system-recommended suspicious threshold (e.g., TR4+ for ACR, P4+ for Park, Category 4+ for EU-TIRADS)
- **Calibration:** Hosmer-Lemeshow test + calibration plot for scoring systems that produce continuous risk scores (Park GLM, cohort-refit GLM)
- **Per-system denominator N** (feature-complete nodules with path labels)

Software: sklearn `roc_auc_score`, `RocCurveDisplay`; lifelines for survival-adjusted follow-up in sensitivity analysis.

---

## 2. Pairwise system comparison

**McNemar test** for each pair of systems on the set of nodules classified by both. Null hypothesis: the two systems misclassify the same nodules. p-value corrected for multiple comparisons (Benjamini-Hochberg FDR).

Primary comparisons of interest:
- ACR strict vs ACR imputed (tests H1)
- Park original vs Park cohort-refit (tests H2)
- ACR 2017 vs K-TIRADS 2021 (generalization: Korean-trained vs American-trained)
- ACR 2017 vs EU-TIRADS (geographic generalization)

---

## 3. Multi-system agreement

**Cochran's Q test** on the 11-dimensional binary suspicious/not vector for each nodule (nodules classified by all 11 systems simultaneously). Post-hoc pairwise McNemar with Bonferroni correction.

**Fleiss' kappa** for multi-rater agreement across all 11 systems.

---

## 4. Subgroup analyses (pre-specified)

For each of the three subgroup factors (composition, size band, reported-vs-computed agreement):
- Repeat per-system AUC computation within each subgroup
- Forest plot of AUC estimates by subgroup for primary 5 systems
- Interaction test: logistic regression with system × subgroup interaction term

---

## 5. Generalization failure analysis (Park 2009)

- Compare per-feature prevalences (X1–X12) in this cohort vs Park 2009 Table 2 (Korean cohort)
- Estimated malignancy prevalence in this cohort vs Park's derivation cohort
- Hosmer-Lemeshow calibration using Park's original regression curve
- Interpretation: distinguish coefficient non-portability from feature-prevalence shift as explanatory mechanism

---

## 6. Reporting

Tables:
1. Cohort characteristics (N by system, feature completeness)
2. Primary performance table (AUC, sens, spec, PPV, NPV per system at recommended threshold)
3. Pairwise McNemar comparison matrix (11×11)
4. Subgroup AUC by composition, size, and score-agreement stratum

Figures:
1. ROC curves: all 11 systems on same axes
2. Calibration plots: Park 2009 original vs cohort-refit
3. Forest plot: AUC by subgroup for 5 primary systems
4. Heatmap: pairwise system agreement (kappa) matrix
5. Nodule-level disagreement rate by feature count

---

## 7. Sensitivity analyses

- Treat NIFTP as malignant vs benign (primary = benign per 2017 reclassification)
- Use maximum suspicious nodule per exam vs independent nodule analysis
- Restrict to nodules with ≥5 of 12 Park features present (feature-rich subset)
- Use only first exam per patient (eliminate repeat-exam correlation)
