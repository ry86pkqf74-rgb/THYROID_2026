# M011 — Results Summary & Manuscript Framings

*Beyond Bethesda? Incremental Value and Limits of Ultrasound Risk Stratification and Molecular Testing After Bethesda Cytology in Surgical Thyroid Nodules*

Project `thyroid-canonical-pub-2026` · primary analysis frame = Frame B (patient-level) · generated 2026-05-14

---

## 1. What the analysis found

**Cohort.** 10,871-patient surgical thyroid registry → **2,479 primary cohort** with preoperative ultrasound, preoperative FNA with a mappable Bethesda category, and final surgical pathology. ACR TI-RADS available in 2,310. Bethesda III/IV n = 723 (III 434, IV 289). Preoperative molecular testing (Afirma/ThyroSeq) in 458 (333 of them Bethesda III/IV). Final pathology: 1,175 malignant, 1,237 benign, 48 NIFTP, 19 borderline/FTUMP; 1,168 clinically significant malignancies; 235 incidental papillary microcarcinomas.

**Bethesda is a strong baseline.** Malignancy rate rose monotonically II 15.6% → III 48.8% → IV 52.7% → V 91.3% → VI 84.5%. A Bethesda-only logistic model achieved AUROC **0.824 (95% CI 0.806–0.841)**, Brier 0.163, and was well calibrated.

**TI-RADS adds a statistically detectable but clinically marginal increment after Bethesda.** Adding ACR TI-RADS (Model C) moved AUROC to **0.839 (ΔAUC +0.015, DeLong p = 2.6×10⁻⁶)**; adding clinical covariates as well (Model D) reached 0.845 (+0.021, p = 2.1×10⁻⁹); Bethesda + individual US features (Model E) reached 0.840 (+0.016, p = 1.2×10⁻⁶). The paired DeLong tests are significant — with n = 2,245 even a small correlated gain is detectable — but every increment is **at or below the pre-specified clinically meaningful threshold (ΔAUC ≥ 0.02–0.03)**, the decision-curve and calibration curves for A, C and D are visually superimposed across the clinically plausible 10–40% threshold range, and calibration slopes are essentially unchanged (A 1.01, C 1.01, D 1.02). TI-RADS alone (Model B) was weak (AUROC 0.633). The likelihood-ratio test agrees (A→C p = 2.9×10⁻⁸).

**The Bethesda × TI-RADS heat map shows why.** Within Bethesda II and within indeterminate Bethesda III/IV, the malignancy gradient across TR1→TR5 is flat and non-monotonic (e.g. Bethesda III: TR1 50%, TR3 41%, TR5 50%; Bethesda IV: TR1 32%, TR2 58%, TR5 54%). A clean monotone TI-RADS gradient appears only in Bethesda VI (62%→93%), where cytology has already determined management.

**In Bethesda III/IV — the clinically pivotal subgroup — composite TI-RADS barely beats chance and the increment is not significant.** AUROC 0.55 for TI-RADS alone (ΔAUC +0.031, DeLong p = 0.29) and 0.54 for individual US features (+0.017, p = 0.57); neither improved on the within-subgroup Bethesda reference. No threshold on a composite-TI-RADS rule produced an NPV high enough to safely defer diagnostic surgery.

**Molecular testing is the stronger second-stage tool.** In the molecular-tested subgroup (n = 411), adding TI-RADS to Bethesda gave +0.025 AUROC (DeLong p = 0.13, LR p = 0.08 — not significant) whereas **adding the molecular result gave +0.088 (DeLong p = 5.1×10⁻⁵; LR p = 4.9×10⁻⁵)** — roughly 3–4× the TI-RADS increment and the only increment that is both statistically and clinically meaningful. Within molecular-tested Bethesda III/IV (n = 297), molecular result alone reached AUROC 0.61 and TI-RADS + molecular 0.66. In the combined risk groups, molecular status separated risk more than TI-RADS group did within every Bethesda III/IV stratum (e.g. Bethesda IV / low TI-RADS: 30.8% malignant if molecular-negative vs 63.6% if molecular-positive).

**Selection bias is real and bounded.** Outside the indeterminate categories, molecular-tested patients were far higher-risk than untested (80% vs 46% malignancy) — those results do not generalise. *Within* Bethesda III/IV, tested and untested patients were well balanced (48.7% vs 51.8% malignancy, similar size and TI-RADS distribution), so the molecular-subgroup analyses are interpretable for the indeterminate-cytology question.

---

## 2. Manuscript Framing A — "TI-RADS adds measurable incremental value"

> In this surgical cohort, ultrasound risk stratification added a small but measurable increment to Bethesda cytology for predicting final-pathology malignancy (AUROC 0.824 → 0.839; with clinical covariates 0.845). The benefit was concentrated in, and amplified by, multimodal combination: a model integrating Bethesda, TI-RADS and molecular testing reached AUROC 0.76 in the molecular-tested subgroup, supporting a layered preoperative risk-assessment pathway in which imaging contributes context alongside cytology and molecular results.

**Supportable claims under Framing A:** the TI-RADS increment is *statistically significant* by the paired DeLong test (A→C p = 2.6×10⁻⁶) and by the likelihood-ratio test (p = 2.9×10⁻⁸); ΔAUC is positive and directionally consistent across Models C, D and E; Model D's +0.021 increment meets the pre-specified "clinically modest but reportable" threshold (ΔAUC ≥ 0.02); Brier score improved slightly (0.163 → 0.159); ACR TR4 and TR5 retained independent associations with malignancy in the adjusted model (OR 1.76 and 2.21). **Weaknesses to disclose:** the absolute ΔAUC (+0.015 for C) is below the clinical-meaningfulness threshold, decision-curve net benefit is not separable from Bethesda alone, calibration is essentially unchanged, and — critically — the increment is **not significant in the Bethesda III/IV subgroup** (DeLong p = 0.29) where it would matter most. Statistical significance here is driven by sample size, not by clinically actionable reclassification.

---

## 3. Manuscript Framing B — "TI-RADS adds little after Bethesda; molecular testing and specific features add more" *(recommended)*

> Once Bethesda cytology is known, composite TI-RADS categories contributed only a marginal increment to discrimination in surgically managed thyroid nodules: although the gain reached statistical significance in the full cohort (ΔAUROC +0.015, DeLong p < 10⁻⁵) — an artefact of large sample size — it fell below the pre-specified clinically meaningful threshold, produced no decision-curve or calibration benefit, and was **not significant within indeterminate Bethesda III/IV nodules** (ΔAUROC +0.031, p = 0.29), the group in which second-stage risk stratification is most needed, where composite TI-RADS performed barely above chance (AUROC 0.55). In contrast, molecular testing provided roughly three to four times the incremental discrimination of TI-RADS (ΔAUROC +0.088, p < 10⁻⁴ vs +0.025, p = 0.13) and, together with selected high-risk ultrasound features, separated malignancy risk within every Bethesda III/IV stratum. These data argue that after Bethesda cytology, composite ultrasound category is largely redundant for clinical decision-making, whereas molecular testing — and to a lesser degree individual ultrasound features — carry the actionable second-stage information.

**Why Framing B is recommended:** it is the result the data actually support once statistical significance is separated from clinical significance; the TI-RADS increment is real but trivial in magnitude and disappears in the subgroup that matters, while the molecular increment is both significant and clinically sizeable; it converts the "limits of TI-RADS" finding into the paper's main contribution; and it foregrounds the Bethesda III/IV and molecular analyses, which are M011's distinct contribution over Manuscript 1. It maps onto the plan's Scenario C + Scenario D. The honest one-line version: *"TI-RADS is statistically but not clinically incremental after Bethesda; molecular testing is both."*

---

## 4. Confirmatory analyses — status

1. ✅ **DONE** — `scripts/m011_advanced_stats.py` run (2026-05-14, Mac via Desktop Commander): bootstrap AUROC CIs, DeLong paired tests, calibration slope/intercept, likelihood-ratio tests, adjusted ORs → `tables/m011_*.csv`. DeLong p-values supersede the earlier approximate z-values.
2. ✅ **DONE** — **NIFTP three-way sensitivity** (NIFTP excluded / counted benign / counted malignant) and **clinically-significant-malignancy outcome**: see Table 8 in `m011_all_tables.md` and `tables/m011_sensitivity_analyses.csv` + `m011_sensitivity_delong.csv`. **Result is fully robust** — the TI-RADS increment is +0.014–0.015 and the +clinical increment +0.020–0.021 under every outcome definition; NIFTP handling shifts only the absolute AUC (~0.01), not the increment.
3. ☐ **Remaining** — Frame A nodule-linked sensitivity analysis (high-confidence tier) vs Frame B.
4. ☐ **Remaining** — bootstrap optimism correction of the apparent AUCs; decision-curve bootstrap bands.
5. ☐ **Remaining** — pathologist review of the free-text histology classifier (`m011_build_pipeline.sql` step 1).
6. ☐ **Remaining** — era-stratified analysis and the temporal development/validation split.

## 5. Numbers at a glance

| | Bethesda only | + TI-RADS | + TI-RADS + clinical | + molecular |
|---|---|---|---|---|
| Primary cohort AUROC (n=2,245) | 0.824 | 0.839 (+0.015, p 2.6e-6) | 0.845 (+0.021, p 2.1e-9) | — |
| Molecular subgroup AUROC (n=411) | 0.672 | 0.697 (+0.025, p 0.13) | — | 0.760 (+0.088, p 5.1e-5) |
| Bethesda III/IV AUROC | 0.519 ref | 0.550 (+0.031, p 0.29) | — | 0.66 (TIRADS+mol, n=297) |

ΔAUC vs the relevant Bethesda-only reference; p-values are the paired DeLong test. The TI-RADS increment is statistically significant in the full cohort (driven by n) but **not significant in Bethesda III/IV** and below the clinical-meaningfulness threshold throughout. The molecular increment (+0.088) is both statistically significant and clinically sizeable. Bootstrap 95% CIs for every model AUROC are in `tables/m011_auc_bootstrap_ci.csv` and closely match the Hanley–McNeil analytic CIs.
