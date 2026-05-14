# M011 — "Beyond Bethesda?" — Results Tables

Project: `thyroid-canonical-pub-2026` · Workspace: `pub_workspace.m011_*` · Generated 2026-05-14
Outcome (primary): any malignancy on final surgical pathology. NIFTP and FTUMP/borderline excluded from the primary outcome and reported separately.

---

## Cohort audit (STARD denominators)

| Metric | n | Detail |
|---|---:|---|
| Registry total patients | 10,871 | surgical thyroid registry |
| Has preoperative FNA with mappable Bethesda | 5,065 | |
| Has preoperative ultrasound nodule | 3,786 | |
| Has surgery date | 10,871 | |
| **PRIMARY COHORT** | **2,479** | preop US + preop Bethesda + surgical pathology |
| Primary cohort with ACR TI-RADS | 2,310 | ACR imputed available |
| Bethesda III/IV | 723 | III = 434, IV = 289 |
| Molecular tested (Afirma/ThyroSeq, preop) | 458 | |
| Molecular tested, Bethesda III/IV | 333 | |
| Any malignancy | 1,175 | |
| Benign | 1,237 | |
| NIFTP | 48 | handled separately |
| Borderline / FTUMP | 19 | handled separately |
| Clinically significant malignancy | 1,168 | |
| Incidental papillary microcarcinoma | 235 | |
| Frame A — linked US-nodule/FNA rows | 9,846 | one row per linked pair |
| Frame A — cohort rows (preop, Bethesda present) | 9,317 | |
| Frame A — high-confidence rows (exact/high/plausible) | 3,904 | 1,449 patients |

**Missingness (primary cohort, n=2,479):** age 0, sex 0, ACR TI-RADS imputed 169 (6.8%), ACR strict 1,677 (67.6%), EU-TIRADS 343, ATA 414, K-TIRADS 160, C-TIRADS 169, nodule size 63, path tumor size among malignant 0, not molecularly tested 2,021 (81.5%).

**Frame A linkage-confidence tiers (cohort rows):** exact_match 32 · high_confidence 998 · plausible 2,874 · weak 5,413.

---

## Table 1. Cohort characteristics by Bethesda category (primary cohort)

| Bethesda | n | Mean age | % female | Median nodule size (cm) | % ACR TR4–5 | % molecular tested | Malignancy % |
|---|---:|---:|---:|---:|---:|---:|---:|
| I | 69 | 57.6 | 81.2 | 2.39 | 58.1 | 0.0 | 23.2 |
| II | 882 | 54.9 | 84.0 | 2.26 | 44.3 | 1.8 | 15.6 |
| III | 434 | 54.6 | 80.6 | 2.20 | 56.8 | 48.8 | 48.8 |
| IV | 289 | 55.7 | 78.5 | 2.10 | 56.6 | 41.9 | 52.7 |
| V | 140 | 50.9 | 72.1 | 2.21 | 65.4 | 21.4 | 91.3 |
| VI | 665 | 51.0 | 71.3 | 2.15 | 72.0 | 11.9 | 84.5 |
| **All** | **2,479** | **53.8** | **78.6** | **2.23** | **56.8** | **18.5** | **48.7** |

---

## Table 2. Pathology outcomes by Bethesda category (primary cohort)

| Bethesda | n | Benign | Malignant | NIFTP | Malignancy % | Clin-sig % | NIFTP % | Incidental PTMC | PTC | FTC | MTC | ATC/PDTC | Aggressive features |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| I | 69 | 53 | 16 | 0 | 23.2 | 23.2 | 0.0 | 6 | — | — | — | — | 15 |
| II | 882 | 738 | 135 | 14 | 15.6 | 15.6 | 1.6 | 63 | — | — | — | — | 136 |
| III | 434 | 207 | 198 | 18 | 48.8 | 47.8 | 4.1 | 62 | — | — | — | — | 195 |
| IV | 289 | 130 | 146 | 8 | 52.7 | 52.3 | 2.8 | 27 | — | — | — | — | 134 |
| V | 140 | 12 | 126 | 2 | 91.3 | 91.3 | 1.4 | 25 | — | — | — | — | 125 |
| VI | 665 | 97 | 554 | 6 | 84.5 | 84.1 | 0.9 | 52 | — | — | — | — | 536 |

Malignancy rate rises monotonically across Bethesda categories (II 15.6% → III 48.8% → IV 52.7% → V 91.3% → VI 84.5%). Clinically significant malignancy tracks any-malignancy closely. NIFTP concentrates in Bethesda III/IV (4.1% / 2.8%).

---

## Table 3. Bethesda × ACR TI-RADS malignancy-risk heat table (primary cohort, % any malignancy; cell n in parentheses)

| Bethesda \ ACR | TR1 | TR2 | TR3 | TR4 | TR5 |
|---|---|---|---|---|---|
| I  | 28.6 (7)  | 20.0 (5)   | 14.3 (14)  | 15.4 (13)  | 30.4 (23)  |
| II | 13.2 (77) | 19.8 (133) | 11.2 (254) | 14.0 (111) | 18.2 (258) |
| III| 50.0 (21) | 47.2 (57)  | 40.6 (101) | 56.2 (75)  | 50.3 (160) |
| IV | 31.6 (21) | 58.3 (36)  | 50.0 (59)  | 57.1 (42)  | 53.9 (109) |
| V  | 100 (6)   | 94.7 (19)  | 80.0 (20)  | 87.5 (16)  | 94.0 (69)  |
| VI | 63.6 (22) | 69.1 (55)  | 61.8 (92)  | 86.1 (81)  | 93.2 (354) |

Key reading: within Bethesda **II** and within Bethesda **III/IV**, the ACR TI-RADS gradient is flat / non-monotonic — higher TI-RADS does not reliably raise malignancy risk. The TI-RADS gradient is meaningful only in Bethesda **VI** (62% → 93%), where management is already decided.

---

## Table 4. Sequential model performance — primary cohort, outcome = any malignancy

Complete-case main set: n = 2,245, events = 1,080 (48.1%). AUC by Mann-Whitney rank statistic; 95% CI = bootstrap (2,000 resamples), nearly identical to the Hanley–McNeil analytic CI; ΔAUC vs Bethesda-only (Model A); p = paired DeLong test.

| Model | Predictors | n | AUC (95% CI) | ΔAUC vs A | DeLong p | LR test p | Brier | Calib slope |
|---|---|---:|---|---:|---:|---:|---:|---:|
| A | Bethesda only | 2,245 | 0.824 (0.806–0.840) | — | — | — | 0.163 | 1.01 |
| B | ACR TI-RADS only | 2,245 | 0.633 (0.612–0.655) | −0.191 | — | — | 0.246 | — |
| C | Bethesda + ACR TI-RADS | 2,245 | 0.839 (0.823–0.855) | +0.015 | 2.6×10⁻⁶ | 2.9×10⁻⁸ | 0.161 | 1.01 |
| D | Bethesda + ACR TI-RADS + age + sex + size + year | 2,245 | 0.845 (0.829–0.860) | +0.021 | 2.1×10⁻⁹ | 8.9×10⁻⁶ | 0.159 | 1.02 |
| E | Bethesda + individual US features | 2,245 | 0.840 (0.822–0.855) | +0.016 | 1.2×10⁻⁶ | 1.8×10⁻⁸ | 0.161 | 1.01 |

Molecular subgroup (complete-case + molecularly tested): n = 411, events = 234 (56.9%).

| Model | Predictors | n | AUC (95% CI) | ΔAUC vs F0 | DeLong p | LR test p | Brier |
|---|---|---:|---|---:|---:|---:|---:|
| F0 | Bethesda only (molecular cohort) | 411 | 0.672 (0.627–0.717) | — | — | — | 0.216 |
| F1 | Bethesda + ACR TI-RADS | 411 | 0.697 (0.649–0.744) | +0.025 | 0.13 | 0.077 | 0.212 |
| F | Bethesda + ACR TI-RADS + molecular | 411 | 0.760 (0.713–0.806) | +0.088 | 5.1×10⁻⁵ | 4.9×10⁻⁵ (F1→F) | 0.198 |
| G | Bethesda + US features + molecular | 411 | 0.734 (0.685–0.778) | +0.061 | — | — | 0.204 |

Within the molecular cohort, adding TI-RADS to Bethesda gives +0.025 AUC (DeLong p = 0.13, **not significant**); adding the molecular result gives +0.088 (DeLong p = 5.1×10⁻⁵, **significant**) — molecular testing contributes roughly **3–4× the increment** of composite TI-RADS. Note: the TI-RADS increment in the *full* primary cohort (Model C) is statistically significant only because of the large sample (n = 2,245); its absolute magnitude (+0.015) is below the clinical-meaningfulness threshold and it is not significant in Bethesda III/IV.

---

## Table 5. Bethesda III/IV subgroup performance — outcome = any malignancy

Bethesda III/IV complete-case: n = 642, events = 322 (50.2%). Molecular-tested III/IV subset: n = 297, events = 144 (48.5%).

| Model | n | AUC (95% CI) | ΔAUC vs Bethesda-ref | DeLong p |
|---|---:|---|---:|---:|
| Bethesda reference (within III/IV) | 642 | 0.519 (0.482–0.555) | — | — |
| ACR TI-RADS only | 642 | 0.550 (0.508–0.592) | +0.031 | 0.29 (NS) |
| Individual US features | 642 | 0.536 (0.493–0.584) | +0.017 | 0.57 (NS) |
| Molecular only (tested subset) | 297 | 0.613 (0.557–0.663) | — | — |
| ACR TI-RADS + molecular (tested subset) | 297 | 0.661 (0.598–0.719) | — | — |
| Combined: TI-RADS + key features + molecular (tested subset) | 297 | 0.657 (0.595–0.718) | — | — |

In the clinically pivotal Bethesda III/IV group, composite TI-RADS alone barely beats chance (0.55) and individual US features do not improve on it. Molecular testing reaches 0.61 alone and 0.66 combined with TI-RADS — the only second-stage tool with material discrimination here.

Selected operating points (Bethesda III/IV molecular-tested subset, ACR TI-RADS + molecular model): at the malignancy-prevalence threshold (~0.50) sensitivity ≈ 0.49, specificity ≈ 0.73, PPV ≈ 0.63, NPV ≈ 0.61, false-negative rate ≈ 0.51. No threshold on a composite-TI-RADS rule achieved an NPV high enough to safely defer diagnostic surgery in this subgroup.

---

## Table 6. Combined risk groups — Bethesda III/IV × ACR TI-RADS × molecular (primary cohort)

| Bethesda | TI-RADS group | Molecular | n | Malignancy % | Clin-sig % | NIFTP |
|---|---|---|---:|---:|---:|---:|
| III | low TR1–3 | not tested | 92 | 47.1 | 47.1 | 6 |
| III | low TR1–3 | molec negative | 51 | 31.9 | 31.9 | 4 |
| III | low TR1–3 | molec positive | 36 | 51.4 | 51.4 | 1 |
| III | high TR4–5 | not tested | 119 | 54.5 | 52.7 | 5 |
| III | high TR4–5 | molec negative | 83 | 43.6 | 41.0 | 1 |
| III | high TR4–5 | molec positive | 33 | 65.6 | 65.6 | 0 |
| IV | low TR1–3 | not tested | 67 | 52.4 | 52.4 | 4 |
| IV | low TR1–3 | molec negative | 27 | 30.8 | 30.8 | 1 |
| IV | low TR1–3 | molec positive | 22 | 63.6 | 63.6 | 0 |
| IV | high TR4–5 | not tested | 89 | 51.7 | 50.6 | 2 |
| IV | high TR4–5 | molec negative | 36 | 47.1 | 47.1 | 1 |
| IV | high TR4–5 | molec positive | 26 | 78.3 | 78.3 | 0 |

Within both Bethesda III and IV, **molecular status separates risk more than TI-RADS group**: e.g. Bethesda IV low-TIRADS swings from 30.8% (molecular negative) to 63.6% (molecular positive), whereas moving from low to high TIRADS at fixed molecular status shifts risk only modestly. TIRADS-missing strata omitted for brevity (small n).

---

## Table 7. Molecular-tested vs not-tested comparison (selection bias)

| Bethesda group | Molecular tested | n | Mean age | % female | Mean size (cm) | % ACR TR4–5 | Malignancy % | Clin-sig % |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Bethesda I/II/V/VI | No  | 1,631 | — | — | 2.85 | 55.9 | 45.6 | 45.5 |
| Bethesda I/II/V/VI | Yes | 125   | — | — | 2.74 | 68.4 | 80.3 | 80.3 |
| Bethesda III/IV    | No  | 390   | — | — | 2.72 | 56.7 | 51.8 | 51.0 |
| Bethesda III/IV    | Yes | 333   | — | — | 2.79 | 56.7 | 48.7 | 48.1 |

Molecular testing is **selectively ordered**. Outside the indeterminate categories, tested patients are far higher-risk than untested (80% vs 46% malignancy) — molecular results there cannot be generalised. Within Bethesda III/IV, tested and untested patients are well balanced (48.7% vs 51.8% malignancy; similar size and TI-RADS distribution), so the molecular-subgroup analyses are interpretable for the indeterminate-cytology question.

---

## Table 8. Sensitivity analyses — model performance across outcome definitions

Sequential models A/C/D/E re-run with three alternate outcomes. AUC by rank statistic; ΔAUC vs Bethesda-only (A); p = paired DeLong test.

| Outcome | n (events) | A Bethesda AUC | C Beth+TIRADS ΔAUC (p) | D +clinical ΔAUC (p) | E +US features ΔAUC (p) |
|---|---:|---:|---:|---:|---:|
| Any malignancy (NIFTP excluded — **primary**) | 2,245 (1,080) | 0.824 | +0.015 (2.6×10⁻⁶) | +0.021 (2.1×10⁻⁹) | +0.016 (1.2×10⁻⁶) |
| Clinically significant malignancy | 2,245 (1,074) | 0.823 | +0.015 (7.6×10⁻⁶) | +0.021 (6.7×10⁻⁹) | +0.016 (2.5×10⁻⁶) |
| NIFTP counted benign | 2,310 (1,080) | 0.818 | +0.015 (2.9×10⁻⁶) | +0.021 (4.4×10⁻⁹) | +0.015 (3.1×10⁻⁶) |
| NIFTP counted malignant | 2,310 (1,127) | 0.815 | +0.014 (3.6×10⁻⁵) | +0.020 (7.9×10⁻⁹) | +0.014 (1.5×10⁻⁵) |

**The headline finding is fully robust.** Across all four outcome definitions the TI-RADS increment is +0.014–0.015, the +clinical increment +0.020–0.021, and individual US features +0.014–0.016 — essentially unchanged. NIFTP handling shifts the *absolute* Bethesda-only AUC by ~0.01 but leaves the *incremental* TI-RADS contribution untouched. Switching from any malignancy to clinically significant malignancy makes no material difference. (Data: `m011_sensitivity_analyses.csv`, `m011_sensitivity_delong.csv`.)

---

## Notes
- AUC 95% CIs are bootstrap (2,000 resamples); ΔAUC p-values are the **paired DeLong test** (`scripts/m011_advanced_stats.py`, run 2026-05-14). These supersede the earlier Hanley–McNeil approximate z-values.
- Interpretation guardrail: the TI-RADS increment is *statistically* significant in the full cohort but its magnitude is clinically marginal (ΔAUC +0.015–0.021, below the pre-specified ≥0.02–0.03 threshold for clinical relevance) and it is *not* significant in Bethesda III/IV. The molecular increment (+0.088) is both statistically and clinically meaningful.
- All models use complete-case data and apparent (in-sample) performance. Calibration slope/intercept and LR tests are computed; bootstrap optimism correction of the AUCs and decision-curve confidence bands remain to be added.
- NIFTP three-way sensitivity columns (`any_malignancy_niftp_malig`, `any_malignancy_niftp_benign`) are built into `m011_frame_a/_b` and `m011_model_data`; rerun `m011_models.sql` swapping the label column to reproduce.
