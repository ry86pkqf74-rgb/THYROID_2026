# M048 v3.2 — Handoff Package (post-verification revision)

**Manuscript:** Racial Disparities in ACR TI-RADS Performance
**Full operative cohort:** n = 3,375 (single-institution, 2000–2025)
**Analytic cohort (Black/White/Asian, used in all regressions):** n = 3,121 (excludes 87 Other and 167 Unknown / NHPI)
**Run:** v3.2 final (1,000-rep mediation; SQL re-extraction)
**MotherDuck migration:** mig_317b (signed off 2026-05-05)
**Database:** thyroid_canonical_publication_v1_0 @ release tag pub_v1_1
**Independent recompute:** `independent_recompute_v3.py` → 5/5 PASS at 0.0% relative difference using the same `prepare_v3_frame` transformations the run script applies. Reviewers running their own bespoke transformations may see ~2–4% differences for Asian M6 because n=204 makes that estimate sensitive to small differences in `surg_year` centering and `days_us_to_surg_approx` scaling.

**Post-verification revision changes (vs initial v3.2 zip):**

- Sensitivity arm S048v3_E_no_Bethesda_VI corrected: previously matched zero rows because the bucket label is `VI_malig` not `VI`; now uses `startswith("VI")`. Drop-Bethesda-VI arm n changed from a buggy 3,121 to the correct 2,599; Black OR 0.417 (0.341–0.509, p<0.0001), Asian OR 1.27 (0.85–1.90, p=0.24).
- Per-patient analysis CSV gained `any_ete_flag`, `gross_ete_flag`, `microscopic_ete_flag`, `ete_grade`, `ln_positive_flag`, and `n_positive_ln_level_mentions` so the Table 9 ETE/LN values are independently reproducible from the package.
- Per-nodule analytic CSV `01b_per_nodule_analysis_dataset.csv` added (37,438 rows × 29 cols).
- Manuscript outline cohort age line corrected: 55 yr median in analytic cohort (54 full cohort, 48 Asian subgroup); explicit cohort flow disclosure added.
- xlsx Table 5 "Param" column populated with the actual interaction term names; xlsx Table 13 "TR" and "n total" columns repopulated.

This zip contains four deliverables for the manuscript-writing chat plus a
data dictionary and figures pointer.

## Contents

| File | Purpose |
|---|---|
| `00_README.md` | This file. |
| `01_per_patient_analysis_dataset.csv` | One row per `research_id` with all analysis-relevant exposures, FNA pattern, Bethesda category, demographics, and post-surgery descriptors (3,375 rows × 38 columns). |
| `01_per_patient_analysis_dataset_DICTIONARY.csv` | Column-by-column data dictionary with dtype and notes on dropped / rescaled covariates. |
| `02_statistical_analysis_and_findings.md` | Full statistical methods + findings: cascade, Bethesda-stratified Model B, race × TR interaction, mediation (1,000 boot), 7 sensitivity arms, disparity-direction quadrant, covariate balance, QA gates, bug-fix audit trail. |
| `03_tables_for_manuscript.xlsx` | 13 manuscript-ready tables in one workbook with a cover sheet. |
| `04_manuscript_outline_v3.2.docx` | Draft outline manuscript (Abstract → Introduction → Methods → Results → Discussion → Limitations → Tables/Figures index → Supplementary). All numbers pre-filled; narrative prose marked `[TO WRITE]` where senior-author input is needed. |
| `_manifest.json` | Machine-readable copy of the headline numbers used to populate the docx. |

## Figures (now bundled in `05_figures/` inside this zip)

Eight 300-dpi PNGs + vector PDFs (also kept under
`/Users/loganglosser/THYROID_2026/M048_submission_package/figures/v3/` for git history):

- Figure 6 — Adjusted OR forest plot (full M6 model)
- Figure 7 — Attenuation cascade M0 → M6, Black + Asian
- Figure 8 — Race × TR interaction subplots
- Figure 9 — Mediation diagram (top mediators per race)
- Figure 10 — Covariate-balance Love plot
- Figure 11 — Disparity-direction quadrant scatter
- Figure 12 — Bethesda-stratified TR ROM forest
- Figure 12b — Per-race × Bethesda × TR ROM heatmap
- Figure 13 — FNA-pattern subplots by race

## Headline numbers

| | Black | Asian |
|---|---|---|
| **M0 race only** | OR 0.317 (0.272–0.369) | OR 1.323 (0.977–1.791) |
| **M3 + genetics + NM** | OR 0.359 (0.305–0.424) | OR 1.431 (1.036–1.977) |
| **M6 fully adjusted** | **OR 0.442 (0.366–0.532), p<0.001** | OR 1.164 (0.803–1.687), p=0.42 |
| **Nodule Model F (cluster-robust)** | OR 0.438 (0.31–0.63), p=7e-6 | OR 1.46 (0.87–2.46), p=0.16 |

**Bethesda II calibration divergence:** Black OR 0.49 (p<0.001), Asian OR 2.32 (p=0.045)
**Black-TR4 under-referral signature:** ROM 34.3% vs ACR mid-high reference 18-28%
**Multifocality (malignant cohort):** 39.62% (correctly populated from cohort_m048_tnm_multifocal_v1)

## Decision items for senior author

1. Final framing of Black M6 OR < 1 (selection / pathway-routing vs TI-RADS calibration vs combined).
2. Asian Bethesda II OR 2.32 (small N: 27 patients) — primary or supplementary observation?
3. Lead with Black-TR4 under-referral signature (clinical hook) or M6 cascade (statistical hook)?
4. Target journal — Thyroid (mid-impact, in-scope) vs JAMA Surg (broader, higher-impact, needs upstream-pathway narrative)?
5. Mediation magnitude treatment — direction reliable; magnitude qualitative; do not quote as share-explained.

## Reproducibility

- Patient master CSV is deterministic given pub_v1_1.
- Bootstrap is seeded (seed=42 in `bootstrap_mediation_product`).
- All intermediate CSVs and figures are committed under `studies/m048_racial_disparities_tirads/v3/` and `M048_submission_package/figures/v3/` on origin/main at git SHA 45d54d4.
- Independent recompute (5/5 PASS at 0.0% rel diff) script: `studies/m048_racial_disparities_tirads/v3/verification/independent_recompute_v3.py`.
- Bug history (Bugs A–E and Issues 1–4) documented in `02_statistical_analysis_and_findings.md` Section 5.
