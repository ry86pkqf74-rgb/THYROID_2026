# M032 era × AJCC8 stage — post–mig_313 delta audit (mig_317)

**Date:** 2026-05-05  
**Actor:** cursor_composer (mig_317)  
**Frozen comparator:** `M032_submission_package_v1_0/06_figures/Fig3_stage_distribution_data.csv` (Figure 3 source data, mig_290)  
**Live rerun:** Same SQL as `build_m032_figures.py::fig3_stage_distribution()` against `thyroid_canonical_publication_v1_0.manuscript_workspace.cohort_m032_descriptive_25yr_v1`  
**Era bins (unchanged v1 policy):** A 1999–2004, B 2005–2009, C 2010–2014, D 2015–2019, E 2020–2025; malignant only; exclude `F_unknown` (same as Fig 3).

## Cohort counts

| Metric | v1 (frozen Fig 3 grain) | Live (post–mig_313) |
|--------|-------------------------|---------------------|
| Malignant in A–E eras (Fig 3 denominator) | 4,016 | 4,016 |
| Malignant all eras (including `F_unknown`) | — | 4,019 |

Per-era row totals are unchanged (261 / 398 / 654 / 1,106 / 1,597): only **within-era stage assignment** shifted after M-stage repair.

## Top 5 cells by absolute count change (|Δn|)

| Era | Stage | v1 n | v2 n | Δn |
|-----|-------|------|------|-----|
| E_2020_2025 | Stage I | 82 | 998 | +916 |
| E_2020_2025 | Stage IV | 666 | 56 | −610 |
| E_2020_2025 | Stage II | 849 | 453 | −396 |
| D_2015_2019 | Stage I | 495 | 624 | +129 |
| D_2015_2019 | Stage IV | 131 | 32 | −99 |

## Top 5 cells by absolute within-era percentage-point change (|Δpp|)

Share within era = 100 × (cell n / era total). Δpp = pct_v2 − pct_v1.

| Era | Stage | v1 % within era | v2 % within era | Δpp |
|-----|-------|-----------------|-----------------|-----|
| E_2020_2025 | Stage I | 5.13 | 62.49 | **+57.36** |
| E_2020_2025 | Stage IV | 41.70 | 3.51 | **−38.19** |
| E_2020_2025 | Stage II | 53.16 | 28.37 | −24.79 |
| D_2015_2019 | Stage I | 44.76 | 56.42 | +11.66 |
| B_2005_2009 | Stage I | 76.88 | 67.59 | −9.29 |

**Max |Δpp|:** **57.36** at era **E_2020_2025**, stage **Stage I** (conjugate collapse of inflated Stage IV / Stage II in the same era).

## Interpretation

- **Stage IV (especially 2020–2025)** was dramatically inflated in v1 relative to live canonical staging after **mig_313** (false M1 / distant-metastasis cascade). The Fig 3 stack for **E** showed **41.7%** “Stage IV” in v1 vs **3.5%** live — consistent with the suspected **temporal correlation** of the corruption (young patients incorrectly staged as M1 → AJCC8 Stage IV).
- **Stage I** in **E** rises from **5.1%** to **62.5%**: patients previously pushed into Stage II/IV by the corrupted M-layer largely return to Stage I/II consistent with post-fix AJCC8 rules.
- Earlier eras move modestly (e.g. D: Stage IV 11.8% → 2.9%); **E** drives the headline magnitude.

## Decision (Step 4 rubric)

Criterion uses **within-era |Δpp|** across era × stage cells.

- **Max |Δpp| = 57.36% ≫ 15%**

**Decision:** **Substantive correction notice required** before any republication or downstream citation of Fig 3 / Table 3 stage-by-era numbers. This is not a footnote-only drift and not a narrow “table patch” band.

## Recommended next actions

1. **Hold M032 v1 Fig 3 / Table 3 stage narratives** as historically tied to pre–mig_313 staging for internal audit only; **do not** present those percentages as current canonical facts.
2. **Cowork / Logan:** open a **correction-class** manuscript revision (Fig 3 + Table 3 + Abstract/Results sentences quoting stage-by-era migration), backed by this folder’s `delta_v1_vs_v2.xlsx` and `m032_era_stage_v2_live.csv`.
3. Optional QA: regenerate Fig 3 PNG only in a **v2** package path (separate prompt); **do not** alter `M032_submission_package_v1_0/`.

## Artifacts

| File | Purpose |
|------|---------|
| `studies/m032_era_stage_v2_post_mig313/m032_era_stage_v2_live.csv` | Live era × stage counts |
| `studies/m032_era_stage_v2_post_mig313/delta_v1_vs_v2.xlsx` | Side-by-side v1 vs v2 + Δn, Δpp |
| `scripts/m032_mig317_era_stage_post_mig313.py` | Repeatable extractor |

## Signoff (MotherDuck)

SQL executed separately:

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT 'mig_317', CURRENT_TIMESTAMP, 'cursor_composer_mig317',
  'mig_317: M032 era × stage refresh post-mig_313. Cohort N_malignant=4019 (4016 in Fig3 A–E grain). Max |Δpp|=57.36 in era=E_2020_2025 stage=Stage_I (Stage_IV Δpp=-38.19). Decision: correction notice / substantive numerical revision required for Fig3+Table3. Delta: studies/m032_era_stage_v2_post_mig313/M032_DELTA_REPORT_v1_vs_v2.md. M032 v1 submission package unchanged.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id='mig_317');
```
