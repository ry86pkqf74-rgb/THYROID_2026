# M032 — v2 numerical refresh post-mig_313 (M-stage repair)

> **Status:** Pre-submission refresh, not a published correction. M032 has not been submitted to Thyroid yet, so v2 numbers replace v1 directly in the working package — no erratum needed.
> **Authority:** mig_321 (Cowork, 2026-05-05). Backing audit: mig_317 cursor refresh.
> **Submission target:** Thyroid (per Logan, 2026-05-05).

---

## Why this refresh

mig_313 (cursor, 2026-05-05) corrected the canonical-layer `m_stage_ajcc8_resolved` defect that had spuriously inflated Stage IV counts in the malignant cohort. The corruption was age-correlated and clustered in recent eras (2015–2025), so M032's era × stage exhibits were the most affected downstream artifact.

**Pre-mig_313 (= v1 / shipped in 02_manuscript.docx, 06_figures, 08_analysis_outputs):** AJCC8 M-stage was back-derived from `stage_group_ajcc8` via a circular AJCC8 logic that misclassified many young Stage I/II patients as M1 → Stage IV. Pre-fix CPM M1 rate = 45.19% (1,816 / 4,019 malignant). Post-fix CPM M1 rate = 2.84% (114 / 4,019).

**Post-mig_313 (= v2 / this refresh):** Stage assignments are now driven by canonical M-stage. The era 2020–2025 Stage IV percentage drops from 41.7% → 3.5% — the headline temporal correction.

mig_317 (cursor) verified the deltas exceed the 15% within-era pp threshold that triggers a substantive numerical revision (max |Δpp| = 57.36% in era E × Stage I). Since M032 is not yet submitted, this is a working refresh, not a published correction notice.

---

## Files added in this refresh

| File | Purpose |
|---|---|
| `06_figures/Fig3_stage_distribution_data_v2.csv` | v2 era × stage counts (ready for Fig 3 regeneration) |
| `08_analysis_outputs/M032_v2_post_mig313_REFRESH_NOTE.md` | this file |

## Files needing regeneration before first submission

The following files still contain v1 (pre-mig_313) numbers and must be rebuilt before submission to Thyroid:

| File | Action | Tool |
|---|---|---|
| `06_figures/Figure3_StageDistributionByEra.png` | Regenerate from `Fig3_stage_distribution_data_v2.csv` | `08_analysis_code/build_m032_figures.py` (will pick up v2 if the script reads the cohort view directly; otherwise re-run with the v2 CSV as input) |
| `04_tables.xlsx` (Table 3 — Stage Migration) | Regenerate stage-by-era cells | `08_analysis_code/build_m032_tables.py` |
| `02_manuscript.docx` (Results §Stage migration; possibly Discussion if it cites era-IV trend) | Hand-edit or regenerate via `build_m032_manuscript_md.py` then re-render to docx | manual review required |
| `03_supplement.docx` (any Supp Table referencing stage by era) | Hand-edit or regenerate | manual review required |
| `08_analysis_outputs/M032_manuscript_numbers_20260504.md` | Append v2 stage-by-era table | done in this commit (see below) |

The `build_m032_*.py` scripts in `08_analysis_code/` query the live `manuscript_workspace.cohort_m032_descriptive_25yr_v1` view, which is **already post-mig_313**. So re-running them produces v2 numbers automatically. **One command rebuilds the package:**

```bash
cd /Users/loganglosser/THYROID_2026
.venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_tables.py
.venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_figures.py
.venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_manuscript_md.py
```

Run those before sending the package to Thyroid.

---

## v2 era × stage table (locked numbers for first submission)

| Era | Stage I | Stage II | Stage III | Stage IV | Unknown | Era total |
|---|---:|---:|---:|---:|---:|---:|
| 1999–2004 | 190 | 44 | 8 | 5 | 14 | 261 |
| 2005–2009 | 269 | 84 | 10 | 8 | 27 | 398 |
| 2010–2014 | 430 | 175 | 16 | 14 | 19 | 654 |
| 2015–2019 | 624 | 402 | 18 | 32 | 30 | 1,106 |
| 2020–2025 | 998 | 453 | 29 | 56 | 61 | 1,597 |
| **Total** | **2,511** | **1,158** | **81** | **115** | **151** | **4,016** |

Era totals match the v1 totals exactly (261 / 398 / 654 / 1,106 / 1,597) — only **within-era stage assignment** shifted.

## v1 → v2 within-era % comparison (reference)

| Era | Stage | v1 % within era | v2 % within era | Δ pp |
|---|---|---:|---:|---:|
| E 2020–2025 | Stage I | 5.13 | 62.49 | **+57.36** |
| E 2020–2025 | Stage IV | 41.70 | 3.51 | **−38.19** |
| E 2020–2025 | Stage II | 53.16 | 28.37 | −24.79 |
| D 2015–2019 | Stage I | 44.76 | 56.42 | +11.66 |
| D 2015–2019 | Stage IV | 11.84 | 2.89 | −8.95 |
| C 2010–2014 | (small shifts <5 pp) | — | — | — |
| B 2005–2009 | Stage I | 76.88 | 67.59 | −9.29 |
| A 1999–2004 | (small shifts <5 pp) | — | — | — |

The headline temporal narrative changes: instead of "Stage IV is rising in recent eras," the v2 picture is "Stage I dominates throughout, with modest growth in absolute Stage II/III/IV cases proportional to overall cohort growth." Discussion sentences should be reviewed against this revised picture before submission.

---

## Headline numbers (cross-reference)

| Metric | v1 | v2 | Change |
|---|---:|---:|---|
| Total cohort | 10,871 | 10,871 | unchanged |
| Malignant analytic | 4,019 | 4,019 | unchanged |
| CPM M1 rate among malignant | 45.19% (1,816) | 2.84% (114) | corrected |
| Era E 2020–2025 Stage IV pct | 41.70% | 3.51% | corrected |
| Era E 2020–2025 Stage I pct | 5.13% | 62.49% | corrected |
| Era E 2020–2025 total | 1,597 | 1,597 | unchanged |
| All era totals | (matched) | (matched) | unchanged |

---

## Backing audit

- `studies/m032_era_stage_v2_post_mig313/M032_DELTA_REPORT_v1_vs_v2.md` — full delta exhibit (cursor mig_317)
- `studies/m032_era_stage_v2_post_mig313/delta_v1_vs_v2.xlsx` — side-by-side workbook
- `studies/m032_era_stage_v2_post_mig313/m032_era_stage_v2_live.csv` — source of truth for v2 counts
- `scripts/m032_mig317_era_stage_post_mig313.py` — repeatable extractor

## Carry-forwards

- **`CF-M032-CORRECTION-NOTICE`**: CLOSED — recategorized to pre-submission numerical refresh; correction notice not required since M032 is not submitted yet.
- **mig_321** (this refresh): signoff in `main.signoff_migration`.

## Author action items added to CLOSEOUT_NOTES.md

1. Run the three rebuild scripts before submission.
2. Hand-review `02_manuscript.docx` Results section §Stage migration and Discussion for any era-Stage-IV interpretive sentences that need rewording.
3. Hand-review `04_tables.xlsx` Table 3 cells.
4. Verify Fig 3 regenerates with v2 data and that y-axis scale + legend still fit.
5. Update Abstract if it quotes any era × stage percentage that changed by >5 pp.
