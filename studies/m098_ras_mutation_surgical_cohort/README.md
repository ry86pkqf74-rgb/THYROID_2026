# M098 — RAS-Mutated Thyroid Surgical Cohort

Single-institution retrospective characterization of 292 patients who underwent thyroid surgery with a preoperatively detected RAS mutation (NRAS, HRAS, or KRAS) on ThyroSeq, Afirma, or combined molecular testing.

## Folder contents

- `manuscript_M098_v1.md` — Full scientific manuscript draft (Abstract / Intro / Methods / Results / Discussion / Limitations / Conclusion / References).
- `plain_language_summary_M098.md` — 600–900 word summary written for patients, families, and non-subspecialty clinicians.
- `statistical_methods_M098.md` — Detailed statistical methodology suitable for peer-reviewer scrutiny or independent re-implementation.
- `data/m098_analytic.csv` — Locked analytic dataset (292 rows × 61 columns).
- `analysis/` — Reproducible scripts and intermediate results.
  - `m098_cohort.sql` — Cohort selection SQL.
  - `m098_run_analysis.py` — Parses BigQuery extract → analytic CSV.
  - `m098_tables.py` — Generates Tables 1–10.
  - `m098_inferential.py` — Comparison family with BH-FDR + four logistic regressions.
  - `m098_survival_and_figures.py` — Kaplan-Meier survival, sensitivity analyses, all 7 figures.
  - `inferential_results.csv` — Family of 21 comparisons with raw + FDR-adjusted p-values.
  - `logit_Outcome*.csv` — Coefficient tables for each logistic regression model.
  - `sensitivity_results.csv` — Results across 6 sensitivity cohorts.
- `tables/` — Markdown + individual CSV for each of Tables 1–10.
- `figures/` — Seven figures at 300-DPI PNG and SVG.

## Reproduction

```bash
cd analysis
python3 m098_run_analysis.py    # parse → data/m098_analytic.csv
python3 m098_tables.py           # → tables/
python3 m098_inferential.py      # → analysis/inferential_results.csv + logit_*.csv
python3 m098_survival_and_figures.py  # → figures/ + sensitivity_results.csv
```

## Headline findings

- Cohort N = 292. Histology classification: Malignant 176 (60.3%), Borderline NIFTP/FTUMP 22 (7.5%), Benign 94 (32.2%).
- Gene distribution (single-only): NRAS 147, HRAS 46, KRAS 14. Multi-gene RAS+ 85.
- Co-mutation: Isolated RAS 273 (93.5%), RAS+TERT 14 (4.8%), RAS+BRAF 4 (1.4%), RAS+TERT+BRAF 1.
- ATA 2015 → 2025 reclassification: 79 up (43.6%), 64 same (35.4%), 38 down (21.0%); 181 patients scored.
- Two findings significant after BH-FDR correction: Co-mutation × ATA-2025 high (q = 0.018) and TERT × ATA-2025 high (q = 0.009).
- Multivariable logistic regression (LN positivity): tumor size OR 1.30 per cm (95% CI 1.09–1.56, p = 0.004).
- KM recurrence-free survival on n = 103 ≥1y FU subset: 13 events / 345 PY = 3.76 per 100 PY.

## Tracking

- **Manuscripts row**: Airtable base `appJYOnUb7KrHKwpV`, table `tblLsp8ls3rU1eEc9`, record `recxlZQ4CSuloxVt8`. Status moved Cohort Definition → Drafting on 2026-05-13.
- **Manuscript Feedback Log entry**: `MFB-2026-05-13-M098-comprehensive-analysis` (record `rec0JioYIGBNyjhru`).
- **Linear project**: <https://linear.app/rostemp/project/m098-ras-mutated-surgical-thyroid-cohort-368dad84d3eb>
- **Linked Notable Finding**: NF-2026-05-13-thyroseq-freeform-fallback-cross-gene-protein-binding (parser-bug source; mitigated by hybrid-evidence + negation-cue filtering used in this analysis).

## Limitations

Single-institution retrospective design. Median follow-up is short for most patients, forcing the survival analyses to the n = 103 ≥1y FU subset. The 2025 ATA system's external recurrence calibration is still maturing. Two of four multivariable regressions failed to converge (Outcomes 2 and 4) due to small subgroup sizes and near-constant outcomes — these are reported descriptively in the manuscript.


## Variant-level cleanup artifacts

The §6 variant-level cleanup pipeline (impossibility filter + negation-cue filter + OCR normalization + raw-text recovery) is implemented in `analysis/m098_variant_cleanup.py`. It produced:

- `data/m098_variant_long.csv` — 611 rows × 17 columns. One row per (patient × molecular episode × variant), with the cleaned `protein_normalized` column, the `protein_norm_status` flag, the `drop_reason` string, and a binary `analytic_keep`.
- `data/m098_variant_long_qc_summary.csv` — counts at each stage: 12 OCR-normalized, 16 impossibility-flagged, 32 negation-contradicted, 8 recovered from raw, final 576 of 611 kept (94.3%).
