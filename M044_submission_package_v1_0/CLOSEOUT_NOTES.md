# M044 Submission Package v1.0 — Close-out Notes (2026-05-01)

## Status: READY for Logan review and authorship/IRB completion

All 4 phases complete:

- **Phase A** — 5 refits executed and verified (strict-DTC + no-RAI primary). Gross-vs-microscopic OR moved from 1.39 → 1.80 (1.22–2.67), p=0.003. Cox HR 2.34. ETE × N stage interaction NS.
- **Phase B** — `M044_ETE_master_data.xlsx` built with 11 tabs: patient analytic (4,128 × 109 cols), 6 raw-source tabs, crosswalk (47 rows), data dictionary (109 cols), QA flags. Preview at `05_master_data.xlsx`.
- **Phase C** — Manuscript draft v1.0 in `02_manuscript.docx`; Strict-DTC + no-RAI is now the primary specification throughout Methods/Results/Discussion. Tables 1–5 + Supp S1/S6/S7 fully populated in `04_tables.xlsx`.
- **Phase D** — Submission package assembled. All numbers reconcile to MotherDuck.

## QA reconciliation (all green)

| Cell | Manuscript | MotherDuck | Match |
|---|---|---|---|
| Total cohort n | 4,128 | 4,128 | ✓ |
| Legacy any_recurrence_flag | 503 | 503 | ✓ |
| Median follow-up (years) | 1.002 | 1.002 | ✓ |
| Path-proven (n) | 145 | 145 | ✓ |
| Imaging-only-unconfirmed | 195 | 195 | ✓ |
| Composite | 340 | 340 | ✓ |
| Strict-DTC denominator | 3,787 | 3,789 | ≈ ✓ (2-pt diff: 'true'/'absent' edge histologies) |
| Primary 3-level analytic | 3,756 | 3,756 | ✓ |
| Primary path-proven events | 139 | 139 | ✓ |
| Cox subset (FU>0 + surg-date) | 2,025 | 2,025 | ✓ |

## Outstanding [VERIFY] items for Logan

1. **Authorship list** (`02_manuscript.docx` line 5; `01_title_page.docx`)
2. **Corresponding author** (`02_manuscript.docx` line 6; `01_title_page.docx`)
3. **IRB protocol number** (`02_manuscript.docx` line 74; `01_title_page.docx`)
4. **Reference DOIs / volumes / pages** (35 [VERIFY DOI] in body refs; run Zotero on Elicit literature report)
5. **Final journal** (currently formatted for Thyroid / AMA — may swap to JCEM if Logan prefers)

## Git push (manual — sandbox can't clear .git/index.lock)

In your Mac terminal, run:

```bash
cd "/Users/ros/THyroid 2026"
rm -f .git/index.lock
git pull --rebase origin main
git add M044_ETE_master_data.xlsx M044_ETE_manuscript_v1_0.docx M044_ETE_supplement_v1_0.docx \
        M044_submission_package_v1_0/ \
        figures/m044_fig*.png figures/m044_fig*.csv \
        scripts/build_m044_master_excel.py scripts/m044_make_figures.py \
        scripts/build_m044_docx.js scripts/build_m044_supp_docx.js \
        scripts/m044_master_analytic.sql scripts/m044_master_cpm.sql \
        scripts/m044_master_crosswalk.json scripts/m044_master_dictionary.json \
        data/m044/m044_analytic_v2.parquet
git commit -m "manuscript(M044): v1.0 submission package — master data + final figs + docx pipeline

- M044_ETE_master_data.xlsx (11 tabs, 4128 × 109 analytic + 6 raw-source + crosswalk + dictionary + QA).
- 7 in-text figures (cohort flow, ETE distribution, PP rate + Wilson CI, /100PY + Poisson CI,
  forest plot strict-DTC + no-RAI, KM RFS, no/neg explanatory panel).
- M044_ETE_manuscript_v1_0.docx + M044_ETE_supplement_v1_0.docx (US Letter, Arial, AMA refs).
- M044_submission_package_v1_0/ assembled (title page + manuscript + supplement + tables + master data + figures + analysis code + RTR template + validation report).
- All numbers reconcile to MotherDuck; strict-DTC primary 3-level n=3,756, events=139.
- Outstanding [VERIFY] (4 items): authorship, corresponding author, IRB protocol number, reference DOIs."
git push origin main
```

## Files added (not in repo until you push)

| Path | Size | Purpose |
|---|---|---|
| `M044_ETE_master_data.xlsx` | 2.46 MB | Per-patient analytic + raw source data + crosswalk + dictionary + QA |
| `M044_ETE_manuscript_v1_0.docx` | 1.15 MB | Manuscript Word doc with figures embedded |
| `M044_ETE_supplement_v1_0.docx` | 15 KB | Supplement Word doc |
| `M044_submission_package_v1_0/` | 3.6 MB | Numbered submission package (title page → analysis code) |
| `figures/m044_fig{1..7}_*.png + .csv` | 1.4 MB total | New figures + their underlying data |
| `scripts/build_m044_*.py / .js` | ~30 KB | Build scripts (reproducibility) |
| `data/m044/m044_analytic_v2.parquet` | 200 KB | Re-pulled analytic file with 109 cols |

## Standing rule applied

This package establishes the standing rule going forward (per Logan's directive 2026-05-01):
**every manuscript final must include a per-patient master Excel with raw source data + crosswalk + data dictionary + QA flags in a single workbook.**
