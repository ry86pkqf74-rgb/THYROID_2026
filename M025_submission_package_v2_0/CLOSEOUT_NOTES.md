# M025 v2.0 submission package — mig_307 closeout

**Status:** CLOSED 2026-05-04 by Cowork. Manuscript headline = patient-level (cursor 1d4ecc1). Nodule-level (mig_306) preserved as sister analysis.

## Git — DONE

- [x] M025 v2.0 package committed across cursor commits `80ef36d` (scaffold), `1d4ecc1` (patient-level pipeline), and `d6932758` (M033 commit that absorbed Cowork closeout artifacts: docx removal, tombstones, .gitignore, mig_307 SQL revision, mig_264 audit reports).
- [x] Pushed to `origin/main`.
- [x] Working tree clean for the package (build_m025_figures.py reverted from cursor-2 abandoned refactor).

## MotherDuck — DONE

- [x] Builders run by cursor (1d4ecc1 + earlier scaffold). Outputs in `08_analysis_outputs/` and `studies/m025_tirads_performance/`.
- [x] mig_307 SQL applied: `signoff_migration` row inserted at 2026-05-05 04:12:09 UTC by `cursor_composer_mig307+cowork_review`.
- [x] Verified: `SELECT * FROM main.signoff_migration WHERE mig_id='mig_307';` returns one row with the patient-level-headline summary text.

## QC — DONE (all gates PASS, see `09_validation_report.md`)

- [x] Patient cohort n = 3,375 (gate PASS); 1,479 malignant (gate PASS); AUC 0.6478 [0.6301–0.6665] (Youden J=0.271 at TR≥TR4).
- [x] Nodule spine: 37,438 rows / 6,523 patients / 3,687 strict-eligible / 631 path-malignant (all gates PASS).
- [x] Per-nodule TR4 ROM 18.7% [16.3–21.5] / TR5 26.1% [23.7–28.6] inside ACR-expected bands.
- [x] Bethesda × TIRADS reconciliation matches `M025_v2_tirads_analysis.sql` §3.
- [x] mig_264 read-only Bethesda-2 audit: 13/360 true-FN candidates; no mig_306 re-run required.
- [x] `M025_submission_package_v1_0/` (frozen patient-level sister, mig_292) untouched and zero-drift verified.

## Manuscript — DOCX DEFERRED

- [ ] Author final M025-content `.docx` files (title page, manuscript, supplement, response-to-reviewers) using:
  - `00_README.md` — working title, primary endpoints, sister-paper map.
  - `08_analysis_code/METHODS_DRAFT.md` — Methods prose, IRB, software/repro.
  - `08_analysis_outputs/m025v2_run_snapshot.json`, `tirads_diagnostic_performance.csv`, `rom_by_tirads.csv`, `nodule_size_analysis.csv`, `unnecessary_fna_analysis.csv` — patient-level numbers (manuscript headline).
  - `08_analysis_outputs/m025v2_threshold_metrics_per_nodule.csv`, `m025v2_auc_summary.csv`, `m025v2_supp_S1*.csv`, `m025v2_bethesda_x_tirads_counts.csv` — nodule-level numbers (sister analysis).
  - `06_figures/*.png` — 5 finished figures (cohort flow, ROC, ROM-by-bucket, patient-vs-nodule ROM, Bethesda × TR heatmap).
  - `studies/m025_tirads_performance/` — supplementary outputs incl. ROC curve PNG, subgroup analysis, multi_tirads_assessment, LaTeX summary table.
- [ ] Cite `M025_submission_package_v1_0/` as the patient-level frozen sister submission.
- [ ] Frame multinodular attribution thesis in Discussion using mig_264 audit findings (3.6% true-FN floor).

The 4 docx that previously sat in this dir contained M044 ETE manuscript carryover content — see `*_DELETED.txt` tombstones for context.
