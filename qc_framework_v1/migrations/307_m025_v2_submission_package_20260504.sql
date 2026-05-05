-- mig_307: M025 submission package v2.0 (patient-level headline + nodule-level sister analysis)
-- Generated: 2026-05-04 | Cursor (mig_307 dispatch). Updated 2026-05-04 by Cowork after review of:
--   - cursor 80ef36d (initial v2.0 package scaffold)
--   - cursor 1d4ecc1 (patient-level pipeline: NIFTP exclusion, Youden J, ACR FNA compliance)
--   - mig_306 (manuscript_workspace.cohort_m025_nodule_level_v1, sister analysis)
--   - mig_264 read-only Bethesda-2 audit
-- DB: thyroid_canonical_publication_v1_0
--
-- Manuscript headline: patient-level analysis from cohort_m025_tirads_performance_v1
--   (n=3,375; AUC=0.6478 [0.6301-0.6665]; Youden J=0.271 at TR>=TR4 threshold).
--   See 08_analysis_outputs/m025v2_run_snapshot.json + studies/m025_tirads_performance/.
-- Sister analysis: nodule-level pivot from cohort_m025_nodule_level_v1 (mig_306, n_strict=3,687).
--   See 08_analysis_outputs/m025v2_threshold_metrics_per_nodule.csv,
--   m025v2_auc_summary.csv, m025v2_supp_S1*.csv, 06_figures/m025v2_fig3b_*.
-- Sister package: M025_submission_package_v1_0/ (frozen, mig_292).
-- DOCX DEFERRED: 01_title_page / 02_manuscript / 03_supplement / 07_response_to_reviewers
-- placeholder docx contained M044 ETE manuscript carryover content; removed from index in this
-- commit (zeroed on disk; .gitignore prevents re-add). Final M025 docx to be authored from
-- METHODS_DRAFT.md + 00_README.md + cursor-1d4ecc1 patient-level outputs.

USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
  'mig_307',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig307+cowork_review',
  'mig_307: M025 submission package v2.0 — closed out across cursor 80ef36d (scaffold), cursor 1d4ecc1 (patient-level pipeline), mig_306 (nodule-level sister), and mig_264 (Bethesda-2 audit). MANUSCRIPT HEADLINE = PATIENT-LEVEL: cohort_m025_tirads_performance_v1 (n=3,375; 1,479 malignant; overall malignancy 43.8%); AUC=0.6478 [0.6301-0.6665]; optimal threshold TR>=TR4 (Youden J=0.271, Sens 71.3% Spec 55.9% PPV 55.7% NPV 71.4%); per-TR ROM TR1=28.2/TR2=32.1/TR3=27.6/TR4=47.4/TR5=58.7; only TR5 within ACR-expected band; ACR FNA compliance flags 1,553 unnecessary FNAs and 472 cancers below threshold; NIFTP-excluded sensitivity computed. SISTER NODULE-LEVEL ANALYSIS: cohort_m025_nodule_level_v1 (mig_306; 37,438 rows / 6,523 patients / 3,687 strict-eligible / 631 path-malignant); per-nodule AUC 0.6399; per-nodule TR4 ROM 18.7% [16.3-21.5] / TR5 26.1% [23.7-28.6] inside ACR bands; difference vs patient-level reflects multinodular attribution. mig_264 read-only Bethesda-2 audit (8 reports + disposition_table.csv): only 13/360 B2+malignant patients are true-FN candidates (D pattern); 173 coverage gaps, 136 not in spine, 21 multinodular attribution; no mig_306 re-run required. DOCX DEFERRED: 4 M044-carryover placeholder docx removed from index + .gitignored (zeroed on disk). v1_0 frozen patient-level package remains sister submission.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_307');

SELECT mig_id, signed_off_at, by_actor, substring(summary, 1, 160) AS summary_head
FROM main.signoff_migration
WHERE mig_id = 'mig_307';
