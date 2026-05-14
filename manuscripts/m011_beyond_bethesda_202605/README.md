# M011 — "Beyond Bethesda?" — Reproducibility README

**Working title:** *Beyond Bethesda? Incremental Value and Limits of Ultrasound Risk Stratification and Molecular Testing After Bethesda Cytology in Surgical Thyroid Nodules*

**Question:** Does TI-RADS, individual ultrasound morphology, and molecular testing add clinically useful risk stratification *after* Bethesda cytology in surgically managed thyroid nodules, particularly Bethesda III/IV nodules?

**Data source:** BigQuery project `thyroid-canonical-pub-2026`, dataset `pub_canonical` (governed canonical layer of the thyroid surgery registry, 10,871 patients). All M011 objects are materialised in `pub_workspace` with the `m011_` prefix.

**Generated:** 2026-05-14. Analyst pipeline: BigQuery SQL + BigQuery ML. Advanced statistics (bootstrap CIs, DeLong tests, calibration slope/intercept, LR tests, decision-curve bands) are in `scripts/` and were *not* run in this session because the Linux sandbox was unavailable — run them to finalise the manuscript numbers.

---

## How to reproduce

Run the SQL files in order against `thyroid-canonical-pub-2026` (BigQuery console, `bq` CLI, or the BigQuery MCP):

| Order | File | Builds |
|---|---|---|
| 1 | `sql/m011_build_pipeline.sql` | `m011_patient_base`, `m011_beth_rollup`, `m011_tirads_rollup`, `m011_molec_rollup`, `m011_usfeat_rollup`, `m011_frame_b`, `m011_frame_a`, `m011_frame_a_primary` |
| 2 | `sql/m011_cohort_audit.sql` | `m011_cohort_audit` |
| 3 | `sql/m011_models.sql` | `m011_model_data`, 17 BQML logistic models, `m011_predictions`, `m011_model_metrics`, `m011_threshold_metrics`, `m011_calibration_bins`, `m011_delta_auc` |
| 4 | `sql/m011_tables.sql` | `m011_tbl1_characteristics`, `m011_tbl2_path_by_bethesda`, `m011_tbl3_beth_tirads_heat`, `m011_tbl6_combined_risk`, `m011_tbl7_molecular_selection` |
| 5 | `scripts/m011_advanced_stats.R` *or* `.py` | bootstrap AUC CIs, DeLong tests, calibration slope/intercept, LR tests, adjusted ORs → CSVs in `tables/` |

`m011_models.sql` step 6d (the `m011_predictions` long table) is built with `ML.PREDICT` over the same filtered rows used to train each model, then `INSERT`-appended with the three reference models (`F0`, `F1`, `SUB_Bethesda_ref`). The exact `ML.PREDICT` UNION is included as a comment block; re-run it after the `CREATE MODEL` statements.

---

## Analytic design

**Two analytic frames** (plan §7):

- **Frame B — patient-level fallback** (`m011_frame_b`): one row per patient. Highest-risk preoperative Bethesda, maximum preoperative TI-RADS (all systems), any preoperative molecular result, patient-level final pathology. **This is the primary analysis frame** — n = 2,479 primary cohort.
- **Frame A — high-confidence nodule-linked** (`m011_frame_a_primary`): one row per linked US-nodule/FNA pair via `imaging_fna_linkage_v3`. US nodule → FNA/Bethesda → patient pathology. 9,317 cohort rows; 3,904 high-confidence-tier rows across 1,449 patients. Use for the lesion-level sensitivity analysis. **Molecular is attached at patient level** because `molecular_test_episode_v2.linked_fna_episode_id` is a within-patient ordinal (values 1–6), not a global FNA key — see playbook pitfall #3.

**Primary cohort** (`in_primary_cohort` flag): preoperative ultrasound nodule + preoperative FNA with mappable Bethesda + final surgical pathology + surgery date. n = 2,479.

**Outcomes** (built into every frame and `m011_model_data`):
- `any_malignancy` — primary outcome; NIFTP and FTUMP/borderline set to NULL (excluded).
- `any_malignancy_niftp_malig` / `any_malignancy_niftp_benign` — NIFTP 3-way sensitivity.
- `clin_sig_malignancy` — malignancy >1 cm OR aggressive histology/feature (gross ETE, vascular invasion, nodal disease, positive margin, MTC/PDTC/ATC/Hürthle).
- `incidental_ptmc_flag` — PTC ≤1 cm without gross ETE / nodal / margin involvement.
- `final_path_class` ∈ {benign, malignant, NIFTP, borderline}; `histology_group` ∈ {PTC, FTC, MTC, Hürthle, PDTC, ATC, Other malignant, benign, NIFTP, borderline}.

**Models** (BigQuery ML `LOGISTIC_REG`, `NO_SPLIT`, complete-case): A Bethesda only · B TI-RADS only · C Bethesda+TI-RADS · D +clinical covariates · E Bethesda+US features · F Bethesda+TI-RADS+molecular · G Bethesda+features+molecular · F0/F1 molecular-cohort references · SUB_* Bethesda III/IV subgroup models.

**Metrics:** AUROC (Mann-Whitney rank statistic, average-rank tie handling) with Hanley-McNeil analytic 95% CI in SQL; bootstrap CIs and DeLong tests in the R/Python script. Brier score, calibration deciles (`m011_calibration_bins`) and logistic calibration slope/intercept (script). Sensitivity/specificity/PPV/NPV/FNR/FPR and decision-curve net benefit across a 0–1 threshold grid (`m011_threshold_metrics`).

---

## Folder contents

```
m011/
  PROJECT_CONTEXT.md                  durable BigQuery / repo / program memory
  MANUSCRIPT_WRITING_PLAYBOOK.md      reusable playbook for all thyroid manuscripts
  README.md                           this file
  M011_RESULTS_SUMMARY.md             results + the two manuscript framings
  sql/
    m011_build_pipeline.sql           frames + outcomes
    m011_cohort_audit.sql             STARD denominators, missingness, linkage tiers
    m011_models.sql                   model data, BQML models, predictions, metrics
    m011_tables.sql                   descriptive + risk tables
  tables/
    m011_all_tables.md                Tables 1-7 + audit, formatted
    m011_model_metrics.csv            AUC/CI/Brier/delta-AUC for all 16 models
    m011_cohort_audit.csv             STARD audit
    (m011_auc_bootstrap_ci.csv, m011_delong_tests.csv,
     m011_calibration_slope_intercept.csv, m011_likelihood_ratio_tests.csv,
     m011_adjusted_odds_ratios_modelD.csv  <- produced by the script)
  figures/
    fig1_stard_flow.svg               STARD cohort flow diagram
    fig2_bethesda_tirads_heatmap.svg  Bethesda x ACR TI-RADS malignancy heat map
    fig3_roc_curves.svg               sequential-model ROC curves
    fig4_calibration.svg              calibration plots (A, C, F)
    fig5_decision_curve.svg           decision-curve analysis
    fig6_combined_risk_groups.svg     Bethesda III/IV x TI-RADS x molecular bars
    fig7_auc_forest.svg               AUROC forest plot, all models
  scripts/
    m011_advanced_stats.R             bootstrap/DeLong/calibration/LR/ORs (R)
    m011_advanced_stats.py            same, Python alternative
```

---

## Headline result (see M011_RESULTS_SUMMARY.md)

In the 2,245-patient complete-case primary cohort, **adding ACR TI-RADS to Bethesda cytology raised AUROC from 0.824 to 0.839 (ΔAUC +0.015, not significant)**; individual US features behaved identically (+0.016). In the molecular-tested subgroup, adding TI-RADS to Bethesda gave +0.025 (NS) whereas **adding the molecular result gave +0.088 (significant)**. Within Bethesda III/IV, composite TI-RADS alone barely exceeded chance (AUROC 0.55); molecular testing reached 0.61–0.66. The data support the cautious "limits" framing: TI-RADS adds little incremental discrimination after Bethesda, while molecular testing is the more useful second-stage tool in indeterminate cytology.

## Key caveats / honest notes
- All model AUCs are **apparent (in-sample)**; bootstrap *CIs* are done (`tables/m011_auc_bootstrap_ci.csv`), but bootstrap *optimism correction* of the point estimates still remains.
- ΔAUC significance: the **paired DeLong test** was run (`tables/m011_delong_tests.csv`, 2026-05-14) and supersedes the approximate SQL z-values. Headline: TI-RADS increment is statistically significant in the full cohort (p < 10⁻⁵) but clinically marginal and **not significant in Bethesda III/IV** (p = 0.29); molecular increment +0.088 is significant (p = 5×10⁻⁵).
- `scripts/m011_advanced_stats.py` was executed on the Mac (system Python 3.9) via Desktop Commander — the `.R` version is an untested equivalent kept for reference.
- `acr_strict` is missing for 67.6% of the primary cohort — use ACR-imputed as primary, ACR-strict as a complete-case sensitivity analysis (consistent with Manuscript 1).
- Histology strings in `manuscript_cohort_v1.histology_final` are free-text; the regex classifier in `m011_build_pipeline.sql` step 1 was spot-checked against the histology distribution but should be reviewed by a pathologist before submission.
- This is a surgery-enriched cohort — malignancy rates (primary cohort 48.7%) are not transferable to unselected outpatient nodules.
