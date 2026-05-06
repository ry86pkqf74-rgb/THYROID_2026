# Post-mig_086 Model Re-Fit Assessment

**Date:** 2026-05-06  
**Scope:** Cox PH recurrence model (Prompt 4) and BQML boosted-tree (Prompt 5)  
**Trigger:** mig_086 legacy-promotion sweep applied at **14:42 UTC** added 55 VIEW facades in `pub_canonical` pointing at `pub_legacy_source_20260416` tables (synoptic_tumor_long_v1, extracted_*, molecular_*, survival_cohort_enriched, etc.)

---

## Timeline

| Time (UTC) | Event |
|---|---|
| 04:19 | `pub_workspace.cohort_m044_ajcc_ete_v1` materialized (BASE TABLE) |
| 06:46 | BQML logistic baseline `recurrence_5y_baseline_v1` trained (AUC 0.738) |
| 12:31 | **Cox PH v1** trained: 2,580 pts / 428 events / C-index 0.674 → `mig_086_cox_recurrence_v1` |
| 12:58 | **BQML boosted v1** trained: 1,285 rows / AUC 0.712 → `mig_087_bqml_boosted_models` |
| **14:42** | **mig_086 applied** — 55 facade VIEWs in pub_canonical → legacy tables |

Both models were trained **before** the 14:42 UTC cutoff.

---

## Cox PH Re-Fit Assessment

### Cohort Delta

| Metric | Pre-mig_086 (v1) | Post-mig_086 (v2 verified) | Delta |
|---|---|---|---|
| N patients | 2,580 | **2,580** | **0** |
| N events | 428 | **428** | **0** |
| Event rate | 16.6% | 16.6% | 0 |

### Why No Change

`pub_workspace.cohort_m044_ajcc_ete_v1` is a **materialized BASE TABLE** (not a VIEW), created at 04:19 UTC.  
mig_086 created VIEW facades in `pub_canonical` — it does **not** retroactively alter any materialized table's contents.  
The Cox PH cohort-pull query references this BASE TABLE directly; its output is therefore identical pre- and post-mig_086.

### Model Metrics: UNCHANGED

| Metric | v1 (canonical) | v2 (verification) | Delta |
|---|---|---|---|
| **C-index (Harrell)** | **0.674** | **0.674** | 0 |
| Brier @ 1y | 0.174 | 0.174 | 0 |
| Brier @ 3y | 0.175 | 0.175 | 0 |
| Brier @ 5y | 0.160 | 0.160 | 0 |
| LLR p-value | < 0.001 | < 0.001 | 0 |

### Top Hazard Ratios: UNCHANGED

| Feature | HR | 95% CI | p |
|---|---|---|---|
| `tumor_size_cm` | 1.08 | 1.05–1.12 | <0.005 |
| `ata_high_risk` | 1.39 | 1.16–1.66 | <0.005 |
| `ata_intermediate_risk` | 0.62 | 0.51–0.75 | <0.005 |
| `histology_ptc` | 0.71 | 0.58–0.85 | <0.005 |
| `stage_iii_iv` | 1.32 | 0.97–1.80 | 0.08 |

### Verdict: **VERIFIED-NO-CHANGE** — v1 remains canonical

---

## BQML Boosted-Tree Re-Fit Assessment

### Cohort Delta

| Metric | Pre-mig_086 (v1) | Post-mig_086 (v2 verified) | Delta |
|---|---|---|---|
| N total rows | 1,285 | **1,285** | **0** |
| N recurrence events | 502 | **502** | **0** |
| Event rate | 39.1% | 39.1% | 0 |

### Why No Change

`pub_canonical.canonical_patient_master` is a **materialized BASE TABLE** directly in pub_canonical.  
mig_086 did not add new columns or rows to CPM — only VIEW facades for separate tables.  
The model's feature columns (age_at_surgery, sex, histology_final, ata_risk_category, ajcc8_stage_group, braf_positive, ete_grade_final_v2, ln_positive_final, tumor_size_cm_dominant, multifocal_flag_path, any_recurrence_flag) are all direct CPM columns unchanged by the sweep.

### Model Metrics: UNCHANGED

| Metric | v1 (canonical) | v2 (verification) | Delta |
|---|---|---|---|
| **AUC (ROC)** | **0.712** | **0.712** | 0 |
| Accuracy | 0.685 | 0.685 | 0 |
| F1 Score | 0.598 | 0.598 | 0 |
| vs logistic baseline | 0.738 (higher) | 0.738 (higher) | 0 |

### QC Assertion Status

| Assertion | Model | AUC | Result |
|---|---|---|---|
| `bqml_recurrence_baseline_auc_gt_0_60` | recurrence_5y_baseline_v1 | 0.738 | ✅ PASS |
| `bqml_recurrence_baseline_auc_gt_0_60` | recurrence_5y_boosted_v1 | 0.712 | ✅ PASS |

### Verdict: **VERIFIED-NO-CHANGE** — v1 remains canonical

---

## Honest Answer: Did mig_086 Actually Move the Needle?

**No, for either model.** The reason is architectural:

- The Cox PH used `cohort_m044_ajcc_ete_v1` — a pre-built materialized TABLE in `pub_workspace`, created earlier in the same day before either model ran. Its contents are fixed at creation time regardless of any subsequent VIEW changes.
- The BQML boosted tree used `canonical_patient_master` — also a BASE TABLE in pub_canonical, unaffected by VIEW facade additions.

mig_086's 55 VIEW facades are beneficial for **future** queries and cohort builds that reference `pub_canonical.<legacy_table>` rather than `pub_legacy_source_20260416.<legacy_table>` directly. They do not retroactively change the data in any existing BASE TABLE.

**What mig_086 enables going forward:**
A rebuilt cohort using `pub_canonical.survival_cohort_enriched` (61,134 rows, 10,507 distinct pts, 161 events — appears to be an outcome-cohort, not recurrence-specific) or a refreshed cohort that JOIN-extends `cohort_m044_ajcc_ete_v1` with newly accessible tables like `synoptic_tumor_long_v1`, `molecular_test_episode_v2`, etc. Such a refresh would be a **future model v3**, not a v2 re-fit.

---

## Governance Records

| Item | ID | Notes |
|---|---|---|
| DFL — Cox PH verification | **DFL-20260506-088** | Airtable `recsL5UFwci002lIP` |
| DFL — BQML boosted verification | **DFL-20260506-089** | Airtable `rec2GznaBr19CQ75B` |
| Migration log — Cox PH v2 | **mig_087_cox_recurrence_v2** | Verification entry, rows_before=rows_after=2580 |
| Migration log — BQML boosted v2 | **mig_088_bqml_boosted_v2** | Verification entry, rows_before=rows_after=1285 |
| Eval log — Cox PH v2 | `cox_recurrence_v2_post_mig_086` | VERIFIED-NO-CHANGE; C-index=0.674 |
| Eval log — BQML boosted v2 | `recurrence_5y_boosted_v2_post_mig_086` | VERIFIED-NO-CHANGE; AUC=0.712 |

---

## Canonical Model IDs (post-verification)

| Model | Canonical ID | Status |
|---|---|---|
| Cox PH recurrence | `cox_recurrence_v1` | **Active canonical** |
| BQML boosted tree | `recurrence_5y_boosted_v1` | **Active canonical** |
| BQML logistic baseline | `recurrence_5y_baseline_v1` | **Active canonical** |

v2 IDs are verification audit entries only — they carry the same metrics and are not superseding models.
