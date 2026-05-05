# M025 v2.0 — validation / reconciliation report

**Status:** CLOSED 2026-05-04 by Cowork review (mig_307 signed off in MotherDuck `main.signoff_migration` at 2026-05-05 04:12:09 UTC).

## Manuscript framing

- **Headline** = patient-level analysis (cursor commit 1d4ecc1).
- **Sister analysis** = nodule-level pivot (mig_306).
- **Frozen sister package** = `M025_submission_package_v1_0/` (patient-level, mig_292).

## Sources

| Layer | Cohort view | Grain | n |
|---|---|---|---:|
| Patient (headline) | `manuscript_workspace.cohort_m025_tirads_performance_v1` | 1 row / patient | 3,375 |
| Nodule (sister) | `manuscript_workspace.cohort_m025_nodule_level_v1` (mig_306) | 1 row / nodule | 37,438 total / 3,687 strict-eligible |
| Strict subset filter | `analytic_eligible_strict_acr_pernodule = TRUE` | — | 3,687 nodules / 631 path-malignant |

## Row-count gates — PASS

| Gate | Expected | Observed | Result |
|---|---:|---:|---|
| Patient cohort n | 3,375 | 3,375 | PASS |
| Patient cohort malignant | 1,479 | 1,479 | PASS |
| Nodule spine total rows | 37,438 | 37,438 | PASS |
| Nodule spine distinct patients | 6,523 | 6,523 | PASS |
| Nodule strict analytic-eligible | 3,687 | 3,687 | PASS |
| Nodule strict path-malignant | 631 | 631 | PASS |

## Headline number checks — PASS

### Patient-level (manuscript headline, cursor 1d4ecc1)

| Metric | Value | Source |
|---|---|---|
| Overall malignancy | 43.8% | `m025v2_run_snapshot.json` |
| AUC (ordinal TR rank) | 0.6478 [0.6301–0.6665] | `m025v2_run_snapshot.json` |
| Optimal threshold (Youden J) | TR≥TR4 (J=0.271) | `m025v2_run_snapshot.json` |
| TR≥TR4 perf | Sens 71.3% / Spec 55.9% / PPV 55.7% / NPV 71.4% | `tirads_diagnostic_performance.csv` |
| TR1 ROM | 28.2% | `rom_by_tirads.csv` (within ACR: False) |
| TR2 ROM | 32.1% | (within ACR: False) |
| TR3 ROM | 27.6% | (within ACR: False) |
| TR4 ROM | 47.4% | (within ACR: False) |
| TR5 ROM | 58.7% | (within ACR: True) |
| Unnecessary FNAs flagged | 1,553 | `unnecessary_fna_analysis.csv` |
| Cancers below threshold | 472 | `unnecessary_fna_analysis.csv` |

### Nodule-level (sister analysis, mig_306)

| Metric | Value | Source |
|---|---|---|
| AUC (ordinal TR rank) | 0.6399 | `m025v2_auc_summary.csv` |
| TR2 nodule ROM | 12.9% [5.1–28.9] (n=31) | `m025v2_threshold_metrics_per_nodule.csv` + Table 3 |
| TR3 nodule ROM | 9.1% [7.8–10.7] (n=1,555) | (below ACR <5% — close) |
| TR4 nodule ROM | **18.7% [16.3–21.5]** (n=860) | **inside ACR 5–20% band** |
| TR5 nodule ROM | **26.1% [23.7–28.6]** (n=1,241) | **inside ACR >20% band** |
| TR≥TR4 thresh | Sens 76.9 / Spec 47.1 / PPV 23.1 / NPV 90.8 | `m025v2_threshold_metrics_per_nodule.csv` |
| Bethesda×TR table reconciles to spine | 3,687 strict total | `m025v2_bethesda_x_tirads_counts.csv` (Table 4) |

### Cross-grain difference (Discussion gold)

Patient-level TR4/TR5 inflate to 47.4%/58.7%; nodule-level land at 18.7%/26.1%. Implies multinodular attribution explains a substantial fraction of operative-cohort ROM elevation.

## mig_264 Bethesda-2 audit — read-only, NO action required

- 360 B2-cytology + path-malignant patients audited via 8 reports + `mig_264_disposition_table.csv`.
- Pattern breakdown: D true-FN candidates = **13 / 360 (3.6%)**; B coverage gaps = 173; A not in spine = 136; C multinodular attribution = 21; F path-bridge timing = 12; E disjoint laterality = 5.
- No Bethesda_final reclassification needed; no mig_306 re-run required.
- Provides Discussion-section evidence for the multinodular-attribution thesis.

## Sister package

`M025_submission_package_v1_0/` remains the frozen **patient-level** submission package (mig_292). No drift; verified live MotherDuck = submission package at 100% match per `M025_drift_report_20260504_2205.md`.

## Open carry-forwards (logged for future v1.1+ work)

- **CF-FNA-SIZE-CM-NULL** — per-nodule FNA size NULL by design in linkage v1.0 (size_score flat 0.5 prior). v1_1 NLP extraction pending.
- **CF-mig_264-BETHESDA2-LINKAGE-MISMAP** — 360 B2+malignant; closed read-only by mig_264, no action.
- **CF-CORTEX-ANALYST-NEEDS-BIND** — `m025_nodule_level_semantic_model.yaml` staged but not bound in Snowsight UI.
- **CF-mig_305** — `VALIDATE_ALL_COHORTS` SP v3 hangs on information_schema check; SP currently at v2 (17 checks, all PASS).

## Sign-off

| Field | Value |
|---|---|
| Migration | mig_307 |
| Signed off in | `thyroid_canonical_publication_v1_0.main.signoff_migration` |
| Timestamp | 2026-05-05 04:12:09 UTC |
| Actor | `cursor_composer_mig307+cowork_review` |
| Git commit | `d6932758` (feat M033) absorbed v2.0 closeout artifacts |
