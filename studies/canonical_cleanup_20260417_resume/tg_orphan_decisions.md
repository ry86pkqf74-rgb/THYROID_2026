# Phase 2 — Tg-lab orphan classification (403 patients)

_Generated 2026-04-18T03:04:35.107952+00:00_  
_Cohort freeze (max `cpm_built_at`): **2026-04-17 06:41:33.062835**_  
_Read-only: no rows deleted from `thyroglobulin_lab_canonical_v1` or `longitudinal_lab_canonical_v1`; no rows inserted into `canonical_patient_master`._

## 3-way counts

| classification | recommendation | n |
|:---|:---|---:|
| `likely_non_cancer`        | DELETE orphan lab rows               | 403 |
| `likely_dropped_from_CPM`  | ADMIT to CPM **OR** refresh lab feed  | 0 |
| `ambiguous`                | HOLD for chart review                | 0 |
| **TOTAL**                  |                                       | 403 |

_Divergence vs the pre-existing `lab_orphan_audit_v1.classification` column: **0 / 403**._

## Classifier rules

1. `n_evidence == 0` → `likely_non_cancer` → recommend **DELETE**.
2. Any of `has_tumor_episode`, `has_synoptic_tumor`, `has_path_synoptic` is TRUE → `likely_dropped_from_CPM` → recommend **ADMIT** (or refresh feed if `lab_first_after_cohort_freeze_flag = TRUE`).
3. Otherwise (only FNA and/or imaging evidence) → `ambiguous` → recommend **HOLD**.

## Temporal-context call-out for `likely_dropped_from_CPM`

For each ADMIT-candidate the table below shows first/last Tg lab date, the earliest cancer-evidence date across the 5 tables, and `lab_first_after_cohort_freeze_flag`. When the flag is TRUE the remediation is **REFRESH the lab feed**, not admit a stale patient to the cohort.

| rid | n_lab | first_tg | last_tg | earliest_cancer_evidence | evidence_tables | lab_first_after_cohort_freeze | recommendation |
|---:|---:|:---|:---|:---|:---|:---:|:---|

## 10 sample rids per class

### likely_non_cancer (403 total — first 10 by rid)

| rid | n_lab | first_tg | last_tg | fna | tum | syn | path | img | earliest_cancer_evidence | lab_first_after_cohort_freeze |
|---:|---:|:---|:---|:---:|:---:|:---:|:---:|:---:|:---|:---:|
| 105 | 40 | 2001-05-17 12:22:00 | 2019-02-14 15:28:00 | . | . | . | . | . | None | F |
| 151 | 23 | 2001-04-26 13:00:00 | 2007-12-21 11:35:00 | . | . | . | . | . | None | F |
| 167 | 5 | 2001-03-05 10:04:00 | 2002-01-10 09:58:00 | . | . | . | . | . | None | F |
| 184 | 41 | 2001-03-02 06:35:00 | 2008-04-28 12:34:00 | . | . | . | . | . | None | F |
| 224 | 12 | 2001-03-08 10:42:00 | 2002-08-28 09:27:00 | . | . | . | . | . | None | F |
| 240 | 2 | 2001-04-03 08:32:00 | 2001-04-03 08:32:00 | . | . | . | . | . | None | F |
| 253 | 28 | 2002-01-22 13:21:00 | 2017-11-16 12:27:23 | . | . | . | . | . | None | F |
| 261 | 7 | 2001-12-13 10:16:00 | 2002-11-08 10:55:00 | . | . | . | . | . | None | F |
| 264 | 15 | 2001-09-18 15:44:00 | 2008-04-29 12:29:00 | . | . | . | . | . | None | F |
| 439 | 66 | 2003-12-26 16:22:00 | 2025-06-10 14:14:00 | . | . | . | . | . | None | F |

### likely_dropped_from_CPM (0 total — first 10 by rid)

| rid | n_lab | first_tg | last_tg | fna | tum | syn | path | img | earliest_cancer_evidence | lab_first_after_cohort_freeze |
|---:|---:|:---|:---|:---:|:---:|:---:|:---:|:---:|:---|:---:|

### ambiguous (0 total — first 10 by rid)

| rid | n_lab | first_tg | last_tg | fna | tum | syn | path | img | earliest_cancer_evidence | lab_first_after_cohort_freeze |
|---:|---:|:---|:---|:---:|:---:|:---:|:---:|:---:|:---|:---:|

## Critical context — `has_op` and delete-impact

| signal | value |
|:---|---:|
| Orphan rids with `has_op = TRUE` (operative episode in our records)   | **403 / 403 (100%)** |
| Sum of orphan Tg-lab rows                                              | 13,873 |
| Total `main.thyroglobulin_lab_canonical_v1` rows                       | 74,258 |
| **Pct of Tg-lab table that would be deleted**                          | **18.68%** |
| Total `main.longitudinal_lab_canonical_v1` rows                        | 75,247 |
| **Pct of longitudinal-lab table that would be deleted**                | **18.44%** |

**Interpretation.** Every one of the 403 orphans had a thyroidectomy on
record — but none have any FNA, tumor-episode, synoptic, path-synoptic,
or imaging-nodule evidence. The most likely explanation is **benign
thyroidectomy patients on long-term Tg surveillance** (post-op goiter,
Graves' disease, indeterminate nodules that turned out benign on path
but got dropped from the cancer cohort). The Tg/longitudinal rows are
real lab values, just for non-cancer patients.

Because `n_lab_first_after_cohort_freeze = 0` and the cohort freeze is
2026-04-17, this is **not a post-freeze feed-drift problem**. These
patients have always been outside the cancer cohort — they just sit in
the lab tables.

**Recommendation:** Approving the delete is correct **if** the lab
tables are scoped to the cancer cohort only. If Logan wants to preserve
benign-thyroidectomy Tg surveillance for a future analysis, the
alternative is to LEAVE the rows and add an
`is_in_canonical_cancer_cohort` flag column instead.

## What Logan needs to decide before Phase 3

1. Approve the **403** `likely_non_cancer` rids for DELETE from `main.thyroglobulin_lab_canonical_v1` and `main.longitudinal_lab_canonical_v1`?
2. Triage the **0** `likely_dropped_from_CPM` rids: how many were post-cohort-freeze feed drift (refresh) vs true admit? See temporal-context table above.
3. The **0** `ambiguous` rids stay HELD pending chart review.

_Full per-patient table_: [`tg_orphan_decisions.csv`](./tg_orphan_decisions.csv)
