# mig_176 — Dominant Nodule v1/v2 Reconciliation Decision Package

**Date:** 2026-04-29  
**Lane:** 65 / mig_176  
**Batch:** `mig_176_dominant_nodule_v1_v2_reconcile_20260429`  
**Posture:** read-only MotherDuck profile and Logan decision package; no data writes.  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Target table:** `main.canonical_patient_master`  
**Replay SQL:** `qc_framework_v1/migrations/176_dominant_nodule_reconcile_probes_20260429.sql`  
**Carry-forward:** `CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT`

## Executive summary

The live profile confirms the carry-forward exactly: **1,065 patients** have both `dominant_nodule_size_cm` (v1) and `dominant_nodule_size_cm_v2` (v2) populated with different values. There are also **166 v2-only patients**, **0 v1-only patients**, **2,374 exact matches**, and **7,266 both-null patients** in the 10,871-row canonical patient master.

The direction is one-sided: in all **1,065 mismatches**, **v2 is larger than v1**. The median absolute difference is **2.54 cm**, the mean absolute difference is **2.84 cm**, and **818/1,065 mismatches (76.8%)** differ by at least 1.0 cm.

Source cross-checking strongly favors v1 for the mismatch rows. For all **1,065/1,065** mismatch patients, v1 exactly matches the live `main.imaging_patient_summary_v1.dominant_nodule_size_cm`, the read-only legacy `us_legacy_20260421.canonical_us_nodule_characteristics_v1` max `size_cm_max`, and the read-only legacy `us_legacy_20260421.imaging_nodule_master_v1` max `max_dimension_cm`. v2 matches **0/1,065** of those source rollups. The independent `ops_dominant_nodule_size_us` tiebreaker has **0 available values** among the mismatch rows and cannot adjudicate.

**Recommendation:** Ratify **R2: prefer v1, else v2** for mig_176b. This preserves the source-supported v1 value for all 1,065 mismatch patients while retaining the 166 v2-only patients as additional coverage in a new resolved field. Also add a `dominant_nodule_size_cm_resolution_rule` audit column in mig_176b and keep `dominant_nodule_size_cm_v2` as a legacy/provenance column until the v2 derivation is traced or retired. Do **not** rename/drop v2 in the apply lane without a separate reader-impact scan.

## Live scope profile

### Full v1/v2 shape

| Metric | Count |
|---|---:|
| Both non-null and mismatched | 1,065 |
| v1 only | 0 |
| v2 only | 166 |
| Both non-null exact match | 2,374 |
| Both null | 7,266 |
| CPM rows | 10,871 |

### Mismatch magnitude

| Absolute-difference band | n |
|---|---:|
| `<0.1 cm` | 15 |
| `0.1–<0.5 cm` | 108 |
| `0.5–<1.0 cm` | 124 |
| `>=1.0 cm` | 818 |

Summary statistics for absolute difference among mismatch rows:

| Statistic | Value |
|---|---:|
| Min | 0.01 cm |
| Median | 2.54 cm |
| Mean | 2.84 cm |
| 95th percentile | 6.59 cm |
| Max | 46.09 cm |

### Directionality

| Direction metric | Value |
|---|---:|
| v1 larger than v2 | 0 |
| v2 larger than v1 | 1,065 |
| Mean signed difference (`v1 - v2`) | -2.84 cm |
| Median signed difference (`v1 - v2`) | -2.54 cm |
| Min signed difference | -46.09 cm |
| Max signed difference | -0.01 cm |

The one-sided pattern argues against random rounding drift and suggests a systematic v2 derivation difference.

## Registry and source provenance

Registry entries for both columns are verified under the same mig_157 batch and carry the same open CF:

| Column | Type | Status | Method | Batch | Registry note |
|---|---|---|---|---|---|
| `dominant_nodule_size_cm` | DOUBLE | verified | `derivation_vs_canonical_path_malignant_patient_rollup_v1` | `mig_157_patient_master_clinical_residual_cluster_20260429` | `CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT: 1065 both-non-null differ; 166 v2-only — cross-feed reconcile.` |
| `dominant_nodule_size_cm_v2` | DOUBLE | verified | `derivation_vs_canonical_path_malignant_patient_rollup_v1` | `mig_157_patient_master_clinical_residual_cluster_20260429` | `CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT: 1065 both-non-null differ; 166 v2-only — cross-feed reconcile.` |

The historical code path indicates:

- v1 is the patient-level imaging rollup from `imaging_patient_summary_v1`, which itself uses `MAX(largest_nodule_cm)` across `imaging_exam_master_v1`.
- script 265 documents `dominant_nodule_size_cm` as sourced from `imaging_patient_summary_v1`, with `canonical_us_nodule_characteristics_v1` and `imaging_nodule_master_v1` as upstream feeders.
- script 368 added parallel `_v2` columns and populated `dominant_nodule_size_cm_v2` from a v2 US cutover stage; the current publication database no longer exposes those v2 staging tables in `main`, so the live replay can only compare the preserved v2 values against current live/legacy feeders.

### Upstream table availability in the locked MotherDuck session

| Catalog | Schema | Table | Type |
|---|---|---|---|
| `thyroid_canonical_publication_v1_0` | `main` | `imaging_patient_summary_v1` | BASE TABLE |
| `Thyroid 2026 UPdated` | `us_legacy_20260421` | `canonical_us_nodule_characteristics_v1` | BASE TABLE |
| `Thyroid 2026 UPdated` | `us_legacy_20260421` | `imaging_nodule_master_v1` | BASE TABLE |

The tables `canonical_us_exam_master_v2`, `canonical_us_patient_master_v2`, and `extracted_tirads_validated_v1` were not present in the current `main` schema at probe time.

## Source cross-validation

Among the 1,065 mismatch patients:

| Source comparison | Has source value | Matches v1 | Matches v2 |
|---|---:|---:|---:|
| Live `imaging_patient_summary_v1.dominant_nodule_size_cm` | 1,065 | 1,065 | 0 |
| Legacy `canonical_us_nodule_characteristics_v1` max `size_cm_max` | 1,065 | 1,065 | 0 |
| Legacy `imaging_nodule_master_v1` max `max_dimension_cm` | 1,065 | 1,065 | 0 |

This is the decisive profile for the recommendation. v1 is source-replayable in current live state; v2 is not replayable from the exposed source rollups.

## Independent OR pre-op tiebreaker

`ops_dominant_nodule_size_us` is not useful for this mismatch cluster:

| Metric | Count |
|---|---:|
| Mismatch denominator | 1,065 |
| Rows with non-empty `ops_dominant_nodule_size_us` | 0 |
| Rows with parsed numeric ops size | 0 |
| v1 exact/near matches to ops | 0 |
| v2 exact/near matches to ops | 0 |

Therefore R5 collapses to its fallback rule and should not be used as a distinct adjudication option for mig_176b.

## Unit/scale sanity check

The v2 values are not explained by a simple mm-vs-cm factor of 10. Only **3** mismatch rows have `v2 / 10` within 0.10 cm of v1; only **1** row has v2 within 0.25 cm of `v1 * 10`. However, v2 has a visibly inflated tail:

| v2 bin among mismatches | n |
|---|---:|
| `<1 cm` | 1 |
| `1–<2 cm` | 40 |
| `2–<4 cm` | 375 |
| `4–<6 cm` | 380 |
| `6–<10 cm` | 249 |
| `10–<15 cm` | 17 |
| `>=15 cm` | 3 |

Additional summary:

| Metric | Value |
|---|---:|
| v2 > 10 cm among mismatches | 19 |
| v2 > 15 cm among mismatches | 3 |
| Mean v2/v1 ratio | 2.65 |
| Median v2/v1 ratio | 2.25 |

Top examples by absolute difference:

| research_id | v1_size | v2_size | abs_diff |
|---:|---:|---:|---:|
| 8931 | 1.91 | 48.00 | 46.09 |
| 12152 | 2.30 | 19.00 | 16.70 |
| 12141 | 1.97 | 18.00 | 16.03 |
| 10480 | 1.05 | 12.40 | 11.35 |
| 5664 | 2.03 | 12.50 | 10.47 |

These values are clinically implausible for many patients and are not supported by the source replay against current/legacy US nodule rollups.

## v2-only rows

The **166 v2-only** rows are analytically useful if isolated by an audit rule, because v1 has no competing value. Their distribution is materially less extreme than the mismatch tail:

| Metric | Value |
|---|---:|
| v2-only rows | 166 |
| Mean | 2.11 cm |
| Median | 1.50 cm |
| 95th percentile | 6.70 cm |
| Min | 0.30 cm |
| Max | 10.70 cm |

| v2-only size bin | n |
|---|---:|
| `<1 cm` | 45 |
| `1–<2 cm` | 64 |
| `2–<4 cm` | 35 |
| `4–<6 cm` | 11 |
| `6–<10 cm` | 10 |
| `10–<15 cm` | 1 |

This supports a hybrid implementation of the R2 rule: use v1 when present, and only use v2 as a coverage extension when v1 is null.

## Candidate rule comparison

| Rule | Resolved size | Mismatch denominator | Resolved rows, all CPM | Mismatch rows resolved | Mismatch cells changed vs current v1 | All-row mean | All-row median | All-row p95 | Mismatch mean | Mismatch median | Mismatch p95 | Pro / Con |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| R1 | v2 if non-null, else v1 | 1,065 | 3,605 | 1,065 | 1,065 | 2.75 | 2.19 | 6.70 | 4.78 | 4.50 | 8.30 | Simple, but chooses the non-replayable and inflated side for every mismatch. |
| R2 | v1 if non-null, else v2 | 1,065 | 3,605 | 1,065 | 0 | 1.91 | 1.89 | 3.01 | 1.95 | 1.94 | 2.90 | Source-supported for all mismatches; preserves 166 v2-only coverage. |
| R3 | max(v1, v2) | 1,065 | 3,605 | 1,065 | 1,065 | 2.75 | 2.19 | 6.70 | 4.78 | 4.50 | 8.30 | Equivalent to R1 because v2 is always larger; inherits v2 inflation. |
| R4 | average of v1/v2 | 1,065 | 3,605 | 1,065 | 1,065 | 2.33 | 2.14 | 4.46 | 3.36 | 3.29 | 5.17 | Smooths but creates synthetic values not present in any source. |
| R5 | ops tiebreak; fallback v2 | 1,065 | 3,605 | 1,065 | 1,065 | 2.75 | 2.19 | 6.70 | 4.78 | 4.50 | 8.30 | No ops signal exists, so it collapses to R1 in this cohort. |
| R6 | keep both | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | Avoids writes but leaves analyst-dependent bifurcation unresolved. |

For reference, current v1 all-row distribution among 3,439 non-null rows is mean **1.90 cm**, median **1.90 cm**, p95 **2.98 cm**. v2 all-row distribution among 3,605 non-null rows is mean **2.75 cm**, median **2.19 cm**, p95 **6.70 cm**.

## Logan decision package

### 1. Recommended rule

**Recommended:** R2 — `resolved = COALESCE(dominant_nodule_size_cm, dominant_nodule_size_cm_v2)`.

Rationale:

1. v1 is exactly replayable from all available live/legacy source rollups for **1,065/1,065** mismatch patients.
2. v2 is always larger in mismatch rows and has an inflated tail, including values up to **48 cm**.
3. The independent ops tiebreaker is absent for all mismatch rows.
4. R2 still preserves the **166 v2-only** rows as additional coverage instead of discarding them.

### 2. Affected patient count

- **Mismatch patients adjudicated:** 1,065.
- **Value changes relative to current v1 field among mismatches if R2 is used for a new resolved column:** 0.
- **Coverage extension from v2-only rows:** 166 additional non-null resolved sizes beyond current v1.
- **Total resolved non-null under R2:** 3,605 patients.

### 3. Distribution shift under recommendation

| Distribution | n | Mean | Median | 95th percentile |
|---|---:|---:|---:|---:|
| Current v1 (`dominant_nodule_size_cm`) | 3,439 | 1.90 | 1.90 | 2.98 |
| R2 resolved (`COALESCE(v1, v2)`) | 3,605 | 1.91 | 1.89 | 3.01 |
| v2-only contribution | 166 | 2.11 | 1.50 | 6.70 |

The recommended rule produces a minimal distribution shift because it does not replace source-supported v1 mismatch values; it only fills v1-null/v2-present rows.

### 4. Deprecation disposition for v2

Do **not** immediately rename/drop `dominant_nodule_size_cm_v2` in mig_176b. Keep it as a legacy/provenance column for at least one apply cycle because it documents the drift and supports audit replay. After reader-impact scanning, consider:

- keeping `dominant_nodule_size_cm_v2` with a registry note marking it `legacy_non_authoritative_v2`, or
- renaming it to `dominant_nodule_size_cm_v2_legacy_raw` only if all downstream readers are migrated.

### 5. Resolution audit column

Yes. mig_176b should add a rule/provenance column such as `dominant_nodule_size_cm_resolution_rule` if Logan ratifies R2.

Suggested rule labels:

| Rule label | Condition |
|---|---|
| `v1_source_replayable` | v1 non-null, regardless of v2 mismatch/match |
| `v2_only_no_v1_source` | v1 null and v2 non-null |
| `both_null` | v1 null and v2 null |

If a separate resolved value column is used, name it `dominant_nodule_size_cm_resolved` and populate with `COALESCE(v1, v2)` only after Logan ratifies.

## Carry-forwards

| Carry-forward | Proposed status from mig_176 | Notes |
|---|---|---|
| `CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT` | Keep open pending Logan ratification and mig_176b apply | mig_176 provides evidence only. |
| `CF-mig176-RECOMMENDED-RULE-R2` | Informational recommendation | R2 is source-supported and minimally shifts distribution. |
| `CF-mig176-V2-NONREPLAYABLE-SOURCE-GAP` | Informational recommendation | v2 values did not match live/legacy source rollups in current state; v2 staging tables were not present in `main`. |

## Recommended mig_176b apply scope after Logan ratification

1. Snapshot `research_id`, `dominant_nodule_size_cm`, and `dominant_nodule_size_cm_v2` to the canonical archive schema.
2. Add `dominant_nodule_size_cm_resolved DOUBLE` and `dominant_nodule_size_cm_resolution_rule VARCHAR`.
3. Populate `dominant_nodule_size_cm_resolved = COALESCE(dominant_nodule_size_cm, dominant_nodule_size_cm_v2)`.
4. Populate resolution labels using the rule table above.
5. Update registry notes for the original v1/v2 columns and the new resolved columns.
6. Keep `CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT` open until the apply verification shows 0 unresolved mismatches in the new resolved layer.

## Out-of-scope actions not performed

- No `UPDATE`, `ALTER`, `CREATE`, or registry mutation was performed.
- No `canonical_patient_master` values were modified.
- No `query_rw` action was used.
- The separate `ops_*` and `mri_*` dominant-nodule columns were not modified.
- No upstream US/imaging extraction rebuild was attempted.

## Logan ratification request

Please ratify one of the following:

1. **R2 / recommended:** Create a resolved dominant-nodule size using `COALESCE(v1, v2)`, with audit labels; preserve v1 for the 1,065 mismatch rows and use v2 only for the 166 v2-only rows.
2. **R1/R3:** Prefer v2/max despite the current source-replay failure and inflated tail.
3. **R4:** Use average, accepting synthetic non-source values.
4. **R6:** Keep both fields and document analyst choice without adding a resolved field.
