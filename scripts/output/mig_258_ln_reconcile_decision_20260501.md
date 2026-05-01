# mig_258 (CF-mig258) — N-stage vs `ln_total_positive` decision memo

**Date:** 2026-05-01  
**DB:** `thyroid_canonical_publication_v1_0` (MotherDuck, `connect_locked`)  
**Repo migration file:** `qc_framework_v1/migrations/259_ln_status_source_cf_mig258_20260501.sql` (numeric **259** avoids collision with `258_m044_surgery_date_lineage_flags_20260501.sql`).

## §1 Distribution (malignant, N1a/N1b × count bucket)

| ajcc8_n_stage | count_bucket | n |
| --- | --- | --- |
| N1a | count_0 | 7 |
| N1a | count_NULL | 1494 |
| N1a | count_pos | 1051 |
| N1b | count_NULL | 8 |
| N1b | count_pos | 75 |

**Disagreement cohort** (N1a/N1b AND (`ln_total_positive` IS NULL OR = 0)): **1,509** rows (dispatch cited 1,501; live MotherDuck = 1,509).

## §2 Probe results (executed 2026-05-01)

### Probe 1 — Examined LN among disagreement rows

| n_disagreement | n_with_lns_examined | n_no_lns_examined |
| --- | --- | --- |
| 1509 | 12 | 1497 |

**Interpretation:** Rule A fallback “use `ln_total_examined` when > 0” only touches **12** patients. The other **1,497** would fall through to sentinel **1** (“≥1 positive”), mixing “unknown true count” with “examined count” semantics.

### Probe 2 — Column lineage (`canonical_column_verification_registry_v1`)

| column | batch_id | verification_method (summary) |
| --- | --- | --- |
| `ajcc8_n_stage` | mig_132 | patient_level_ajcc_overlay_dominant_tumor_mig266b_family |
| `ln_total_positive`, `ln_total_examined`, `ln_positive_flag` | mig_133 | derivation_ln_core_path_malignant_and_level_rollups_mig89 |

N-stage and LN totals are **independent derivation lanes**; large NULL/`0` LN counts alongside N1a/N1b reflect **path/LN rollup sparsity**, not necessarily incorrect staging.

### Probe 3 — Concordant N1a + positive count

| n | mean_count |
| --- | --- |
| 1051 | 6.12 |

### Supplement — `ln_positive_flag` in disagreement cohort (1509 rows)

| ln_pos_flag_true | ln_pos_flag_false | other (mostly NULL) |
| --- | --- | --- |
| 10 | 404 | 1095 |

**Interpretation:** Structured “LN positive” flag does **not** resolve the gap. **Rule B** (“count/flag is truth → set N0 when count is 0”) would conflict with **404** patients who are N1a/N1b but have `ln_positive_flag = FALSE`, and would discard AJCC N-stage for **~1.5k** patients with missing counts.

### Dry-run — proposed `ln_status_source` (Rule C)

| ln_status_source | n (all CPM rows) |
| --- | --- |
| both | 1126 |
| staging | 1509 |
| NULL | 8236 |

No malignant rows produced `count`-only (all patients with `ln_total_positive > 0` have N1a/N1b in the current cohort).

## §3 Rule choice (for Logan)

| Rule | Verdict |
| --- | --- |
| **A** (N-stage truth → impute count) | **Not recommended** as primary: sentinel 1 for ~1.5k patients misrepresents precision; using `ln_total_examined` as positive count is **clinically wrong** for the 12 with examined > 0. |
| **B** (count truth → downgrade N) | **Not recommended**: would drop **1,509** N1+ patients from N-filtered manuscripts; **404** explicit false flags vs N1a/b show staging and rollup are not comparable truths. |
| **C** (lossless domains + explicit source) | **Recommended**: add `ln_status_source` ∈ {`both`, `staging`, `count`}; keep `ajcc8_n_stage` and `ln_total_positive` unchanged. Manuscripts declare whether they stratify by **staging**, **structured count**, or **both** (restrict to `both` for count–N concordant analyses). |

**Carry-forward:** CF-mig258-NSTAGE-LNCOUNT-RECONCILE closed by Rule **C** once migration applied and Snowflake flat export refreshed. CF-mig258-MANUSCRIPT-FILTER-UPDATE: M037 / M044 queries should reference `ln_status_source` in methods and, where appropriate, filter `both` for LN-count Table 1 cells.

## §4 Apply

Run:

`duckdb MotherDuck` or repo helper: execute `qc_framework_v1/migrations/259_ln_status_source_cf_mig258_20260501.sql` with RW token.

Post-apply Snowflake: re-export CPM; verify `staging` bucket = 1509; disagreement predicate count → **0** only if using imputed logic (not required under Rule C).
