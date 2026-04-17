# THYROID v1_1 Finalization Pass — PLAN

**Database:** `thyroid_canonical_publication_v1_0` (publication DB)
**Date:** 2026-04-16
**Branch:** `cleanup/v1_1_finalization-20260416`
**Authoritative source for findings:** `PART2_DETAIL_CROSSVAL_FINDINGS_20260416.md`
(§7 CORRECTION ADDENDUM is binding — findings 4.1, 5.1, 5.2 were withdrawn,
do **not** "fix" them.)

---

## Pre-flight ground truth (replayed 2026-04-16)

| audit § | metric | expected | replay |
|---|---|---|---|
| 1.1 | `max_tirads_ever` CPM-too-low patients (vs `canonical_us_nodule_characteristics_v1`) | 1,503 | **1,503** |
| 2.1 | thyroglobulin_lab orphan patients (not in CPM) | 537 | **537** |
| 2.1 | longitudinal_lab orphan patients (not in CPM) | 537 | **537** |
| 2.2 | `n_fna_episodes` mismatches vs `fna_episode_master_v2` count | 5,007 | **5,028** |
| 3.1 | `rai_max_dose_mci=0` while `rai_treatment_episode_v2.MAX(dose_mci)>0` | 214 | **213** |
| 3.3 | `n_tg_measurements_structured` mismatches | 1,444 | **1,637** |
| 3.3 | `n_tgab_measurements` mismatches | 1,675 | **1,755** |
| 3.4 | `tg_peak` mismatches | 503 | **505** |
| 3.4 | `tg_nadir` mismatches | 535 | **537** |
| 5.3 | `any_confirmed_complication_flag=FALSE` while phenotype confirms | 128 | **174** |

CPM = 10,871 patients × 1,500 columns. main BASE TABLEs = 114. ws views = 65.
9 `DEPRECATED__/deprecated__` columns currently in CPM.
0 `_backup`/`_pre###`/`_v221`/`_legacy` tables in `main` — Acceptance #1 already
satisfied (Script 257(c) is a no-op sweep but still verifies).

---

## Real findings to fix (only the post-correction-addendum set)

| # | audit § | severity | finding | script |
|---|---|---|---|---|
| 1 | 1.1 | CRITICAL | `max_tirads_ever` ignores `tirads_acr_recalculated` | **252** |
| 2 | 2.1 | CRITICAL | 537 lab-orphan patients (Tg + longitudinal) | **253** |
| 3 | 2.2 | CRITICAL | `n_fna_episodes` cluster at 11/12 (broadcast leak) | **254** |
| 4 | 3.1 | HIGH | `rai_max_dose_mci=0` for 213 patients with detail dose >0 | **255** |
| 5 | 3.3 | HIGH | `n_tg_measurements_structured`/`n_tgab_measurements` undercount | **255** |
| 6 | 3.4 | HIGH | `tg_peak` / `tg_nadir` mismatch | **255** |
| 7 | 5.3 | HIGH | `any_confirmed_complication_flag` only sees 3 of 9 entities | **256** |

Items intentionally NOT touched (per §7 addendum): 4.1 T3b restage,
5.1 `any_recurrence_flag`, 5.2 635 operative-cohort orphans.

---

## Script plan

| script | task | extends/supersedes |
|---|---|---|
| **252** | Recompute `max_tirads_ever`, `imaging_tirads_worst`, `preop_tirads_best` from `canonical_us_nodule_characteristics_v1` (built by Script 246). Re-annotate `us_nodules_tirads.tirads_worst_category_v12` as `legacy`. | extends 246 |
| **253** | Triage 537 lab orphans. Cancer-evidence test (FNA+tumor+synoptic+path+imaging+op_episode). Auto-archive zero-evidence orphans; route any-evidence orphans to `manuscript_workspace.lab_orphan_cohort_review_v1` for human decision. | new |
| **254** | Rebuild `n_fna_episodes`, `n_fna_cytology_records`, `prm_first_fna_date`, `prm_last_fna_date` from `fna_episode_master_v2` GROUP BY. Add `worst_bethesda_source` column (no value mutation). | supersedes the broadcast leak |
| **255** | Single-pass rebuild of (`rai_max_dose_mci`, `n_tg_measurements_structured`, `n_tgab_measurements`, `tg_peak`, `tg_nadir`, `tg_mean`) from canonical sources. Adds `rai_max_dose_source` + `tg_peak_source` provenance cols. Updates `data_dictionary_v240`. | extends 242 |
| **256** | Rebuild `any_confirmed_complication_flag` from full `complication_phenotype_v1`. ADD COLUMN `comp_hematoma_confirmed`, `comp_seroma_confirmed`, `comp_chyle_leak_confirmed`, `comp_wound_infection_confirmed`, `comp_vocal_cord_paralysis_confirmed`, `comp_vocal_cord_paresis_confirmed`. Register all in `data_dictionary_v240`. Existing 3 columns (`comp_hypocalcemia_*`, `comp_hypoparathyroidism_*`, `comp_rln_*`) untouched. | extends complication phenotype rollup |
| **257** | (a) Snapshot CPM, then DROP COLUMN for the 9 `DEPRECATED__/deprecated__` cols (verify successor populated first via `manuscript_workspace.legacy_column_sweep_v1_1`). (b) Drop any `data_dictionary_v240` `status='deprecated'` rows whose target now no longer exists in CPM. (c) Sweep `main` for `_backup\|_pre\d+\|_predup\|_v221\|_legacy\|_old` (currently 0 hits — verifies). (d) Rebuild `main.__readme` + `manuscript_workspace.detail_table_registry_v1` row counts via Script 247's logic. | extends 247, 249 |
| **258** | Re-run Script 250's pointer rebuild logic so `canonical_detail_pointer_v1` reflects post-cleanup state: `canonical_us_nodule_characteristics_v1` (TIRADS), `canonical_tumor_characteristics_v1`, `thyroglobulin_lab_canonical_v1`. Assert 0 unset/TODO and zero registry-vs-pointer drift. | extends 250 |
| **259** | Final verification + emit `studies/v1_1_finalization/FINALIZATION_REPORT_v1_1.md`. Replay all 6 finding queries (post-fix), CPM hash-of-row-hashes, archive table inventory, dictionary status breakdown, pointer health. | new |

All scripts share the contract:

1. Connect via `_md_connect.connect_locked()` — locks search path to publication DB and asserts CPM=10,871 invariant.
2. Default mode `--dry-run`; mutations only with `--apply`.
3. Snapshot any mutated table to `"Thyroid 2026 UPdated".archive_pub_v1_0.<table>_pre<scriptnum>_<UTC tsZ>` with `COMMENT ON TABLE` explaining provenance.
4. Self-verification SELECT after the fix; persist before/after counts to `manuscript_workspace.v1_1_finalization_audit_v1` (created by Script 252 first-run).
5. All cross-table joins use `TRY_CAST(research_id AS INTEGER)`.
6. Decision JSON to `scripts/output/<scriptnum>_decision_log.json`; run log to `scripts/output/<scriptnum>_run.log`.

---

## Acceptance criteria mapping

| # | criterion | script(s) | check |
|---|---|---|---|
| 1 | `main` has 0 `_backup\|_pre###\|_predup\|_v221\|_legacy\|_old` | 257 | preflight + post |
| 2 | CPM rows = 10,871; col count down by exactly 9 | 257, 259 | row hash + col count diff |
| 3 | All 6 replay queries → 0 (or documented residual for 2.1) | 252, 254, 255, 256 | per-script self-verify + 259 final |
| 4 | `detail_table_registry_v1`: 0 null/TODO `feeds_master_columns`; every CPM domain present | 258 | strict pointer assertion |
| 5 | `canonical_detail_pointer_v1` resolves to existing tables | 258 | join-existence assertion |
| 6 | Archive has new `_pre257_<tsZ>` and `_pre253_<tsZ>` snapshots | 253, 257 | archive table inventory |
| 7 | `data_dictionary_v240`: 0 `status='deprecated'` pointing to existing CPM column | 257 | post-drop assertion |
| 8 | `studies/v1_1_finalization/FINALIZATION_REPORT_v1_1.md` exists + committed; CPM row-hash-of-row-hashes recorded | 259 | file existence + git status |

---

## Execution order

```
252 → 253 → 254 → 255 → 256 → 257 → 258 → 259
```

Each runs `--dry-run` first, then `--apply` after dry-run is reviewed for
expected counts. Any script that fails a self-verify halts the chain.
