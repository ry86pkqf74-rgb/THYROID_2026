# THYROID Canonical Publication v1_1 — Finalization Report

**Database:** `thyroid_canonical_publication_v1_0`
**Run date (UTC):** 2026-04-17T02:09:26.075749+00:00
**Branch:** cleanup/v1_1_finalization-20260416

This report is the read-only verification artifact for Scripts 252–258. Every fix has a snapshot in `"Thyroid 2026 UPdated".archive_pub_v1_0` with a `pre<scriptnum>_<UTC tsZ>` suffix.

## 1. Audit replay results (post-fix)

| Audit § | Metric | After | Target | Status |
|---|---|---|---|---|
| §1.1 | max_tirads_ever undercount | 0 | 0 | ✓ |
| §2.1 (Tg) | Tg lab orphans (post-archive) | 403 | 403 | ✓ |
| §2.1 (Long) | Longitudinal lab orphans (post-archive) | 403 | 403 | ✓ |
| §2.2 | n_fna_episodes mismatch | 0 | 0 | ✓ |
| §3.1 | rai_max_dose_mci=0 with detail>0 | 0 | 0 | ✓ |
| §3.3 | n_tg_measurements_structured mismatch | 0 | 0 | ✓ |
| §3.3 | n_tgab_measurements mismatch | 0 | 0 | ✓ |
| §3.4 | tg_peak mismatch | 0 | 0 | ✓ |
| §3.4 | tg_nadir mismatch | 0 | 0 | ✓ |
| §5.3 | any_confirmed_complication_flag undercount | 0 | 0 | ✓ |

**§2.1 residual note:** 537 lab-orphan patients existed at v1_0. Script 253 archived 134 zero-evidence orphans (no FNA, tumor episode, synoptic, path, imaging, or operative record) to `"Thyroid 2026 UPdated".archive_pub_v1_0.thyroglobulin_lab_canonical_v1_orphans_pre253_*`. The remaining 403 have at least one cancer-evidence record and were routed to `manuscript_workspace.lab_orphan_cohort_review_v1` for human cohort decision. DO NOT auto-merge.

## 2. canonical_patient_master state

- **Rows:** 10,871 (locked at 10,871 — invariant)
- **Columns:** 1,494 (start of v1_0 = 1,500; +3 provenance cols added by 254/255 (`worst_bethesda_source`, `rai_max_dose_source`, `tg_peak_source`); -9 deprecated cols dropped by 257; net = 1,494)
- **Hash-of-row-hashes (SHA-256):** `8c897c30ceaa9e31bdfcc90e62d813b4733a152392897559d7ddfed391142a30`

This hash is computed as `sha256(concat(research_id || md5(cpm row) || newline) ordered by research_id)`. Re-run Script 259 to recompute and compare.

## 3. Archive inventory (`"Thyroid 2026 UPdated".archive_pub_v1_0`)

Tables created during the v1_1 pass (matching `pre252_..pre258_`): **10**

| Snapshot table | Rows |
|---|---|
| `__readme_pre257_20260417T020326Z` | 114 |
| `canonical_detail_pointer_v1_pre258_20260417T020547Z` | 1 |
| `canonical_patient_master_pre252_20260417T015203Z` | 10871 |
| `canonical_patient_master_pre254_20260417T015520Z` | 10871 |
| `canonical_patient_master_pre255_20260417T015724Z` | 10871 |
| `canonical_patient_master_pre256_20260417T015909Z` | 10871 |
| `canonical_patient_master_pre257_20260417T020239Z` | 10871 |
| `longitudinal_lab_canonical_v1_orphans_pre253_20260417T015343Z` | 2713 |
| `thyroglobulin_lab_canonical_v1_orphans_pre253_20260417T015343Z` | 2713 |
| `view_ddl_snapshot_pre257_20260417T020239Z` | 65 |

## 4. data_dictionary_v240 status breakdown

| Status | Count |
|---|---|
| authoritative | 1474 |
| (null) | 26 |
| removed | 25 |
| provisional | 3 |
| legacy | 1 |

## 5. canonical_detail_pointer_v1 + detail_table_registry_v1 health

- CPM columns:                       **1,494**
- CPM cols mapped via pointer view:  **1,273** (85.21%)
- Distinct drill-down tables:        **90**
- Unresolved drill-down references:  **0** (must be 0)

Priority drill-downs (verified mapping ≥ 1 CPM column):
- `canonical_us_nodule_characteristics_v1`  — TIRADS per-nodule-per-exam
- `canonical_tumor_characteristics_v1`      — per-resected-tumor
- `thyroglobulin_lab_canonical_v1`          — Tg/TgAb

## 6. main schema legacy-pattern sweep

`main` BASE TABLEs matching `_backup|_pre###|_predup|_v221|_legacy|_old`: **0** (must be 0).

## 7. Acceptance criteria

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | `main` has 0 legacy-pattern tables | ✓ | `payload.legacy_count = 0` |
| 2 | CPM rows = 10,871; cols dropped by 9 | ✓ | `rows=10871; cols=1494 (1,500 start +3 -9 = 1,494)` |
| 3 | All 6 replay queries → 0 (or §2.1 documented residual) | ✓ | see §1 above; lab-orphan residual = 403 routed to review |
| 4 | registry: 0 NULL/TODO/(unset) `feeds_master_columns` | ✓ | verified by Script 258 phase C |
| 5 | pointer view resolves every detail_table_name to existing table | ✓ | `n_unresolved_drilldown_tables = 0` |
| 6 | archive has new `_pre257_<tsZ>` and `_pre253_<tsZ>` snapshots | ✓ | see §3 (table count = 10) |
| 7 | dict: 0 `status='deprecated'` rows pointing to live CPM column | ✓ | verified by Script 257 phase C |
| 8 | this report exists; CPM row-hash recorded | ✓ | `8c897c30ceaa9e31...` |

## 8. v1_1_finalization_audit_v1 history

| Script | Finding | Metric | Before | After | Target | Status |
|---|---|---|---|---|---|---|
| 252 | audit_1_1 | max_tirads_ever_undercount | 1503 | 0 | 0 | OK |
| 253 | audit_2_1 | thyroglobulin_lab_orphans | 537 | 403 | 403 | OK |
| 253 | audit_2_1 | longitudinal_lab_orphans | 537 | 403 | 403 | OK |
| 254 | audit_2_2 | n_fna_episodes_mismatch | 5028 | 0 | 0 | OK |
| 255 | audit_3_1 | rai_max_dose_mci_zero_gt0 | 213 | 0 | 0 | OK |
| 255 | audit_3_3 | n_tg_measurements_mismatch | 1637 | 0 | 0 | OK |
| 255 | audit_3_3 | n_tgab_measurements_mismatch | 1755 | 0 | 0 | OK |
| 255 | audit_3_4 | tg_peak_mismatch | 505 | 0 | 0 | OK |
| 255 | audit_3_4 | tg_nadir_mismatch | 537 | 0 | 0 | OK |
| 256 | audit_5_3 | any_confirmed_complication_flag_undercount | 174 | 0 | 0 | OK |
| 257 | criteria_1_2_4_5 | clean_house_summary | 1503 | 1494 | 1494 | OK |
| 258 | criteria_4_5 | registry_pointer_health | 1271 | 1273 | 1273 | OK |

## 9. Items left at human-review status (intentional)

- **403 lab-orphan patients** parked in `manuscript_workspace.lab_orphan_cohort_review_v1` per audit §2.1 / §7.3 protocol. Each row carries the cancer-evidence vector (`has_fna`, `has_tumor`, `has_syn`, `has_path`, `has_imaging`, `has_op`) plus per-patient lab-row counts and date span. Decision: re-admit to CPM via cohort pipeline, or archive after sign-off.

- The 4.1 (`ajcc8_t_stage` T3b restage), 5.1 (`any_recurrence_flag`), and 5.2 (635 op-only orphans) findings were withdrawn by the audit addendum (§7.1, §7.2, §7.3) — intentionally NOT touched.

## 10. Proposed v1_2 candidates (out of scope for this pass)

- **Triage the 403 cancer-evidence lab orphans** in `manuscript_workspace.lab_orphan_cohort_review_v1` (per-patient cohort decision).
- **Imaging exam_date data quality** (audit §1.5): 2,061 of 37,016 rows in `imaging_nodule_master_v1` lack `exam_date`. Add `exam_date_quality` column + `imaging_nodule_master_clean_v1` view.
- **`n_us_exams` provenance opacity** (audit §1.4): document the union-of-sources rollup or add `n_us_exams_source` column.
- **`multifocal_flag_path` ghost TRUE** (audit §4.2): 245 patients flagged TRUE without supporting synoptic or NLP evidence.
- **`path_tumor_size_cm` semantics** (audit §4.3): document `dominant` vs `max` rule, add invariant check.
- **`ln_count_reconciled` provenance** (audit §5.4): publish `ln_count_source` column for stratification.
- **Molecular test date imputation** (audit §2.4): 9,280/10,126 molecular episodes lack `test_date_native` AND `resolved_test_date`.
- **Allowlist tightening** (Script 250 footnote): the 229 natively-derived CPM cols allowlisted in pointer mapping deserve another pass — estimated lift to 88-92% mapped if 1-2 source tables are discovered.

---

_End of report. Generated by `259_final_verification_lock.py` (2026-04-17T02:09:26.075920+00:00)._
