# Cursor Agent Task — manuscript_workspace tier3_helper batch verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post mig_127 audit refinement)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 60-90 minutes (~15-20 small helper tables, batch verification)
**Run order:** Lane 21 of next 3-prompt batch (run middle — workspace helpers cleanup)

---

## 1. Goal

Verify the **tier3_helper batch** in `manuscript_workspace` schema. These are small adjudication-queue, audit, and backfill helper tables that support the canonical layer but are not first-class canonicals themselves.

Probe at start to find the current open scope:
```sql
SELECT table_name, schema_name, n_columns_total, n_not_started, priority_tier
FROM main.canonical_table_signoff_registry_v1
WHERE table_status='not_started' 
  AND priority_tier='tier3_helper'
ORDER BY n_not_started ASC, table_name;
```

Likely candidates (from 2026-04-29 probe):
| Table | Cols / not_started |
|---|---|
| biochemical_concern_backfill_v1 | 2 / 1 |
| nucmed_tgab_max_backfill_v1 | 2 / 1 |
| registry_v2_cpm_cols_without_registry_v1 | 1 / 1 |
| cpm_hypopara_adjudication_queue_v1 | 6 / 2 |
| path_stage_raw_backfill_v1 | 3 / 2 |
| us_llm_absorption_gap_v1 | 4 / 2 |
| verification_low_concordance_v1 | 4 / 2 |
| cpm_ete_self_contradiction_queue_v1 | 10 / 3 |
| cpm_hypopara_adjudication_log_v1 | 5 / 3 |
| qc_event_issues_v1 | 6 / 3 |
| us_llm_absorption_deferred_multi_nodule_v1 | 4 / 3 |
| __conventions | 5 / 4 |
| cohort_view_duplicate_review_v1 | 5 / 4 |
| max_stimulated_tg_backfill_v1 | 5 / 4 |
| recurrence_path_proven_candidates_v1 | 5 / 4 |
| tier2_completeness_v1 | 4 / 4 |
| cpm_is_malignant_flag_review_v1 | 9 / 5 |
| nlp_rollup_promotion_audit_v1 | 6 / 5 |
| qc_violations_v1 | 7 / 5 |
| recurrence_imaging_suspicious_candidates_v1 | 6 / 5 |
| us_llm_absorption_mapping_v1 | 5 / 5 |
| ... |

Total: ~15-25 tables, ~50-100 not_started cols. Pick the ones with ≤ 5 not_started cols for this batch (target ~15 tables / ~50 cols).

---

## 2. Methodology — workspace-helper batch verification

These are NOT primary canonicals. They're audit/queue/backfill artifacts used in the cleanup process. Verification should be **pragmatic and category-driven**.

### 2a. Categorize each table by purpose
Inspect `notes` column on the registry row + table contents:
- **Adjudication queue** (cpm_*_adjudication_queue): per-pt review queue; verify natural key + status enum
- **Adjudication log** (cpm_*_adjudication_log): append-only review history; verify timestamps monotonic
- **Backfill** (*_backfill_v1): one-off correction tables; verify row count + value plausibility
- **Audit/QC** (qc_*, *_review_v1, verification_low_*): sample-based verification artifacts
- **Mapping** (us_llm_absorption_mapping_v1): relational mapping; verify referential integrity
- **Special** (__conventions, __readme, tier2_completeness_v1): meta tables; verify content matches their purpose

### 2b. Per-col verification (lightweight)
For each not_started col, pick the appropriate method:
- IDs (research_id, episode_id, etc.) → `na_identifier_skip` if the table is just keyed on these
- Status enum cols (resolved, pending, in_progress) → vocab cleanliness check + `enum_vocabulary_validation`
- Free-text notes → `presence_check` (non-null where expected) + sample inspection
- Counts (n_*) → math integrity (sum/count plausibility)
- Source pointers (table_name, mention_id) → referential integrity vs main.canonical_*
- Timestamps → existence + monotonicity (created < updated)

### 2c. Batch-strategy
Don't try to verify EVERY tier3_helper in one lane — that scales poorly. Pick a subset (~15 tables) and document explicitly which are deferred. Target: bring `not_started` count for tier3_helper down by ~50 cols.

### 2d. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_manuscript_workspace_helper_batch_signoff.sql`
- One UPDATE block per category (with category in the verification_method label)
- Multiple table_status updates
- Document at the end: which tables were closed, which were deferred + why

---

## 3. Acceptance gates

- ~15 tables flipped to verified (or document why fewer)
- All flipped tables: 0 not_started, 0 failed
- Per-col verification_method labels reflect category (e.g., `enum_vocabulary_validation`, `presence_check_sample_inspected`, `referential_integrity_vs_canonical_<X>`)
- 5-gate audit re-run: gate 1 increases by ~15; gate 5 unchanged (tier3_helper tables don't match `canonical_%` filter — they're in workspace schema)

---

## 4. Don't touch (active parallel lanes)

- `canonical_recurrence_v1` Script 203 rebuild — Cursor lane 19 (paused)
- 5 tier3_extraction tables — Sibling lane 20
- `canonical_patient_master` operative-cluster slice — Sibling lane 22

---

## 5. Reference reading

Required:
- Auto-memory: `project_cleanliness_audit_2026-04-29.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Auto-memory: `project_op_procedure_codes_mig_118_closeout.md` (hybrid pattern)
- Repo: `qc_framework_v1/migrations/109_verified_tables_cleanliness_audit_20260429.sql` (raw-mirror-exempt template)
- Repo: `qc_framework_v1/migrations/126_meta_registries_pair_signoff_20260429.sql` (Cursor 18's meta-consistency template — closely analogous)

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing the batch
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Surgical git add

---

## 7. If something unexpected surfaces

- A "helper" table contains real clinical data (not just metadata) → reclassify as canonical_-tier candidate; STOP and ask Logan
- A queue table has no resolution mechanism (all rows pending forever) → flag for follow-up but verify the schema
- `__readme` / `__conventions` admin tables — sample the content; mark ALL cols as `meta_documentation_table` if appropriate
- Strong cross-table dependencies (e.g., backfill table requires a master rebuild) → CF, do not block sign-off

---

End of prompt. Lane 21 of new 3-prompt batch. Closes ~15 tier3_helper tables in manuscript_workspace. After this lands, the manuscript_workspace footprint is meaningfully cleaner.
