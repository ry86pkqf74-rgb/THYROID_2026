# Cursor Agent Task — 5 tier3_extraction tables `error` col verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post mig_127 audit refinement)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting (current tip after Cursor 17/18 mig_125/126 + Cowork mig_127)
**Estimated effort:** 30-45 minutes (5 tables × 1 col each = 5 col flips)
**Run order:** Lane 20 of next 3-prompt batch (run first — small, builds pattern for raw-mirror-exempt extension)

---

## 1. Goal

Close the **last 5 tier3_extraction note_entities_llm_* tables** under Protocol v2.

| Table | n_columns_total | not_started | The 1 col |
|---|---:|---:|---|
| note_entities_llm_airway_invasion_v2 | 14 | 1 | `error` (INTEGER) |
| note_entities_llm_ete_subgrade_v1 | 14 | 1 | `error` (INTEGER) |
| note_entities_llm_parathyroid_detail_v1 | 14 | 1 | `error` (INTEGER) |
| note_entities_llm_t4b_invasion_v1 | 14 | 1 | `error` (INTEGER) |
| note_entities_llm_vascular_invasion_v2 | 14 | 1 | `error` (INTEGER) |

These are raw LLM-output mirrors (similar to the 10 tier3_extraction tables that mig_109 already raw-mirror-exempted). They're missing the same `error` col flip — almost certainly `error=0` indicates clean extraction; non-zero indicates failure.

---

## 2. Methodology — raw-mirror-exempt extension

Pattern reference: `qc_framework_v1/migrations/109_verified_tables_cleanliness_audit_20260429.sql` (raw-mirror-exempt tagging for 10 tier3_extraction mirrors).

### 2a. Probe the `error` col on each
```sql
SELECT 
  'airway_invasion_v2' AS tbl, COUNT(*) AS rows,
  COUNT(*) FILTER (WHERE error = 0) AS error_zero,
  COUNT(*) FILTER (WHERE error != 0) AS error_nonzero,
  COUNT(*) FILTER (WHERE error IS NULL) AS error_null,
  STRING_AGG(DISTINCT CAST(error AS VARCHAR), ',' ORDER BY CAST(error AS VARCHAR)) AS distinct_vals
FROM main.note_entities_llm_airway_invasion_v2
UNION ALL
SELECT 'ete_subgrade_v1', COUNT(*), COUNT(*) FILTER (WHERE error = 0), 
  COUNT(*) FILTER (WHERE error != 0), COUNT(*) FILTER (WHERE error IS NULL),
  STRING_AGG(DISTINCT CAST(error AS VARCHAR), ',')
FROM main.note_entities_llm_ete_subgrade_v1
UNION ALL
SELECT 'parathyroid_detail_v1', COUNT(*), COUNT(*) FILTER (WHERE error = 0),
  COUNT(*) FILTER (WHERE error != 0), COUNT(*) FILTER (WHERE error IS NULL),
  STRING_AGG(DISTINCT CAST(error AS VARCHAR), ',')
FROM main.note_entities_llm_parathyroid_detail_v1
UNION ALL
SELECT 't4b_invasion_v1', COUNT(*), COUNT(*) FILTER (WHERE error = 0),
  COUNT(*) FILTER (WHERE error != 0), COUNT(*) FILTER (WHERE error IS NULL),
  STRING_AGG(DISTINCT CAST(error AS VARCHAR), ',')
FROM main.note_entities_llm_t4b_invasion_v1
UNION ALL
SELECT 'vascular_invasion_v2', COUNT(*), COUNT(*) FILTER (WHERE error = 0),
  COUNT(*) FILTER (WHERE error != 0), COUNT(*) FILTER (WHERE error IS NULL),
  STRING_AGG(DISTINCT CAST(error AS VARCHAR), ',')
FROM main.note_entities_llm_vascular_invasion_v2;
```

### 2b. Verification disposition
The `error` col is the LLM extractor's success/fail signal. Per the established raw-mirror-exempt pattern (mig_109), per-row verification is NOT required — these are raw LLM output mirrors whose content is already QC-checked at extraction time.

For each of the 5 tables, the `error` col can be flipped to `verified` via `verification_method='raw_llm_mirror_error_distribution_audit'` with notes documenting the error distribution per probe.

### 2c. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_tier3_extraction_error_col_batch_signoff.sql`
- 5 col flips (one per table)
- 5 table_status updates: not_started → verified
- Single migration covering all 5

Note: the table-level signoff_migration column on each of the 5 will follow the mig_109 raw-mirror-exempt pattern (signoff_migration=`'qc_framework_v1/migrations/<N>_tier3_extraction_error_col_batch_signoff.sql'`).

---

## 3. Acceptance gates

- All 5 not_started cols flipped to verified
- 5 table_status updates: not_started → verified (n_verified+n_na = n_columns_total, 0 not_started, 0 failed)
- Error distribution documented per table in close-out
- 5-gate audit re-run: gate 1 increases by 5 (66 → 71); gate 5 unchanged (these are tier3 tables that the audit query filters by `LIKE 'canonical_%'` — they don't appear in gate 5 anyway, but verify)

---

## 4. Don't touch (active parallel lanes)

- `canonical_recurrence_v1` Script 203 rebuild — Cursor lane 19 (paused at Logan-approval gate, may resume)
- `canonical_patient_master` operative-cluster slice — Sibling lane 22
- `manuscript_workspace.*` tier3_helper batch — Sibling lane 21

---

## 5. Reference reading

Required:
- Auto-memory: `project_cleanliness_audit_2026-04-29.md` (mig_109 5-gate + raw-mirror-exempt pattern)
- Auto-memory: `feedback_audit_regex_word_boundary.md` (mig_117 + 127 audit refinements)
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Repo: `qc_framework_v1/migrations/109_verified_tables_cleanliness_audit_20260429.sql` (raw-mirror-exempt template)
- Repo: `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` (most recent audit refinement)

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing all 5 tier3_extraction tables
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Surgical git add (no `-A`, no `scripts/output/` sweeps)

---

## 7. If something unexpected surfaces

- `error` col has values other than {0, NULL, small_int} → indicates extraction failures; document distribution
- Some tables have `error_nonzero > 5%` → consider whether to flag (not blocking; raw mirror)
- Table missing the `error` col entirely → information_schema quick check; rare but possible if extractor schema evolved
- `error_null` substantial → check if NULL means unknown vs success

---

End of prompt. Lane 20 of new 3-prompt batch. Closes the last 5 raw LLM-output mirrors. After this lands, all 15 tier3_extraction note_entities_llm_* tables are raw-mirror-exempt verified.
