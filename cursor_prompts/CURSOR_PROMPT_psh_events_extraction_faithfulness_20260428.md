# Cursor Agent Task — `canonical_psh_events_v1` Verification (Protocol v2)

**Generated:** 2026-04-28 (Cowork session, post-mig_102)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — re-pull before starting (Cowork is also pushing).
**Estimated effort:** 30-60 minutes, mechanical, autonomous (no Logan touchpoints expected)
**Run order:** Run after `cursor_prompt_medications_classifier_20260428.md` (currently in flight) finishes.

---

## 1. Goal

Verify `canonical_psh_events_v1` (Past Surgical History) under Protocol v2 using the **extraction-faithfulness pattern** established in mig_102.

---

## 2. Why this is short

Probed 2026-04-28: `canonical_psh_events_v1` is a clean single-source LLM extraction.
- 3,919 rows / 1,878 patients
- 100% of rows come from `note_entities_llm_past_surgical_hx` (no legacy or synthetic sources to handle)
- Build is presumed deterministic SELECT * + json_extract_string from the upstream's parsed_json (verify by reading the build SQL)

Compare to mig_102 / canonical_parathyroid_events_v1 closed today: same shape, same approach, finished in ~20 min. PSH should be even quicker because it has ~half the rows.

---

## 3. Don't touch (active parallel lanes)

- `canonical_pmh_events_v1` — separate Cursor lane (sibling prompt: `cursor_prompt_pmh_events_3_source_verification_20260428.md`)
- `canonical_parathyroid_patient_rollup_v1` — Cowork's lane
- `canonical_pathology_clinical_events_v1` / `canonical_cervical_ln_clinical_events_v1` — Cowork's queue
- Any table touched by an active medications-classifier run

---

## 4. Reference reading (Cowork auto-memory)

Before you start, read these in `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/`:

- **`feedback_extraction_faithfulness_llm_canonical.md`** — full pattern spec
- **`project_parathyroid_events_mig_102_closeout.md`** — most recent close-out using the pattern
- `feedback_motherduck_direct_check.md` — re-query MD before recommending
- `feedback_surgical_git_add.md` — staging convention
- `feedback_phi_safety.md` — never print clinical notes content
- `reference_protocol_v2_md_accounts.md` — `.eras` MD account hosts the publication DB

In the repo:
- `qc_framework_v1/migrations/102_parathyroid_events_table_signoff.sql` — copy-paste structural template
- `qc_framework_v1/migrations/59_parathyroid_detail_canonical_tier2_v1.sql` — example of an LLM-extraction build SQL (parathyroid case)

---

## 5. Schema reference (probed 2026-04-28)

```
research_id            VARCHAR
source_table           VARCHAR  ← all rows = note_entities_llm_past_surgical_hx
source_row_id          VARCHAR
source_note_type       VARCHAR
llm_confidence         DOUBLE
extractor_name         VARCHAR
finding_text           VARCHAR
finding_value          VARCHAR
finding_value_norm     VARCHAR
finding_date           DATE
mention_note_date      DATE
finding_status         VARCHAR
evidence_strength      VARCHAR
days_from_first_thyroidectomy  BIGINT
is_preexisting         BOOLEAN
anchor_source          VARCHAR
med_status             VARCHAR
evidence_span_hash     VARCHAR
build_ts               TIMESTAMP
```

Registry status (re-confirm before starting): expect ~15 not_started cols, ~4 already-na cols.

---

## 6. Methodology — copy-paste from mig_102

### 6a. Locate the build SQL
Find the build SQL that wrote `canonical_psh_events_v1`. Search:
```bash
grep -rn "canonical_psh_events_v1" /Users/ros/THyroid\ 2026/qc_framework_v1 /Users/ros/THyroid\ 2026/scripts | head
```
Verify the build is `SELECT *` + `json_extract_string` from `note_entities_llm_past_surgical_hx`. If not (e.g., extra normalization, post-build UPDATEs), STOP and ask Logan — verification methodology may need adapting.

### 6b. Probe natural key + row counts
```sql
SELECT
  (SELECT COUNT(*) FROM main.canonical_psh_events_v1) AS canonical_n,
  (SELECT COUNT(*) FROM main.note_entities_llm_past_surgical_hx WHERE error=0) AS upstream_err0_n;
```
Should be 3,919 = 3,919.

Find the natural-key col: probably `source_row_id` (= `note_row_id` in upstream). Verify uniqueness on both sides via COUNT vs COUNT DISTINCT.

### 6c. Mass-equivalence per col
Build a single SUM(CASE WHEN c.<col> IS DISTINCT FROM f.<col> THEN 1 ELSE 0 END) probe across all not_started cols joined on natural key. Pattern from mig_102:

```sql
WITH fresh AS (
  SELECT
    note_row_id AS source_row_id,
    json_extract_string(parsed_json, '$.<key1>') AS f_<col1>,
    TRY_CAST(json_extract_string(parsed_json, '$.<key2>') AS <type>) AS f_<col2>,
    ...
  FROM main.note_entities_llm_past_surgical_hx WHERE error = 0
)
SELECT
  COUNT(*) AS n_joined,
  SUM(CASE WHEN c.<col1> IS DISTINCT FROM f.f_<col1> THEN 1 ELSE 0 END) AS d_<col1>,
  ...
FROM main.canonical_psh_events_v1 c
JOIN fresh f USING (source_row_id);
```

Adjusting for the actual JSON keys + col types from build SQL.

### 6d. Cross-validation — opportunity for extra CFs
The verified `canonical_operative_events_v1` (signed off mig_90) covers the same patients' surgical events. If the LLM extracted PSH mentions of prior thyroidectomy that contradict the operative_events record (e.g., LLM says "no prior thyroid surgery" but operative_events records a 2018 lobectomy for the same patient), surface as CF-PSH-OP-DRIFT.

Pattern (rough sketch):
```sql
-- pts where operative_events records a thyroid surgery date but PSH says no_prior_thyroid_surgery
WITH op_pts AS (
  SELECT DISTINCT research_id FROM main.canonical_operative_events_v1
  WHERE LOWER(COALESCE(procedure_text,'')) LIKE '%thyroid%' AND surgery_date_native IS NOT NULL
),
psh_no AS (
  SELECT DISTINCT research_id FROM main.canonical_psh_events_v1
  WHERE LOWER(COALESCE(finding_value_norm,'')) LIKE '%no_prior_thyroid%'
    OR (LOWER(COALESCE(finding_value_norm,'')) LIKE '%thyroid%' AND finding_status='absent')
)
SELECT COUNT(*) FROM op_pts JOIN psh_no USING (research_id);
```
This is just a starter — adapt to the actual finding_value_norm vocab.

### 6e. Sign-off migration
Numbered `qc_framework_v1/migrations/<N>_psh_events_table_signoff.sql` using the next available migration number. Follow the 3-section structure of mig_102:
- Section a: flip cleanly-matching cols via `verification_method='extraction_faithfulness_vs_upstream_json'`
- Section b: flip any drifted cols via specific tag (e.g., `extraction_faithfulness_with_<reason>`) with a specific note
- Section c: recompute `canonical_table_signoff_registry_v1` counts + sign off

---

## 7. Acceptance gates

- 15 not_started cols flipped to verified or na
- table_status='verified' in `canonical_table_signoff_registry_v1`
- Notes on each verified col include the verification method, upstream table, row count match
- Notes on table-level signoff include CF list (if any)

---

## 8. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Surgical `git add` by explicit path (memory: `feedback_surgical_git_add.md`)
- Commit message: title + per-col summary + carry-forward list (if any)
- Push to `origin/main`
- DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ — use `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for any new timestamp values (memory: `reference_duckdb_timestamp_tz.md`)

---

## 9. If something unexpected surfaces

- Build SQL is NOT a clean json_extract_string SELECT * → STOP, this prompt's methodology won't work. Document what you found and ask Logan.
- Mass-equivalence shows widespread drift (>1% on multiple cols) → STOP, investigate. Could indicate post-build UPDATEs, build_ts re-runs, etc.
- Cross-validation against operative_events surfaces large CF-PSH-OP-DRIFT → document the count + sample 5-10 rows for Logan adjudication; do NOT block sign-off (this is an LLM-quality CF, similar to CF-102-HYPOPT-MISS).

---

End of prompt. Update Cowork `MEMORY.md` with a one-line index entry referencing the new mig close-out memory you create.
