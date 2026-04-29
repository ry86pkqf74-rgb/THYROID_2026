# Cursor Agent Task — `canonical_molecular_genetics_from_notes_v2` Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_118)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting (current tip `976cf8f` after mig_118)
**Estimated effort:** 45-60 minutes (28 cols, mention-grain LLM extraction)
**Run order:** Lane 16 of 3-prompt batch (run last — closes molecular family fully)

---

## 1. Goal

Verify `canonical_molecular_genetics_from_notes_v2` under Protocol v2.

| Stat | Value |
|---|---|
| Rows | 1,738 |
| Patients | 605 |
| Cols total | 28 |
| not_started | 17 |
| na | 11 |

This is the **LLM-from-notes** path of the molecular genetics family. The master `canonical_molecular_genetics_v2` (1,384 / 1,151 — closed by Cursor 9 mig_116 on 2026-04-28) merges this LLM-from-notes path with the structured-feed path.

Closing this table completes the molecular family verification fully.

---

## 2. Schema preview

| Col | Type | Category | Disposition |
|---|---|---|---|
| research_id | VARCHAR | na | already na |
| note_row_id | VARCHAR | na | already na |
| note_date | VARCHAR | na | already na ⚠️ check format vs reality |
| note_type | VARCHAR | na | already na |
| entity_type | VARCHAR | adjudicated | not_started |
| entity_value_raw | VARCHAR | source | not_started |
| entity_value_norm | VARCHAR | adjudicated | not_started |
| present_or_negated | VARCHAR | adjudicated | not_started |
| confidence | DOUBLE | adjudicated | not_started |
| confidence_score | DOUBLE | derived | not_started |
| evidence_span | VARCHAR | source | not_started |
| evidence_start | BIGINT | source | not_started |
| evidence_end | BIGINT | source | not_started |
| extraction_method | VARCHAR | adjudicated | not_started |
| extractor_name | VARCHAR | adjudicated | not_started |
| extractor_version | VARCHAR | adjudicated | not_started |
| llm_model | VARCHAR | na | already na |
| llm_provider | VARCHAR | na | already na |
| llm_prompt_version | VARCHAR | adjudicated | not_started |
| extraction_run_id | VARCHAR | na | already na |
| extracted_at | VARCHAR | na | already na |
| verification_status | VARCHAR | derived | not_started |
| verification_step | VARCHAR | adjudicated | not_started |
| entity_domain | VARCHAR | na | already na |
| source_episode_id | VARCHAR | na | already na |
| linked_test_episode_id | VARCHAR | na | already na |
| built_at | TIMESTAMP WITH TIME ZONE | adjudicated | not_started ⚠️ TZ |
| builder_version | VARCHAR | adjudicated | not_started |

⚠️ **`built_at` is TIMESTAMP WITH TIME ZONE** — per `reference_duckdb_timestamp_tz.md`, all build_ts cols should be plain TIMESTAMP. Flag CF-mig<N>-MGFN-BUILT-AT-TZ-RETYPE for future cleanup, OR allowlist as provenance with TZ note. The audit allowlist already covers `built_at` (added by mig_117) so this won't fail gate 5; flag as a data-type cleanliness CF.

---

## 3. Methodology — extraction-faithfulness vs upstream LLM mirror

Pattern reference: `qc_framework_v1/migrations/110_pathology_clinical_events_table_signoff.sql` (UNNEST extraction-faithfulness) AND mig_116 (Cursor 9's molecular_v2 close-out — same family).

### 3a. Locate build SQL + upstream
```bash
grep -rn "canonical_molecular_genetics_from_notes_v2" scripts qc_framework_v1 | head -20
```
Likely a Script in the 380s+ range that promotes `note_entities_llm_molecular_genetics` raw mirror into the canonical layer. Read the SQL.

Upstream is almost certainly `main.note_entities_llm_molecular_genetics` (raw LLM mirror, exempt from per-col verification per mig_109 raw-mirror tagging).

### 3b. Probe natural key
Likely `(research_id, note_row_id, evidence_start, entity_value_raw)` — the same partition-key probe pattern as mig_362 / mig_118. Test:
```sql
SELECT COUNT(*), COUNT(DISTINCT (research_id, note_row_id, evidence_start, entity_value_raw))
FROM main.canonical_molecular_genetics_from_notes_v2;
-- Both should be 1,738
```

### 3c. Extraction-faithfulness probe
For each not_started source/derived col, EXCEPT ALL multiset compare canonical vs upstream filtered:
```sql
WITH up AS (
  SELECT research_id, note_row_id, entity_type, entity_value_raw, entity_value_norm,
         present_or_negated, confidence, evidence_span, evidence_start, evidence_end,
         extraction_method, extractor_name, extractor_version
  FROM main.note_entities_llm_molecular_genetics
  -- Filter: same predicate as Script 38X build (likely WHERE error=0 OR present_or_negated IS NOT NULL)
),
ca AS (
  SELECT research_id, note_row_id, entity_type, entity_value_raw, entity_value_norm,
         present_or_negated, confidence, evidence_span, evidence_start, evidence_end,
         extraction_method, extractor_name, extractor_version
  FROM main.canonical_molecular_genetics_from_notes_v2
)
SELECT
  (SELECT COUNT(*) FROM (SELECT * FROM up EXCEPT ALL SELECT * FROM ca)) AS up_minus_ca,
  (SELECT COUNT(*) FROM (SELECT * FROM ca EXCEPT ALL SELECT * FROM up)) AS ca_minus_up;
```
Expected: 0 drift in both directions if extraction-faithful.

### 3d. Cross-validation against verified molecular_genetics_v2 (Cursor 9, mig_116)
Cursor 9 closed `canonical_molecular_genetics_v2` (the master). Some `canonical_molecular_genetics_from_notes_v2` rows feed into the master. Verify the merge worked:
```sql
-- How many from_notes rows participated in the master build?
SELECT 
  COUNT(DISTINCT fn.research_id) AS from_notes_pts,
  COUNT(DISTINCT fn.research_id) FILTER (WHERE master.research_id IS NOT NULL) AS pts_in_master
FROM main.canonical_molecular_genetics_from_notes_v2 fn
LEFT JOIN main.canonical_molecular_genetics_v2 master USING (research_id);
-- 605 / 605 expected if every from_notes patient is also in master
```
Drift here may indicate filter mismatch in the master build — flag as CF-mig<N>-MOLECULAR-FROM-NOTES-MERGE-DRIFT.

### 3e. Vocab + derived-col checks
- `entity_type` vocab clean (probably {gene, mutation, variant, fusion, copy_number, ...})
- `present_or_negated` enum: {present, negated, ambiguous, ...}
- `confidence` distribution sane (0.0-1.0; check for clustering at exactly 0.9 like mig_118)
- `confidence_score` should equal `confidence` (probably duplicate col) — verify
- `verification_status` should be {extraction_only, manual_review, ...} — single canonical value likely

### 3f. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_molecular_genetics_from_notes_v2_signoff.sql`
- 17 col flips: 12-13 via `verification_method='extraction_faithfulness_vs_note_entities_llm_molecular_genetics'`
- Maybe 2-3 via `verification_method='build_provenance_consistency'` for built_at, builder_version, extractor_*
- 11 already-na carry over
- table_status update

---

## 4. Acceptance gates

- All 17 not_started cols flipped (or fewer if methodology surfaces additional na candidates)
- 0 drift on extraction-faithfulness for source/adjudicated cluster
- Cross-validation against master molecular_genetics_v2 surfaces ≤ 5 patient drift
- vocab clean: entity_type, present_or_negated, verification_status enums all in expected set
- Patient count: 605 patients (a subset of 1,151 master pts — only those with note-extraction findings)

---

## 5. Don't touch (active parallel lanes)

- `canonical_path_malignant_patient_rollup_v1` / `canonical_path_benign_patient_rollup_v1` — Cursor lane 12
- `canonical_ete_event_resolved_v1` / `canonical_ete_inline_adjudication_v1` — Cursor lane 13
- `canonical_recurrence_v1` — Sibling Cursor lane 14
- `canonical_survival_followup_v1` — Sibling Cursor lane 15
- `canonical_molecular_genetics_v2` — Already verified by Cursor 9 mig_116 (don't re-verify; just cross-validate against)

---

## 6. Reference reading

Required:
- Auto-memory: `project_molecular_v2_schema.md` (master + from_notes architecture)
- Auto-memory: `project_op_procedure_codes_mig_118_closeout.md` (extraction-faithfulness pattern reference, mention-grain)
- Auto-memory: `project_round2_llm_integration_script_386_closeout.md` (LLM canonical layer context)
- Auto-memory: `feedback_extraction_faithfulness_llm_canonical.md`
- Auto-memory: `reference_duckdb_timestamp_tz.md` (TIMESTAMP WITH TIME ZONE warning for built_at)
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Repo: `qc_framework_v1/migrations/116_molecular_genetics_v2_signoff.sql` (sibling close-out — read this for cross-validation patterns)
- Repo: `qc_framework_v1/migrations/110_pathology_clinical_events_table_signoff.sql` (extraction-faithfulness UNNEST template)
- Repo: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (mention-grain hybrid pattern)

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing molecular_genetics_from_notes_v2
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Surgical git add

---

## 8. If something unexpected surfaces

- Patient count != 605 → cohort drift; reconcile against master molecular_genetics_v2
- `confidence` uniformly 0.9 (like mig_118) → likely upstream LLM extractor default; OK
- `confidence_score` differs from `confidence` → unexpected; investigate (maybe `confidence_score` is recalibrated)
- Filter predicate in upstream filter unclear (`WHERE error=0` vs `WHERE present_or_negated IS NOT NULL`) → probe both, report drift
- `built_at` TIMESTAMP WITH TIME ZONE → file CF-mig<N>-MGFN-BUILT-AT-TZ-RETYPE; do not block sign-off (allowlist already covers it)
- Drift > 5 between from_notes and master → check whether master filtered out some from_notes rows (e.g., low-confidence threshold); document

---

End of prompt. Lane 16 of new 3-prompt batch. Closes the molecular family fully (master mig_116 + from_notes mig_<N>). Update `MEMORY.md` with close-out entry — this is the second molecular family table and likely the last for the v1.0 publication scope.
