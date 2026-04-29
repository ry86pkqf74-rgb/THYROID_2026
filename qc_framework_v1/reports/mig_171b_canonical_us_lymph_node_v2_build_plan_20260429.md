# mig_171b — canonical US lymph-node v2 BUILD SQL + verification plan

**Date:** 2026-04-29
**Batch:** `mig_171b_canonical_us_lymph_node_v2_build_20260429`
**Upstream design commit:** `f6d0313`
**Posture:** SQL authoring + read-only verification preview only. **No MotherDuck data writes were executed.**
**Ratification status:** `PENDING_RATIFICATION_BEFORE_DATA_WRITE`

## Executive summary

This pass converts the mig_171 design/skeleton into a full ratification-ready SQL build artifact for:

1. `main.canonical_us_lymph_node_events_v2` — one row per US lymph-node observation per ultrasound exam.
2. `main.canonical_us_lymph_node_patient_rollup_v2` — one row per canonical patient, with event rollups plus pathology-derived `tp_*` bridge fields.
3. `main.val_mig171b_canonical_us_ln_build_v1` — post-apply validation gate table.

The SQL artifact is intentionally **not executed**. It is ready for Logan/Cowork review and ratification before any data write.

## Artifact authored

| artifact | purpose |
|---|---|
| `qc_framework_v1/migrations/171b_canonical_us_lymph_node_v2_build_20260429.sql` | Full draft build SQL: source shell archive, events build, patient rollup build, validation table, post-apply probes. |

## Source inputs and write boundaries

### Sources used by the build SQL

All sources are in `thyroid_canonical_publication_v1_0.main`:

| source | role |
|---|---|
| `canonical_us_lymph_node_v2` | Existing dated shell / US exam spine; retained as source, not destructively replaced. |
| `canonical_us_exam_master_VIEW_v2` | Exam ID reuse source for `(research_id, exam_date)`. |
| `clinical_note_ln_extracted_v1` | Detail source for US-specific imaging LN observations after modality + date gates. |
| `canonical_path_malignant_events_v1` | Pathology-derived patient bridge for the nine `tp_*` columns. |
| `canonical_patient_master` | Cohort spine only for rollup row parity; not updated. |

### Explicitly not used

- No raw `clinical_notes_long.note_text` selection.
- No archive/legacy schema as source.
- No `canonical_patient_master` updates.
- No destructive replacement of the existing `main.canonical_us_lymph_node_v2` shell.

## Read-only authoring probes run

### Pull status

`git pull --ff-only origin main` completed and returned **Already up to date**. There are pre-existing unrelated local/untracked files in the workspace; they were not staged or modified.

### Clinical-note US candidate gate

Read-only profiling of `clinical_note_ln_extracted_v1` found:

| metric | value |
|---|---:|
| Imaging extraction rows (`ok`, `evidence_source_modality='imaging'`) | 592 |
| Imaging extraction patients | 326 |
| Rows with explicit US/ultrasound/sonogram language | 196 |
| Patients with explicit US/ultrasound/sonogram language | 137 |
| Explicit-US rows with parseable date after MM/DD/YYYY handling | 172 |

The draft SQL therefore only ingests clinical-note detail rows that pass all three gates:

1. `extraction_status='ok'`
2. `evidence_source_modality='imaging'`
3. US-specific language in evidence/entity/note-type text plus parseable `exam_date`

### Events CTE dry-run preview

The events CTE was executed as a **read-only SELECT** by substituting the terminal `CREATE TABLE AS` with an aggregate query. No table was created or replaced.

| metric | value |
|---|---:|
| Proposed event rows | 6,973 |
| Distinct `ln_event_id` | 6,973 |
| Distinct patients | 4,110 |
| Clinical US detail rows | 172 |
| Legacy shell rows retained | 6,801 |
| Fallback LN-only exam IDs | 159 |

Interpretation: the build preserves all 6,801 dated legacy shell rows and adds 172 date-valid US-specific clinical-note detail rows. 159 events require fallback `us_exam_id` values and must be resolved by the downstream exam-master rebuild/verification gate.

## Build design decisions encoded in SQL

### Event ID and exam ID recipe

`us_exam_id` is derived as:

1. Reuse exactly one `canonical_us_exam_master_VIEW_v2.us_exam_id` for matching `(CAST(research_id AS VARCHAR), exam_date)`.
2. Else fallback to `md5('US_EXAM_V2|' || research_id || '|' || exam_date)`.

`ln_event_id` is:

`md5('US_LN_EVENT_V2|' || research_id || '|' || us_exam_id || '|' || ln_index/textual || '|' || side || '|' || neck_level || '|' || source_table || '|' || source_row_id)`

This locks the hybrid reuse/fallback recipe requested by mig_171 and avoids the old non-portable direct `us_exam_id` behavior.

### PHI safety

`evidence_text` is snippet-limited via `LEFT(REGEXP_REPLACE(...), 240)`. The SQL never selects `clinical_notes_long.note_text`.

### Patient rollup grain

The rollup is CPM-wide, so expected row count is:

- rows = 10,871
- distinct `research_id` = 10,871

US LN rollup fields are derived from `canonical_us_lymph_node_events_v2`. The nine `tp_*` bridge fields are derived from `canonical_path_malignant_events_v1`, not from US imaging alone, because the CF-mig150 `tp_*` columns are pathology/LN burden concepts rather than purely US imaging concepts.

## Post-apply validation plan

The SQL creates `main.val_mig171b_canonical_us_ln_build_v1` with these gates:

| gate | requirement | failure implication |
|---|---|---|
| `G1_event_id_unique` | `ln_event_id` unique | Build grain collision. |
| `G2_event_exam_date_nonnull` | 0 null `exam_date` | Invalid clinical date handling. |
| `G3_source_modality_us_only` | Only `US` modality | Non-US modality leakage. |
| `G4_evidence_snippet_limited` | max evidence length ≤ 240 | PHI/full-note leakage risk. |
| `G5_rollup_row_count` | 10,871 rows / 10,871 distinct rids | Patient-spine violation. |
| `G6_rollup_has_findings_bidirectional` | `has_us_ln_findings` equals event existence | Rollup boolean drift. |
| `G7_rollup_event_counts_match` | Rollup event/suspicious counts match events | Aggregation bug. |
| `G8_events_resolve_existing_exam_master` | all reused IDs resolve to exam master | Bad exam ID reuse. |
| `G9_fallback_exam_ids_pending_rebuild` | WARN if fallback IDs exist | Downstream exam-master rebuild required. |
| `G10_pm_anti_join_rollup` | every CPM patient appears in rollup | Cohort coverage bug. |

Expected post-apply status is all PASS except `G9`, which may WARN until the exam-master rebuild accepts fallback LN-only exam IDs.

## Ratification checklist before data write

Before executing the SQL against MotherDuck, ratify:

1. The existing `canonical_us_lymph_node_v2` shell should remain as source/compatibility table; new build lands in `canonical_us_lymph_node_events_v2` and patient rollup.
2. US-specific gate for clinical-note rows is acceptable: explicit `ultrasound|sonogram|sonographic` language required.
3. `tp_*` bridge fields belong in this patient rollup as pathology-derived bridge fields, with no CPM update in mig_171b.
4. Fallback exam IDs are acceptable only with a follow-up exam-master rebuild/verification gate.
5. Snippet length cap of 240 chars is acceptable for audit evidence.
6. Validation table WARN on fallback exam IDs is acceptable pre-exam-master rebuild.

## Carry-forwards

| CF | status | note |
|---|---|---|
| `CF-mig171b-EXAM-MASTER-REBUILD` | open | 159 fallback IDs in read-only preview require downstream exam-master rebuild/verification. |
| `CF-mig171b-RAW-JSON-REPLAY-DEFERRED` | open | `note_entities_llm_cervical_ln_detail` remains replay/audit source; not directly parsed in this SQL. |
| `CF-mig150-TP-UPSTREAM-NOT-IN-MAIN` | partially addressed | Upstream bridge source is authored in rollup SQL; CPM writes require separate ratification. |

## Out-of-scope

This pass did not:

- execute any MotherDuck DDL/DML;
- register new tables in canonical registries;
- update `canonical_patient_master`;
- rebuild `canonical_us_exam_master_VIEW_v2`;
- parse raw note text;
- stage, commit, or push unrelated workspace changes.
