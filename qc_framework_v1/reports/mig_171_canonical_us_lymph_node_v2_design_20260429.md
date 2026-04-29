# mig_171 — canonical_us_lymph_node_v2 BUILD design + skeleton

**Date:** 2026-04-29  
**Batch:** `mig_171_canonical_us_lymph_node_v2_build_20260429`  
**Lane:** 60 / mig_171  
**Posture:** read-only MotherDuck profiling + repository-only design artifacts. No database writes, no INSERTs, no registry writes, no CPM updates.  
**Ratification status:** `CF-mig171-DESIGN-RATIFICATION-PENDING` — Logan/Cowork ratification required before mig_171b applies anything.

## Executive summary

This lane confirms that the current `main.canonical_us_lymph_node_v2` is a shell-like US lymph-node table: 6,801 rows / 4,077 patients, but 6,793 rows are marked `nlp_backfill_pending`, only 8 rows have `suspicious_flag=TRUE`, and zero rows have populated laterality, neck level, or size fields. The required design therefore keeps the US lymph-node event grain explicit and defers all data population to mig_171b.

Key design decisions for ratification:

1. **Events grain:** one row per US lymph-node observation per ultrasound exam, keyed by `ln_event_id`; per-side/per-exam fallback is allowed only when the upstream source lacks discrete node indexing.
2. **Patient rollup grain:** one row per CPM patient, with US LN rollups and explicit bridge placeholders for the nine `tp_*` CF columns.
3. **Exam ID recipe:** mig_171b must not blindly reuse the old `md5(research_id || '|' || date)` recipe. For each staged LN event, first reuse the current exam-master `us_exam_id` for the same `(research_id, exam_date)` when it exists; for LN-only exam dates, use a locked deterministic fallback and verify that the rebuilt exam master resolves every event.
4. **Source priority:** use live `main` sources only. Candidate source ranking is: existing US LN shell/date spine + `clinical_note_ln_extracted_v1` imaging rows for US details; `note_entities_llm_cervical_ln_detail` as replay/provenance source; `canonical_cervical_ln_clinical_events_v1` for cross-checks; `canonical_path_malignant_events_v1` only for pathology overlap and the `tp_*` bridge, not as a US event source.
5. **Date policy:** clinical dates are `DATE`; provenance timestamps remain `TIMESTAMP`.
6. **Research ID type:** new tables use `VARCHAR` `research_id` to align with CPM and avoid the mig_170 key dtype drift trap.

## §2 Required source inventory — observed outputs

### §2a Existing US-related tables in `main`

Observed no `canonical_us_lymph_node_v1`; this is a fresh BUILD, not a v1 refactor.

| table_name |
|---|
| `canonical_us_exam_master_VIEW_v2` |
| `canonical_us_lymph_node_v2` |
| `canonical_us_nodule_v2` |
| `canonical_us_patient_master_VIEW_v2` |
| `canonical_us_thyroid_gland_v2` |

### §2b CPM columns carrying `CF-mig150-TP-UPSTREAM-NOT-IN-MAIN`

| column_name | status |
|---|---|
| `tp_central_examined` | verified |
| `tp_central_positive_total` | verified |
| `tp_ln_central_positive` | verified |
| `tp_ln_ene` | verified |
| `tp_ln_examined` | verified |
| `tp_ln_largest_deposit_cm` | verified |
| `tp_ln_lateral_positive` | verified |
| `tp_ln_levels_involved` | verified |
| `tp_ln_positive` | verified |

All nine rows carry the mig_150 note that `tp_*` means LN examine/positive/ENE/size at primary tumor pathology grain, with lineage pending a downstream LN canonical build.

### §2c LN extraction sources in `main`

| table_name |
|---|
| `canonical_cervical_ln_clinical_events_v1` |
| `canonical_cervical_ln_clinical_patient_rollup_v1` |
| `canonical_us_lymph_node_v2` |
| `clinical_note_ln_extracted_v1` |
| `note_entities_llm_cervical_ln_detail` |

### §2d Path-malignant LN event shape

`main.canonical_path_malignant_events_v1` exposes the following LN/nodal columns:

| column_name |
|---|
| `extranodal_extension` |
| `ln_examined` |
| `ln_involved` |
| `lymphatic_invasion` |
| `nodal_disease_positive_count` |
| `nodal_disease_total_count` |

### §2e `clinical_notes_long` shape

The live metadata columns are:

| column_name |
|---|
| `excel_row_0based` |
| `ingest_script_version` |
| `ingest_sheet_spec` |
| `ingested_at_utc` |
| `note_index` |
| `note_text` |
| `note_type` |
| `research_id` |
| `source_column` |
| `source_sheet` |
| `source_workbook` |

The design lane did not select `note_text`; future mig_171b should continue PHI-safe extraction by joining through precomputed entity tables unless explicitly authorized.

## Supplemental read-only probes

### Current US LN shell profile

| n_rows | n_patients | null_exam_date | suspicious_true | neck_level_populated | laterality_populated | size_populated | nlp_backfill_pending_true |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 6,801 | 4,077 | 0 | 8 | 0 | 0 | 0 | 6,793 |

### Current US exam master profile

| n_rows | n_patients | distinct_exam_ids | null_exam_date | has_us_ln_findings_true | sum_ln_total | sum_abnormal_ln |
|---:|---:|---:|---:|---:|---:|---:|
| 11,759 | 4,360 | 11,759 | 0 | 6,801 | 6,801 | 8 |

### Candidate source coverage

| source | n_rows | n_patients |
|---|---:|---:|
| `canonical_cervical_ln_clinical_events_v1` | 4,493 | 1,643 |
| `canonical_cervical_ln_clinical_patient_rollup_v1` | 1,643 | 1,643 |
| `canonical_path_malignant_events_v1` | 6,689 | 4,137 |
| `canonical_us_lymph_node_v2` | 6,801 | 4,077 |
| `clinical_note_ln_extracted_v1` | 7,751 | 3,588 |
| `clinical_notes_long` | 11,050 | 5,593 |
| `note_entities_llm_cervical_ln_detail` | 10,084 | 5,106 |

### Clinical LN extraction status

| extraction_status | evidence_source_modality | n_rows | n_patients |
|---|---|---:|---:|
| ok | surgical_path | 3,761 | 1,242 |
| json_error | nan | 2,550 | 1,941 |
| ok | imaging | 592 | 326 |
| no_ln_found | nan | 433 | 394 |
| ok | pathology | 226 | 145 |
| ok | clinical | 184 | 137 |
| ok | ambiguous | 5 | 3 |

Only the `evidence_source_modality='imaging'` subset is eligible to populate US LN event attributes without breaking the US-only modality contract. Surgical-path/pathology/clinical rows are cross-validation or non-US lineage evidence.

### Canonical cervical LN event entity mix

| entity_type | present_or_negated | n_rows | n_patients |
|---|---|---:|---:|
| `ln_level` | present | 2,874 | 974 |
| `ln_level` | negated | 1,086 | 848 |
| `ln_size` | present | 159 | 127 |
| `fna_of_ln` | present | 90 | 82 |
| `ln_morphology` | present | 78 | 70 |
| `ln_laterality` | present | 61 | 56 |
| `ln_number_per_level` | present | 44 | 37 |
| `hilum_status` | present | 34 | 32 |
| `washout_tg` | present | 13 | 12 |
| `suspicious_features_count` | present | 12 | 12 |
| `microcalcifications_ln` | present | 12 | 12 |

### Exam-id portability probes

| probe | value |
|---|---:|
| Current USLN direct `us_exam_id` rows joining exam master | 19 / 6,801 |
| Current USLN distinct exam ids | 6,801 |
| Rows joining exam master by `(research_id, exam_date)` | 6,801 / 6,801 |
| Candidate legacy hash `md5(rid||'|'||date)` matching current exam master | 19 / 6,801 |
| Candidate hash `md5(rid||'|'||date||'|US')` matching current exam master | 0 / 6,801 |
| Exam master duplicate `(research_id, exam_date)` pairs | 0 / 11,759 |

Interpretation: direct event-level `us_exam_id` portability is currently broken, but `(research_id, exam_date)` is unique and resolves 100% of existing USLN rows to the exam master. mig_171b should lock the recipe by adopting the exam-master ID when the exam already exists and using a deterministic fallback only for LN-only dates that the exam-master view will inherit from the rebuilt LN table.

### Cohort coverage and CF burden

| metric | value |
|---|---:|
| CPM rows | 10,871 |
| CPM patients with current USLN row | 4,077 |
| Percent CPM patients with current USLN row | 37.50% |

| CF column | non-null CPM rows |
|---|---:|
| `tp_central_examined` | 3,986 |
| `tp_central_positive_total` | 3,986 |
| `tp_ln_central_positive` | 2,307 |
| `tp_ln_ene` | 1,212 |
| `tp_ln_examined` | 3,946 |
| `tp_ln_largest_deposit_cm` | 772 |
| `tp_ln_lateral_positive` | 2,307 |
| `tp_ln_levels_involved` | 3,986 |
| `tp_ln_positive` | 3,764 |

## §3 Design decisions

### 1. Events table grain

**Ratification recommendation:** one row per US lymph-node observation per ultrasound exam.

Use `ln_index` when the upstream source provides discrete nodes. For sources that only provide a side-level or exam-level statement, mig_171b may emit a single textual-only observation with `ln_index=NULL`, `side` populated when available, and `n_us_ln_events_textual_only` incremented in the rollup. This preserves an event grain without inventing node counts.

### 2. Exam ID recipe

The current table shows why the recipe must be explicitly locked: only 19 of 6,801 current USLN event IDs directly join the exam master, while all 6,801 join by `(research_id, exam_date)`.

**Proposed recipe for mig_171b:**

1. Stage all candidate US LN events with `research_id` as `VARCHAR` and `exam_date` as `DATE`.
2. For each candidate, if `canonical_us_exam_master_VIEW_v2` has exactly one row for `(CAST(research_id AS VARCHAR), exam_date)`, adopt that `us_exam_id`.
3. If no exam-master row exists, use fallback `md5('US_EXAM_V2|' || research_id || '|' || CAST(exam_date AS VARCHAR))` and verify that the rebuilt exam-master view resolves that ID after the LN table is populated.
4. Build `ln_event_id` as `md5('US_LN_EVENT_V2|' || research_id || '|' || us_exam_id || '|' || COALESCE(CAST(ln_index AS VARCHAR),'textual') || '|' || COALESCE(side,'unspecified') || '|' || COALESCE(neck_level,'unspecified') || '|' || COALESCE(source_table,'') || '|' || COALESCE(source_row_id,''))`.

This recipe avoids blindly reusing the legacy Script 364b `md5(research_id || '|' || date)` hash while preserving exam-master portability.

### 3. Source priority

| priority | source | role | rationale |
|---:|---|---|---|
| 1 | `clinical_note_ln_extracted_v1` where `extraction_status='ok'` and `evidence_source_modality='imaging'` | US LN event attributes | Provides laterality/level/size/status columns; imaging modality filter preserves US-only scope. |
| 2 | `canonical_us_lymph_node_v2` existing shell | Date/exam spine and legacy backfill context | 6,801 dated rows / 4,077 patients, but mostly shell; usable as spine, not as final detail source. |
| 3 | `note_entities_llm_cervical_ln_detail` | Extraction-faithfulness replay/provenance | 10,084 JSON rows / 5,106 patients; use to re-derive structured extraction rows and debug JSON errors. |
| 4 | `canonical_cervical_ln_clinical_events_v1` | Cross-validation / non-US LN mention context | Verified clinical-note LN canonical, but not US-specific enough to populate US event fields without modality gating. |
| 5 | `canonical_path_malignant_events_v1` | Pathology overlap and `tp_*` bridge | Supplies `ln_examined`, `ln_involved`, nodal counts, and ENE for the nine PM `tp_*` CFs; not a US event source. |
| 6 | `clinical_notes_long` | Raw text fallback only | PHI-bearing; avoid direct raw text scans in routine builds unless an authorized extraction run is opened. |

### 4. Patient rollup definition

The patient rollup should be cohort-wide (one row per CPM patient after mig_171b, not only one row per patient with LN events). Recommended rollups:

| rollup column | definition |
|---|---|
| `has_us_ln_findings` | `TRUE` if at least one US LN event exists for the patient; `FALSE` only after cohort-wide fill; otherwise unknown during staging. |
| `n_us_ln_events` | Count of rows in `canonical_us_lymph_node_events_v2`. |
| `n_us_ln_exams` | Count distinct `us_exam_id`. |
| `first_us_ln_exam_date`, `last_us_ln_exam_date` | Min/max `exam_date`. |
| `any_us_ln_suspicious` | `BOOL_OR(suspicious_flag IS TRUE)`. |
| `n_us_ln_suspicious` | Count events with `suspicious_flag IS TRUE`. |
| `max_us_ln_short_axis_mm`, `max_us_ln_long_axis_mm`, `max_us_ln_size_mm`, `max_us_ln_size_cm` | Max numeric sizes by patient. |
| `us_ln_sides_observed`, `us_ln_levels_observed` | Stable sorted distinct list of non-null side/level tokens. |
| `any_us_ln_extranodal_extension`, `any_us_ln_biopsy_recommended`, `any_us_ln_fna_mentioned`, `any_us_ln_washout_tg_mentioned` | Tri-state boolean rollups using `BOOL_OR(field IS TRUE)` and preserving unknowns at storage boundary. |
| `n_us_ln_events_textual_only` | Events with no size/side/level but retained as evidence. |
| `n_us_ln_events_high_confidence` | Events with `confidence >= 0.80` or source-specific equivalent. |

For the nine PM `tp_*` CF columns, the rollup skeleton carries bridge placeholders. The design recommendation is that `tp_*` re-derivation should be based on pathology overlap (`canonical_path_malignant_events_v1`) plus any ratified LN clinical canonical, not on US imaging alone. mig_171b must explicitly document whether these fields belong in the US LN rollup or a broader cervical/pathology LN rollup before mig_171c updates CPM.

### 5. Column list

The skeleton SQL defines:

- `main.canonical_us_lymph_node_events_v2`: patient/exam/event identifiers, LN location fields, size fields, morphology/suspicion fields, source/provenance fields, and `exam_date DATE`.
- `main.canonical_us_lymph_node_patient_rollup_v2`: US LN patient rollups plus bridge placeholders for:
  - `tp_central_examined`
  - `tp_central_positive_total`
  - `tp_ln_central_positive`
  - `tp_ln_ene`
  - `tp_ln_examined`
  - `tp_ln_largest_deposit_cm`
  - `tp_ln_lateral_positive`
  - `tp_ln_levels_involved`
  - `tp_ln_positive`

### 6. Cohort coverage probe

Current USLN coverage is 4,077 / 10,871 CPM patients (37.50%). Source candidates suggest potential expansion via LLM-derived cervical LN detail (5,106 patients) and clinical notes (5,593 patients), but modality gating will reduce the eligible US-specific subset.

### 7. Date type policy

- `exam_date`, `first_us_ln_exam_date`, and `last_us_ln_exam_date` are `DATE`.
- `build_ts`, `extracted_at`, and source ingestion timestamps are `TIMESTAMP` because they are audit/provenance timestamps, not clinical dates.
- Any VARCHAR source date (`note_date`, `entity_date`) must be parsed to `DATE` in mig_171b staging and routed to review if parsing fails.

## §4 Skeleton SQL

See `qc_framework_v1/migrations/171_canonical_us_lymph_node_v2_skeleton_20260429.sql`. The file contains only `CREATE TABLE IF NOT EXISTS` statements for:

- `main.canonical_us_lymph_node_events_v2`
- `main.canonical_us_lymph_node_patient_rollup_v2`

No `INSERT`, `CREATE TABLE AS SELECT`, registry DML, or CPM DML appears in the skeleton.

## §5 Verification plan for mig_171b

### Table-level gates

| gate | requirement |
|---|---|
| Row grain | `ln_event_id` unique; no duplicate `(research_id, us_exam_id, ln_index, side, neck_level, source_table, source_row_id)` unless explicitly classified as multi-source corroboration. |
| Cohort spine | Patient rollup has exactly 10,871 rows and 10,871 distinct `research_id` after cohort-wide fill. |
| Type policy | All clinical date columns are `DATE`; provenance columns are `TIMESTAMP`; `research_id` is `VARCHAR`. |
| No cross-DB sourcing | Build SQL sources only from live `main.*`; no archive or attached legacy schema in source CTEs. |
| No raw text leakage | `evidence_text` must be trimmed/snippet-safe; no full-note dumps in reports/logs. |

### Column verification pattern

For every populated event column:

1. **Source-of-truth recompute:** Re-derive the column from the ranked upstream source CTEs and compare mass equivalence to the built table.
2. **Extraction faithfulness:** For LLM-derived columns, re-derive from `note_entities_llm_cervical_ln_detail` / normalized extraction rows where `extraction_status='ok'` and compare counts and distinct values. This tests extraction faithfulness, not clinical truth.
3. **Overlap cross-validation:** Compare patient-level suspicious/size/level claims against `canonical_cervical_ln_clinical_events_v1` and path/nodal claims against `canonical_path_malignant_events_v1` to surface source-quality CFs.
4. **Boolean uniformity:** Run Type-A and Type-B checks: stored TRUE must have upstream support, and upstream TRUE must appear in storage unless deliberately excluded by modality/source rules.
5. **Date checks:** `exam_date IS NOT NULL` for events; invalid source dates go to a review queue. `DATE_TRUNC`/cast comparisons must be calendar-safe.
6. **Exam ID portability:** Every `(research_id, us_exam_id, exam_date)` in events must resolve exactly once in the rebuilt exam master. Every exam-master LN count must equal event aggregation.

### Specific verification probes to include in mig_171b

- `COUNT(*)`, `COUNT(DISTINCT ln_event_id)`, and duplicate key review for events.
- Rollup row count = CPM row count = 10,871.
- Event-to-exam-master anti-join count = 0.
- Event `source_modality` distinct values = only `US`.
- Event suspicious counts vs rollup `n_us_ln_suspicious` sum.
- Rollup `has_us_ln_findings` vs event existence, both directions.
- `tp_*` bridge fields vs `canonical_path_malignant_events_v1` patient aggregate, with explicit discrepancy queue.
- Registry DDL/DML dry-run review before any future table registration.

## §6 Carry-forwards to open

| CF | Type | Rationale |
|---|---|---|
| `CF-mig171-DESIGN-RATIFICATION-PENDING` | informational | This design must be ratified before any mig_171b apply. |
| `CF-mig171-EXAM-ID-RECIPE-LOCK` | informational | Current USLN direct `us_exam_id` only joins exam master for 19 / 6,801 rows; lock the hybrid reuse/fallback recipe. |
| `CF-mig171-SOURCE-COVERAGE-note_entities_llm_cervical_ln_detail` | coverage | JSON source has broad coverage but needs extraction-faithfulness replay and JSON-error handling. |
| `CF-mig171-SOURCE-COVERAGE-canonical_cervical_ln_clinical_events_v1` | coverage | Useful overlap source but not US-specific enough to populate US event attributes without modality gating. |
| `CF-mig171-SOURCE-COVERAGE-canonical_path_malignant_events_v1` | coverage | Needed for the `tp_*` pathology bridge but is not a US event source. |

## §7 Artifacts

| Artifact | Purpose |
|---|---|
| `qc_framework_v1/migrations/171_canonical_us_lymph_node_v2_skeleton_20260429.sql` | CREATE TABLE skeleton only; no INSERTs. |
| `qc_framework_v1/migrations/171_design_probes_20260429.sql` | Commented read-only probe SQL for Logan/Cowork replay. |
| `qc_framework_v1/reports/mig_171_canonical_us_lymph_node_v2_design_20260429.md` | This design document with observed probe outputs and verification plan. |

## §8 Out-of-scope confirmation

This lane did not:

- execute skeleton DDL against MotherDuck;
- insert or update any data;
- register tables/columns in canonical registries;
- modify any `canonical_patient_master` column;
- touch `canonical_us_exam_master_VIEW_v2`, `canonical_us_nodule_v2`, `canonical_us_thyroid_gland_v2`, or existing `canonical_us_lymph_node_v2`;
- source from non-`main` schemas.
