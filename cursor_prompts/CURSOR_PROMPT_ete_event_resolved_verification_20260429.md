# Cursor Agent Task — `canonical_ete_event_resolved_v1` Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 90-150 minutes (62 cols, multi-source cross-table)
**Run order:** Lane 13 of next batch (run last — depends on multiple verified families)

---

## 1. Goal

Verify `canonical_ete_event_resolved_v1` under Protocol v2.

| Stat | Value |
|---|---|
| Rows | 6,689 |
| Patients | 4,137 |
| Cols total | 62 |
| not_started | 57 |
| na | 5 |

This is the **Tier 2 ETE adjudication table** — joins multiple verified canonicals into a per-tumor row enriched with ETE grade, staging, recurrence, and survival cols. Not a rollup; an enriched event-grain table.

---

## 2. Source map (12+ verified upstream tables)

The 62 cols span ~5 source clusters:

### 2a. ETE-specific (cols 7-22, ~16 cols)
Source: `canonical_ete_subgrade_events_v1` (verified mig_114, 17 cols / 5 verified + 12 na) and `canonical_path_malignant_events_v1` (verified mig_89, 56/56)

Cols include: `ete_grade`, `ete_grade_source`, `is_gross_ete`, `is_microscopic_ete`, `any_ete_present`, `is_no_ete`, `is_unresolved_ete`, `is_no_ete_data`, `path_event_ete_raw`, `patient_master_ete_*` (×4), `general_llm_ete_grade`, `mig54_fresh_llm_*` (×4)

### 2b. Inline adjudication (cols 28-35, ~8 cols)
Source: `canonical_ete_inline_adjudication_v1` (NOT YET VERIFIED — 12 cols, 9 not_started)

Cols include: `inline_patient_grade`, `inline_patient_set`, `inline_patient_evidence`, `inline_event_grade`, `inline_event_evidence`, `inline_event_set`, `pm_disagreement_flag`, `open_self_contradiction_flag`, `legacy_gross_ete_effective`

⚠️ **Block-coupling:** if inline_adjudication isn't verified yet, you may need to verify it FIRST, or do them as a paired migration. Check status at start.

### 2c. Tumor + staging (cols 36-45, ~10 cols)
Source: `canonical_path_malignant_events_v1` (verified)

Cols: `size_greatest_dimension_cm`, `primary_histology`, `histology_variant`, `laterality`, `multifocal_flag`, `reported_t_stage_ajcc8`, `derived_t_stage_ajcc8`, `t_stage_discordance_flag`, `ajcc_overall_stage`

### 2d. Recurrence (cols 46-57, ~12 cols)
Source: `canonical_recurrence_v1` (NOT YET VERIFIED — 12 cols, 11 not_started, on Cowork's small-Tier-2 forward backlog)

Cols: `recurrence_path_proven`, `recurrence_path_proven_date`, `recurrence_path_proven_source`, `days_to_path_proven`, `recurrence_imaging_suspicious`, `recurrence_imaging_suspicious_date`, `recurrence_imaging_modality_summary`, `recurrence_imaging_modalities_all`, `recurrence_imaging_finding_text`, `recurrence_imaging_n_events`, `days_to_imaging_suspicious`, `recurrence_imaging_then_path_confirmed`, `recurrence_status_final`

⚠️ **Block-coupling:** if recurrence_v1 isn't verified yet, recurrence cols here are extraction-faithfulness against the upstream snapshot. Document them as `verification_method='extraction_faithfulness_against_recurrence_v1_snapshot'` and add CF-mig<N>-ETE-EVENT-RESOLVED-RECURRENCE-PENDING — flip to derivation_re_derivation when recurrence_v1 verifies.

### 2e. Survival (cols 58-60, 3 cols)
Source: `canonical_survival_followup_v1` (NOT YET VERIFIED — 13 cols, 9 not_started)

Cols: `last_known_alive_date` (TIMESTAMP — DATE violation candidate), `vital_status`

⚠️ Same block-coupling as recurrence.

### 2f. Cohort + IDs (cols 1-9, ~9 cols)
Cols: `research_id`, `path_surgery_id`, `surgery_episode_id_global`, `tumor_ordinal`, `specimen_id`, `synoptic_row_ix`, `cohort_ptc`, `cohort_descriptive_full`, `analytic_eligible`

`synoptic_row_ix` per `reference_synoptic_row_ix.md`: don't synthesize via ROW_NUMBER; comes from Script 108 pandas-load-order.

### 2g. Build metadata (cols 61-62, na)
`build_script`, `build_ts` (TIMESTAMP WITH TIME ZONE — note ZONE; cast on insert)

---

## 3. Methodology — extraction-faithfulness vs verified upstream + cross-table joins

Pattern reference: `qc_framework_v1/migrations/114_ete_subgrade_family_signoff.sql` (cross-table crosswalk #8) AND `qc_framework_v1/migrations/110_pathology_clinical_events_table_signoff.sql` (cross-table joins).

### 3a. Locate build SQL
```bash
grep -rn "canonical_ete_event_resolved_v1" scripts qc_framework_v1 | head -20
```
Likely in 350-380 range or qc_framework migrations. Read the build SQL — it should be a series of LEFT JOINs from path_malignant_events to each upstream canonical.

### 3b. Probe natural key
Probable key: `(research_id, path_surgery_id, tumor_ordinal)` or `(research_id, specimen_id)`.
```sql
SELECT COUNT(*), COUNT(DISTINCT (research_id, path_surgery_id, tumor_ordinal)) 
FROM main.canonical_ete_event_resolved_v1;
```

### 3c. Per-cluster verification

For each cluster (2a-2g), use the matching pattern:
- 2a/2c: extraction-faithfulness against verified ete_subgrade_events + path_malignant_events
- 2b: defer to inline_adjudication_v1 verification (see §3d below)
- 2d/2e: extraction-faithfulness against current state of recurrence_v1 + survival_followup_v1, with CF for "block on downstream verification"
- 2f: probe natural key, then `verification_method='extraction_faithfulness'` against path_malignant_events
- 2g: na for build_script/build_ts/synoptic_row_ix

### 3d. Decide on inline_adjudication_v1 pairing

`canonical_ete_inline_adjudication_v1` is small (12 cols / 9 not_started). Two options:
- **Option A (recommended):** verify it INSIDE this migration as a sub-block. Single sign-off covers both.
- **Option B:** verify ete_event_resolved_v1 only, with cluster 2b cols flipped via `extraction_faithfulness_against_inline_adjudication_v1_snapshot` + CF.

Pick A unless inline_adjudication has structural issues (e.g., NULL cols, ambiguous keys).

### 3e. Cross-validation against verified ete_subgrade
This is the highest-value sanity check. The events_resolved table SHOULD reproduce ete_subgrade events 1:1 on `(research_id, path_surgery_id, tumor_ordinal)` for the ETE cols.
```sql
SELECT 
  SUM(CASE WHEN er.ete_grade IS DISTINCT FROM es.ete_grade_final THEN 1 ELSE 0 END) AS drift_grade,
  SUM(CASE WHEN er.is_gross_ete IS DISTINCT FROM es.is_gross_ete THEN 1 ELSE 0 END) AS drift_gross,
  ...
FROM main.canonical_ete_event_resolved_v1 er
LEFT JOIN main.canonical_ete_subgrade_events_v1 es 
  ON er.research_id=es.research_id AND er.path_surgery_id=es.path_surgery_id AND er.tumor_ordinal=es.tumor_ordinal;
```
Drift > 0 → indicates resolved-table is stale relative to ete_subgrade events; flag CF.

### 3f. ⚠️ Date type CF
`last_known_alive_date` is TIMESTAMP — clinical date violation. Flag CF-mig<N>-ETE-EVENT-LAST-ALIVE-RETYPE (joins CF-100-DATE-RETYPE).

### 3g. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_ete_event_resolved_signoff.sql`
- 57 col flips for ete_event_resolved_v1
- (Optional) 9 col flips for ete_inline_adjudication_v1 (Option A above)
- 1 (or 2) table_status updates

---

## 4. Acceptance gates

- All 57 not_started cols flipped (or fewer if inline_adjudication paired in)
- 0 drift on extraction-faithfulness against ete_subgrade_events + path_malignant_events for clusters 2a/2c
- table_status='verified' on resolved (and inline_adjudication if paired)
- Per-tumor row count = 6,689 (= patient-tumor distinct count from path_malignant)
- Patient count = 4,137 = path_malignant_rollup patients (matching #s)
- CFs filed:
  - CF-mig<N>-ETE-EVENT-RESOLVED-RECURRENCE-PENDING (until recurrence_v1 verified)
  - CF-mig<N>-ETE-EVENT-RESOLVED-SURVIVAL-PENDING (until survival_followup_v1 verified)
  - CF-mig<N>-ETE-EVENT-LAST-ALIVE-RETYPE (join CF-100-DATE-RETYPE)

---

## 5. Don't touch (active parallel lanes)

- `canonical_operative_procedure_codes_v1` — Cowork's lane (mig_118)
- `canonical_frozen_section_patient_rollup_v1` — Sibling Cursor lane 11
- `canonical_path_malignant_patient_rollup_v1` / `canonical_path_benign_patient_rollup_v1` — Sibling Cursor lane 12
- `canonical_recurrence_v1` / `canonical_survival_followup_v1` — Cowork's likely-next lane (do NOT verify these here)
- `canonical_us_*_v2` — Cursor lane 10 if still running

---

## 6. Reference reading

Required:
- Auto-memory: `project_invasion_family_signoff_2026-04-28.md` (12-rule clinical library; ETE family context)
- Auto-memory: `project_ete_documentation_rate.md` (ETE x-sentinel; cohort math)
- Auto-memory: `project_ctc_equivalence_verification_pattern.md` (CTC pattern context)
- Auto-memory: `feedback_extraction_faithfulness_llm_canonical.md`
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Auto-memory: `reference_synoptic_row_ix.md` (synoptic_row_ix not SQL-reproducible)
- Repo: `qc_framework_v1/migrations/114_ete_subgrade_family_signoff.sql` (cross-table crosswalk template)
- Repo: `qc_framework_v1/migrations/110_pathology_clinical_events_table_signoff.sql` (multi-source template)
- Repo: `qc_framework_v1/migrations/95_ete_taxonomy_and_invasion_rollups.sql` (ETE-resolved build context)

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit (covers ete_event_resolved + optionally ete_inline_adjudication)
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Note: build_ts on this table is TIMESTAMP WITH TIME ZONE — strip on next rebuild via `feedback_alter_view_dependents.md` pattern
- Surgical git add

---

## 8. If something unexpected surfaces

- Drift > 0 on ETE-grade cluster 2a → check whether ete_subgrade was rebuilt after ete_event_resolved; trigger rebuild conversation with Logan
- Inline adjudication has > 100 NULL cols → STOP, may indicate Script 108 dependency that's broken; ask Logan
- Recurrence cols 100% NULL → expected if recurrence_v1 hasn't propagated; verify against pre-build snapshot in archive_pub_v1_0
- `synoptic_row_ix` not 1:1 with path_malignant_events_v1 ordering → DON'T re-derive; this is by-design (Script 108 pandas global index)
- t_stage_discordance_flag rate > 30% → unusual; investigate before sign-off
- More than 1 patient with multiple `surgery_episode_id_global` for same path_surgery_id → key violation, escalate

---

End of prompt. Lane 13 of next 3-prompt batch. Closes the ETE-resolved layer + (optionally) ete_inline_adjudication. Update `MEMORY.md` with close-out entry — note this is the first multi-source-cross-table verification at full Tier 2 enrichment scale.
