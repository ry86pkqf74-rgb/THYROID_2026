# Cursor Agent Task — `canonical_survival_followup_v1` Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_118)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting (current tip `976cf8f` after mig_118)
**Estimated effort:** 30-45 minutes (13 cols, cohort-wide derivation)
**Run order:** Lane 15 of 3-prompt batch (run middle — sibling to recurrence_v1)

---

## 1. Goal

Verify `canonical_survival_followup_v1` under Protocol v2.

| Stat | Value |
|---|---|
| Rows | 10,871 |
| Patients | 10,871 (cohort-wide; one row per patient) |
| Cols total | 13 |
| not_started | 9 |
| na | 4 (research_id, build_ts, build_script, extraction_run_id) |

Cohort-wide derivation table — vital status + last followup contact for every patient in master cohort.

Also referenced by **Lane 13 (ete_event_resolved)** — verifying survival_followup_v1 unblocks the `CF-mig<N>-ETE-EVENT-RESOLVED-SURVIVAL-PENDING` carry-forward there.

---

## 2. Schema preview

| Col | Type | Category |
|---|---|---|
| research_id | VARCHAR | na (already) |
| vital_status_current | VARCHAR | adjudicated |
| death_date | DATE | adjudicated |
| death_date_source | VARCHAR | adjudicated |
| last_known_alive_date | DATE | adjudicated |
| last_followup_source | VARCHAR | adjudicated |
| days_from_first_surgery_to_last_contact | BIGINT | adjudicated |
| followup_complete_at_5yr | BOOLEAN | adjudicated |
| followup_complete_at_10yr | BOOLEAN | adjudicated |
| first_surgery_date | DATE | adjudicated |
| build_ts | TIMESTAMP | na (already) |
| build_script | VARCHAR | na (already) |
| extraction_run_id | VARCHAR | na (already) |

✓ **Dates clean:** All clinical date cols (`death_date`, `last_known_alive_date`, `first_surgery_date`) are DATE type. No CF-DATE-RETYPE expected for this table.

---

## 3. Methodology — derivation re-derivation against verified upstream

Pattern reference: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (cohort-wide derivation).

### 3a. Locate build SQL
```bash
grep -rn "canonical_survival_followup_v1" scripts qc_framework_v1 | head -20
```
Almost certainly Script 364B per `qa/qa_script_364B_survival_followup.json` + `scripts/output/364B_*.log` references. Read the SQL.

### 3b. Probe staleness
```sql
SELECT 
  (SELECT MAX(build_ts) FROM main.canonical_survival_followup_v1) AS surv_build_ts,
  -- Upstream candidates
  (SELECT MAX(build_ts) FROM main.canonical_operative_events_v1) AS op_build_ts;
```

### 3c. Per-col derivation map (best-guess; confirm against build SQL)

| Col | Likely upstream + derivation |
|---|---|
| `vital_status_current` | enum {alive, deceased, unknown} — from raw demographics + last clinical contact |
| `death_date` | DATE from demographics or death-record source |
| `death_date_source` | VARCHAR provenance: {demographics_dod, clinical_note, ssa_match, ...} |
| `last_known_alive_date` | MAX(date across all clinical/lab/imaging contact rows) per patient when not deceased |
| `last_followup_source` | which canonical the last_known_alive_date came from (path, fna, lab, imaging, op, etc.) |
| `days_from_first_surgery_to_last_contact` | DATE_DIFF('day', first_surgery_date, COALESCE(death_date, last_known_alive_date)) |
| `followup_complete_at_5yr` | days_from_first_surgery_to_last_contact >= 5*365 OR death within 5yr |
| `followup_complete_at_10yr` | days_from_first_surgery_to_last_contact >= 10*365 OR death within 10yr |
| `first_surgery_date` | MIN(surgery_date_native) FROM canonical_operative_events_v1 per research_id |

### 3d. Cohort parity
```sql
SELECT 
  (SELECT COUNT(*) FROM main.canonical_survival_followup_v1) AS surv_n,
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_survival_followup_v1) AS surv_pts,
  (SELECT COUNT(*) FROM main.canonical_patient_master) AS master_n;
-- All 10,871
```

### 3e. Internal consistency probes
- vital_status_current='deceased' iff death_date IS NOT NULL
- vital_status_current='alive' iff death_date IS NULL AND last_known_alive_date IS NOT NULL
- followup_complete_at_5yr = TRUE implies days_from_first_surgery_to_last_contact >= 1825 OR death within 5yr
- followup_complete_at_10yr = TRUE implies same with 3650 day threshold
- first_surgery_date <= last_known_alive_date for alive patients
- first_surgery_date <= death_date for deceased patients

### 3f. Cross-validation against verified upstream
- `first_surgery_date` should match `MIN(surgery_date_native)` from canonical_operative_events_v1 per patient (verified mig_362)
- Verify on a sample of 100 patients

### 3g. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_survival_followup_v1_signoff.sql`
- 9 col flips: `verification_method='derivation_re_derivation'` (or `..._with_internal_consistency` for the followup_complete_at_*yr cols)
- 4 already-na: research_id, build_ts, build_script, extraction_run_id
- table_status update

---

## 4. Acceptance gates

- All 9 not_started cols flipped to verified
- 0 drift on derivation re-derivation per col (cohort-wide, expect 100% match if not stale)
- Cohort parity: 10,871 patients = patient_master count
- Internal consistency probes (3e above): 0 errors
- Cross-validation against operative_events: < 5% drift expected (some patients have first_surgery_date from non-operative source)
- vital_status_current vocab clean: only {alive, deceased, unknown}

---

## 5. Don't touch (active parallel lanes)

- `canonical_path_malignant_patient_rollup_v1` / `canonical_path_benign_patient_rollup_v1` — Cursor lane 12
- `canonical_ete_event_resolved_v1` / `canonical_ete_inline_adjudication_v1` — Cursor lane 13
- `canonical_recurrence_v1` — Sibling Cursor lane 14
- `canonical_molecular_genetics_from_notes_v2` — Sibling Cursor lane 16

---

## 6. Reference reading

Required:
- Auto-memory: `project_op_path_consolidation_script_361_closeout.md`
- Auto-memory: `project_op_procedure_consolidation_script_362_closeout.md`
- Auto-memory: `project_op_procedure_codes_mig_118_closeout.md` (hybrid pattern context — internal-consistency variant)
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md` (3 reusable patterns)
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Repo: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (cohort-wide derivation template)
- Repo: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (hybrid pattern context — internal-consistency for derived cols)

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing survival_followup_v1
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Surgical git add

---

## 8. If something unexpected surfaces

- Patient count != 10,871 → cohort drift; reconcile vs patient_master
- vital_status_current='deceased' patients with death_date NULL → encoding error; STOP and ask Logan
- followup_complete_at_*yr=TRUE but days_from_first_surgery < threshold → potential bug in the rollup; document
- last_followup_source values not in expected set → check Script 364B logic for source taxonomy
- last_known_alive_date < first_surgery_date for any patient → temporal violation; CF or stop

---

End of prompt. Lane 15 of new 3-prompt batch. Closes survival_followup_v1 (cohort-wide). Update `MEMORY.md` with close-out entry. Note when this lands, Lane 13 ete_event_resolved CF-mig<N>-ETE-EVENT-RESOLVED-SURVIVAL-PENDING can be flipped from `extraction_faithfulness_against_survival_followup_v1_snapshot` to `derivation_re_derivation_post_survival_verified`.
