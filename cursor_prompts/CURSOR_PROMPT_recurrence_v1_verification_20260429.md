# Cursor Agent Task — `canonical_recurrence_v1` Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_118)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting (current tip `976cf8f` after mig_118)
**Estimated effort:** 30-45 minutes (12 cols, cohort-wide derivation)
**Run order:** Lane 14 of 3-prompt batch (run first — small, builds pattern)

---

## 1. Goal

Verify `canonical_recurrence_v1` under Protocol v2.

| Stat | Value |
|---|---|
| Rows | 10,871 |
| Patients | 10,871 (cohort-wide; one row per patient) |
| Cols total | 12 |
| not_started | 11 |
| na | 1 (research_id) |

Cohort-wide derivation table — assigns recurrence status to every patient in the master cohort, regardless of whether they ever recurred.

This is also referenced by **Lane 13 (ete_event_resolved)** — verifying recurrence_v1 unblocks the `CF-mig<N>-ETE-EVENT-RESOLVED-RECURRENCE-PENDING` carry-forward there.

---

## 2. Schema preview

| Col | Type | Category |
|---|---|---|
| research_id | VARCHAR | na (already) |
| recurrence_confirmed | BOOLEAN | adjudicated |
| recurrence_type | VARCHAR | adjudicated |
| recurrence_date | DATE | adjudicated |
| recurrence_site | VARCHAR | adjudicated |
| recurrence_histology | VARCHAR | adjudicated |
| recurrence_evidence_source | INTEGER | adjudicated |
| recurrence_definition | VARCHAR | adjudicated |
| first_surgery_date | TIMESTAMP | adjudicated |
| time_to_recurrence_days | INTEGER | derived |
| biochemical_tg_nadir | INTEGER | adjudicated |
| biochemical_tg_at_recurrence | INTEGER | adjudicated |

⚠️ **Date type CFs** — flag during verification:
- `first_surgery_date` is **TIMESTAMP** → clinical date violation per `feedback_clinical_dates_calendar_only.md`. Flag CF-mig<N>-RECURRENCE-FIRST-SURGERY-DATE-RETYPE (joins CF-100/117/118/119 batch).
- `recurrence_date` is DATE → clean ✓

---

## 3. Methodology — derivation re-derivation against verified upstream

Pattern reference: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (cohort-wide derivation).

### 3a. Locate build SQL
```bash
grep -rn "canonical_recurrence_v1" scripts qc_framework_v1 | head -20
```
Likely Script 364B (per `qa/qa_script_364B_survival_followup.json` + log files in `scripts/output/`). Memory `project_op_path_consolidation_script_361_closeout.md` references the family.

### 3b. Probe staleness
```sql
SELECT 
  (SELECT MAX(build_ts) FROM main.canonical_recurrence_v1) AS rec_build_ts,
  -- Upstream candidates: canonical_path_malignant_events_v1, canonical_invasion_events_v1
  (SELECT MAX(build_ts) FROM main.canonical_path_malignant_events_v1) AS path_malig_build_ts;
```
If recurrence build_ts < verified upstream → REBUILD pattern (mig_101 / mig_106). If ≥ → derivation re-derivation only.

### 3c. Per-col derivation map (best-guess; confirm against build SQL)

| Col | Likely upstream + derivation |
|---|---|
| `recurrence_confirmed` | BOOL_OR(any path-proven OR imaging-suspicious within encoded recurrence definition) |
| `recurrence_type` | enum {biochemical, structural, regional, distant, none} — derived from evidence sources |
| `recurrence_date` | MIN(date among recurrence evidence rows) |
| `recurrence_site` | first/dominant site from evidence (lateral_neck, central_neck, distant_metastasis, etc.) |
| `recurrence_histology` | path-proven recurrence histology, if any |
| `recurrence_evidence_source` | INTEGER bitmask or rank-coded: 1=biochem, 2=imaging, 4=path, 8=clinical |
| `recurrence_definition` | VARCHAR config: 'ATA_2024' or similar — should be uniform across cohort |
| `first_surgery_date` | MIN(surgery_date_native) FROM canonical_operative_events_v1 per research_id |
| `time_to_recurrence_days` | DATE_DIFF('day', first_surgery_date::DATE, recurrence_date) — only when recurrence_confirmed |
| `biochemical_tg_nadir` | MIN(canonical_labs_thyroglobulin_v1.value_normalized) post-first-surgery |
| `biochemical_tg_at_recurrence` | thyroglobulin nearest recurrence_date |

### 3d. Cohort parity check
```sql
SELECT 
  (SELECT COUNT(*) FROM main.canonical_recurrence_v1) AS rec_n,
  (SELECT COUNT(*) FROM main.canonical_patient_master) AS master_n;
-- Both should be 10,871
```

### 3e. Cross-validation (optional — most useful if Cursor 8 labs verified)
- `biochemical_tg_nadir` should reference `canonical_labs_thyroglobulin_v1` (verified mig_115)
- Verify lab-source crosswalk: how many patients have biochemical_tg_nadir NOT NULL — should match patients with any post-surgery thyroglobulin in labs canonical

### 3f. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_recurrence_v1_signoff.sql`
- 11 col flips: `verification_method='derivation_re_derivation'` (or `..._post_rebuild` if rebuilt)
- 1 already-na: research_id
- table_status update at the end

---

## 4. Acceptance gates

- All 11 not_started cols flipped to verified
- 0 drift on derivation re-derivation per col (or document if drift indicates stale rollup → trigger rebuild)
- Cohort parity: 10,871 patients = patient_master count
- recurrence_confirmed=TRUE patients < 10,871 (sanity — most patients don't recur)
- time_to_recurrence_days NULL when recurrence_confirmed=FALSE
- recurrence_date NULL when recurrence_confirmed=FALSE
- CF rows recorded for VARCHAR/TIMESTAMP date violations (first_surgery_date)

---

## 5. Don't touch (active parallel lanes)

- `canonical_path_malignant_patient_rollup_v1` / `canonical_path_benign_patient_rollup_v1` — Cursor lane 12
- `canonical_ete_event_resolved_v1` / `canonical_ete_inline_adjudication_v1` — Cursor lane 13
- `canonical_survival_followup_v1` — Sibling Cursor lane 15
- `canonical_molecular_genetics_from_notes_v2` — Sibling Cursor lane 16

---

## 6. Reference reading

Required:
- Auto-memory: `project_op_path_consolidation_script_361_closeout.md`
- Auto-memory: `project_op_procedure_consolidation_script_362_closeout.md`
- Auto-memory: `project_op_procedure_codes_mig_118_closeout.md` (most recent close-out, hybrid pattern context)
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md` (3 reusable patterns + cohort-wide derivation)
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Auto-memory: `feedback_audit_regex_word_boundary.md` (post-mig_117 audit pattern)
- Repo: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (cohort-wide derivation template)
- Repo: `qc_framework_v1/migrations/108_complications_patient_rollup_signoff.sql` (cohort-wide rollup template)

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing recurrence_v1
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Pre-snapshot (only if rebuilding): `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig<N>_20260429`
- Surgical git add (no `-A`, no `scripts/output/` sweeps)

---

## 8. If something unexpected surfaces

- Patient count != 10,871 → indicates cohort drift; STOP and reconcile against patient_master
- recurrence_confirmed=TRUE but time_to_recurrence_days IS NULL on >5% of confirmed rows → check date-source logic
- biochemical_tg_nadir > biochemical_tg_at_recurrence on >10% of rows → unexpected (nadir should be ≤ recurrence value); investigate
- recurrence_evidence_source values outside {1,2,3,4,5,6,7,8,15,...} → check encoding scheme; document
- recurrence_definition has multiple distinct values → expected if cohort spans definition revisions; document

---

End of prompt. Lane 14 of new 3-prompt batch. Closes recurrence_v1 (cohort-wide). Update `MEMORY.md` with close-out entry. Note when this lands, Lane 13 ete_event_resolved CF-mig<N>-ETE-EVENT-RESOLVED-RECURRENCE-PENDING can be flipped from `extraction_faithfulness_against_recurrence_v1_snapshot` to `derivation_re_derivation_post_recurrence_verified`.
