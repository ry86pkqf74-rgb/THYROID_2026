# Cursor Agent Task — `canonical_recurrence_v1` vs `canonical_recurrence_resolved_v1` PATH-PROVEN RECONCILE

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (focused upstream investigation + fix)
**Run order:** Lane 33c — **HIGH PRIORITY: cross-SSOT drift between two verified upstream canonicals.**

---

## 1. Goal

Investigate and fix the **22-patient drift** between two verified upstream canonicals:

- `canonical_recurrence_v1` (mig_123 rebuild SSOT, post-mig_139 PM resync) — `recurrence_confirmed = FALSE`
- `canonical_recurrence_resolved_v1` (mig_125 SSOT) — `recurrence_status_final = 'path_proven'`

These two canonicals **disagree on 22 patients** about whether a path-proven recurrence exists. Both are signed off in `canonical_table_signoff_registry_v1` as `verified`. **One of them is wrong.**

This was surfaced during Cowork verification of mig_144 (US imaging cluster) when `imaging_suspicious_recurrence_flag` was cross-validated against both recurrence canonicals.

Reproducer query:

```sql
WITH cr AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, recurrence_confirmed
  FROM main.canonical_recurrence_v1
),
crr AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, recurrence_status_final
  FROM main.canonical_recurrence_resolved_v1
)
SELECT cr.rid
FROM cr JOIN crr USING (rid)
WHERE COALESCE(cr.recurrence_confirmed,FALSE) = FALSE
  AND crr.recurrence_status_final = 'path_proven';
```

Expect 22 rids. Confirm before proceeding.

---

## 2. Methodology — investigation-first, fix-second

### 2a. Identify the 22 affected research_ids

Run the reproducer above; save the rid list as `manuscript_workspace.cr_crr_reconcile_candidates_20260429` for traceability.

### 2b. Pull the underlying evidence for each rid

For each of 22 rids, gather:

```sql
-- 1. canonical_recurrence_v1 row
SELECT * FROM main.canonical_recurrence_v1 WHERE CAST(research_id AS VARCHAR)='<rid>';

-- 2. canonical_recurrence_resolved_v1 row
SELECT * FROM main.canonical_recurrence_resolved_v1 WHERE CAST(research_id AS VARCHAR)='<rid>';

-- 3. Underlying pathology evidence (re-operation specimens that prove recurrence)
SELECT note_row_id, specimen_label, final_diagnosis, malignant_finding,
       extrathyroidal_extension, ln_metastasis_present, surgery_date
FROM main.canonical_path_malignant_events_v1
WHERE CAST(research_id AS VARCHAR)='<rid>'
ORDER BY surgery_date;

-- 4. Operative re-procedure evidence
SELECT operative_event_id, surgery_date, procedure_summary
FROM main.canonical_operative_events_v1
WHERE CAST(research_id AS VARCHAR)='<rid>'
ORDER BY surgery_date;

-- 5. NLP recurrence signals
SELECT note_id, note_date, finding_status, evidence_quote
FROM main.note_entities_llm_recurrence
WHERE CAST(research_id AS VARCHAR)='<rid>' AND error=0
ORDER BY note_date;
```

### 2c. Decision rule (clinical)

For each rid, classify:

- **CR is wrong** (FALSE should be TRUE): if path_malignant_events shows ≥2 surgical specimens with the second being malignant disease at a known recurrence site (cervical LN, contralateral lobe, distant met biopsy, or tumor bed), AND the timing is post-index (per `feedback_etevent_resolved_cross_check.md` patterns).
- **CRR is wrong** (path_proven should be different): if the path-proven label was assigned based on a peri-index specimen (not a true post-index recurrence specimen) OR a misclassified specimen (e.g., contralateral lobectomy at index counted as a post-index lesion).
- **Both partially right, definitions differ**: document which clinical interpretation each canonical encodes; pick the one Logan ratifies as SSOT.

### 2d. Apply the fix

Based on the decision rule, pick ONE direction:

**Option A — CR was missing 22 confirmed recurrences (most likely scenario):**

```sql
-- Pre-snapshot
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig153_crr_reconcile_20260429 AS
SELECT * FROM main.canonical_recurrence_v1
WHERE CAST(research_id AS VARCHAR) IN (<the 22 rids>);

-- Update CR.recurrence_confirmed = TRUE for the 22 rids
-- Also pull recurrence_date / recurrence_definition / recurrence_evidence_source
-- from CRR or rebuild from path_malignant_events as appropriate.
UPDATE main.canonical_recurrence_v1 SET
  recurrence_confirmed = TRUE,
  recurrence_date = ...,         -- derive from CRR or path_malignant_events
  recurrence_definition = 'surgical_pathology',
  recurrence_evidence_source = 'reoperation_pathology'  -- adjust per evidence
WHERE CAST(research_id AS VARCHAR) IN (<the 22 rids>);

-- Then re-resync PM mig_139 style (or just touch the 22 rids)
UPDATE main.canonical_patient_master pm
SET recurrence_confirmed = cr.recurrence_confirmed,
    recurrence_date = cr.recurrence_date,
    recurrence_definition = cr.recurrence_definition,
    recurrence_evidence_source = cr.recurrence_evidence_source,
    recurrence_histology = cr.recurrence_histology,
    recurrence_site = cr.recurrence_site,
    recurrence_type = cr.recurrence_type,
    time_to_recurrence_days = cr.time_to_recurrence_days,
    biochemical_tg_at_recurrence = cr.biochemical_tg_at_recurrence
FROM main.canonical_recurrence_v1 cr
WHERE CAST(pm.research_id AS VARCHAR) = CAST(cr.research_id AS VARCHAR)
  AND CAST(cr.research_id AS VARCHAR) IN (<the 22 rids>);
```

**Option B — CRR over-classified 22 patients as path_proven:**

```sql
-- Pre-snapshot canonical_recurrence_resolved_v1
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_resolved_v1_pre_mig153_crr_reconcile_20260429 AS
SELECT * FROM main.canonical_recurrence_resolved_v1
WHERE CAST(research_id AS VARCHAR) IN (<the 22 rids>);

-- Demote 22 from 'path_proven' to 'imaging_only_unconfirmed' or 'none' as appropriate
UPDATE main.canonical_recurrence_resolved_v1 SET
  recurrence_status_final = '<imaging_only_unconfirmed | none>'
WHERE CAST(research_id AS VARCHAR) IN (<the 22 rids>);
```

**Option C — Hybrid**: split the 22 between A and B based on per-rid evidence.

### 2e. Post-verify drift = 0

```sql
WITH cr AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, recurrence_confirmed FROM main.canonical_recurrence_v1
),
crr AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, recurrence_status_final FROM main.canonical_recurrence_resolved_v1
)
SELECT COUNT(*) AS still_drifting
FROM cr JOIN crr USING (rid)
WHERE COALESCE(cr.recurrence_confirmed,FALSE) = FALSE
  AND crr.recurrence_status_final = 'path_proven';
```

Expect 0.

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/153_cr_vs_crr_path_proven_reconcile_20260429.sql`

```
batch_id = 'mig_153_cr_vs_crr_path_proven_reconcile_20260429'
verification_method (whichever direction):
  - 'cross_ssot_reconcile_cr_to_crr_via_path_malignant_evidence' (Option A)
  - 'cross_ssot_reconcile_crr_to_cr_demote_misclassified' (Option B)
```

---

## 3. Acceptance gates

- 22 rids resolved (each gets a per-rid disposition)
- Post-fix drift = 0 between cr.recurrence_confirmed and crr.recurrence_status_final='path_proven'
- PM `recurrence_confirmed` re-synced if CR was the wrong side (mig_139-style pattern)
- Pre-snapshots in archive_pub_v1_0
- gate 4 = 0
- close-out memo with the per-rid disposition table

---

## 4. STOP conditions (clinical complexity)

If you encounter any of these, STOP and ask Logan:

- Per-rid evidence is genuinely ambiguous (specimen labels unclear, dates suggest peri-index timing, NLP signals contradictory)
- More than 5 of the 22 rids have evidence pointing in opposite directions
- The "right" answer requires a clinical judgment call on what "path_proven" means (e.g., does a single positive node biopsy at the time of central neck dissection completion count as "recurrence" if it was anticipated? What about residual disease vs new recurrence?)

In any of those cases, prepare a per-rid disposition table for Logan's review BEFORE writing.

---

## 5. Reference reading

Required:
- Auto-memory: `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md`
- Auto-memory: `project_recurrence_resolved_v1_mig_125_closeout.md` (SSOT enum: `imaging_only_unconfirmed`)
- Auto-memory: `feedback_etevent_resolved_cross_check.md` (event-grain INNER JOIN with CAST(rid AS VARCHAR))
- Auto-memory: `feedback_no_cross_db_canonical_sourcing.md`
- Repo: `qc_framework_v1/migrations/123_canonical_survival_followup_v1_signoff.sql` (mig_123 builder context)
- Repo: `qc_framework_v1/migrations/125_canonical_recurrence_resolved_v1_signoff.sql` (or whatever number mig_125 was)
- Repo: `qc_framework_v1/migrations/139_cpm_recurrence_spine_resync_20260429.sql` (PM resync pattern)
- Repo: `scripts/203b_canonical_recurrence_harmonized_20260429.py` (CR builder)

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing the reconcile
- Surgical git add (migration file + per-rid disposition CSV/Markdown if produced)
- Pre-snapshot before any UPDATE
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP

---

## 7. Why this matters

For Logan's manuscript-ready cohort: the 22-pt drift means downstream survival analysis on recurrence-as-event will be ambiguous depending on which canonical is queried. After fix, both canonicals agree, statisticians can pick either as SSOT, and the PM `recurrence_confirmed` count is fully reconciled.

If Option A (CR was missing 22), PM `recurrence_confirmed` TRUE goes 514 → 536. If Option B (CRR over-classified), CRR `path_proven` count drops by 22.

---

End of prompt. Lane 33c — Cross-SSOT reconcile.
