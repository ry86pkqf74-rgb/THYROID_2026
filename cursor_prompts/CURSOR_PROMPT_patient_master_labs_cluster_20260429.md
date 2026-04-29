# Cursor Agent Task — `canonical_patient_master` LABS CLUSTER slice (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~65 cols)
**Run order:** Lane 25 of new 3-prompt batch

---

## 1. Goal

Continue patient_master verification with the **labs cluster** (~65 cols matching `lab_*`, `*thyroglobulin*`, `*tg_*`, `*tsh*`, `*calcium*`, `*pth*`, `*vitamin_d*`).

Probe scope:
```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name LIKE 'lab_%' OR column_name LIKE '%thyroglobulin%' 
       OR column_name LIKE '%tg_%' OR column_name LIKE 'tg_%'
       OR column_name LIKE '%tsh%' OR column_name LIKE '%calcium%'
       OR column_name LIKE '%pth%' OR column_name LIKE '%vitamin_d%'
       OR column_name LIKE '%biochemical%')
ORDER BY column_name;
```

Sub-clusters (mapping to verified canonical_labs_*_v1 family from mig_115):
- **`*thyroglobulin*` / `*tg_*`** (~25 cols): from `canonical_labs_thyroglobulin_v1` (incl TgAb shared table)
- **`*tsh*`** (~12 cols): from `canonical_labs_tsh_v1`
- **`*calcium*`** (~10 cols): from `canonical_labs_calcium_v1`
- **`*pth*`** (~8 cols): from `canonical_labs_pth_v1`
- **`*vitamin_d*`** (~5 cols): from `canonical_labs_vitamin_d_v1`
- **`*biochemical*`** (~5 cols): cross-lab derivations

---

## 2. Methodology — derivation re-derivation against verified labs family

Pattern reference: `qc_framework_v1/migrations/115_labs_family_signoff.sql` (Cursor 8) + Lane 23 pathology template.

### 2a. Per-col derivation map (representative)

**Per-analyte cols (× 5 analytes, similar pattern):**
- `<analyte>_n_measurements` → `COUNT(*) FROM canonical_labs_<analyte>_v1` per pt
- `<analyte>_max_value` → `MAX(value_normalized)` per pt
- `<analyte>_min_value` → `MIN(value_normalized)` per pt
- `<analyte>_first_date` → `MIN(lab_datetime::DATE)` per pt
- `<analyte>_last_date` → `MAX(lab_datetime::DATE)` per pt
- `<analyte>_post_op_*` → joined to first_surgery_date filter

**Thyroglobulin-specific (more granular):**
- `tg_nadir` / `tg_nadir_post_op` → minimum thyroglobulin post-first-surgery (excludes pre-op)
- `tg_max_post_op` → maximum
- `tg_undetectable_ever` → BOOL_OR(value < threshold)
- `tg_stimulated_*` → from `lab_test_kind` flag (stimulated vs suppressed)
- `tgab_*` → TgAb cols share canonical_labs_thyroglobulin_v1 table; filter on `lab_test_kind`

**Cross-lab:**
- `biochemical_concern_*` → derived from Tg + TSH + TgAb trajectories

### 2b. ⚠️ NULL vs 0 caveat
Per `feedback_recurrence_imaging_n_events_null.md`: many `lab_*_n_measurements` cols may be NULL for patients without that lab. COALESCE before IS DISTINCT FROM compares.

### 2c. ⚠️ Date-type CFs
Lab date cols on patient_master should be DATE. `lab_datetime` (TIMESTAMP) on the source canonicals is allowlisted; on patient_master, derived `*_first_date` / `*_last_date` should be DATE.

### 2d. ⚠️ TgAb shares Thyroglobulin table
Per `project_lab_consolidation_script_347_closeout.md`: TgAb measurements live in `canonical_labs_thyroglobulin_v1` differentiated by `lab_test_kind`. When deriving `tgab_*` patient_master cols, filter on `lab_test_kind` (e.g., `WHERE lab_test_kind ILIKE '%antibody%'`).

### 2e. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_patient_master_labs_cluster_signoff.sql`

---

## 3. Acceptance gates

- ~65 labs-cluster cols flipped
- 0 drift on derivation re-derivation per col
- Cohort parity 10,871 confirmed
- CF rows for any date violations
- TgAb cols correctly filter `lab_test_kind` (verify via spot-check)

---

## 4. Don't touch (active parallel lanes)

- `canonical_recurrence_v1` mig_123 — Lane 19 RESUME (in flight; recurrence_v1 itself uses biochemical_tg_nadir; don't re-verify those)
- `canonical_patient_master` OPERATIVE cluster — Lane 22 (sibling)
- PATHOLOGY cluster — Sibling Lane 23
- LYMPH_NODE cluster — Sibling Lane 24
- `manuscript_workspace.*` tier3_helper — Lane 21

---

## 5. Reference reading

Required:
- Auto-memory: `project_lab_consolidation_script_347_closeout.md` (lab canonical structure + TgAb shared table)
- Auto-memory: `project_lab_ingestion_refactor_script_348_closeout.md`
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md` (NULL coalesce pattern)
- Repo: `qc_framework_v1/migrations/115_labs_family_signoff.sql` (Cursor 8 close-out — primary reference)
- Repo: `_lab_value_normalizer.py` (lab value normalization logic)

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing labs cluster
- Surgical git add
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP

---

## 7. If something unexpected surfaces

- `tg_nadir` post-op definition unclear → check whether "post-op" means after first_surgery_date or after each surgery; document
- TgAb cols off because filter ILIKE pattern doesn't match → grep `lab_test_kind` distinct values in canonical_labs_thyroglobulin_v1 first
- Lab values seem out-of-range → check normalizer didn't get applied; cross-reference with `_lab_value_normalizer.py`
- Cross-lab biochemical concern cols depend on a manuscript_workspace helper → check whether the helper is verified; CF if not

---

End of prompt. Lane 25 of new 3-prompt batch.
