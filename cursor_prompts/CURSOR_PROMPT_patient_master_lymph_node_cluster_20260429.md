# Cursor Agent Task — `canonical_patient_master` LYMPH_NODE CLUSTER slice (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~80 cols)
**Run order:** Lane 24 of new 3-prompt batch

---

## 1. Goal

Continue patient_master verification with the **lymph_node cluster** (~80 cols matching `ln_*`, `*lymph_node*`, `cervical_*`, `*ene_*`, `*lateral_neck*`, `*central_neck*`).

Probe scope:
```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name LIKE 'ln_%' OR column_name LIKE '%lymph_node%' 
       OR column_name LIKE 'cervical_%' OR column_name LIKE '%ene_%'
       OR column_name LIKE '%lateral_neck%' OR column_name LIKE '%central_neck%'
       OR column_name LIKE '%nodal%')
ORDER BY column_name;
```

Sub-clusters:
- **`ln_*` / `*lymph_node*`** (~50 cols): nodal status, counts, ratios — derive from `canonical_cervical_ln_clinical_events_v1` (mig_111) + `canonical_cervical_ln_clinical_patient_rollup_v1` (mig_113) + `canonical_us_lymph_node_v2` (mig_117)
- **`cervical_*`** (~15 cols): cervical findings — from cervical_ln canonicals + path_malignant_events_v1
- **`*ene_*` / extracapsular extension** (~5 cols): from `canonical_ete_subgrade_events_v1` (mig_114) + invasion_events
- **`*lateral_neck*` / `*central_neck*`** (~10 cols): regional dissection findings — from operative_events + path_malignant_events

---

## 2. Methodology — derivation re-derivation against verified LN family + cross-table joins

Pattern reference: Lane 23 pathology template + `qc_framework_v1/migrations/113_cervical_ln_clinical_patient_rollup_signoff.sql`.

### 2a. Per-col derivation map (representative)
- `ln_any_metastasis` → `BOOL_OR(canonical_cervical_ln_clinical_events_v1.metastasis_flag)` per pt
- `ln_n_positive` → counts from path_malignant_events_v1 (positive nodes per pt)
- `ln_n_examined` → counts (total nodes per pt)
- `ln_ratio_positive` → derived ratio
- `cervical_ln_first_date` → MIN(date) from cervical_ln events
- `lateral_neck_dissection_*` → from operative_procedure_codes_v1 (mig_118) — proc IN ('lateral_neck_dissection','modified_radical_neck_dissection')
- `central_neck_dissection_*` → from operative_procedure_codes_v1 — proc='central_neck_dissection'
- `*ene_*` → `BOOL_OR` from canonical_ete_subgrade_events_v1 (LN-ENE subset)
- `cervical_*_us_*` → from canonical_us_lymph_node_v2

### 2b. ⚠️ NULL vs FALSE/0 caveat
Per `feedback_recurrence_imaging_n_events_null.md`: many LN cols may be NULL for patients without LN data. Use COALESCE in drift probes.

### 2c. ⚠️ Date-type CFs
Watch for TIMESTAMP / VARCHAR clinical date cols → CF-mig<N>-PM-LN-DATE-RETYPE.

### 2d. Cross-source disambiguation
LN data exists in 4+ canonicals (cervical_ln_clinical events, cervical_ln_clinical_rollup, us_lymph_node_v2, path_malignant_events). When `pm.ln_*` cols disagree with one source but agree with another, flag the source-precedence rule and document.

### 2e. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_patient_master_lymph_node_cluster_signoff.sql`

---

## 3. Acceptance gates

- ~80 lymph_node-cluster cols flipped
- 0 drift on derivation re-derivation per col
- Cohort parity 10,871 confirmed
- CF rows for any date violations
- gate 4 = 0

---

## 4. Don't touch (active parallel lanes)

- `canonical_recurrence_v1` mig_123 — Lane 19 RESUME (in flight)
- `canonical_patient_master` OPERATIVE cluster — Lane 22 (sibling)
- PATHOLOGY cluster — Sibling Lane 23
- LABS cluster — Sibling Lane 25
- `manuscript_workspace.*` tier3_helper — Lane 21

---

## 5. Reference reading

Required:
- Auto-memory: `project_op_path_consolidation_script_361_closeout.md`
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md`
- Auto-memory: `feedback_etevent_resolved_cross_check.md` (CAST(rid AS VARCHAR) for ete cross-checks)
- Repo: `qc_framework_v1/migrations/111_cervical_ln_clinical_events_table_signoff.sql`
- Repo: `qc_framework_v1/migrations/113_cervical_ln_clinical_patient_rollup_signoff.sql`
- Repo: `qc_framework_v1/migrations/117_us_v2_family_signoff.sql` (us_lymph_node_v2 close-out)
- Repo: `qc_framework_v1/migrations/89_path_malignant_table_signoff.sql` (positive nodes derivation)
- Repo: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (procedure-code source)

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing lymph_node cluster
- Surgical git add
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP

---

## 7. If something unexpected surfaces

- Drift > 5% on `ln_n_positive` → likely path_malignant_events vs cervical_ln_clinical disagreement; document precedence
- Patients with `ln_n_examined < ln_n_positive` → impossible; flag as data integrity issue
- ENE flag rate > 30% → unusual high; investigate
- More than 10 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 24 of new 3-prompt batch.
