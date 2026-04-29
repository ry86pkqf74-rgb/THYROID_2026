# Cursor Agent Task — `canonical_patient_master` PATHOLOGY CLUSTER slice (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~82 cols)
**Run order:** Lane 23 of new 3-prompt batch (continuation of Lane 22 patient_master cluster pattern)

---

## 1. Goal

Continue the `canonical_patient_master` verification effort by closing the **pathology cluster** (~82 cols matching `path_*`, `*histology*`, `*tumor*`, `*stage_*`, `bethesda_*`, `*synoptic*`).

This is a sibling of Lane 22 (operative cluster ~125 cols). Pattern is the same: thematic-cluster derivation re-derivation against verified upstream.

Probe scope at start:
```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name LIKE 'path_%' OR column_name LIKE '%histology%' 
       OR column_name LIKE '%tumor%' OR column_name LIKE '%stage_%'
       OR column_name LIKE 'bethesda_%' OR column_name LIKE '%synoptic%'
       OR column_name LIKE '%t_stage%' OR column_name LIKE '%n_stage%' OR column_name LIKE '%m_stage%')
ORDER BY column_name;
```

Target ~82 cols. Sub-clusters:
- **`path_*`** (~60 cols): pathology findings — derive from `canonical_path_malignant_events_v1` (verified mig_89) + `canonical_path_benign_events_v1` (mig_97b) + `canonical_pathology_clinical_events_v1` (mig_110)
- **`*histology*` / `*tumor*`** (~10 cols): tumor characterization — from path events
- **`*stage_*` / t/n/m**(~8 cols): AJCC staging — from path events + ete_subgrade_events (mig_114)
- **`bethesda_*`** (~3 cols): from `canonical_fna_events_v1` (verified mig_78/96)
- **`*synoptic*`** (~5 cols): synoptic_row_ix references — per `reference_synoptic_row_ix.md`, NEVER synthesize via ROW_NUMBER

---

## 2. Methodology — derivation re-derivation against verified path family

Pattern reference: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` + Lane 22 (when complete) operative cluster template.

### 2a. Probe upstream verified state
```sql
SELECT 
  (SELECT MAX(build_ts) FROM main.canonical_patient_master) AS pm_build,
  (SELECT MAX(build_ts) FROM main.canonical_path_malignant_events_v1) AS pme_build,
  (SELECT MAX(build_ts) FROM main.canonical_path_benign_events_v1) AS pbe_build,
  (SELECT MAX(build_ts) FROM main.canonical_pathology_clinical_events_v1) AS pce_build,
  (SELECT MAX(build_ts) FROM main.canonical_fna_events_v1) AS fna_build;
```

### 2b. Per-col derivation (representative)
- `path_any_malignant_event` → `BOOL_OR(rid IN canonical_path_malignant_events_v1)`
- `path_dominant_histology` → mode of `canonical_path_malignant_events_v1.primary_histology` per pt
- `path_n_tumors_total` → `COUNT(*) FROM canonical_path_malignant_events_v1` per pt
- `path_highest_stage_ajcc8` → rank-stage on `derived_t_stage_ajcc8`
- `path_earliest_malignant_date` → `MIN(path_date)` from path_malignant_events
- `path_*_synoptic_row_ix` → preserve from upstream; do NOT re-derive (memory: `reference_synoptic_row_ix.md`)
- `bethesda_final` / `_name` → from canonical_fna_events_v1 last/dominant Bethesda per pt
- `*_t_stage_*` / `*_n_stage_*` / `*_m_stage_*` → via `canonical_path_malignant_events_v1` staging cols

### 2c. ⚠️ Date-type CFs to watch for
- Any TIMESTAMP cols on path-related dates → flag CF-mig<N>-PM-PATH-DATE-RETYPE (joins CF-100/117/119/120/mig122 batch)

### 2d. Per-col drift probes
```sql
WITH derived AS (
  SELECT research_id, BOOL_OR(<flag>) AS expected
  FROM main.canonical_path_malignant_events_v1 GROUP BY research_id
)
SELECT COUNT(*) FILTER (WHERE pm.<col> IS DISTINCT FROM d.expected) AS drift
FROM main.canonical_patient_master pm
LEFT JOIN derived d USING (research_id);
```

⚠️ **For boolean/count cols, COALESCE may be needed** per `feedback_recurrence_imaging_n_events_null.md`: NULL vs FALSE/0 produces false drift on patients without events. Apply `COALESCE(d.expected, FALSE)` or `COALESCE(d.expected, 0)`.

### 2e. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_patient_master_pathology_cluster_signoff.sql`
- ~82 col flips with per-cluster verification_method labels
- Update n_verified count; do NOT flip table_status (other clusters still pending)

---

## 3. Acceptance gates

- ~82 pathology-cluster cols flipped (or document why fewer)
- 0 drift on derivation re-derivation per col (or document expected drift)
- Cohort parity 10,871 confirmed
- CF rows for any date violations
- 5-gate audit: gate 1 unchanged (patient_master still not_started overall); gate 4 = 0

---

## 4. Don't touch (active parallel lanes)

- `canonical_recurrence_v1` Script 203 RW + mig_123 — Lane 19 RESUME (in flight; do NOT verify recurrence cluster on patient_master in this lane — defer until post-mig_123)
- `canonical_patient_master` OPERATIVE cluster — Lane 22 (sibling; if still in flight, don't touch op_* cols)
- LYMPH_NODE cluster — Sibling Lane 24
- LABS cluster — Sibling Lane 25
- `manuscript_workspace.*` tier3_helper batch — Lane 21 (sibling)

---

## 5. Reference reading

Required:
- Auto-memory: `project_path_gland_family_complete_2026-04-28.md`
- Auto-memory: `project_op_path_consolidation_script_361_closeout.md`
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md` (NULL vs 0 / FALSE coalesce pattern)
- Auto-memory: `reference_synoptic_row_ix.md` (synoptic_row_ix not SQL-reproducible)
- Repo: `qc_framework_v1/migrations/89_path_malignant_table_signoff.sql`
- Repo: `qc_framework_v1/migrations/97b_path_benign_synoptic_row_ix_inherit.sql`
- Repo: `qc_framework_v1/migrations/110_pathology_clinical_events_table_signoff.sql`

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing the pathology cluster
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Surgical git add

---

## 7. If something unexpected surfaces

- Drift > 5% on path_* cols → indicates patient_master is stale; CF, do not block
- Stage cols disagreeing across path_malignant_events vs ete_subgrade_events → expected for some patients (ete_subgrade refines stage); document the resolution rule
- bethesda_final NULL for patients with FNA → check Script 89 logic for Bethesda selection
- More than 10 cols can't be cleanly mapped to verified upstream → STOP, surface to Logan

---

End of prompt. Lane 23 of new 3-prompt batch. Continues patient_master cluster verification (after Lane 22 operative). Update `MEMORY.md` with close-out entry.
