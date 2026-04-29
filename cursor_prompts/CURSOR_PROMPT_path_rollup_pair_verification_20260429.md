# Cursor Agent Task — `canonical_path_malignant_patient_rollup_v1` + `canonical_path_benign_patient_rollup_v1` Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 60-90 minutes (2 small rollups, both already have verified events sources)
**Run order:** Lane 12 of next batch (run middle — closes path family in parallel to lane 11 frozen)

---

## 1. Goal

Close out the **path family** by verifying both rollups in one batch:

| Table | Rows | Patients | Cols total | not_started | na |
|---|---:|---:|---:|---:|---:|
| canonical_path_malignant_patient_rollup_v1 | 4,137 | 4,137 | 17 | 14 | 3 |
| canonical_path_benign_patient_rollup_v1 | 10,871 | 10,871 | 16 | 13 | 3 |

Source events tables already verified:
- `canonical_path_malignant_events_v1` — verified mig_89 (56/56 cols verified, 100% coverage)
- `canonical_path_benign_events_v1` — verified mig_97b (51 verified + 4 na = 55)

Note the patient-count asymmetry:
- malignant rollup = 4,137 (events scope — only patients with any malignant path)
- benign rollup = 10,871 (cohort scope — full cohort, with `any_benign_event` flag)

---

## 2. Methodology — derivation re-derivation against verified events

Pattern reference: `qc_framework_v1/migrations/101_path_gland_patient_rollup_signoff.sql` AND `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql`.

### 2a. Probe staleness first
```sql
SELECT 
  (SELECT MAX(build_ts) FROM main.canonical_path_malignant_events_v1) AS malig_events_build_ts,
  (SELECT MAX(build_ts) FROM main.canonical_path_malignant_patient_rollup_v1) AS malig_rollup_build_ts,
  (SELECT MAX(build_ts) FROM main.canonical_path_benign_events_v1) AS benign_events_build_ts,
  (SELECT MAX(build_ts) FROM main.canonical_path_benign_patient_rollup_v1) AS benign_rollup_build_ts;
```

If rollup build_ts < events build_ts → REBUILD pattern (pre-snapshot, CREATE OR REPLACE TABLE via Script 363/97b/89 logic, then re-derive).
If rollup build_ts ≥ events build_ts → derivation re-derivation only.

### 2b. Per-col derivation map (path_malignant rollup, 14 not_started)

| Col | Derivation source |
|---|---|
| `any_malignant_event` | `BOOL_OR(events present)` |
| `n_malignant_surgeries` | `COUNT(DISTINCT path_surgery_id)` |
| `n_tumors_total` | `COUNT(*) FROM events` (or DISTINCT specimen_id) |
| `earliest_malignant_path_date` | `MIN(events.path_date)` |
| `latest_malignant_path_date` | `MAX(events.path_date)` |
| `highest_stage_ajcc8` | rank-stage(events.derived_t_stage_ajcc8) — use Script staging order |
| `highest_stage_ajcc7` | rank-stage(events.path_event_ajcc7) similar |
| `any_ett` | `BOOL_OR(events.ett_flag)` or extrathyroidal-tissue equivalent |
| `any_metastasis` | `BOOL_OR(events.metastasis_flag)` |
| `dominant_histology` | mode of events.primary_histology per patient (highest count) |
| `bethesda_final` | sourced from `canonical_fna_events_v1` (verified) — last/dominant Bethesda per patient |
| `bethesda_final_name` | crosswalk of bethesda_final number → name |
| `regex_path_outcome` | sourced from raw path_synoptics or events — verify origin in Script 89 |
| `poc_tumor_1_histologic_type` | first-tumor histology from path_synoptics POC (raw) — likely needs cross-table |

### 2c. Per-col derivation map (path_benign rollup, 13 not_started)

| Col | Derivation source |
|---|---|
| `any_benign_event` | `BOOL_OR(events present)` per patient — TRUE for ~5,673 of 10,871 |
| `n_benign_synoptics` | `COUNT(*) FROM events` per patient |
| `any_mng` | `BOOL_OR(events.mng_flag)` or events.diagnosis IN ('multinodular goiter', ...) |
| `any_hashimotos` | `BOOL_OR(events.hashimotos_flag)` |
| `any_lymphocytic_thyroiditis` | `BOOL_OR(events.lymphocytic_thyroiditis_flag)` |
| `any_graves` | `BOOL_OR(events.graves_flag)` |
| `any_follicular_adenoma` | `BOOL_OR(events.follicular_adenoma_flag)` |
| `earliest_benign_path_date` | `MIN(events.path_date)` |
| `latest_benign_path_date` | `MAX(events.path_date)` |
| `any_concomitant_malignant` | cross-table: TRUE if same patient ALSO has any_malignant_event |
| `bethesda_final` / `_name` | from FNA events (same as malignant rollup) |
| `regex_path_outcome` | from path_synoptics raw (same logic as malignant) |

Note: `build_script`, `build_ts`, `research_id` are na (3 cols).

### 2d. ⚠️ Date type CFs (path_malignant rollup specifically)
Per `feedback_clinical_dates_calendar_only.md`:
- `earliest_malignant_path_date` and `latest_malignant_path_date` are **TIMESTAMP** in path_malignant_rollup → CLINICAL DATE VIOLATION
- `earliest_benign_path_date` and `latest_benign_path_date` in path_benign_rollup are **DATE** → clean ✓

Disposition for malignant rollup: flag CF-mig<N>-PATH-MALIG-DATE-RETYPE (joins CF-100-DATE-RETYPE). Don't block sign-off.

### 2e. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_path_rollup_pair_signoff.sql`
- 14 col flips for path_malignant rollup
- 13 col flips for path_benign rollup
- 2 table_status updates
- Single migration covering both

---

## 3. Acceptance gates

- All not_started cols flipped to verified
- 0 drift on derivation re-derivation per col
- For both rollups: table_status='verified'
- Patient cohort math:
  - path_malignant: 4,137 patients (events-scope; should match `SELECT COUNT(DISTINCT research_id) FROM canonical_path_malignant_events_v1`)
  - path_benign: 10,871 patients (cohort-scope; matches canonical_patient_master)
- Cross-table sanity:
  - `path_benign.any_concomitant_malignant=TRUE` patients = patients that appear in BOTH path_benign_events AND path_malignant_events. Verify with explicit JOIN.
  - `bethesda_final` not_null only when patient has any FNA — sanity check coverage

---

## 4. Don't touch (active parallel lanes)

- `canonical_operative_procedure_codes_v1` — Cowork's lane (mig_118)
- `canonical_frozen_section_patient_rollup_v1` — Sibling Cursor lane 11
- `canonical_ete_event_resolved_v1` — Sibling Cursor lane 13
- `canonical_us_*_v2` — Cursor lane 10 if still running

---

## 5. Reference reading

Required:
- Auto-memory: `project_op_path_consolidation_script_361_closeout.md` (path family build via Script 361)
- Auto-memory: `project_path_gland_family_complete_2026-04-28.md` (rebuild-then-verify pattern)
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Repo: `qc_framework_v1/migrations/89_path_malignant_table_signoff.sql` (events sign-off — derivation reference)
- Repo: `qc_framework_v1/migrations/97b_path_benign_synoptic_row_ix_inherit.sql` (events sign-off — derivation reference)
- Repo: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (rollup template)

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing both path rollups
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Pre-snapshots (only if rebuilding): `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_<malig|benign>_patient_rollup_v1_pre_mig<N>_20260429`
- Surgical git add

---

## 7. If something unexpected surfaces

- `dominant_histology` ties (multiple histologies tied for most-common per patient) → use lexicographic tie-breaker; document method in verification_note
- `highest_stage_ajcc8` patients with all-NULL stages → treat as NULL, not '0'/'X'
- `bethesda_final` source ambiguity (which Bethesda — last? worst?) → check Script 89 logic; document
- `any_concomitant_malignant` discrepancy with explicit JOIN → indicates rollup is stale; trigger rebuild path
- Patients in events but missing from rollup → expected if rollup was built before mig_87/97b backfills; trigger rebuild

---

End of prompt. Lane 12 of next 3-prompt batch. Closes the path family. Update `MEMORY.md` with close-out entry — note this is the 5th-and-6th rollup verified following the established derivation re-derivation pattern.
