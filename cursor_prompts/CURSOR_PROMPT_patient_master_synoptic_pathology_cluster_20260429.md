# Cursor Agent Task — `canonical_patient_master` SYNOPTIC-PATHOLOGY CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~32 cols)
**Run order:** Lane 39 of next 4-prompt batch (mig_149)

---

## 1. Goal

Continue patient_master verification with the **synoptic-pathology cluster** (~32 unverified `syn_*` cols). These are PM-side rollups from the structured synoptic-pathology slice (CAP synoptic templates parsed into structured fields per `reference_synoptic_row_ix.md`).

Probe scope:

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master' AND column_name LIKE 'syn\_%' ESCAPE '\'
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY column_name;
```

Confirm count is **exactly 32** before proceeding.

Sub-clusters:

- **Synoptic gland morphometry** (~9 cols): `syn_isthmus_size_cm`, `syn_isthmus_weight_g`, `syn_left_lobe_size_cm`, `syn_left_lobe_weight_g`, `syn_right_lobe_size_cm`, `syn_right_lobe_weight_g`, `syn_total_weight_g`, `syn_n_parathyroid_identified`, `syn_parathyroid_in_specimen`
- **Synoptic histopathology — benign/inflammatory** (~7 cols): `syn_adenomatoid_nodules`, `syn_chronic_thyroiditis`, `syn_colloid_nodule`, `syn_follicular_adenoma`, `syn_graves`, `syn_hashimoto`, `syn_hyperplastic_nodules`, `syn_multinodular_goiter`
- **Synoptic invasion / margin / grade** (~7 cols): `syn_architecture`, `syn_capsular_invasion_clean`, `syn_carcinoma_on_frozen`, `syn_lymphatic_invasion_clean`, `syn_margin_distance_mm_num`, `syn_margin_distance_mm_raw_str`, `syn_necrosis_clean`
- **Synoptic mitotic / Ki67 / cytology** (~5 cols): `syn_mitotic_rate_numeric`, `syn_mitotic_rate_qualifier`, `syn_ki67_index`, `syn_histologic_grade`, `syn_hurthle_cell_change`, `syn_c_cell_hyperplasia`
- **Synoptic procedural metadata** (~2 cols): `syn_central_dissection`, `syn_io_rln_monitoring`

---

## 2. Methodology — derivation re-derivation against synoptic-pathology spine

Pattern reference: `qc_framework_v1/migrations/89_path_malignant_table_signoff.sql` (path_malignant precedent) + `reference_synoptic_row_ix.md` (synoptic_row_ix is Script 108 pandas-load-order global index — never synthesize via ROW_NUMBER).

### 2a. Per-col derivation map

- Each `syn_*` col rolls up from the `path_synoptics` source table (or its successor canonical) at patient grain — patient may have multiple synoptic rows (per surgery/specimen), so aggregation rule matters
- For BOOLEAN flags: BOOL_OR per pt
- For numeric morphometry: typically MAX or "from primary specimen" — verify the build SSOT
- `syn_*_clean` cols: cleaned/normalized version of the raw synoptic field; verify the cleaning rule
- `syn_*_raw_str`: raw synoptic text passthrough

### 2b. ⚠️ Watch for VARCHAR-typed measurements

mig_144 uncovered `us_*_volume_ml` was VARCHAR with embedded units (e.g. `"7.9 mL"`). Same risk here — `syn_isthmus_size_cm` / `syn_*_weight_g` etc. may be VARCHAR with `" cm"` / `" g"` suffix instead of DOUBLE. **Verify data_type in information_schema.columns; if VARCHAR, sample 5 rows and decide retype-vs-CF (CF-mig149-SYN-MEASUREMENT-VARCHAR-RETYPE).**

### 2c. ⚠️ findings vs staging (Logan-ratified)

Per `feedback_findings_vs_staging.md`: synoptic findings are primary; staging cols (e.g., AJCC t-implications) follow findings. `syn_capsular_invasion_clean` and `syn_lymphatic_invasion_clean` are findings — they should NOT be inferred from staging cols.

### 2d. ⚠️ Cohort-uniformity sanity check (CRITICAL)

For every BOOLEAN col flipped: §2c sweep. Watch for near-uniform-FALSE (helper artifacts) AND near-uniform-TRUE. Common-finding BOOLEANs that should have signal:
- `syn_hashimoto` should be 10-30% TRUE (hashimoto coexists with thyroid cancer in ~20% pts)
- `syn_chronic_thyroiditis` similar
- `syn_capsular_invasion_clean` should match path_malignant's capsular invasion rate (~15-25%)
- `syn_multinodular_goiter` ~ 30-50%

Flag any BOOLEAN with TRUE-count = 0 OR > 99%.

### 2e. ⚠️ Calendar-only dates

If any `syn_*_date` cols exist (probably not in this set), confirm DATE typing.

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/149_patient_master_synoptic_pathology_cluster_signoff_20260429.sql`

```
batch_id = 'mig_149_patient_master_synoptic_pathology_cluster_20260429'
verification_method options:
  - 'derivation_vs_path_synoptics_source'
  - 'patient_level_aggregate_synoptic_per_specimen'
  - 'derivation_vs_canonical_path_malignant_events_v1'
  - 'extraction_faithfulness_synoptic_cleaned_field'
  - 'auto_passthrough_raw_str'
```

---

## 3. Acceptance gates

- ~32 cols flipped
- 0 drift on derivation re-derivation
- VARCHAR-vs-DOUBLE measurement check completed (CF if dirty)
- Cohort-uniformity sweep clean on BOOLEANs (no degenerate-FALSE OR near-uniform-TRUE)
- gate 4 = 0
- PM `n_verified` advances by exactly the cluster count

---

## 4. Don't touch (active parallel lanes)

- mig_142 RAI PM cluster (BLOCKED on mig_148 RAI upstream)
- mig_145 CT, mig_146 MRI+PET, mig_147 nucmed (in flight)
- mig_148 RAI upstream (in flight)
- Sibling lanes 40-42 in this 4-prompt batch (parathyroid, postop+TP, meds+radtx)

---

## 5. Reference reading

Required:
- Auto-memory: `reference_synoptic_row_ix.md`
- Auto-memory: `feedback_findings_vs_staging.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Repo: `qc_framework_v1/migrations/89_path_malignant_table_signoff.sql`
- Repo: `qc_framework_v1/migrations/120_path_rollup_pair_signoff.sql`

---

## 6. File / commit conventions

Same as siblings: surgical git add, single commit, calendar-DATE for dates, VARCHAR retype if dirty.

---

## 7. If something unexpected surfaces

- VARCHAR measurement cols with embedded units → flag CF-mig149-SYN-MEASUREMENT-VARCHAR-RETYPE; do NOT silently retype in-lane (ALTER TABLE is structural — needs Logan go)
- syn_io_rln_monitoring overlaps with operative/RLN cluster from earlier lanes → confirm not double-counted
- syn_*_clean cleaning rule undocumented → STOP, ask Logan
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 39 of 4-prompt batch (target: PM `n_verified` advances by 32).
