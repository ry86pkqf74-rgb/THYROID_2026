# Cursor Agent Task — `canonical_patient_master` CT IMAGING CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Estimated effort:** 2-3 hours (~29 cols)
**Run order:** Lane 35 of new 4-prompt batch (mig_145)

---

## 1. Goal

Continue patient_master verification with the **CT imaging cluster** (~29 unverified cols covering CT exam metadata, thyroid + LN findings, airway/tracheal involvement, and substernal extension).

Probe scope:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND column_name LIKE 'ct\_%' ESCAPE '\'
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY column_name;
```

Confirm count is **exactly 29** before proceeding.

Sub-clusters:

- **CT exam metadata** (~8 cols): `ct_first_date`, `ct_first_days_from_surg`, `ct_last_date`, `ct_last_days_from_surg`, `ct_n_exams`, `ct_contrast_first`, `ct_exam_type_first`, `ct_indication_first`, `ct_indication_last`
- **CT thyroid findings** (~7 cols): `ct_thyroid_details_last`, `ct_thyroid_enlarged_any`, `ct_thyroid_heterogeneous_any`, `ct_thyroid_nodule_any`, `ct_thyroid_normal_any`, `ct_thyroid_not_visualized_any`, `ct_thyroid_other_abnormality_any`, `ct_thyroid_postsurgical_any`
- **CT lymph node findings** (~6 cols): `ct_largest_ln_short_axis_mm`, `ct_ln_details_last`, `ct_ln_enlarged_any`, `ct_ln_locations_last`, `ct_ln_suspicious_any`, `ct_pathologic_ln_any`
- **CT airway / tracheal** (~5 cols): `ct_airway_comment_last`, `ct_airway_compromise_any`, `ct_substernal_extension_any`, `ct_tracheal_deviation_any`, `ct_tracheal_narrowing_any`
- **CT goiter** (~1 col): `ct_goiter_present_any`

---

## 2. Methodology — derivation against CT upstream chain (verify upstream FIRST)

### 2a. ⚠️ Upstream dependency check (FIRST STEP)

**Before any flips:** verify the CT upstream canonical exists. Likely candidates:
- `canonical_ct_neck_*` or `canonical_imaging_ct_*` family — check `information_schema.tables WHERE table_name LIKE 'canonical_ct%' OR table_name LIKE 'canonical_imaging_ct%'`
- Tier-1 LLM source: `note_entities_llm_imaging_ct_*` or `note_entities_llm_ct_*`
- `manuscript_workspace.imaging_ct_*` helpers

If NO verified canonical exists, derive from raw exam table or NLP entities directly. Document the SSOT chain explicitly in each col's `verification_method`. Open `CF-mig145-CT-UPSTREAM-CANONICAL-PENDING` if upstream is unverified — but proceed if NLP-Tier-1 is the SSOT.

### 2b. Per-col derivation map (representative)

- `ct_first_date` / `ct_last_date` → MIN/MAX exam date per pt
- `ct_n_exams` → COUNT(DISTINCT exam_id) per pt
- `ct_contrast_first` → contrast type/route from earliest exam
- `ct_thyroid_*_any` → BOOL_OR over per-exam thyroid finding flags
- `ct_largest_ln_short_axis_mm` → MAX(short_axis_mm) per pt across LN findings (size threshold ~ 1cm short-axis = pathologic)
- `ct_ln_suspicious_any` → BOOL_OR
- `ct_pathologic_ln_any` → BOOL: short_axis ≥ 1cm OR shape/morphology abnormal
- `ct_airway_compromise_any` → BOOL_OR
- `ct_tracheal_deviation_any` / `ct_tracheal_narrowing_any` → BOOL_OR
- `ct_substernal_extension_any` → BOOL_OR
- `ct_*_details_last` / `ct_*_indication_*` / `ct_*_locations_last` → STRING_AGG most-recent per pt; use list_sort for set-equal probes

### 2c. ⚠️ Cohort-uniformity sanity check (CRITICAL)

For every BOOLEAN flipped: §2c sweep. Expected:
- `ct_thyroid_normal_any` should be high (most CT exams routine)
- `ct_pathologic_ln_any` should be substantially less than 50% (advanced/recurrent disease)
- `ct_airway_compromise_any` rare (< 5%)
- `ct_tracheal_deviation_any` rare to moderate
- `ct_thyroid_not_visualized_any` should be > 0 (post-thyroidectomy CTs)
- `ct_goiter_present_any` should be moderate (5-20%)

Flag any near-uniform-TRUE OR near-uniform-FALSE BOOLEANs.

### 2d. ⚠️ Calendar-only dates

`ct_first_date`, `ct_last_date` MUST be DATE. Open `CF-mig145-PM-CT-DATE-RETYPE` if TIMESTAMP/VARCHAR.

### 2e. ⚠️ NULL vs 0/FALSE

`ct_n_exams` is NULL for patients with no CT (not 0). All `_any` BOOLEANs are NULL for patients with no CT (not FALSE).

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/145_patient_master_ct_imaging_cluster_signoff_20260429.sql`

```
batch_id = 'mig_145_patient_master_ct_imaging_cluster_20260429'
verification_method options:
  - 'derivation_vs_canonical_ct_<source>_v1'
  - 'extraction_faithfulness_vs_note_entities_llm_ct'
  - 'patient_level_aggregate_ct_per_exam'
  - 'cross_validate_ct_pathologic_ln_size_threshold'
```

---

## 3. Acceptance gates

- ~29 cols flipped
- Upstream dependency confirmed before flips (or CF-PENDING opened)
- Cohort-uniformity sweep clean on every BOOLEAN
- Date cols are DATE; CF for violations
- gate 4 = 0
- PM `n_verified` advances by exactly the cluster count

---

## 4. Don't touch (active parallel lanes)

- RAI mig_142, SmallClusters mig_143 — in flight
- US+imaging-generic mig_144 — sibling Lane 34
- MRI+PET mig_146 — sibling Lane 36
- Nucmed mig_147 — sibling Lane 37

---

## 5. Reference reading

Required:
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_extraction_faithfulness_llm_canonical.md`
- Repo: `qc_framework_v1/migrations/117_us_v2_family_signoff.sql` (US precedent for imaging cluster pattern)

---

## 6. File / commit conventions

Same as siblings.

---

## 7. If something unexpected surfaces

- No verified CT canonical exists, NLP entities also missing → STOP, ask Logan; CT cluster may need an upstream-build lane first
- `ct_pathologic_ln_any` derived rule (≥ 1cm short-axis) doesn't match existing PM build → check existing rule SSOT
- Substernal extension semantics ambiguous (mediastinal vs sub-clavicular) → document the rule
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 35 of 4-prompt batch (target: PM `n_verified` advances by 29).
