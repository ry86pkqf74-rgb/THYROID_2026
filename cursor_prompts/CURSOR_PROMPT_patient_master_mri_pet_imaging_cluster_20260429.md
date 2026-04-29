# Cursor Agent Task — `canonical_patient_master` MRI + PET IMAGING CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Estimated effort:** 3-4 hours (~49 cols — biggest of the imaging batch)
**Run order:** Lane 36 of new 4-prompt batch (mig_146)

---

## 1. Goal

Continue patient_master verification with the **MRI + PET imaging cluster** (~49 unverified cols covering MRI exam metadata + thyroid/LN/vocal-cords findings, and PET FDG-avidity + distant metastasis + radiotracer + SUV measurements).

Probe scope:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name LIKE 'mri\_%' ESCAPE '\' OR column_name LIKE 'pet\_%' ESCAPE '\')
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY column_name;
```

Confirm count is **exactly 49** (25 MRI + 24 PET) before proceeding.

Sub-clusters:

- **MRI exam metadata** (~7 cols): `mri_first_date`, `mri_first_days_from_surg`, `mri_last_date`, `mri_last_days_from_surg`, `mri_n_exams`, `mri_contrast_used_any`, `mri_exam_type_first`, `mri_indication_first`
- **MRI findings** (~10 cols): `mri_has_data`, `mri_has_dimensions`, `mri_has_dominant_nodule`, `mri_impression_first`, `mri_impression_last`, `mri_key_findings_last`, `mri_recommendation_last`, `mri_thyroid_assessment_worst`, `mri_thyroid_enlarged_any`, `mri_thyroid_nodule_any`
- **MRI LN + invasion + substernal** (~5 cols): `mri_ln_mentioned_any`, `mri_pathologic_ln_any`, `mri_mass_effect_any`, `mri_substernal_any`, `mri_substernal_extension_any`
- **MRI vocal cords** (~2 cols): `mri_vocal_cords_described`, `mri_vocal_cords_normal`
- **PET exam metadata** (~10 cols): `pet_first_date`, `pet_last_date`, `pet_n_exams`, `pet_radiotracer_primary`, `pet_indication_first`, `pet_impression_last`, `pet_other_*` family for non-FDG tracers
- **PET FDG-avidity + mets** (~6 cols): `pet_fdg_avid_cervical_ln_ever`, `pet_fdg_avid_thyroid_bed_ever`, `pet_distant_met_sites`, `pet_distant_mets_ever`, `pet_overall_worst`, `pet_has_data`
- **PET SUV quantitative** (~2 cols): `pet_suv_max_cervical_ln`, `pet_suv_max_thyroid_bed`

---

## 2. Methodology

### 2a. ⚠️ Upstream dependency check (FIRST STEP)

Verify MRI + PET upstream canonicals exist:
- `canonical_mri_*` family — check `information_schema.tables`
- `canonical_pet_*` family
- Tier-1 LLM: `note_entities_llm_imaging_mri_*` / `note_entities_llm_imaging_pet_*`
- `manuscript_workspace.imaging_mri_*` / `imaging_pet_*` helpers

If no verified canonical, derive from raw exam tables / NLP entities. Document SSOT chain in each col's `verification_method`.

### 2b. Per-col derivation map (representative)

- `mri_first_date` / `mri_last_date` / `mri_n_exams` → MIN/MAX/COUNT per pt
- `mri_thyroid_*_any` → BOOL_OR over per-exam findings
- `mri_pathologic_ln_any` / `mri_substernal_*` / `mri_mass_effect_any` → BOOL_OR
- `mri_vocal_cords_normal` → BOOL: at least one MRI noted "vocal cords normal"
- `mri_thyroid_assessment_worst` → max severity per pt across MRI exams (severity ladder: normal < indeterminate < suspicious < malignant)
- `pet_radiotracer_primary` → most common tracer ('FDG' / '18F-DOPA' / 'Ga-68 DOTATATE' / etc.)
- `pet_fdg_avid_cervical_ln_ever` / `pet_fdg_avid_thyroid_bed_ever` → BOOL_OR
- `pet_distant_mets_ever` → BOOL_OR
- `pet_distant_met_sites` → STRING_AGG distinct sites per pt; list_sort
- `pet_suv_max_cervical_ln` / `pet_suv_max_thyroid_bed` → MAX SUV per pt
- `pet_overall_worst` → severity ladder per pt
- `pet_other_*` → non-FDG tracer subset (medullary cancer scans, dotatate, etc.)

### 2c. ⚠️ Cohort-uniformity sanity check (CRITICAL)

For every BOOLEAN flipped: §2c sweep. Expected:
- `mri_thyroid_nodule_any` should be moderate-low (most thyroid imaging is US, not MRI)
- `mri_vocal_cords_normal` should be substantial when MRI was performed
- `pet_distant_mets_ever` should be rare (~ 5-10% in advanced disease subset)
- `pet_fdg_avid_thyroid_bed_ever` should be moderate (post-thyroidectomy fdg-uptake)
- `mri_n_exams=NULL` / `pet_n_exams=NULL` should be HIGH (most pts don't get MRI/PET)

Flag any near-uniform BOOLEAN. Most MRI/PET cols will have NULL for ~70-90% of cohort — that's expected, not degenerate.

### 2d. ⚠️ NULL semantics

For 70-90% of cohort, MRI/PET cols are NULL (no scan ever performed). NULL ≠ FALSE. Use COALESCE in IS DISTINCT FROM probes only.

### 2e. ⚠️ Calendar-only dates

`mri_first_date` / `mri_last_date` / `pet_first_date` / `pet_last_date` MUST be DATE. CF-mig146-PM-MRI-PET-DATE-RETYPE if TIMESTAMP/VARCHAR.

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/146_patient_master_mri_pet_imaging_cluster_signoff_20260429.sql`

```
batch_id = 'mig_146_patient_master_mri_pet_imaging_cluster_20260429'
verification_method options:
  - 'derivation_vs_canonical_mri_<source>_v1'
  - 'derivation_vs_canonical_pet_<source>_v1'
  - 'extraction_faithfulness_vs_note_entities_llm_imaging_mri'
  - 'extraction_faithfulness_vs_note_entities_llm_imaging_pet'
  - 'patient_level_aggregate_per_exam'
  - 'pet_suv_quantitative_max_per_pt'
  - 'severity_ladder_aggregate_worst'
```

---

## 3. Acceptance gates

- ~49 cols flipped
- Upstream dependency confirmed; CF-PENDING opened if unverified
- Cohort-uniformity sweep clean on every BOOLEAN
- All MRI/PET date cols are DATE
- gate 4 = 0
- PM `n_verified` advances by exactly the cluster count

---

## 4. Don't touch (active parallel lanes)

- RAI mig_142, SmallClusters mig_143 — in flight
- US+imaging-generic mig_144, CT mig_145, Nucmed mig_147 — sibling lanes

---

## 5. Reference reading

Required:
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md`
- Auto-memory: `feedback_extraction_faithfulness_llm_canonical.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Repo: `qc_framework_v1/migrations/117_us_v2_family_signoff.sql` (imaging precedent)

---

## 6. File / commit conventions

Same as siblings.

---

## 7. If something unexpected surfaces

- No upstream canonical for MRI or PET → STOP, ask Logan; may need a separate upstream-build lane first
- PET radiotracer field has dirty / inconsistent values (e.g., 'fdg' vs 'FDG' vs '18F-FDG') → propose vocab normalization CF
- `pet_suv_max_*` numerical scale ambiguous (SUVmax vs SUVmean) → confirm with Logan; PET SUV semantics are standardized but extraction may not be
- MRI vocal cord findings overlap with mri_vocal_* cols deferred from earlier lanes → check overlap
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 36 of 4-prompt batch (target: PM `n_verified` advances by 49).
