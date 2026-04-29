# Cursor Agent Task — `canonical_patient_master` PARATHYROID + POSTOP + TP CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Estimated effort:** 2-3 hours (~38 cols)
**Run order:** Lane 40 of next 4-prompt batch (mig_150)

---

## 1. Goal

Continue patient_master verification with a **3-bucket bundle** (~38 cols):

- **Parathyroid** (~16 cols): `para_*` + `pth_*` (post-op PTH labs)
- **Postop hospital course** (~12 cols): `postop_*`
- **Thyroid-perioperative (TP)** (~10 cols): `tp_*`

Probe scope:

```sql
SELECT column_name, data_type,
  CASE
    WHEN column_name LIKE 'para_%' OR column_name LIKE 'pth_%' OR column_name LIKE 'parathyroid_%' THEN 'parathyroid'
    WHEN column_name LIKE 'postop_%' THEN 'postop'
    WHEN column_name LIKE 'tp_%' THEN 'tp'
  END AS bucket
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name LIKE 'para_%' OR column_name LIKE 'pth_%' OR column_name LIKE 'parathyroid_%'
       OR column_name LIKE 'postop_%' OR column_name LIKE 'tp_%')
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY bucket, column_name;
```

Confirm count is **~38** (16 parathyroid + 12 postop + 10 tp). Document if discrepant.

---

## 2. Methodology — derivation re-derivation per bucket

### 2a. Parathyroid bucket

Anchor canonicals: `canonical_parathyroid_events_v1` (mig_102 verified) + `canonical_parathyroid_patient_rollup_v1` (mig_106 verified) + lab canonicals for PTH.

Per-col derivation:
- `para_n_glands_identified` / `_excised` / `_biopsied` → COUNT per pt from parathyroid_events
- `para_max_cellularity_pct` / `_min_cellularity_pct` → MAX/MIN per pt
- `para_max_gland_weight_g` → MAX per pt
- `para_has_pathologic_glands` → BOOL_OR per pt
- `para_abnormality_type` → STRING_AGG distinct types per pt; list_sort
- `para_specimen_included` → BOOL: parathyroid in any thyroid specimen
- `para_removal_intent` / `_incidental_status_refined` → categorical per-pt rollup
- `pth_nadir` / `pth_nadir_30d` → MIN(pth_value) per pt within window from `canonical_labs_pth_v1` (verify upstream exists; check `project_lab_consolidation_script_347_closeout.md`)
- `pth_nadir_days_postop` → days from `pth_nadir` measurement to first_surgery_date

Methods: `derivation_vs_canonical_parathyroid_events_v1`, `derivation_vs_canonical_parathyroid_patient_rollup_v1`, `derivation_vs_canonical_labs_pth_v1`, `patient_level_aggregate_parathyroid_per_gland`.

### 2b. Postop bucket

Anchor canonicals: `canonical_complications_events_v1` (mig_99 verified) + lab canonicals.

Likely cols include `postop_low_pth_flag`, `postop_pth_min_days_postop`, `postop_pth_min_value`, `postop_pth_n_measurements`, `postop_pth_source_reliability`. Some overlap with parathyroid bucket — DON'T DOUBLE-VERIFY a col that's already in para bucket.

Per-col derivation:
- `postop_pth_min_value` → MIN(pth) within postop window from labs_pth_v1
- `postop_pth_min_days_postop` → days from min_pth measurement to first_surgery_date
- `postop_low_pth_flag` → BOOL: pth < 12 pg/mL within window (verify threshold SSOT)
- `postop_pth_n_measurements` → COUNT measurements within window per pt

Methods: `derivation_vs_canonical_labs_pth_v1`, `postop_window_aggregate`.

### 2c. TP (thyroid-perioperative) bucket

Anchor: depends on what `tp_*` cols actually contain — probe first. Likely thyroid-hormone perioperative state (TSH/T3/T4 around surgery).

Methods: depends on probe; likely `derivation_vs_canonical_labs_tsh_v1` etc.

### 2d. ⚠️ Cohort-uniformity sanity check

Every BOOLEAN col flipped: §2c sweep. Watch for near-uniform-FALSE OR -TRUE. Hypoparathyroidism rate (postop_low_pth_flag) is typically 10-30% transient, 1-5% permanent — flag if > 50% or < 1%.

### 2e. ⚠️ Date types

`pth_nadir_days_postop` is INTEGER (days), not date. But check any `*_date` cols. CF-mig150-DATE-RETYPE if violation.

### 2f. ⚠️ Cross-validate vs complications mig_98f hypoparathyroidism + mig_98g hypocalcemia

Per `project_complications_events_verified_2026-04-28.md`: complications canonical has hypoparathyroidism (mig_98f) and hypocalcemia_clinical (mig_98g) sub-mig closeouts. Post-op low PTH should align with hypoparathyroidism finding_status='present'. Document any drift.

### 2g. Sign-off SQL

File: `qc_framework_v1/migrations/150_patient_master_parathyroid_postop_tp_cluster_signoff_20260429.sql`

```
batch_id = 'mig_150_patient_master_parathyroid_postop_tp_cluster_20260429'
```

---

## 3. Acceptance gates

- ~38 cols flipped (16+12+10 = 38)
- Cross-validation between parathyroid bucket and postop hypoparathyroidism logic
- Cohort-uniformity sweep clean
- gate 4 = 0
- PM `n_verified` advances by cluster count

---

## 4. Don't touch (active parallel lanes)

- mig_142 RAI (BLOCKED), mig_145/146/147 imaging (in flight), mig_148 RAI upstream (in flight)
- Sibling lanes 39, 41, 42 in this batch

---

## 5. Reference reading

Required:
- Auto-memory: `project_parathyroid_events_mig_102_closeout.md`
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md`
- Auto-memory: `project_complications_events_verified_2026-04-28.md` (mig_98f hypoparathyroidism, mig_98g hypocalcemia_clinical)
- Auto-memory: `project_lab_consolidation_script_347_closeout.md`
- Auto-memory: `feedback_clinical_dates_calendar_only.md`

---

## 6. File / commit conventions

Same as siblings.

---

## 7. If something unexpected surfaces

- `tp_*` cols ambiguous semantics → STOP, probe Tier-1 source first; ask Logan
- Hypoparathyroidism rate disagrees with complications mig_98f by >5% → flag CF-mig150-PARA-HYPOPARA-COMPL-DRIFT
- Postop PTH threshold (12 pg/mL standard, but some institutions use 10 or 15) undocumented → STOP, ask Logan
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 40 of 4-prompt batch.
