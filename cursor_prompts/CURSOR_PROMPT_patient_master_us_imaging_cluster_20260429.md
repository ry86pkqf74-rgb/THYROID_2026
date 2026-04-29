# Cursor Agent Task — `canonical_patient_master` US + IMAGING-GENERIC CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~23 cols)
**Run order:** Lane 34 of new 4-prompt batch (mig_144). Sibling lanes: mig_145 CT, mig_146 MRI+PET, mig_147 nucmed.

---

## 1. Goal

Continue patient_master verification with the **US (ultrasound) cluster + cross-modality imaging-generic cluster** (~23 unverified cols).

Probe scope:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name LIKE 'us\_%' ESCAPE '\'
       OR column_name LIKE 'imaging\_%' ESCAPE '\'
       OR column_name = 'prm_imaging_data_completeness')
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY column_name;
```

Confirm count is **exactly 23** before proceeding (12 us_* + 10 imaging_* + 1 prm_imaging_data_completeness).

Sub-clusters:

- **US dates + counts** (~6 cols): `us_first_exam_date`, `us_first_exam_days_from_surg`, `us_last_exam_date`, `us_last_exam_days_from_surg`, `us_most_recent_date`, `us_most_recent_days_from_surg`, `us_n_reports`
- **US gland morphometry** (~4 cols): `us_isthmus_thickness_mm`, `us_left_lobe_volume_ml`, `us_right_lobe_volume_ml`, `us_total_volume_ml`
- **US v2 NLP backfill flag** (~1 col): `us_v2_any_nlp_backfill_pending`
- **Imaging-generic structured nodule cluster** (~10 cols): `imaging_has_isthmus_nodule`, `imaging_has_left_nodule`, `imaging_has_right_nodule`, `imaging_has_structured_components`, `imaging_ln_abnormal`, `imaging_n_nodule_records`, `imaging_nodule_size_cm`, `imaging_nodule_size_cm_source`, `imaging_suspicious_recurrence_flag`, `imaging_suspicious_unconfirmed`
- **PRM imaging completeness** (~1 col): `prm_imaging_data_completeness`

---

## 2. Methodology — derivation re-derivation against verified US v2 family

Pattern reference: `qc_framework_v1/migrations/117_us_v2_family_signoff.sql` + `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md` (date-coercion patterns).

### 2a. Per-col derivation map (representative)

- `us_first_exam_date` / `us_last_exam_date` / `us_most_recent_date` → MIN / MAX / latest from `canonical_us_thyroid_gland_v2` (verified 28/32 mig_117) + `canonical_us_nodule_v2` (verified 53/57 mig_117) + `canonical_us_lymph_node_v2` (verified 23/29 mig_117). Use the union of all 3 US v2 families.
- `us_first_exam_days_from_surg` etc. → `(us_*_date - first_surgery_date)` calendar days
- `us_n_reports` → COUNT(DISTINCT exam_id) per pt across the US v2 union
- `us_left_lobe_volume_ml` / `us_right_lobe_volume_ml` / `us_total_volume_ml` → derive from `canonical_us_thyroid_gland_v2` (gland-level v2)
- `us_isthmus_thickness_mm` → from `canonical_us_thyroid_gland_v2`
- `us_v2_any_nlp_backfill_pending` → BOOL flag for "any US v2 row had nlp_backfill_status='pending'"
- `imaging_has_isthmus_nodule` / `imaging_has_left_nodule` / `imaging_has_right_nodule` → BOOL_OR over US v2 nodule + CT thyroid_details + MRI thyroid_nodule signals; cross-modal aggregate
- `imaging_n_nodule_records` → COUNT across US v2 nodule + structured imaging records
- `imaging_nodule_size_cm` / `_source` → MAX nodule size + which modality reported it
- `imaging_suspicious_recurrence_flag` → BOOL: any imaging modality flagged suspicious-for-recurrence
- `imaging_suspicious_unconfirmed` → from canonical_recurrence_resolved_v1 SSOT enum (per `project_recurrence_resolved_v1_mig_125_closeout.md`); enum is `imaging_only_unconfirmed` not `imaging_only_suspicious`
- `prm_imaging_data_completeness` → derived multi-modality completeness score; check existing PM build SSOT

### 2b. ⚠️ Calendar-only dates (Logan-ratified)

Per `feedback_clinical_dates_calendar_only.md`: clinical event date cols (`us_*_date`) MUST be DATE not TIMESTAMP. Audit existing types; if any TIMESTAMP, open `CF-mig144-PM-US-DATE-RETYPE`.

### 2c. ⚠️ Cohort-uniformity sanity check (CRITICAL)

Per the mig_135 21-degenerate-FALSE incident AND the mig_141 2-near-uniform-TRUE incident: for **every BOOLEAN col flipped** in this lane:

```sql
SELECT
  SUM(CASE WHEN <col> THEN 1 ELSE 0 END) AS n_true,
  SUM(CASE WHEN NOT <col> THEN 1 ELSE 0 END) AS n_false,
  SUM(CASE WHEN <col> IS NULL THEN 1 ELSE 0 END) AS n_null
FROM main.canonical_patient_master;
```

**Flag if n_true OR n_false is < 1% of cohort (degenerate near-uniform):**
- Type A real cohort invariance → keep verified, tag `CF-mig144-COHORT-NEAR-UNIFORM-<col>` informational
- Type B upstream not extracted → flip to `na`, open `CF-mig144-EXPAND-UPSTREAM-<col>`
- Type C helper-script artifact → flip to `na`, document the script SSOT

For US specifically:
- `imaging_has_isthmus_nodule` likely rare (isthmus nodules less common than lobar)
- `imaging_suspicious_recurrence_flag` should align with canonical_recurrence_v1 `recurrence_confirmed=TRUE` count (514 pts post-mig_139)
- `us_v2_any_nlp_backfill_pending` may be 0 if backfill is complete, or substantial — investigate

### 2d. ⚠️ Cross-modal aggregate semantics

`imaging_has_*_nodule` cols aggregate ACROSS modalities (US + CT + MRI). When deriving, do NOT restrict to US only. The 'imaging_' prefix means "any imaging source", not "US-only".

### 2e. ⚠️ NULL vs FALSE caveat

Per `feedback_recurrence_imaging_n_events_null.md`: `us_n_reports` should be NULL for patients with no US (not 0). COALESCE in IS DISTINCT FROM probes.

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/144_patient_master_us_imaging_cluster_signoff_20260429.sql`

```
batch_id = 'mig_144_patient_master_us_imaging_cluster_20260429'
verification_method options:
  - 'derivation_vs_canonical_us_thyroid_gland_v2'
  - 'derivation_vs_canonical_us_nodule_v2'
  - 'derivation_vs_canonical_us_lymph_node_v2'
  - 'derivation_us_v2_union_three_families'
  - 'patient_level_aggregate_us_per_exam'
  - 'cross_modal_imaging_aggregate'
  - 'derivation_vs_canonical_recurrence_resolved_v1' (imaging_suspicious_*)
  - 'prm_rule_imaging_completeness_chain'
```

---

## 3. Acceptance gates

- ~23 cols flipped
- 0 drift on per-col derivation re-derivation
- Cohort-uniformity sweep on every BOOLEAN; flag any near-uniform (<1% TRUE or <1% FALSE)
- All US date cols are DATE; CF rows for any TIMESTAMP/VARCHAR
- gate 4 = 0
- PM `n_verified` advances by exactly the cluster count

---

## 4. Don't touch (active parallel lanes)

- RAI cluster — Lane 31 mig_142 (in flight; touches `nucmed_has_rai_scan` — exclude that col from this lane's nucmed predicates if it leaks)
- SMALL-CLUSTERS bundle — Lane 32 mig_143 (in flight)
- CT cluster — Sibling Lane 35 mig_145
- MRI+PET cluster — Sibling Lane 36 mig_146
- Nucmed cluster — Sibling Lane 37 mig_147

---

## 5. Reference reading

Required:
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `project_recurrence_resolved_v1_mig_125_closeout.md` (imaging_only_unconfirmed enum)
- Repo: `qc_framework_v1/migrations/117_us_v2_family_signoff.sql`

---

## 6. File / commit conventions

Same as siblings: surgical git add, single commit, DATE-typed clinical dates, CAST CURRENT_TIMESTAMP AS TIMESTAMP for build_ts, explicit not_started filter.

---

## 7. If something unexpected surfaces

- Cross-modal aggregates depend on CT/MRI/PET cols not yet verified → derive from raw exam tables OR open dependency CF (`CF-mig144-CROSS-MODAL-UPSTREAM-PENDING`)
- `imaging_suspicious_*` semantics drift from canonical_recurrence_resolved_v1 → check enum strings carefully
- `us_v2_any_nlp_backfill_pending=TRUE` for substantial cohort fraction → flag as known-pending status, not a bug
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 34 of 4-prompt batch (target: PM `n_verified` advances by 23).
