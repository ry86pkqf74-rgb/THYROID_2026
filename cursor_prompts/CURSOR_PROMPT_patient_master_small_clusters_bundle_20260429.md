# Cursor Agent Task — `canonical_patient_master` SMALL CLUSTERS BUNDLE (FNA + Demographics + Frozen Section + Staging) (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_136 PMH+PSH landing)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~30 cols across 4 small clusters)
**Run order:** Lane 32 of new 4-prompt batch (mig_143)

---

## 1. Goal

Continue patient_master verification with a **bundle of 4 small clusters** that share the property of being low-risk, well-anchored against verified canonicals, and small enough that solo-lane treatment would be inefficient. Total ~30 cols across:

- **FNA cluster** (~12 cols) — Bethesda categories, FNA-path concordance, FNA pathway status
- **Demographics cluster** (~10 cols) — sex, race, AGES score, BMI sources
- **Frozen Section cluster** (~5 cols) — frozen-section flags + syn/path SSOT pointers
- **Staging cluster** (~3 cols) — AJCC 7/8 calculability + missing components

Probe scope (expect total ~30 cols across 4 buckets):

```sql
SELECT column_name, data_type,
  CASE
    WHEN column_name LIKE 'fna_%' OR column_name ILIKE '%bethesda%' THEN 'fna'
    WHEN column_name LIKE 'ages_%' OR column_name LIKE 'bmi_%' OR column_name IN ('race','sex','weight_kg_note') THEN 'demographics'
    WHEN column_name LIKE 'frozen%' OR column_name ILIKE '%frozen_section%' THEN 'frozen_section'
    WHEN column_name LIKE 'ajcc%' THEN 'staging'
  END AS bucket
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (
        column_name LIKE 'fna_%' OR column_name ILIKE '%bethesda%'
     OR column_name LIKE 'ages_%' OR column_name LIKE 'bmi_%' OR column_name IN ('race','sex','weight_kg_note')
     OR column_name LIKE 'frozen%' OR column_name ILIKE '%frozen_section%'
     OR column_name LIKE 'ajcc%'
  )
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY bucket, column_name;
```

Confirm bucket counts match: fna=12, demographics=10, frozen_section=5, staging=3. If discrepancy, document and proceed with actual counts.

---

## 2. Methodology — derivation re-derivation per cluster

Process all 4 clusters in **a single mig_143 commit**. Use distinct `verification_method` values per cluster so the registry preserves cluster identity within a single batch_id.

### 2a. FNA cluster (12 cols) — derive against canonical_fna_events_v1 (mig_96, 38/38 verified)

Cols: `fna_bethesda_confidence`, `fna_bethesda_final`, `fna_bethesda_source`, `fna_confidence`, `fna_path_concordance_category`, `fna_path_concordant`, `fna_path_outcome`, `fna_pathway_status`, `n_bethesda_calculated_fnas`, `n_bethesda_number_only_fnas`, `worst_bethesda_num`, `worst_bethesda_source`

Derivation map:
- `fna_bethesda_final` / `worst_bethesda_num` → MAX(bethesda_category) per pt from canonical_fna_events_v1; remember `fna_date_resolved` 2-digit YY → 20YY rule
- `fna_bethesda_confidence` / `fna_bethesda_source` → from FNA event with worst Bethesda
- `n_bethesda_calculated_fnas` → COUNT WHERE bethesda_method='calculated' per pt
- `n_bethesda_number_only_fnas` → COUNT WHERE bethesda_method='number_only' per pt
- `fna_path_concordance_category` / `fna_path_concordant` → cross-walk FNA Bethesda → final pathology histology; SSOT rule (Bethesda VI vs final benign = discordant, etc.)
- `fna_path_outcome` → categorical join FNA + path outcome
- `fna_pathway_status` → derived workflow status

`verification_method='derivation_vs_canonical_fna_events_v1'` for direct mappings; `'derivation_fna_path_concordance_chain'` for concordance cols.

### 2b. Demographics cluster (10 cols) — derive against cohort/clinical_master sources

Cols: `ages_calculable_flag`, `ages_score`, `bmi_combined`, `bmi_missingness_reason`, `bmi_note_extracted`, `bmi_note_source`, `bmi_source`, `race`, `sex`, `weight_kg_note`

Derivation map:
- `sex` / `race` → from clinical_master / cohort definition (1:1 passthrough); audit for normalization (sex IN ('M','F','MALE','FEMALE','m','f') — should be SSOT enum)
- `bmi_combined` / `bmi_source` / `bmi_note_*` / `bmi_missingness_reason` → multi-source BMI derivation; check existing PM build SSOT for hierarchy (anesthesia_bmi > clinic_bmi > note_bmi)
- `weight_kg_note` → NLP-extracted from notes; cross-source confidence
- `ages_score` (ATA AGES staging) → derived from age + grade + extent + size; arithmetic check: AGES = 0.05 × age (if ≥40) + 1 (if grade 2-3) + 1 (if extrathyroidal) + 3 (if metastatic) + 0.2 × tumor_size_cm — verify formula matches existing PM build
- `ages_calculable_flag` → BOOL: all 4 input components non-null

`verification_method='derivation_vs_clinical_master_demographics'` / `'derivation_ages_arithmetic'` / `'derivation_bmi_hierarchy'`.

### 2c. Frozen Section cluster (5 cols) — derive against canonical_frozen_section_events_v1 (mig_100, 31/31 verified)

Cols: `frozen_any_performed_flag`, `frozen_n_total`, `frozen_source_hierarchy`, `syn_frozen_section`, `syn_frozen_section_result`

Derivation map:
- `frozen_any_performed_flag` → BOOL: any frozen_section_events row per pt
- `frozen_n_total` → COUNT per pt
- `frozen_source_hierarchy` → categorical: which source (op_note > path_synoptic > path_full)
- `syn_frozen_section` / `syn_frozen_section_result` → from synoptic-pathology slice of the frozen-section chain

`verification_method='derivation_vs_canonical_frozen_section_events_v1'`.

### 2d. Staging cluster (3 cols)

Cols: `ajcc7_missing_components`, `ajcc8_calculable_flag`, `ajcc8_missing_components`

Derivation map:
- `ajcc8_calculable_flag` → BOOL: T + N + M + age_at_dx all non-null
- `ajcc7_missing_components` / `ajcc8_missing_components` → STRING_AGG(missing_component) per pt; list_sort for set-equal probes

`verification_method='derivation_ajcc_calculability_check'`.

### 2e. ⚠️ Logan-ratified 2-digit year (FNA dates)

Per `reference_2digit_year_convention.md`: `canonical_fna_events_v1.fna_date_resolved` carries 2-digit-year content where YY → 20YY. PM-side cols deriving from fna_date_resolved should already inherit this — verify spot-check.

### 2f. ⚠️ Cohort-uniformity sanity check (CRITICAL)

Run §2d sweep on every BOOLEAN flipped. Expected:
- `fna_path_concordant` should have non-zero TRUE and FALSE
- `frozen_any_performed_flag` should be ~30-50% of cohort
- `ajcc8_calculable_flag` should be high (~80-90%)
- `ages_calculable_flag` similar
- `sex` / `race` should have meaningful distributions (not 100% one value)

### 2g. Sign-off SQL

File: `qc_framework_v1/migrations/143_patient_master_small_clusters_bundle_signoff_20260429.sql`

```
batch_id = 'mig_143_patient_master_small_clusters_bundle_20260429'
```

Use sub-cluster suffix in `verification_method` so the registry preserves bucket identity.

---

## 3. Acceptance gates

- ~30 cols flipped across 4 buckets (fna=12, demographics=10, frozen=5, staging=3)
- 0 drift on derivation re-derivation
- Cohort-uniformity sweep clean
- Sex / race have meaningful distributions (not single-value); CF if otherwise
- gate 4 = 0
- PM `n_verified` advances by exactly the cluster count

---

## 4. Don't touch (active parallel lanes)

- MOLECULAR — Lane 27 (mig_137, ~3 cols)
- RECURRENCE-RESPONSE — Lane 28 (mig_138, ~4 cols)
- ETE — Sibling Lane 29 (mig_140)
- SURVIVAL — Sibling Lane 30 (mig_141)
- RAI — Sibling Lane 31 (mig_142)

---

## 5. Reference reading

Required:
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `reference_2digit_year_convention.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md`
- Auto-memory: `project_frozen_section_mig_100_closeout.md`
- Auto-memory: `project_path_gland_family_complete_2026-04-28.md` (frozen-section close-out paired)
- Repo: `qc_framework_v1/migrations/96_fna_days_to_surgery_recompute.sql`
- Repo: `qc_framework_v1/migrations/100_frozen_section_signoff.sql` (or similar mig_100 file)

---

## 6. File / commit conventions

Same as siblings. Single commit closing the bundle. Surgical git add. DATE-typed clinical dates.

---

## 7. If something unexpected surfaces

- AGES formula doesn't match existing PM build → STOP, ask Logan; the formula has historical variants (AMES vs AGES vs MACIS)
- BMI source hierarchy SSOT undocumented → STOP, ask Logan
- `race` col uses raw EHR codes instead of SSOT enum (Hispanic/Latino vs ethnicity, etc.) → flag for normalization CF, do not fail
- AJCC missing-components STRING_AGG ordering inconsistent → use list_sort for set-equal probe (per `feedback_no_crossdomain_linkage_ids.md` set-equal pattern)
- More than 3 cols (across all 4 buckets) can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 32 of 4-prompt batch (target: PM `n_verified` 881 → 911 ≈ 57% complete).

After this batch lands, the next priority clusters per handoff §9C are: imaging (~126 cols, may need sub-clustering by modality CT/MRI/PET/US) → Other / residual (~446 cols, needs further sub-clustering investigation).
