# Cursor Agent Task — `canonical_patient_master` CLINICAL-RESIDUAL CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_142b)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting
**Estimated effort:** 3-4 hours (~60 cols)
**Run order:** Lane 46 of next 4-prompt batch (mig_157)

---

## 0. Cleanliness & safety preamble (MUST READ)

Read §0 of the pathology_invasion prompt — same governance rules. Lane 46-specific risk: this lane spans multiple thematic clinical domains (surgery NLP, diagnosis, demographics, gland morphometry, voice/wound, post-op labs residual, histology). **Each sub-block has a distinct SSOT** — do not collapse the methodology string into a one-size-fits-all `derivation_vs_canonical_path_malignant`. Match the methodology to the actual upstream for each sub-block.

---

## 1. Goal

Verify the **clinical-residual cluster** — 60 unverified cols on `canonical_patient_master` covering symptom NLP (`sx_*`), diagnosis, demographics provenance, gland weight morphometry, lateral neck dissection v10, voice/wound clinical outcomes, post-op lab residual (calcium nadir, TSH suppressed, Tg/TgAb), histology variants/aggressive flags, and first/last clinical event dates.

### 1a. Pre-flight probe (must return exactly 60)

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    -- Symptom NLP (5 cols)
    'sx_llm_extraction_method','sx_llm_mean_confidence','sx_llm_n_source_notes',
    'sx_nlp_any_symptom_data','sx_nlp_dysphagia','sx_nlp_dyspnea','sx_nlp_hoarseness','sx_nlp_neck_mass',
    -- Diagnosis (4 cols)
    'diagnosis_confidence','diagnosis_full','diagnosis_primary','diagnosis_variant',
    -- Demographics provenance (2 cols)
    'demo_confidence','demo_source',
    -- First/last clinical dates (8 cols)
    'first_recurrence_date','first_recurrence_days_from_surg','first_tg_date','first_tg_days_from_surg',
    'last_contact_date','last_contact_days_from_surg','last_contact_source','last_tg_date','last_tg_days_from_surg',
    -- Earliest complication (1 col)
    'earliest_complication_days',
    -- Gland morphometry (7 cols)
    'gland_weight_combined_g','gland_weight_final_g','gland_weight_isthmus_g',
    'gland_weight_left_lobe_g','gland_weight_right_lobe_g','gland_weight_source','gland_weight_total_reported_g',
    -- Lateral neck dissection v10 (4 cols)
    'lateral_detection_method','lateral_levels_v10','lateral_side_v10','lateral_source_v10',
    -- Voice / wound (4 cols)
    'voice_data_confidence','voice_outcome_category','wound_infection_status',
    'concern_highest_tier',
    -- Post-op lab residual (8 cols)
    'calcium_nadir','calcium_nadir_30d','calcium_nadir_days_postop','calcium_supplement_required',
    'tsh_suppressed_ever','tsh_suppressed_ever_source','tsh_suppressed_ever_threshold_0_5','tsh_suppressed_first_date',
    -- TgAb residual (4 cols)
    'tgab_interference_flag','tgab_last_value','tgab_nadir','tgab_peak',
    -- Histology / aggressive variant (5 cols)
    'aggressive_variant_flag','dominant_nodule_size_cm','dominant_nodule_size_cm_v2',
    'histologic_types_all','histologic_variants_all',
    -- Preop imaging + molecular (3 cols)
    'preop_imaging_size_cm','preop_sweep_genes_found_v11','high_risk_molecular_v7',
    'multifocal_flag_path'
  )
ORDER BY column_name;
```

Confirm count is **exactly 60**. (If you see 59 or 61, investigate before proceeding.)

### 1b. Sub-clusters

- **157a — Symptom NLP (8 cols):** sx_llm_*, sx_nlp_*
- **157b — Diagnosis (4 cols):** diagnosis_*
- **157c — Demographics provenance (2 cols):** demo_*
- **157d — First/last clinical dates (9 cols):** first_recurrence_*, first_tg_*, last_contact_*, last_tg_*
- **157e — Earliest complication (1 col):** earliest_complication_days
- **157f — Gland morphometry (7 cols):** gland_weight_*
- **157g — Lateral neck dissection v10 (4 cols):** lateral_*_v10
- **157h — Voice / wound / concern (4 cols):** voice_*, wound_*, concern_*
- **157i — Post-op calcium + TSH suppressed (8 cols):** calcium_nadir*, tsh_suppressed_*
- **157j — TgAb residual (4 cols):** tgab_*
- **157k — Histology + aggressive variant (5 cols):** aggressive_variant_flag, dominant_nodule_size_cm*, histologic_types_all, histologic_variants_all
- **157l — Preop + molecular residual (4 cols):** preop_imaging_size_cm, preop_sweep_genes_found_v11, high_risk_molecular_v7, multifocal_flag_path

---

## 2. Methodology

### 2a. SSOT pointers (verify each lives in `main` first!)

Pre-check:
```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema='main' AND (
  table_name LIKE 'note_entities_llm_%' OR table_name LIKE 'canonical_%'
) ORDER BY 1;
```

Per sub-block:
- **Symptom NLP (sx_*):** `note_entities_llm_presenting_symptoms` (verified live). Apply `extraction_faithfulness_vs_note_entities_llm_presenting_symptoms`.
- **Diagnosis:** `canonical_path_malignant_patient_rollup_v1` (diagnosis_primary should match `path_malignant.primary_dx`).
- **Demographics:** structured upstream — likely `canonical_patient_demographics_*` if it exists, else passthrough from `patient_master_source` (verify which lives in `main`).
- **First/last dates:**
  - first_recurrence_*: `canonical_recurrence_v1` (mig_123 rebuild). MIN(recurrence_date_resolved).
  - first_tg_/last_tg_: `canonical_labs_thyroglobulin_v1`. MIN/MAX(lab_datetime::DATE).
  - last_contact_: derived from MAX of multiple feeds (notes, labs, imaging) — verify the resolution logic.
- **Earliest complication:** `canonical_complications_events_v1` (verified per `project_complications_events_verified_2026-04-28.md`). MIN(timing_days).
- **Gland morphometry:** structured pathology + canonical_path_malignant.
- **Lateral neck dissection v10:** versioned cleaned cols — verify v10 cleaning rule from build script.
- **Voice / wound:** `canonical_complications_events_v1` (mig_98c voice/nerve, mig_98d seroma) + clinical_notes_long for wound infection.
- **Calcium nadir / TSH suppressed:** `canonical_labs_*` (calcium, tsh) + 30d-postop window logic. Same SSOT as mig_150 postop_low_*_flag (PTH < 15 / Ca < 8.0). Verify alignment.
- **TgAb:** `canonical_labs_thyroglobulin_v1` includes Tg+TgAb (per `project_lab_consolidation_script_347_closeout.md`).
- **Histology:** `canonical_path_malignant_patient_rollup_v1`. `histologic_types_all` and `histologic_variants_all` are STRING_AGG aggregations — verify ordering (per `feedback_string_agg_ordering`).
- **Aggressive variant:** derived from histologic variants list (tall cell, columnar, hobnail, etc.).
- **Preop imaging size:** US imaging master + path size — cross-source.
- **Preop sweep genes v11:** `canonical_molecular_genetics_v2`.
- **High risk molecular v7:** versioned ATA-aligned high-risk molecular profile.
- **multifocal_flag_path:** `canonical_path_malignant_patient_rollup_v1` (multifocal field).

### 2b. ⚠️ Cohort-uniformity sweep (REQUIRED — both directions)

This lane has many BOOLEAN cols. Run sweep on every one. Watch for:
- `sx_nlp_*` symptoms: should be MIXED with reasonable rates (e.g., dysphagia 5-15%, hoarseness 3-10%, neck_mass 30-50% as presenting).
- `tsh_suppressed_ever*` / `tsh_suppressed_first_date`: should match the underlying lab logic; presence flag (Type-A) likely.
- `aggressive_variant_flag`: 5-15% TRUE expected.
- `multifocal_flag_path`: 30-50% TRUE expected.
- `tgab_interference_flag`: 5-25% TRUE.

### 2c. ⚠️ Date-type policy (CRITICAL)

ALL of these MUST be DATE (clinical event dates per `feedback_clinical_dates_calendar_only.md`):
- first_recurrence_date
- first_tg_date
- last_contact_date
- last_tg_date
- tsh_suppressed_first_date

If any are VARCHAR or TIMESTAMP, retype-or-CF. mig_146b/147b precedent for VARCHAR-with-date-name retype.

### 2d. ⚠️ VARCHAR-with-units retype audit

`gland_weight_*_g` should be DOUBLE. `dominant_nodule_size_cm*` should be DOUBLE. `preop_imaging_size_cm` should be DOUBLE. `calcium_nadir*` numeric. `tgab_*` numeric except interference_flag (BOOLEAN).
Flag any VARCHAR with embedded units; apply mig_144b retype pattern.

### 2e. ⚠️ Cross-source spot-check (REQUIRED)

- Pick 5 rids with `first_recurrence_date IS NOT NULL`. Verify against canonical_recurrence_v1 MIN(recurrence_date_resolved).
- Pick 5 rids with `aggressive_variant_flag=TRUE`. Verify path_malignant.histologic_variant ladder.
- Pick 5 rids with `tsh_suppressed_ever=TRUE`. Verify underlying TSH lab values.
- Pick 5 rids with `gland_weight_total_reported_g > 0`. Verify against pathology gland weight in path_synoptics or path_gland.

### 2f. Single-value placeholder audit

VARCHAR provenance cols likely 1 distinct (e.g. `demo_source`, `gland_weight_source`, `lateral_source_v10`, `last_contact_source`, `tsh_suppressed_ever_source`, `sx_llm_extraction_method`). Apply `CF-mig157-VALUE-DEGENERATE-UPSTREAM-<col>` informational notes.

---

## 3. Sign-off SQL

File: `qc_framework_v1/migrations/157_patient_master_clinical_residual_cluster_signoff_20260429.sql`

```
batch_id = 'mig_157_patient_master_clinical_residual_cluster_20260429'
verification_method options:
  extraction_faithfulness_vs_note_entities_llm_presenting_symptoms (sx_*)
  derivation_vs_canonical_recurrence_v1 (first_recurrence_*)
  derivation_vs_canonical_labs_thyroglobulin_v1 (first_tg_/last_tg_/tgab_)
  derivation_vs_canonical_labs_<analyte>_v1 (calcium / TSH)
  derivation_vs_canonical_complications_events_v1 (earliest_complication_days, voice_*, wound_*)
  derivation_vs_canonical_path_malignant_patient_rollup_v1 (diagnosis_*, histologic_*, multifocal_flag_path)
  derivation_vs_canonical_molecular_genetics_v2 (preop_sweep_genes_v11, high_risk_molecular_v7)
  cross_source_resolution_<rule> (last_contact_, gland_weight_final_g w/ source tiebreaker)
  helper_<placeholder>_pending_real_extraction (Type-B/C reclassifications)
```

Sub-blocks (12):
- 157a — Symptom NLP (8 cols)
- 157b — Diagnosis (4 cols)
- 157c — Demographics provenance (2 cols)
- 157d — First/last clinical dates (9 cols)
- 157e — Earliest complication (1 col)
- 157f — Gland morphometry (7 cols)
- 157g — Lateral neck dissection v10 (4 cols)
- 157h — Voice / wound / concern (4 cols)
- 157i — Calcium + TSH suppressed (8 cols)
- 157j — TgAb residual (4 cols)
- 157k — Histology + aggressive variant (5 cols)
- 157l — Preop + molecular residual (4 cols)
- 157m — Resync `canonical_table_signoff_registry_v1`

### 3a. Pre-snapshot block at top

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig157_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig157_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE table_name='canonical_patient_master' AND column_name IN (<60 cols>);
```

---

## 4. Required CFs

- `CF-mig157-COHORT-UNIFORM-FALSE-<col>` — list each near-uniform-FALSE
- `CF-mig157-COHORT-NEAR-UNIFORM-TRUE-<col>` — list each Type-A
- `CF-mig157-VALUE-DEGENERATE-UPSTREAM-<col>` — list each 1-distinct VARCHAR
- `CF-mig157-CLINICAL-DATE-RETYPE` — list any *_date col not DATE-typed
- `CF-mig157-GLAND-WEIGHT-VARCHAR-RETYPE` — open if any *_g cols VARCHAR
- `CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT` — drift between dominant_nodule_size_cm and _v2
- `CF-mig157-CALCIUM-VS-MIG150-CROSS` — cross-validate calcium_nadir family vs mig_150 postop_low_calcium_flag
- `CF-mig157-TSH-SUPPRESSED-VS-MIG150-CROSS` — cross-validate
- `CF-mig157-LAST-CONTACT-MULTI-FEED-RESOLUTION` — document the tiebreaker rule (which feed wins)
- `CF-mig157-AGGRESSIVE-VARIANT-LADDER` — document the ladder (tall cell, hobnail, columnar, etc.)

---

## 5. Apply + verify (Logan-only after Cowork independent verification)

Same as Lane 43 §5. NO MD writes from agent.

---

## 6. Git workflow

```bash
git add qc_framework_v1/migrations/157_patient_master_clinical_residual_cluster_signoff_20260429.sql
git -c user.name="Logan Glosser" -c user.email="logan.glosser@gmail.com" commit -m "qc: mig_157 CPM clinical-residual cluster sign-off (60 cols)"
git push origin main
```

---

## 7. Done definition

- [ ] Pre-flight probe returns exactly 60
- [ ] All 60 cols flipped (verified or na)
- [ ] Methodology string matched to the correct LIVE-in-`main` SSOT for each sub-block
- [ ] Cohort-uniformity sweep documented for every BOOLEAN
- [ ] All 5 *_date cols verified DATE-typed (or CF-DATE-RETYPE opened)
- [ ] All numeric measurement cols verified DOUBLE (or VARCHAR-RETYPE CF opened)
- [ ] Cross-validation against mig_150 calcium/TSH cluster documented
- [ ] Cross-validation against canonical_recurrence_v1 / canonical_complications_events_v1 documented
- [ ] Pre-snapshot created in archive_pub_v1_0
- [ ] No verification_method strings name dead/archived tables
- [ ] SQL file committed + pushed; NO MD writes from agent
