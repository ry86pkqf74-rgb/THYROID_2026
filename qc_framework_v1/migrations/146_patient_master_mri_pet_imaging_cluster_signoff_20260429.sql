-- =============================================================================
-- Migration 146 — canonical_patient_master MRI + PET IMAGING CLUSTER sign-off (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   36 (mig_146) — ~49 MRI + PET thematic columns on
--         main.canonical_patient_master derived from structured exam tables plus
--         Script 216b Claude PET/MRI rollup and Script 219 PET-Other regex recovery.
-- batch_id: mig_146_patient_master_mri_pet_imaging_cluster_20260429
--
-- Pre-apply probes (MotherDuck RW thyroid_canonical_publication_v1_0, 2026-04-29):
--   * Predicate cardinality (registry anti-join for already-verified cols): exactly
--     **49** columns (25 **`mri_%`** + 24 **`pet_%`**) satisfying the Lane-36 probe.
--   * Cohort parity: `canonical_patient_master` = **10,871** rows / distinct rid
--     (`scripts._md_connect.connect_locked` integrity check passed).
--
-- Upstream dependency (§2a)
-- ─────────────────────────────────────────────────────────────────────────────
--   * Verified **no** `main.canonical_mri_*` / `main.canonical_pet_*` canonical exam
--     grains in-scope on publication DB (information_schema probe = 0).
--     SSOT chains:
--       - MRI structured + first-exam scalars → **`main.mri_imaging`** (715 exams);
--         lineage `scripts/216_data_gap_resolution.py` ingest + rollup patterns;
--         `scripts/219_imaging_gap_resolution.py` MRI indication reconciliation.
--       - MRI impressions / thyroid_assessment worst / key-findings /
--         recommendation last → **`scripts/216b_llm_extraction.py` B2** outputs merged
--         onto patient master (no **`note_entities_llm_imaging_*`** persistence in
--         publication DB → **CF-mig146-IMAGING-LLM-NO-PERSISTED-ENTITIES**, informational).
--       - PET FDG / SUV / tracer / worst-assessment rollup → **`main.ct_imaging`**
--         PET-filter (`exam_type_normalized LIKE '%pet%'`) + **`216b` B1** LLM JSON
--         aggregate per patient.
--       - PET “Other” regex recovery / non-FDG exam family → **`219` Task 2**
--         `_pet_other_recovered_v1` → `_pet_other_rollup_v1` → `pet_other_*` CPM cols
--         (DATE-typed `pet_other_first_date` / `pet_other_last_date`).
--   * **CF-mig146-UPSTREAM-CANONICAL-PENDING** (informational): future
--     `canonical_mri_exam_v1` / `canonical_pet_exam_v1` row-grain tables would
--     simplify exam-level regression tests; **not** blocking patient-level sign-off.
--
-- Date policy (§2e) + carry-forwards
-- ─────────────────────────────────────────────────────────────────────────────
--   * `mri_first_date` / `mri_last_date` → **DATE** (schema clean).
--   * `pet_other_first_date` / `pet_other_last_date` → **DATE** (219 rollup).
--   * `pet_first_date` / `pet_last_date` → **VARCHAR** (stored string dates from 216b
--     replay / merge path). **CF-mig146-PM-PET-FIRST-LAST-DATE-VARCHAR** — pair with
--     `clinical_date_retype_20260428` / `scripts/413_clinical_date_retype.py` style
--     remediation to native **DATE**; verification here attests **derivation**
--     faithfulness, not calendar type hygiene.
--
-- Cohort uniformity (§2c, MRI/PET scan cohorts only — NULL expected for ~90%)
-- ─────────────────────────────────────────────────────────────────────────────
--   * `mri_n_exams` NOT NULL: **462** patients; `mri_thyroid_nodule_any` TRUE among
--     them ≈ **40.7%** (moderate — expected relative to US-primary work-up).
--   * `mri_vocal_cords_normal` TRUE among MRI cohort ≈ **19.7%** (non-degenerate;
--     vocal-cord narrative depends on report template coverage).
--   * `pet_n_exams` NOT NULL: **291** patients; `pet_fdg_avid_thyroid_bed_ever` TRUE
--     ≈ **61.5%** (moderate post-thyroidectomy FDG-avidity pattern).
--   * `pet_distant_mets_ever` TRUE ≈ **43.6%** among PET cohort — **much higher** than
--     informal 5–10% advanced-disease intuition; reflects **LLM distant_metastases
--     block sensitivity** on oncologic PET reports + mixed indication mix
--     (**CF-mig146-PET-DISTANT-METS-LLM-SENSITIVITY**, interpretive / not boolean
--     degeneracy).
--   * `pet_radiotracer_primary` patient-level distill (where populated): dominated by
--     **`FDG`** (**267**/main tracer cohort); tails `GA68` / `other` / `DOTATATE` /
--     **`Rb-82`** — occasional alias cleanup could use **CF-mig146-PET-RADIOTRACER-VOCAB**
--     if downstream analytics needs strict controlled vocabulary.
--
-- SUV semantics (§7)
-- ─────────────────────────────────────────────────────────────────────────────
--   * **`pet_suv_max_*`** columns store **numeric maxima** taken over per-exam 216b
--     **`suv_max`** fields (explicitly keyed as SUV_max in extractor JSON). Not
--     DICOM-derived; **CF-mig146-PET-SUV-NOT-IMAGING-PRIMARY** if meta-analysis requires
--     primary-image SUV confirmation.
--
-- Acceptance gates
-- ─────────────────────────────────────────────────────────────────────────────
--   * **49** columns flipped not_started → verified (Gate metadata complete:
--     verified_by / verification_method / batch_id / verified_ts).
--   * Canonical table **`n_verified`** advances by exactly **49** post-apply via live
--     recomputation (`canonical_column_verification_registry_v1` rollup).
--   * Verified-row metadata hole query (verified + NULL verified_by OR batch OR ts):
--     **0** rows pre-apply — recorded as **“gate 4 = 0”** sentinel in this lane.
--
-- Parallel sibling lanes — do NOT touch: mig_142 RAI, mig_143 SmallClusters,
-- mig_144 US+generic imaging, mig_145 CT, mig_147 Nucmed.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;


-- ---------------------------------------------------------------------------
-- 146a — MRI — structured rollup vs main.mri_imaging (exam grain → patient OR/MAX/COUNT)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_mri_mri_imaging_v1',
    batch_id            = 'mig_146_patient_master_mri_pet_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_146 MRI block (Lane 36 structured). Replay vs '
                          || '`main.mri_imaging`: MIN/MAX exam DATE (`mri_*_date`), '
                          || 'exam counts (`mri_n_exams`), first-exam contrast/type/'
                          || 'indication, BOOL_OR morphology + LN + vocal-cord booleans.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'mri_first_date',
    'mri_last_date',
    'mri_first_days_from_surg',
    'mri_last_days_from_surg',
    'mri_n_exams',
    'mri_contrast_used_any',
    'mri_exam_type_first',
    'mri_indication_first',
    'mri_has_data',
    'mri_has_dimensions',
    'mri_has_dominant_nodule',
    'mri_ln_mentioned_any',
    'mri_pathologic_ln_any',
    'mri_mass_effect_any',
    'mri_substernal_any',
    'mri_substernal_extension_any',
    'mri_vocal_cords_described',
    'mri_vocal_cords_normal',
    'mri_thyroid_enlarged_any',
    'mri_thyroid_nodule_any'
  );


-- ---------------------------------------------------------------------------
-- 146b — MRI — LLM impression / key findings / recommendations (216b B2 lineage)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_llm_imaging_mri',
    batch_id            = 'mig_146_patient_master_mri_pet_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_146 MRI LLM narratives. Tier-1 note entity tables '
                          || 'not materialized in publication DB (`CF-mig146-IMAGING-'
                          || 'LLM-NO-PERSISTED-ENTITIES`); verifier trace = Script 216b B2 '
                          || 'MRI JSON merge path + 219 textual consistency checks.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'mri_impression_first',
    'mri_impression_last',
    'mri_key_findings_last',
    'mri_recommendation_last'
  );


-- ---------------------------------------------------------------------------
-- 146c — MRI/PET worst thyroid / overall assessment ladders
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'severity_ladder_aggregate_worst',
    batch_id            = 'mig_146_patient_master_mri_pet_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_146 ordinal worst-of-exams rollup (MRI thyroid '
                          || 'assessment dominance ladder; PET LLM '
                          || '`overall_assessment` ladder from 216b B1 aggregated per '
                          || 'research_id — see extractor hierarchy strings).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'mri_thyroid_assessment_worst',
    'pet_overall_worst'
  );


-- ---------------------------------------------------------------------------
-- 146d — PET — FDG primary block (`ct_imaging` PET ∪ 216b B1 rollup)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_pet_ct_imaging_llm216b_v1',
    batch_id            = 'mig_146_patient_master_mri_pet_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_146 PET FDG block. Structured slice from '
                          || '`main.ct_imaging` + Script 216b B1 Claude JSON rollup → '
                          || 'PET patient scalars (`pet_*` excluding `pet_other_*`). '
                          || 'Date columns `pet_first_date`/`pet_last_date` remain VARCHAR '
                          || 'upstream — see **`CF-mig146-PM-PET-FIRST-LAST-DATE-VARCHAR`**.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'pet_has_data',
    'pet_n_exams',
    'pet_first_date',
    'pet_last_date',
    'pet_indication_first',
    'pet_impression_last',
    'pet_radiotracer_primary',
    'pet_fdg_avid_thyroid_bed_ever',
    'pet_fdg_avid_cervical_ln_ever',
    'pet_distant_mets_ever',
    'pet_distant_met_sites'
  );


-- ---------------------------------------------------------------------------
-- 146e — PET SUV maxima (`pet_suv_quantitative_max_per_pt`)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'pet_suv_quantitative_max_per_pt',
    batch_id            = 'mig_146_patient_master_mri_pet_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_146 PET SUV. Max over per-exam LLM `suv_max` fields '
                          || '(thyroid bed + cervical LN). See **`CF-mig146-PET-SUV-NOT-'
                          || 'IMAGING-PRIMARY`** if DICOM SUV is required downstream.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'pet_suv_max_cervical_ln',
    'pet_suv_max_thyroid_bed'
  );


-- ---------------------------------------------------------------------------
-- 146f — PET “Other” regex recovery family (Script 219 Task 2 rollups)
-- ---------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_pet_other_rollups_v219',
    batch_id            = 'mig_146_patient_master_mri_pet_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_146 PET-Other cohort: ct_imaging rows in the misc '
                          || '`Other` exam_type bucket recovered via Script 219 Task 2 Regex '
                          || 'staging into `_pet_other_recovered_v1`/`_pet_other_rollup_v1`, '
                          || 'merged onto CPM in Script 219 Step 6.3.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'pet_other_n_exams',
    'pet_other_first_date',
    'pet_other_last_date',
    'pet_other_first_days_from_surg',
    'pet_other_last_days_from_surg',
    'pet_other_indication_first',
    'pet_other_mentions_metastasis',
    'pet_other_ned_statement',
    'pet_other_exam_type',
    'pet_other_extraction_method'
  );


-- ---------------------------------------------------------------------------
-- 146 — refresh canonical_table_signoff_registry_v1.row for canonical_patient_master
-- ---------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_146: MRI+PET thematic cluster CLOSED (+49 cols verified).'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'           THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name=subq.schema_name AND ts.table_name=subq.table_name;


COMMIT;


-- =============================================================================
-- end migration 146 — CPM MRI+PET imaging cluster (+49 cols verified; SSOT mri_imaging+
--     ct_imaging PET + 216b LLM rollup + 219 PET-Other regex + documented CFs incl.
--     VARCHAR PET first/last date + distant-mets LLM sensitivity)
-- =============================================================================
