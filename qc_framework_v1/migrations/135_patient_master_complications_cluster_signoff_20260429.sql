-- =============================================================================
-- Migration 135 -- canonical_patient_master COMPLICATIONS CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   25 (complications thematic cluster) — Protocol v2 patient_master verification.
-- Note:   Repo mig_134 is the **labs** cluster (Labs Lane label in-file). This file is
--         **mig_135** complications (per sequential numbering after 134_labs).
--
-- Cowork survey (~78 cols) vs full predicate on live MotherDuck (2026-04-29):
--   Broad `information_schema` probe with nerve/rln/death matching yielded **158** cols;
--   after **excluding** survival/sibling slices (**death_***, **prm_***, **mri_vocal_***,
--   **syn_io_*** reserved for survival / PRM / imaging / operative lanes), **147** cols
--   remained `not_started` and are flipped here (**128** `comp_*` + **5** legacy `*_status`
--   VARCHARs + **mortality_type** + **13** refined **`rln_*`** spine).
--
-- Upstream verified SSOT families (see qc_framework_v1/migrations 98a-d notes +
-- `108_complications_patient_rollup_signoff.sql`, `102_parathyroid_events_table_signoff.sql`):
--   * canonical_complications_events_v1 — mig_99 family / per-entity 98a-h apply passes
--   * canonical_complications_patient_rollup_v1 — mig_108
--   * canonical_parathyroid_events_v1 — mig_102 (cross-check hypopara + hypocal clinical)
--   * extracted_rln_injury_refined_v2 + complications table — refined **rln_*** CPM cols
--   * comp_mortality_* + mortality_type — mig_98h / survival integration (death_* **deferred**
--     to survival lane — same row-level SSOT, distinct column slice)
--
-- Methodology (Protocol v2):
--   * **NULL vs 0** — count / days_postop columns use NULL for absent-event patients;
--     COALESCE in replay probes (feedback_recurrence_imaging_n_events_null.md).
--   * **CPM vs rollup naming** — `comp_*_confirmed` / evidence_tier / phenotype timing
--     trace through complication_phenotype + events; naive equality to
--     `ever_*_definitive` on rollup is **not** the verification contract (tier asymmetry;
--     informational only if spot-probed).
--   * **DATE policy** — complication detection dates on CPM are DATE where named *_date;
--     TIMESTAMP carry-forwards must follow CF-100-DATE-RETYPE (no VARCHAR *_date in this slice).
--
-- Pre-apply integrity (MotherDuck RW 2026-04-29): cohort parity 10,871 x 10,871.
-- Post-apply: refresh canonical_table_signoff_registry_v1; gate-4 metadata complete on flipped cols.
--
-- =============================================================================

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- 135a — 13 cols — refined RLN / voice spine
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'rln_refined_extracted_rln_injury_v2',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'RLN refined spine: extracted_rln_injury_refined_v2 / complications tier labels '
                          || '(complements comp_rln_injury_* phenotype cols).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'rln_classification', 'rln_injury_days_postop', 'rln_injury_detection_date', 'rln_injury_detection_days_from_surg', 'rln_injury_evidence', 'rln_injury_is_confirmed', 'rln_injury_tier', 'rln_injury_type', 'rln_laterality', 'rln_permanent_flag', 'rln_status', 'rln_temporality', 'rln_transient_flag'
  );


-- -----------------------------------------------------------------------------
-- 135b — 5 cols — legacy complications table VARCHAR status
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_legacy_complication_status_varchar_complications_table',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'chyle_leak_status', 'hematoma_status', 'hypocalcemia_status', 'hypoparathyroidism_status', 'seroma_status'
  );


-- -----------------------------------------------------------------------------
-- 135c — 4 cols — mortality integration (98h / survival chain)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mortality_survival_chain_derivation_mig_98h',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_mortality_any_evidence', 'comp_mortality_definitive', 'comp_mortality_probable_or_better', 'mortality_type'
  );


-- -----------------------------------------------------------------------------
-- 135d — 12 cols — chyle_leak events family (mig_98b)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_chyle_mig_98b',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_chyle_leak_any_evidence', 'comp_chyle_leak_confirmed', 'comp_chyle_leak_days_postop', 'comp_chyle_leak_days_postop_v2', 'comp_chyle_leak_definitive', 'comp_chyle_leak_evidence_tier', 'comp_chyle_leak_permanent', 'comp_chyle_leak_probable_or_better', 'comp_chyle_leak_suspected', 'comp_chyle_leak_timing_window', 'comp_chyle_leak_transient', 'comp_chyle_leak_treatment_req'
  );


-- -----------------------------------------------------------------------------
-- 135e — 12 cols — hematoma events family (mig_98e)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_hematoma_mig_98e',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_hematoma_any_evidence', 'comp_hematoma_confirmed', 'comp_hematoma_days_postop', 'comp_hematoma_days_postop_v2', 'comp_hematoma_definitive', 'comp_hematoma_evidence_tier', 'comp_hematoma_permanent', 'comp_hematoma_probable_or_better', 'comp_hematoma_suspected', 'comp_hematoma_timing_window', 'comp_hematoma_transient', 'comp_hematoma_treatment_req'
  );


-- -----------------------------------------------------------------------------
-- 135f — 12 cols — seroma events family (mig_98d)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_seroma_mig_98d',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_seroma_any_evidence', 'comp_seroma_confirmed', 'comp_seroma_days_postop', 'comp_seroma_days_postop_v2', 'comp_seroma_definitive', 'comp_seroma_evidence_tier', 'comp_seroma_permanent', 'comp_seroma_probable_or_better', 'comp_seroma_suspected', 'comp_seroma_timing_window', 'comp_seroma_transient', 'comp_seroma_treatment_req'
  );


-- -----------------------------------------------------------------------------
-- 135g — 12 cols — wound_infection events family
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_wound_infection_mig_98_family',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_wound_infection_any_evidence', 'comp_wound_infection_confirmed', 'comp_wound_infection_days_postop', 'comp_wound_infection_days_postop_v2', 'comp_wound_infection_definitive', 'comp_wound_infection_evidence_tier', 'comp_wound_infection_permanent', 'comp_wound_infection_probable_or_better', 'comp_wound_infection_suspected', 'comp_wound_infection_timing_window', 'comp_wound_infection_transient', 'comp_wound_infection_treatment_req'
  );


-- -----------------------------------------------------------------------------
-- 135h — 12 cols — rln_injury events family (mig_98c)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_rln_mig_98c',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_rln_injury_any_evidence', 'comp_rln_injury_confirmed', 'comp_rln_injury_days_postop', 'comp_rln_injury_days_postop_v2', 'comp_rln_injury_definitive', 'comp_rln_injury_evidence_tier', 'comp_rln_injury_permanent', 'comp_rln_injury_probable_or_better', 'comp_rln_injury_suspected', 'comp_rln_injury_timing_window', 'comp_rln_injury_transient', 'comp_rln_injury_treatment_req'
  );


-- -----------------------------------------------------------------------------
-- 135i — 21 cols — vocal cord paralysis / paresis / aggregate (mig_98c)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_voice_nerve_mig_98c',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_vc_paralysis_confirmed', 'comp_vc_paralysis_days_postop', 'comp_vc_paralysis_days_postop_v2', 'comp_vc_paralysis_evidence_tier', 'comp_vc_paralysis_permanent', 'comp_vc_paralysis_suspected', 'comp_vc_paralysis_timing_window', 'comp_vc_paralysis_transient', 'comp_vc_paralysis_treatment_req', 'comp_vc_paresis_confirmed', 'comp_vc_paresis_days_postop', 'comp_vc_paresis_days_postop_v2', 'comp_vc_paresis_evidence_tier', 'comp_vc_paresis_permanent', 'comp_vc_paresis_suspected', 'comp_vc_paresis_timing_window', 'comp_vc_paresis_transient', 'comp_vc_paresis_treatment_req', 'comp_vocal_cord_paralysis_any_evidence', 'comp_vocal_cord_paralysis_definitive', 'comp_vocal_cord_paralysis_probable_or_better'
  );


-- -----------------------------------------------------------------------------
-- 135j — 16 cols — hypoparathyroidism + permanent source (mig_98f + CF-102)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_complication_cross_validate_parathyroid_mig_102',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_hypopara_permanent_limitation_note', 'comp_hypopara_permanent_source', 'comp_hypoparathyroidism_any_evidence', 'comp_hypoparathyroidism_confirmed', 'comp_hypoparathyroidism_days_postop', 'comp_hypoparathyroidism_days_postop_v2', 'comp_hypoparathyroidism_definitive', 'comp_hypoparathyroidism_evidence_tier', 'comp_hypoparathyroidism_new_postop', 'comp_hypoparathyroidism_permanent', 'comp_hypoparathyroidism_preexisting', 'comp_hypoparathyroidism_probable_or_better', 'comp_hypoparathyroidism_suspected', 'comp_hypoparathyroidism_timing_window', 'comp_hypoparathyroidism_transient', 'comp_hypoparathyroidism_treatment_req'
  );


-- -----------------------------------------------------------------------------
-- 135k — 17 cols — hypocalcemia clinical (mig_98g + parathyroid cross-check)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_events_hypocalcemia_mig_98g_parathyroid_102',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_hypocalcemia_clinical_any_evidence', 'comp_hypocalcemia_clinical_definitive', 'comp_hypocalcemia_clinical_new_postop', 'comp_hypocalcemia_clinical_permanent', 'comp_hypocalcemia_clinical_preexisting', 'comp_hypocalcemia_clinical_probable_or_better', 'comp_hypocalcemia_clinical_transient', 'comp_hypocalcemia_confirmed', 'comp_hypocalcemia_days_postop', 'comp_hypocalcemia_days_postop_v2', 'comp_hypocalcemia_evidence_source', 'comp_hypocalcemia_evidence_tier', 'comp_hypocalcemia_permanent', 'comp_hypocalcemia_suspected', 'comp_hypocalcemia_timing_window', 'comp_hypocalcemia_transient', 'comp_hypocalcemia_treatment_req'
  );


-- -----------------------------------------------------------------------------
-- 135l — 11 cols — misc rollup (airway / pneumothorax / wound_dehiscence / voice notes)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_complications_patient_rollup_mig_108_misc',
    batch_id            = 'mig_135_patient_master_complications_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_135 complications cluster (Lane 25). '
                          || 'SSOT: canonical_complications_events_v1 + rollup mig_108; '
                          || 'hypopara/hypocal cross-check mig_102 where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'comp_airway_complication_any_evidence', 'comp_airway_complication_definitive', 'comp_airway_complication_probable_or_better', 'comp_pneumothorax_any_evidence', 'comp_pneumothorax_definitive', 'comp_pneumothorax_probable_or_better', 'comp_voice_permanence_noted', 'comp_voice_resolution_noted', 'comp_wound_dehiscence_any_evidence', 'comp_wound_dehiscence_definitive', 'comp_wound_dehiscence_probable_or_better'
  );


-- -----------------------------------------------------------------------------
-- 135m — refresh canonical_table_signoff_registry_v1 for CPM (partial progress)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
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
                        || ' | mig_135: Complications thematic cluster CLOSED (147 cols). '
                        || 'Deferred in this lane: death_* (survival), prm_*, mri_vocal_*, syn_io_rln_monitoring.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


-- -----------------------------------------------------------------------------
-- 135n — Informational CF: CPM confirmed vs rollup ever_*_definitive naming asymmetry
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig135-PM-COMPL-ROLLUP-SEMANTICS: CPM comp_* flags trace '
            || 'complication_phenotype + events lineage; not bitwise identical to '
            || 'canonical_complications_patient_rollup_v1 ever_*_definitive aliases.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_135_patient_master_complications_cluster_20260429'
  AND column_name IN (
    'comp_chyle_leak_confirmed', 'comp_seroma_confirmed', 'comp_hematoma_confirmed',
    'comp_wound_infection_confirmed', 'comp_hypoparathyroidism_confirmed',
    'comp_hypocalcemia_confirmed'
  );


COMMIT;

-- =============================================================================
-- end migration 135 — CPM complications cluster verified (147 cols flipped this lane)
-- =============================================================================
