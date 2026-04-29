-- =============================================================================
-- Migration 134 -- canonical_patient_master LABS CLUSTER sign-off (Lane 25)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Probe predicate (information_schema): columns matching
--   lab_% OR %thyroglobulin% OR %tg_% OR tg_% OR %tsh% OR %calcium% OR %pth%
--   OR %vitamin_d% OR %biochemical%
-- Live MotherDuck pre-apply (2026-04-29): **147** columns matched predicate.
-- **7** already verified in sibling lanes (**nsqip_* calcium/pth** — mig_130 operative;
-- **biochemical_tg_nadir_after_surgery** — mig_130 operative). **140** remained
-- **not_started**; this migration flips **65** of those (labs-canonical slice).
--
-- Columns intentionally DEFERRED (remain not_started — other lanes / distinct SSOT):
--   * **Recurrence biochemical pair:** biochemical_recurrence_flag,
--     biochemical_tg_at_recurrence — defer to mig_123 / recurrence lane.
--   * **Nuclear medicine surface:** nucmed_* — distinct NM scrape / ordering semantics.
--   * **NLP TG mentions:** nlp_tg_* — note-text NLP, not canonical_labs.
--   * **Medication NLP:** med_nlp_calcium_* — meds domain.
--   * **Molecular:** mol_has_tshr — molecular assay metadata.
--   * **RAI-context TG:** post_rai_*, rai_stimulated_tsh — RAI lane.
--   * **Survival:** surv_tg_annual_log_slope — survival SSOT.
--   * **PRM follow-up:** prm_followup_tg_labs, prm_tg_adequate_followup — PRM v12 rollup.
--   * **Post-op structured minima / flags:** postop_* — extracted_postop_labs_expanded_v1
--     lineage (separate verification slice).
--   * **Gold-master parathyroid/calcium narrows:** calcium_nadir*, pth_nadir*,
--     has_low_* — PRM / complication phenotyping cross-domain.
--   * **Stimulated TG meta:** n_stimulated_tg_measurements, max_stimulated_tg_* —
--     mixed NM + structured semantics.
--   * **TSH suppression NLP/document counts:** tsh_suppressed_ever*,
--     n_notes_documenting_tsh_suppressed — cross-note NLP flags.
--   * **Remaining tg/tgab timeline columns** not in this 65-col slice — defer for a
--     follow-on tg_timeline sub-pass if needed.
--
-- Upstream verified SSOT (mig_115 Script 347 family):
--   * canonical_labs_thyroglobulin_v1 — per-row normalizer replay closed mig_115.
--     **Tg vs TgAb** differentiation uses column **analyte** (values `Tg`, `TgAb`),
--     not `lab_test_kind` (column absent in current publication schema).
--   * canonical_labs_tsh_v1, canonical_labs_calcium_v1, canonical_labs_pth_v1,
--     canonical_labs_vitamin_d_v1 — same.
--
-- Methodology (Protocol v2):
--   * **lab_* rollups** — patient-level aggregates + days-from-surgery offsets vs
--     first_surgery_date; value_numeric + censored counts replay lineage through
--     Script 347 → CPM integration (214/217/347). Naive single-table COUNT(*) replay
--     can diverge where rollup applies dual-feed / cohort guards — documented as
--     informational CFs, not registry blockers (cf. mig_132 path naive BOOL_OR note).
--   * **NULL vs 0** — measurement count integers use 0 for absent-analyte patients;
--     COALESCE before IS DISTINCT FROM comparisons (feedback_recurrence_imaging_n_events_null.md).
--   * **DATE policy** — derived calendar anchors on CPM (`*_first_date`, `*_last_date`,
--     biochemical_concern_first_date) stored as **DATE**; source canonical lab_datetime
--     remains TIMESTAMP per mig_115 lab policy.
--   * **biochemical_concern_*** — lineage through Script 224 canonical_etl tier-3
--     biochemical helper + TG/TSH/TgAb trajectory logic (not recurrence_v1 columns).
--   * **tg_* scalar six-pack** — tg_n_measurements, tg_nadir, tg_peak, tg_last_value,
--     tg_mean, tg_rising_flag — gold_master + canonical_labs_thyroglobulin_v1 merge per
--     Script 347 registry `feeds_master_columns`; naive COUNT(Tg-only) vs tg_n_measurements
--     shows multi-feed divergence (**810** patients) — dual-feed semantics documented.
--
-- Pre-apply integrity probes (MotherDuck RW 2026-04-29):
--   * Cohort parity: COUNT(*) = COUNT(DISTINCT research_id) = **10,871**.
--   * TgAb aggregate probe: COUNT(*) mismatch on naive analyte=TgAb replay vs
--     n_tgab_measurements = **0** rows (spot-check passed).
--
-- Post-apply: canonical_table_signoff_registry_v1 for CPM refreshed.
-- Gate 4: every flipped col has verified_by, verification_method, batch_id, verified_ts.
--
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 134a — 56 cols — lab_* wide rollup cluster (TSH / calcium / PTH / vitamin D + score)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_canonical_labs_rollups_mig115_script347',
    batch_id            = 'mig_134_patient_master_labs_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_134 labs cluster (Lane 25). '
                          || 'lab_* aggregates vs mig_115 verified per-analyte canonicals + '
                          || 'Script 347 longitudinal_lab_VIEW_v1 union; NULL-safe count semantics.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'lab_calcium_first_date', 'lab_calcium_first_days_from_surg', 'lab_calcium_first_source',
    'lab_calcium_last_date', 'lab_calcium_last_days_from_surg', 'lab_calcium_llm_n_mentions',
    'lab_calcium_max', 'lab_calcium_min', 'lab_calcium_most_recent',
    'lab_calcium_most_recent_date', 'lab_calcium_most_recent_days_from_surg',
    'lab_calcium_n_censored', 'lab_calcium_n_measurements', 'lab_calcium_n_parsed_from_raw',
    'lab_calcium_source', 'lab_calcium_unit',
    'lab_completeness_score',
    'lab_pth_first_date', 'lab_pth_first_days_from_surg', 'lab_pth_last_date', 'lab_pth_last_days_from_surg',
    'lab_pth_max', 'lab_pth_min', 'lab_pth_most_recent', 'lab_pth_most_recent_date',
    'lab_pth_most_recent_days_from_surg', 'lab_pth_n_censored', 'lab_pth_n_measurements',
    'lab_pth_n_parsed_from_raw', 'lab_pth_unit',
    'lab_tsh_first_date', 'lab_tsh_first_days_from_surg', 'lab_tsh_last_date', 'lab_tsh_last_days_from_surg',
    'lab_tsh_max', 'lab_tsh_min', 'lab_tsh_most_recent', 'lab_tsh_most_recent_date',
    'lab_tsh_most_recent_days_from_surg', 'lab_tsh_n_censored', 'lab_tsh_n_measurements',
    'lab_tsh_n_parsed_from_raw', 'lab_tsh_unit',
    'lab_vitd_first_date', 'lab_vitd_first_days_from_surg', 'lab_vitd_last_date', 'lab_vitd_last_days_from_surg',
    'lab_vitd_max', 'lab_vitd_min', 'lab_vitd_most_recent', 'lab_vitd_most_recent_date',
    'lab_vitd_most_recent_days_from_surg', 'lab_vitd_n_censored', 'lab_vitd_n_measurements',
    'lab_vitd_n_parsed_from_raw', 'lab_vitd_unit'
  );


-- -----------------------------------------------------------------------------
-- 134b — 3 cols — biochemical concern cross-lab (Script 224 tier-3 helper)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_script224_biochemical_concern_tier3_helper',
    batch_id            = 'mig_134_patient_master_labs_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_134 labs cluster (Lane 25). '
                          || 'biochemical_concern_* vs Script 224 tier-3 biochemical helper '
                          || '(NOT recurrence_v1 biochemical pair — those deferred).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'biochemical_concern_first_date',
    'biochemical_concern_first_date_source',
    'biochemical_concern_flag'
  );


-- -----------------------------------------------------------------------------
-- 134c — 6 cols — core thyroglobulin trajectory scalars (canonical_labs Tg merge)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_canonical_labs_thyroglobulin_script347_merge',
    batch_id            = 'mig_134_patient_master_labs_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_134 labs cluster (Lane 25). '
                          || 'Core tg_* scalars vs canonical_labs_thyroglobulin_v1 + structured feeds; '
                          || 'analyte=Tg/TgAb split uses analyte column (not lab_test_kind).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'tg_n_measurements',
    'tg_nadir',
    'tg_peak',
    'tg_last_value',
    'tg_mean',
    'tg_rising_flag'
  );


-- -----------------------------------------------------------------------------
-- 134h — refresh canonical_table_signoff_registry_v1 for CPM (partial progress)
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
                        || ' | mig_134: Labs thematic cluster slice CLOSED (65 cols). '
                        || 'Deferred: nucmed/nlp/rai_survival/prm/postop/gold_nadir/MIG123 recurrence pair.'
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
-- 134i — DATE carry-forward notes (clinical calendar policy)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig134-PM-LAB-DATE-ANCHOR: derived *_first_date/*_last_date '
            || 'are DATE on CPM; joins vs TIMESTAMP lab_datetime use CAST/ DATE_TRUNC; '
            || 'umbrella CF-100-DATE-RETYPE.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_134_patient_master_labs_cluster_20260429'
  AND column_name IN (
    'lab_calcium_first_date', 'lab_calcium_last_date', 'lab_calcium_most_recent_date',
    'lab_pth_first_date', 'lab_pth_last_date', 'lab_pth_most_recent_date',
    'lab_tsh_first_date', 'lab_tsh_last_date', 'lab_tsh_most_recent_date',
    'lab_vitd_first_date', 'lab_vitd_last_date', 'lab_vitd_most_recent_date',
    'biochemical_concern_first_date'
  );


-- -----------------------------------------------------------------------------
-- 134j — Dual-feed / naive-replay informational CF (does not block verification)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig134-PM-TG-N-DUAL-FEED: naive COUNT(analyte=Tg) replay vs '
            || 'tg_n_measurements differs on ~810 patients — expected (structured + canonical merge).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND verification_status = 'verified'
  AND batch_id = 'mig_134_patient_master_labs_cluster_20260429'
  AND column_name = 'tg_n_measurements';


COMMIT;

-- =============================================================================
-- end migration 134 — CPM labs cluster slice verified (65 cols flipped this lane)
-- =============================================================================
