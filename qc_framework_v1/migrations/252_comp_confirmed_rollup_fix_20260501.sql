-- =============================================================================
-- Migration 252 -- CPM complication confirmed rollup repair
-- =============================================================================
-- Date:   2026-05-01
-- Lane:   mig_252 / CF-COMP-CONFIRMED-ROLLUP-BUG
-- Scope:  main.canonical_patient_master only; no table rebuild.
--
-- GOVERNANCE: Do not apply until Logan signs off on the dry-run artifact from
--   qc_framework_v1/scripts/build_mig252_comp_rollup_dryrun.py.
--   Dry-run 20260501T101111Z produced main_mutations=0 and:
--     any_confirmed_complication_flag: 2490 -> 400
--     M038 >=200g subset:              146  -> 10
--
-- Root cause lineage:
--   * scripts/364_complications_consolidation.py builds
--     canonical_complications_patient_rollup_v1 tier flags correctly with
--     finding_status='present'.
--   * scripts/364_cpm_feeder_repoint.py repointed CPM aggregate
--     any_confirmed_complication_flag to n_complication_types_present > 0,
--     where n_complication_types_present means any present evidence, not
--     present + definitive/probable evidence.
--   * Earlier Script 256 repaired under-filled CPM flags from
--     complication_phenotype_v1.confirmed_flag, but that source can carry
--     absent/possible negation evidence as confirmed-like rollup input.
--
-- Corrected definitions:
--   * *_definitive            = present + evidence_strength='definitive'
--   * *_probable_or_better    = present + evidence_strength IN ('definitive','probable')
--   * *_confirmed             = same as *_probable_or_better
--   * *_any_evidence          = present, any evidence_strength
--   * *_suspected             = present + evidence_strength='possible'
--   * any_confirmed_*         = any present + definitive/probable event
--   * n_confirmed_complications = count distinct complication_type meeting
--                                 present + definitive/probable criteria
--
-- Vocal-cord alias policy:
--   canonical_complications_events_v1 has complication_type='vocal_cord_paralysis'
--   but no separate 'vc_paresis' type after Script 364 vocabulary
--   consolidation. This migration sets comp_vc_paresis_confirmed/suspected
--   FALSE under source-strict rules. Logan sign-off is required for this
--   alias policy before apply.
-- =============================================================================

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre252_comp_rollup_20260501 AS
SELECT * FROM main.canonical_patient_master;

-- MotherDuck allows writes to only one database inside a transaction. Keep the
-- cross-database archive snapshot outside the canonical-DB mutation transaction.
BEGIN TRANSACTION;

CREATE TEMP TABLE _mig252_corrected_rollup AS
SELECT
  CAST(pm.research_id AS VARCHAR) AS research_id,
  COALESCE(BOOL_OR(e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS any_confirmed_complication_flag,
  COALESCE(BOOL_OR(e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS any_confirmed_complication,
  COUNT(DISTINCT CASE WHEN e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable') THEN LOWER(e.complication_type) END) AS n_confirmed_complications,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'airway_complication' AND e.finding_status = 'present'), FALSE) AS comp_airway_complication_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'airway_complication' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_airway_complication_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'airway_complication' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_airway_complication_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'chyle_leak' AND e.finding_status = 'present'), FALSE) AS comp_chyle_leak_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'chyle_leak' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_chyle_leak_confirmed,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'chyle_leak' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_chyle_leak_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'chyle_leak' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_chyle_leak_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'chyle_leak' AND e.finding_status = 'present' AND e.evidence_strength = 'possible'), FALSE) AS comp_chyle_leak_suspected,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hematoma' AND e.finding_status = 'present'), FALSE) AS comp_hematoma_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hematoma' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_hematoma_confirmed,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hematoma' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_hematoma_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hematoma' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_hematoma_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hematoma' AND e.finding_status = 'present' AND e.evidence_strength = 'possible'), FALSE) AS comp_hematoma_suspected,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypocalcemia_clinical' AND e.finding_status = 'present'), FALSE) AS comp_hypocalcemia_clinical_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypocalcemia_clinical' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_hypocalcemia_clinical_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypocalcemia_clinical' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_hypocalcemia_clinical_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypocalcemia_clinical' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_hypocalcemia_confirmed,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypocalcemia_clinical' AND e.finding_status = 'present' AND e.evidence_strength = 'possible'), FALSE) AS comp_hypocalcemia_suspected,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypoparathyroidism' AND e.finding_status = 'present'), FALSE) AS comp_hypoparathyroidism_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypoparathyroidism' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_hypoparathyroidism_confirmed,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypoparathyroidism' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_hypoparathyroidism_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypoparathyroidism' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_hypoparathyroidism_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'hypoparathyroidism' AND e.finding_status = 'present' AND e.evidence_strength = 'possible'), FALSE) AS comp_hypoparathyroidism_suspected,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'mortality' AND e.finding_status = 'present'), FALSE) AS comp_mortality_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'mortality' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_mortality_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'mortality' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_mortality_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'pneumothorax' AND e.finding_status = 'present'), FALSE) AS comp_pneumothorax_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'pneumothorax' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_pneumothorax_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'pneumothorax' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_pneumothorax_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'rln_injury' AND e.finding_status = 'present'), FALSE) AS comp_rln_injury_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'rln_injury' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_rln_injury_confirmed,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'rln_injury' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_rln_injury_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'rln_injury' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_rln_injury_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'rln_injury' AND e.finding_status = 'present' AND e.evidence_strength = 'possible'), FALSE) AS comp_rln_injury_suspected,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'seroma' AND e.finding_status = 'present'), FALSE) AS comp_seroma_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'seroma' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_seroma_confirmed,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'seroma' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_seroma_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'seroma' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_seroma_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'seroma' AND e.finding_status = 'present' AND e.evidence_strength = 'possible'), FALSE) AS comp_seroma_suspected,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'vocal_cord_paralysis' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_vc_paralysis_confirmed,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'vocal_cord_paralysis' AND e.finding_status = 'present' AND e.evidence_strength = 'possible'), FALSE) AS comp_vc_paralysis_suspected,
  FALSE AS comp_vc_paresis_confirmed,
  FALSE AS comp_vc_paresis_suspected,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'vocal_cord_paralysis' AND e.finding_status = 'present'), FALSE) AS comp_vocal_cord_paralysis_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'vocal_cord_paralysis' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_vocal_cord_paralysis_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'vocal_cord_paralysis' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_vocal_cord_paralysis_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'wound_dehiscence' AND e.finding_status = 'present'), FALSE) AS comp_wound_dehiscence_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'wound_dehiscence' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_wound_dehiscence_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'wound_dehiscence' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_wound_dehiscence_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'wound_infection' AND e.finding_status = 'present'), FALSE) AS comp_wound_infection_any_evidence,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'wound_infection' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_wound_infection_confirmed,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'wound_infection' AND e.finding_status = 'present' AND e.evidence_strength = 'definitive'), FALSE) AS comp_wound_infection_definitive,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'wound_infection' AND e.finding_status = 'present' AND e.evidence_strength IN ('definitive','probable')), FALSE) AS comp_wound_infection_probable_or_better,
  COALESCE(BOOL_OR(LOWER(e.complication_type) = 'wound_infection' AND e.finding_status = 'present' AND e.evidence_strength = 'possible'), FALSE) AS comp_wound_infection_suspected
FROM main.canonical_patient_master pm
LEFT JOIN main.canonical_complications_events_v1 e
  ON CAST(e.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
GROUP BY 1;

-- Fail closed if the dry-run target does not preserve CPM grain.
SELECT CASE WHEN (SELECT COUNT(*) FROM _mig252_corrected_rollup) <> 10871
  THEN error('mig_252 abort: corrected rollup row count is not 10871')
  ELSE 0 END;
SELECT CASE WHEN (SELECT COUNT(DISTINCT research_id) FROM _mig252_corrected_rollup) <> 10871
  THEN error('mig_252 abort: corrected rollup distinct research_id count is not 10871')
  ELSE 0 END;

UPDATE main.canonical_patient_master AS pm
SET
    any_confirmed_complication_flag = dr.any_confirmed_complication_flag,
    any_confirmed_complication = dr.any_confirmed_complication,
    n_confirmed_complications = dr.n_confirmed_complications,
    comp_airway_complication_any_evidence = dr.comp_airway_complication_any_evidence,
    comp_airway_complication_definitive = dr.comp_airway_complication_definitive,
    comp_airway_complication_probable_or_better = dr.comp_airway_complication_probable_or_better,
    comp_chyle_leak_any_evidence = dr.comp_chyle_leak_any_evidence,
    comp_chyle_leak_confirmed = dr.comp_chyle_leak_confirmed,
    comp_chyle_leak_definitive = dr.comp_chyle_leak_definitive,
    comp_chyle_leak_probable_or_better = dr.comp_chyle_leak_probable_or_better,
    comp_chyle_leak_suspected = dr.comp_chyle_leak_suspected,
    comp_hematoma_any_evidence = dr.comp_hematoma_any_evidence,
    comp_hematoma_confirmed = dr.comp_hematoma_confirmed,
    comp_hematoma_definitive = dr.comp_hematoma_definitive,
    comp_hematoma_probable_or_better = dr.comp_hematoma_probable_or_better,
    comp_hematoma_suspected = dr.comp_hematoma_suspected,
    comp_hypocalcemia_clinical_any_evidence = dr.comp_hypocalcemia_clinical_any_evidence,
    comp_hypocalcemia_clinical_definitive = dr.comp_hypocalcemia_clinical_definitive,
    comp_hypocalcemia_clinical_probable_or_better = dr.comp_hypocalcemia_clinical_probable_or_better,
    comp_hypocalcemia_confirmed = dr.comp_hypocalcemia_confirmed,
    comp_hypocalcemia_suspected = dr.comp_hypocalcemia_suspected,
    comp_hypoparathyroidism_any_evidence = dr.comp_hypoparathyroidism_any_evidence,
    comp_hypoparathyroidism_confirmed = dr.comp_hypoparathyroidism_confirmed,
    comp_hypoparathyroidism_definitive = dr.comp_hypoparathyroidism_definitive,
    comp_hypoparathyroidism_probable_or_better = dr.comp_hypoparathyroidism_probable_or_better,
    comp_hypoparathyroidism_suspected = dr.comp_hypoparathyroidism_suspected,
    comp_mortality_any_evidence = dr.comp_mortality_any_evidence,
    comp_mortality_definitive = dr.comp_mortality_definitive,
    comp_mortality_probable_or_better = dr.comp_mortality_probable_or_better,
    comp_pneumothorax_any_evidence = dr.comp_pneumothorax_any_evidence,
    comp_pneumothorax_definitive = dr.comp_pneumothorax_definitive,
    comp_pneumothorax_probable_or_better = dr.comp_pneumothorax_probable_or_better,
    comp_rln_injury_any_evidence = dr.comp_rln_injury_any_evidence,
    comp_rln_injury_confirmed = dr.comp_rln_injury_confirmed,
    comp_rln_injury_definitive = dr.comp_rln_injury_definitive,
    comp_rln_injury_probable_or_better = dr.comp_rln_injury_probable_or_better,
    comp_rln_injury_suspected = dr.comp_rln_injury_suspected,
    comp_seroma_any_evidence = dr.comp_seroma_any_evidence,
    comp_seroma_confirmed = dr.comp_seroma_confirmed,
    comp_seroma_definitive = dr.comp_seroma_definitive,
    comp_seroma_probable_or_better = dr.comp_seroma_probable_or_better,
    comp_seroma_suspected = dr.comp_seroma_suspected,
    comp_vc_paralysis_confirmed = dr.comp_vc_paralysis_confirmed,
    comp_vc_paralysis_suspected = dr.comp_vc_paralysis_suspected,
    comp_vc_paresis_confirmed = dr.comp_vc_paresis_confirmed,
    comp_vc_paresis_suspected = dr.comp_vc_paresis_suspected,
    comp_vocal_cord_paralysis_any_evidence = dr.comp_vocal_cord_paralysis_any_evidence,
    comp_vocal_cord_paralysis_definitive = dr.comp_vocal_cord_paralysis_definitive,
    comp_vocal_cord_paralysis_probable_or_better = dr.comp_vocal_cord_paralysis_probable_or_better,
    comp_wound_dehiscence_any_evidence = dr.comp_wound_dehiscence_any_evidence,
    comp_wound_dehiscence_definitive = dr.comp_wound_dehiscence_definitive,
    comp_wound_dehiscence_probable_or_better = dr.comp_wound_dehiscence_probable_or_better,
    comp_wound_infection_any_evidence = dr.comp_wound_infection_any_evidence,
    comp_wound_infection_confirmed = dr.comp_wound_infection_confirmed,
    comp_wound_infection_definitive = dr.comp_wound_infection_definitive,
    comp_wound_infection_probable_or_better = dr.comp_wound_infection_probable_or_better,
    comp_wound_infection_suspected = dr.comp_wound_infection_suspected,
    cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM _mig252_corrected_rollup dr
WHERE CAST(pm.research_id AS VARCHAR) = dr.research_id;

-- Re-verify affected CPM columns in the canonical registry.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'mig_252_strict_present_definitive_probable_from_canonical_complications_events_v1',
    batch_id = 'mig_252_comp_confirmed_rollup_fix_20260501',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
      || ' | mig_252: repaired complication confirmed/tier rollups from '
      || 'canonical_complications_events_v1 using finding_status=present; '
      || 'confirmed/probable_or_better require evidence_strength IN (definitive,probable); '
      || 'any_evidence requires present; suspected requires present+possible. '
      || 'Dry-run artifact: exports/mig252_comp_rollup_dryrun_20260501T101111Z. '
      || 'any_confirmed_complication_flag 2490->400; M038 >=200g 146->10.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN (
    'any_confirmed_complication_flag','any_confirmed_complication','n_confirmed_complications',
    'comp_airway_complication_any_evidence','comp_airway_complication_definitive','comp_airway_complication_probable_or_better',
    'comp_chyle_leak_any_evidence','comp_chyle_leak_confirmed','comp_chyle_leak_definitive','comp_chyle_leak_probable_or_better','comp_chyle_leak_suspected',
    'comp_hematoma_any_evidence','comp_hematoma_confirmed','comp_hematoma_definitive','comp_hematoma_probable_or_better','comp_hematoma_suspected',
    'comp_hypocalcemia_clinical_any_evidence','comp_hypocalcemia_clinical_definitive','comp_hypocalcemia_clinical_probable_or_better','comp_hypocalcemia_confirmed','comp_hypocalcemia_suspected',
    'comp_hypoparathyroidism_any_evidence','comp_hypoparathyroidism_confirmed','comp_hypoparathyroidism_definitive','comp_hypoparathyroidism_probable_or_better','comp_hypoparathyroidism_suspected',
    'comp_mortality_any_evidence','comp_mortality_definitive','comp_mortality_probable_or_better',
    'comp_pneumothorax_any_evidence','comp_pneumothorax_definitive','comp_pneumothorax_probable_or_better',
    'comp_rln_injury_any_evidence','comp_rln_injury_confirmed','comp_rln_injury_definitive','comp_rln_injury_probable_or_better','comp_rln_injury_suspected',
    'comp_seroma_any_evidence','comp_seroma_confirmed','comp_seroma_definitive','comp_seroma_probable_or_better','comp_seroma_suspected',
    'comp_vc_paralysis_confirmed','comp_vc_paralysis_suspected','comp_vc_paresis_confirmed','comp_vc_paresis_suspected',
    'comp_vocal_cord_paralysis_any_evidence','comp_vocal_cord_paralysis_definitive','comp_vocal_cord_paralysis_probable_or_better',
    'comp_wound_dehiscence_any_evidence','comp_wound_dehiscence_definitive','comp_wound_dehiscence_probable_or_better',
    'comp_wound_infection_any_evidence','comp_wound_infection_confirmed','comp_wound_infection_definitive','comp_wound_infection_probable_or_better','comp_wound_infection_suspected'
  );

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified = subq.n_verified,
    n_not_started = subq.n_not_started,
    n_failed = COALESCE(subq.n_failed, 0),
    n_na = subq.n_na,
    table_status = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/252_comp_confirmed_rollup_fix_20260501.sql',
    notes = COALESCE(ts.notes,'')
      || ' | mig_252: CPM complication confirmed/tier rollup repaired from strict present evidence in canonical_complications_events_v1.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1 (
  run_id, started_at, ended_at, phases_applied,
  critical_findings_cleared, high_findings_cleared, med_findings_cleared,
  held_for_adjudication
)
VALUES (
  'mig_252_comp_confirmed_rollup_fix_20260501',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'dry_run_signed_off_update_cpm_complication_confirmed_rollups_registry_refresh',
  '0',
  '1',
  '0',
  '0'
);

-- Post-apply expected checks:
--   SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
--   SELECT SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END)
--   FROM main.canonical_patient_master; -- expected 400 at dry-run time
--   SELECT COUNT(*) AS n, SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) AS events
--   FROM manuscript_workspace.cohort_m038_massive_goiter_v1
--   WHERE gland_weight_final_g >= 200; -- expected 475 / 10 at dry-run time

COMMIT;

-- =============================================================================
-- End mig_252
-- =============================================================================