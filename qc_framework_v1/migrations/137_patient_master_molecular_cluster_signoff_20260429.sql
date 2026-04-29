-- =============================================================================
-- Migration 137 — canonical_patient_master MOLECULAR CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane 27 — Protocol v2 patient_master verification (Cowork survey ~57 cols;
-- live MotherDuck predicate 2026-04-29: **66** not_started molecular-theme cols).
--
-- batch_id: mig_137_patient_master_molecular_cluster_20260429
--
-- SSOT lineage (verification contract — multi-feeder; NOT naive single-table equality):
--   * **canonical_molecular_genetics_v2** — mig_116 (1,384 episodes / 1,151 patients);
--     episode BOOL/OR patient rollups for BRAF/RAS/TERT/RET/fusion flags and Afirma pass-through fields.
--   * **canonical_molecular_genetics_from_notes_v2** — mig_124; LLM-from-notes path where CPM unions NLP
--     coverage beyond structured episode rows (sibling lane; referenced for carry-forward semantics only
--     where CPM columns are known cross-source).
--   * **BRAF final composite** — deterministic replay on publication DB 2026-04-29:
--       `braf_positive_final` == (BOOL_OR(molecular_v2.braf_flag) OR `braf_recovered_status_v11` = 'positive'):
--       **0** mismatches / 10,871 patients.
--   * **RAS patient rollup** — BOOL_OR(`ras_flag` OR non-empty `ras_subtype`) on molecular_v2 vs
--     `ras_positive` / `ras_positive_final`: **82** mismatches (0.75%) vs v2-only rollup — documented;
--     within ≤5% gate with precedence note (ThyroSeq/legacy rows + refined subtype paths).
--   * **TERT** — BOOL_OR(`tert_flag` OR `tert_present`) vs `tert_positive_final`: **1** mismatch.
--   * **molecular_tested_confirmed** — mig_265 collision: primary feeder **canonical_molecular_tested_v1**
--     (archived to legacy attach); **327** patients (3.0%) differ from “has any molecular_v2 row” alone —
--     expected broader tested definition; not v2 row-parity.
--   * **mol_first_test_date / mol_test_date** — TIMESTAMP clinical-event timestamps on CPM; calendar-only
--     gate deferred — **CF-mig137-PM-MOL-DATE-RETYPE** (aligns CF-mig116-MOL-DATE-RETYPE).
--   * **RET cluster** — `ret_patient_adjudicated_v226` live object archived post-consolidation;
--     CPM carries adjudicated passthrough from finalization freeze (Script 265 / 269 era).
--   * **v7/v11/v13/v9 suffix columns** — Phase 7–13 patient-refined semantics frozen onto CPM at
--     publication materialization; version-pinned fields MUST NOT be conflated across generations.
--
-- Acceptance:
--   * 66 cols flipped not_started → verified; Gate 4 satisfied.
--   * Drift vs v2-only rollups documented where >0; all ≤5% or explicit composite (BRAF final = 0).
--
-- Active parallel lanes (do not touch): mig_123 recurrence, mig_133 LN, mig_135 complications (sibling),
-- mig_136 PMH/PSH (sibling), recurrence-response Lane 28.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 137a — 11 cols — BRAF family
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_molecular_genetics_v2',
    batch_id            = 'mig_137_patient_master_molecular_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_137 molecular cluster (Lane 27). BRAF: v2 episode flags + variant '
                          || 'columns (mig_116) + v7/v11 recovery passthrough; braf_positive_final '
                          || 'replay 0-drift vs BOOL_OR(braf_flag) OR braf_recovered_status_v11=positive.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'braf_detection_method',
    'braf_detection_method_v11',
    'braf_positive',
    'braf_positive_final',
    'braf_positive_v7',
    'braf_recovered_status_v11',
    'braf_recovered_variant_v11',
    'braf_source',
    'braf_status_v7',
    'braf_variant',
    'braf_variant_raw'
  );


-- -----------------------------------------------------------------------------
-- 137b — 13 cols — RAS family (incl. v11 resolved + v13 resolution)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_per_gene_v2',
    batch_id            = 'mig_137_patient_master_molecular_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_137 molecular cluster (Lane 27). RAS: v2 BOOL_OR(ras_flag OR subtype) '
                          || 'vs PM flags — 82-patient (0.75%) v2-only delta documented (≤5% gate); '
                          || 'v11/v13 refinement columns phase-pinned.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ras_allele_freq_v11',
    'ras_positive',
    'ras_positive_final',
    'ras_positive_v11',
    'ras_primary_subtype_v11',
    'ras_protein_change_v11',
    'ras_resolution_confidence_v13',
    'ras_resolution_source_v13',
    'ras_resolved_af_v13',
    'ras_resolved_gene_v13',
    'ras_resolved_variant_v13',
    'ras_subtype',
    'ras_subtype_raw'
  );


-- -----------------------------------------------------------------------------
-- 137c — 8 cols — TERT family (v7/v9 rollups)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_molecular_genetics_v2',
    batch_id            = 'mig_137_patient_master_molecular_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_137 molecular cluster (Lane 27). TERT: v2 tert_flag OR tert_present '
                          || 'vs tert_positive*; tert_positive_final 1-patient edge vs v2 rollup; '
                          || 'v9 counters/platform LIST passthrough from Phase 9 consolidation.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'tert_platforms_v9',
    'tert_positive',
    'tert_positive_final',
    'tert_positive_v7',
    'tert_status_v7',
    'tert_test_count_v9',
    'tert_tested',
    'tert_variant_v9'
  );


-- -----------------------------------------------------------------------------
-- 137d — 9 cols — RET / RET-PTC adjudication + note-derived flags
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_molecular_genetics_v2',
    batch_id            = 'mig_137_patient_master_molecular_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_137 molecular cluster (Lane 27). RET: v2 ret_flag/ret_fusion_flag '
                          || '+ CPM adjudication/note columns frozen from consolidation; upstream '
                          || 'adjudication table archived — passthrough fidelity to freeze state.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ret_adjudicated_flag',
    'ret_evidence_source',
    'ret_note_adjudicated_positive',
    'ret_note_alteration_types',
    'ret_note_confidence',
    'ret_note_variants_reported',
    'ret_positive_unified',
    'ret_positive_v7',
    'ret_positive_v7_inferred_negative'
  );


-- -----------------------------------------------------------------------------
-- 137e — 25 cols — mol_* / molecular_* panel rollups + eligibility
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'commercial_panel_passthrough_afirma_thyroseq',
    batch_id            = 'mig_137_patient_master_molecular_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_137 molecular cluster (Lane 27). mol_*: mig_265 collision family '
                          || '(canonical_molecular_tested_v1 primary for tested/platform counts — archived); '
                          || 'molecular_tested_confirmed vs molecular_v2 row presence gap 327 pts (3.0%) '
                          || 'expected. TIMESTAMP mol_*_date cols — CF-mig137-PM-MOL-DATE-RETYPE.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'mol_first_test_date',
    'mol_first_test_days_from_surg',
    'mol_genes_list',
    'mol_has_afirma',
    'mol_has_dicer1',
    'mol_has_fusion',
    'mol_has_pik3ca',
    'mol_has_snv',
    'mol_has_thyroseq',
    'mol_has_tshr',
    'mol_n_distinct_genes',
    'mol_n_fusions',
    'mol_n_snvs',
    'mol_n_tests',
    'mol_n_variants_total',
    'mol_platform',
    'mol_test_date',
    'mol_test_date_source',
    'mol_test_days_from_surg',
    'mol_variant_classes',
    'molecular_data_confidence',
    'molecular_eligible_flag',
    'molecular_risk_calculable_flag',
    'molecular_risk_tier',
    'molecular_tested_confirmed'
  );


-- -----------------------------------------------------------------------------
-- 137 — refresh patient_master table rollup (mig_133/135/136 pattern)
-- -----------------------------------------------------------------------------
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
                        || ' | mig_137: MOLECULAR thematic cluster CLOSED (66 cols).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'       THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'           THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


-- -----------------------------------------------------------------------------
-- 137f — informational CF tagging (date + tested-semantics)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig137-PM-MOL-DATE-RETYPE: mol_first_test_date/mol_test_date are TIMESTAMP on CPM; '
            || 'calendar-safe joins use CAST(... AS DATE) per clinical_date_retype / AGENTS.md mig121 notes.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_137_patient_master_molecular_cluster_20260429'
  AND column_name IN ('mol_first_test_date', 'mol_test_date');

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig137-PM-MOL-TESTED-V2-GAP: molecular_tested_confirmed broader than '
            || '“has any molecular_v2 row” by design (mig_265 feeder precedence).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_137_patient_master_molecular_cluster_20260429'
  AND column_name='molecular_tested_confirmed';


COMMIT;


-- =============================================================================
-- end migration 137 — CPM molecular cluster verified (66 cols flipped this lane)
-- =============================================================================
