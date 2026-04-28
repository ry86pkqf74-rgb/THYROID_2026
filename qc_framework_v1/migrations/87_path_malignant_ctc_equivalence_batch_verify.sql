-- =============================================================================
-- Migration 87 -- canonical_path_malignant_events_v1 BATCH VERIFY (36 cols)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Batch-verify 36 inherited columns via mass-equivalence check against
--         the immediate upstream that produced canonical_path_malignant_events_v1.
--
-- Method: mechanical_derivation_compare (faithful-copy equivalence)
-- Source: archive_pub_v1_0.canonical_tumor_characteristics_v1_pre361_20260422_002245
--         (in "Thyroid 2026 UPdated"). Read-only verification reference; NOT a
--         build-time source for canonical (canonical was originally COPIED from
--         the live CTC at Script 361 run time; archived snapshot was taken
--         immediately pre-361 and is therefore the value-source-of-truth for
--         canonical's inherited columns).
--
-- Logan-ratified architecture (this session): use the archived CTC pre361 as
-- the read-only verification target for canonical's inherited columns. This is
-- consistent with feedback_no_cross_db_canonical_sourcing.md (read-only check
-- is permitted; we do not source canonical from archive).
--
-- Mass-equivalence join key (uniquely identifies a row):
--   (research_id, surgery_episode_id, tumor_ordinal, surgery_date, synoptic_row_ix)
--   using IS NOT DISTINCT FROM for NULL-safe equality.
--
-- Probe results (this session):
--   Total canonical rows : 6,689
--   Joined (incl 6 dup)  : 6,695
--   Rows matched per col :
--     -- Bucket B (15 cols): laterality, site, size_greatest_dimension_cm,
--        primary_histology, histology_variant, extrathyroidal_extension,
--        lymphatic_invasion, vascular_invasion, angioinvasion_quantify,
--        perineural_invasion, capsular_invasion, margin_status, ln_examined,
--        ln_involved, extranodal_extension                  -> 6,695/6,695 each
--     -- Bucket C inherited (11 cols): tumor_size_cm_per_surgery,
--        nodal_disease_positive_count, nodal_disease_total_count,
--        number_of_tumors, multifocality_flag, data_completeness_pct,
--        ajcc7_stage_calculable_flag, ajcc8_stage_calculable_flag,
--        gross_ete                                          -> 9 cols at 6,695/6,695
--                                                              gross_ete at 6,689/6,695
--                                                              (6 join-duplicate artifact)
--     -- Bucket C AJCC7/8 staging (10 cols): t_stage_ajcc7/8, n_stage_ajcc7/8,
--        m_stage_ajcc7/8, overall_stage_ajcc7/8, stage_group_ajcc7/8
--                                                          -> 6,695/6,695 each
--     -- Bucket D inherited (2 cols): staging_source_note,
--        stage_migration_7_to_8                            -> 6,695/6,695 each
--   TOTAL: 36 cols verified at 100% (or 99.91% w/ join-artifact CF on gross_ete).
--
-- Post-mig_87 table progress:
--   Verified: 38 / 47 not-na cols (2 prev + 36 this batch)
--   Remaining (not-na): 6 cols requiring per-col handling because not in archive
--     (added by Script 361 UPDATEs from TEM v2 / STF v1):
--       discordance_histology_flag, discordance_t_stage_flag,
--       discordance_laterality_flag, discordance_notes (TEM v2),
--       linkage_score, linkage_confidence_tier (STF v1)
--   Plus 12 na_provenance cols (deferred to Step D batch flip).
--
-- Carry-forwards:
--   CF-87-AJCC: AJCC7/8 staging values inherited from CTC pre361 are verified
--     as faithful copies. The findings-vs-staging derivation correctness
--     (Logan's airway-invasion rule extended to ETE/multifocality/nodal) is a
--     separate validation question that operates UPSTREAM of canonical (in
--     CTC's build pipeline, scripts 251/266). Defer to a future round that
--     either (a) restores CTC and validates its staging derivation against
--     findings, or (b) re-derives staging post-canonical from the verified
--     finding columns and audits diff vs current values.
--   CF-87-GROSS-ETE: 6 of 6,695 join-duplicate rows show inconsistent gross_ete
--     between paired archive rows. The canonical row's value matches at least
--     one archive row in every case (6,689/6,689 canonical rows match), so
--     this is a join-artifact, not a data issue. Defer (cosmetic).
--
-- No row-level data writes to main.canonical_path_malignant_events_v1.
-- Read-only verification reference: archive table not modified.
-- No canonical_logan_review_log_v1 entries (no Logan corrections needed).
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 87a: flip 36 inherited cols to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    upstream_source     = '"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_tumor_characteristics_v1_pre361_20260422_002245 (CTC pre-Script-361 snapshot; canonical was COPIED from live CTC at Script 361 run time)',
    batch_id            = 'mig_87_path_malignant_ctc_equivalence',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_87: 6,695/6,695 MATCH (or 6,689/6,695 for gross_ete -- '
                          || '6 join-duplicate artifact) under faithful-copy equivalence join '
                          || 'on (rid, sid, ordinal, surgery_date, synoptic_row_ix). '
                          || 'CTC pre361 archive is the immediate upstream that produced canonical '
                          || 'via Script 361 SELECT * + malignancy filter. CF-87-AJCC: findings-vs-staging '
                          || 'derivation correctness deferred (upstream concern in CTC build).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_path_malignant_events_v1'
  AND column_name IN (
    -- Bucket B (15)
    'laterality', 'site', 'size_greatest_dimension_cm',
    'primary_histology', 'histology_variant', 'extrathyroidal_extension',
    'lymphatic_invasion', 'vascular_invasion', 'angioinvasion_quantify',
    'perineural_invasion', 'capsular_invasion', 'margin_status',
    'ln_examined', 'ln_involved', 'extranodal_extension',
    -- Bucket C inherited derived (9)
    'tumor_size_cm_per_surgery', 'gross_ete',
    'nodal_disease_positive_count', 'nodal_disease_total_count',
    'number_of_tumors', 'multifocality_flag',
    'data_completeness_pct',
    'ajcc7_stage_calculable_flag', 'ajcc8_stage_calculable_flag',
    -- Bucket C AJCC7/8 staging (10)
    't_stage_ajcc7', 'n_stage_ajcc7', 'm_stage_ajcc7', 'overall_stage_ajcc7', 'stage_group_ajcc7',
    't_stage_ajcc8', 'n_stage_ajcc8', 'm_stage_ajcc8', 'overall_stage_ajcc8', 'stage_group_ajcc8',
    -- Bucket D inherited (2)
    'staging_source_note', 'stage_migration_7_to_8'
  );

-- 87b: recompute table_signoff_registry counts
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total, n_verified = subq.n_verified,
    n_not_started = subq.n_not_started, n_failed = COALESCE(subq.n_failed, 0), n_na = subq.n_na,
    table_status = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress' ELSE 'not_started' END
FROM (
  SELECT schema_name, table_name, COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_path_malignant_events_v1' GROUP BY 1,2
) subq WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 87 -- 36 cols verified via CTC pre361 equivalence
-- Table progress: 38 verified / 9 not_started / 9 na = 56 total
--                  (12 na_provenance: 9 already-na + 3 reclassed-not_started)
-- =============================================================================
