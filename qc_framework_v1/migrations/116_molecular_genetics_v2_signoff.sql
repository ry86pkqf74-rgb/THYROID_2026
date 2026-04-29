-- =============================================================================
-- Migration 116 -- canonical_molecular_genetics_v2 SIGN-OFF (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Scope: canonical_molecular_genetics_v2 (molecular genetics master), 74 cols.
--   69 not_started columns are verified here; 5 identifier/linkage columns
--   remain na (research_id, molecular_episode_id, linked_fna_episode_id,
--   linked_nodule_id, linked_surgery_episode_id).
--
-- Current table state at verification:
--   Rows       : 1,384
--   Patients   : 1,151
--   Builder    : v3_2026-04-21
--   Source mix :
--      - molecular_testing                 859 rows
--      - thyroseq_molecular_enrichment     443 rows (script_269_backfill)
--      - extracted_braf_recovery_v1         46 rows (script_269_backfill)
--      - ret_patient_adjudicated_v226       36 rows (script_269_backfill)
--
-- Methodology:
--   1) Source-family archive replay against the archived consolidation source:
--      "Thyroid 2026 UPdated".molecular_legacy_20260421.
--      molecular_test_episode_v2.
--      Join keys:
--        * molecular_testing rows: research_id + molecular_episode_id
--        * script_269_backfill rows: research_id + report_source_table + platform
--      Results: 1,384/1,384 canonical rows joined, 0 no-join rows. Source-
--      preserved fields (dates/platform/flags/specimen/adjudication/source)
--      matched with 0 drift. resolved_test_date matched the archived ISO date
--      after the canonical MM/DD/YYYY formatting transform.
--
--   2) Parser/result fields were verified by deterministic provenance and
--      internal non-regression probes because the consolidation parser code is
--      not present as a standalone repo script. Checks included parser/status
--      source distributions, non-empty report_text_ref on 1,384/1,384 rows,
--      expected source families only, variant/fusion array counts (738 tests /
--      936 variant structs; 48 tests / 60 fusion structs), status vocabulary
--      mapping via manuscript_workspace.canonical_molecular_genetics_v2_status_clean
--      (GEN12 nonstandard flag = 0), platform vocabulary via GEN02 clean view,
--      BRAF variant recovery via GEN16 clean view, and source-specific raw/
--      derived presence profiles.
--
-- Acceptance probes:
--   - Row parity: 1,384 rows / 1,151 patients (as expected from handoff).
--   - Platform canonical vocabulary: only Afirma, ThyroSeq, NGS_unspecified.
--   - ROM ordering invariant low <= point <= high: 0 violations.
--   - ROM 0-100 range: 2 known OCR exceptions (research_id 2130: 599%; 8715:
--     395%) already tracked by GEN07; not a sign-off blocker because ordering
--     invariant passes and exceptions are documented.
--   - Linked IDs: linked_fna_episode_id populated on 374 rows; linked_nodule_id
--     and linked_surgery_episode_id populated on 0 rows. Existing GEN15 / README
--     guidance says the stored FNA linkage is deprecated/legacy; use
--     manuscript_workspace.canonical_molecular_genetics_v2_fna_rebind for clean
--     linkage instead. Registry retains linked_* as na.
--
-- Carry-forwards (not blocking):
--   CF-mig116-MOL-DATE-RETYPE: test_date_native is TIMESTAMP and
--      resolved_test_date is VARCHAR despite being clinical molecular-test
--      event dates. Both were faithfully verified; defer DATE retyping to a
--      dedicated clinical-date cleanup pass.
--   CF-mig116-MOL-LINKAGE-ID: linked_fna_episode_id remains populated on 374
--      rows even though current no-cross-domain-linkage guidance prefers
--      research_id plus governed linkage views. Do not consume the stored ID;
--      use the GEN15 fna_rebind view. Existing registry status remains na.
--   CF-GEN07-ROM-OCR: 2 ROM percentages outside 0-100 (599%, 395%) are retained
--      as source-faithful OCR/parser outputs and documented for hand correction.
--
-- Executed against MotherDuck thyroid_canonical_publication_v1_0 via
-- scripts/_md_connect.connect_locked().
-- =============================================================================

-- 116a: source-preserved fields verified against archived molecular_test_episode_v2.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'source_family_archive_replay_molecular_test_episode_v2',
    batch_id            = 'mig_116_molecular_genetics_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_116: verified by source-family archive replay '
                          || 'against "Thyroid 2026 UPdated".molecular_legacy_20260421.'
                          || 'molecular_test_episode_v2. 1,384/1,384 canonical rows '
                          || 'joined to archived source (859 molecular_testing by '
                          || 'research_id+molecular_episode_id; 525 script_269_backfill '
                          || 'rows by research_id+report_source_table+platform). '
                          || '0 drift for source-preserved fields under IS DISTINCT '
                          || 'FROM comparison; resolved_test_date matches source ISO '
                          || 'date after canonical MM/DD/YYYY formatting transform.'
WHERE schema_name='main'
  AND table_name='canonical_molecular_genetics_v2'
  AND verification_status='not_started'
  AND column_name IN (
    'test_date_native',
    'resolved_test_date',
    'platform',
    'platform_version',
    'bethesda_category',
    'specimen_site_normalized',
    'braf_flag',
    'braf_variant',
    'ras_flag',
    'ras_subtype',
    'ret_flag',
    'ret_fusion_flag',
    'tert_flag',
    'ntrk_flag',
    'eif1ax_flag',
    'tp53_flag',
    'pax8_pparg_flag',
    'cna_flag',
    'fusion_flag',
    'loh_flag',
    'alk_flag',
    'high_risk_marker_flag',
    'inadequate_flag',
    'cancelled_flag',
    'overall_result_class',
    'ingestion_source',
    'adjudication_status',
    'molecular_confidence'
  );

-- 116b: parser/source-text/result columns verified by provenance + invariant probes.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'parser_provenance_and_internal_nonregression',
    batch_id            = 'mig_116_molecular_genetics_v2_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_116: verified by parser/source-text provenance '
                          || 'and internal non-regression probes. Source families are '
                          || 'limited to molecular_testing (859), thyroseq_molecular_'
                          || 'enrichment (443), extracted_braf_recovery_v1 (46), and '
                          || 'ret_patient_adjudicated_v226 (36); report_text_ref '
                          || 'present on 1,384/1,384 rows. Variant structs: 738 tests '
                          || '/ 936 structs; fusion structs: 48 tests / 60 structs. '
                          || 'GEN12 status-clean view reports 0 nonstandard status rows; '
                          || 'GEN02 platform-clean and GEN16 BRAF-variant views cover '
                          || 'known derived-field governance. ROM ordering low<=point<=high '
                          || 'has 0 violations; 2 known out-of-0-100 OCR exceptions remain '
                          || 'documented under CF-GEN07-ROM-OCR.'
WHERE schema_name='main'
  AND table_name='canonical_molecular_genetics_v2'
  AND verification_status='not_started'
  AND column_name IN (
    'platform_raw',
    'parser',
    'parse_status',
    'n_fields_parsed',
    'test_result_summary',
    'rom_descriptor',
    'rom_percent_raw',
    'rom_percent_low',
    'rom_percent_high',
    'rom_percent_point',
    'rom_description',
    'specimen_adequacy_raw',
    'specimen_adequacy_norm',
    'gene_mutations_raw',
    'gene_mutations_status',
    'gene_fusions_raw',
    'gene_fusions_status',
    'cna_raw',
    'cna_status',
    'gep_raw',
    'gep_status',
    'gep_detail',
    'parathyroid_raw',
    'parathyroid_status',
    'medullary_raw',
    'medullary_status',
    'gene_mutations_variants',
    'gene_fusions_list',
    'tert_present',
    'tert_promoter_variant',
    'afirma_braf_result',
    'afirma_mtc_result',
    'afirma_tert_c228t_result',
    'afirma_tert_c250t_result',
    'afirma_retptc_result',
    'report_text_ref',
    'report_text_source',
    'report_text_length',
    'report_source_table',
    'builder_version',
    'built_at'
  );

-- 116c: recompute table_signoff_registry counts and sign off.
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed,0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/116_molecular_genetics_v2_signoff.sql',
    notes             = 'mig_116: Protocol v2 molecular genetics v2 sign-off. '
                        || '1,384 rows / 1,151 patients verified. Source-family '
                        || 'archive replay vs molecular_legacy_20260421.molecular_test_episode_v2 '
                        || 'joined 1,384/1,384 canonical rows with 0 no-join and '
                        || '0 source-preserved field drift (resolved_test_date '
                        || 'matches after ISO-to-MM/DD/YYYY canonical formatting). '
                        || 'Parser/result columns verified by provenance and '
                        || 'internal non-regression probes: expected source families '
                        || 'only, clean platform vocab, GEN12 status vocab clean, '
                        || '738 variant-array tests / 936 structs, 48 fusion-array '
                        || 'tests / 60 structs, ROM ordering invariant 0 violations. '
                        || 'Carry-forwards: CF-mig116-MOL-DATE-RETYPE (TIMESTAMP/VARCHAR '
                        || 'clinical date cols), CF-mig116-MOL-LINKAGE-ID (374 stored '
                        || 'linked_fna_episode_id rows; use GEN15 fna_rebind view), '
                        || 'CF-GEN07-ROM-OCR (2 source-faithful OCR ROM values outside 0-100).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_molecular_genetics_v2'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 116 -- canonical_molecular_genetics_v2 closed under Protocol v2
-- =============================================================================