-- =============================================================================
-- Migration 117 -- US v2 imaging family Protocol v2 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork lane 10)
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Scope:  Close the three Tier-1 source ultrasound v2 imaging canonicals:
--           main.canonical_us_nodule_v2
--           main.canonical_us_thyroid_gland_v2
--           main.canonical_us_lymph_node_v2
--
-- Methodology: multi_source_derivation_plus_domain_sanity
--   * Build lineage reviewed:
--       - Script 362 built canonical_us_nodule_v2 from CUNC/CUNM plus
--         tirads_v2_nodules_raw and legacy us_nodules_tirads shell rows.
--         Scripts 374, 377, and 378 subsequently disambiguated/recomputed
--         TIRADS fields and absorbed LLM/raw TIRADS sidecars.
--       - Script 364 built canonical_us_thyroid_gland_v2 from
--         raw.ultrasound_reports plus us_nodules_tirads shell fallback rows.
--       - Script 364b built canonical_us_lymph_node_v2 from
--         raw.ultrasound_reports.lymph_node_assessment plus CPM lnus_* rows;
--         source_modality is constrained to US.
--   * Natural keys were checked live in MotherDuck:
--       - nodule: (research_id, us_exam_id, nodule_id) = 37,579/37,579
--         and (research_id, us_exam_id, nodule_index_within_exam) = 37,579/37,579.
--       - gland: (research_id, us_exam_id) = 13,578/13,578.
--       - lymph node: (research_id, us_exam_id, us_ln_id) = 6,801/6,801
--         and (research_id, us_exam_id, us_ln_index_within_exam) = 6,801/6,801.
--   * Clinical date policy: all three exam_date columns are DATE. extracted_at
--     remains provenance and is intentionally excluded/na.
--   * Domain sanity checks:
--       - TIRADS guard view has 0 band mismatches and 0 concordance mismatches
--         across 37,579 nodule rows.
--       - Nodule sonographic vocab checks for composition/echogenicity/shape/
--         margins and TR1-TR5 category fields are clean.
--       - Gland dimensional and volume ranges have 0 out-of-range values under
--         conservative ultrasound plausibility bounds; 6,785 shell rows are the
--         documented us_nodules_tirads fallback cohort.
--       - LN modality vocabulary is clean (source_modality='US' for all rows),
--         suspicion_level enum is clean, and the 6,793 shell rows are the known
--         source-limited USLN01 state rather than registry drift.
--   * Cross-table FNA check: canonical_fna_events_v1 has no nodule_id or US exam
--     reference columns, so direct nodule_id orphan testing is not applicable.
--
-- Live verification metrics (2026-04-29):
--   Table                              rows   patients  not_started closed
--   canonical_us_nodule_v2            37,579   6,523    53
--   canonical_us_thyroid_gland_v2     13,578  10,859    28
--   canonical_us_lymph_node_v2         6,801   4,077    23
--
-- Carry-forwards / non-blocking known limitations:
--   * CF-117-US-EXAM-ID-PORTABILITY: us_exam_id is intentionally not portable
--     across all three v2 children because cunc-derived nodule hashes differ
--     from the newer gland/LN hash recipe. By (research_id, exam_date), 8,825
--     of 9,006 multisource exam groups (97.99%) have >1 us_exam_id. This was
--     approved as a documented carry-forward during this lane; downstream exam
--     master logic must continue joining by (research_id, exam_date) and prefer
--     nodule us_exam_id when available.
--   * CF-117-US-NODULE-RANGE: 21 nodule rows have size_cm_max >20 cm and 484
--     rows have prior_size_mm_max >200 mm, inherited from upstream CUNC/INM
--     sources. Values are preserved, not normalized in this sign-off.
--   * CF-117-US-LATERALITY-RAW: canonical_us_nodule_v2.laterality contains
--     detailed raw location phrases (e.g. right lower pole) rather than only
--     normalized side labels. This is source-faithful and should be normalized
--     in a future clinical-feature pass if side-only analysis requires it.
--   * CF-117-US-GLAND-PARENCHYMA: gland parenchymal phenotype columns are all
--     NULL by known parser limitation tracked by USGLAND02.
--   * CF-117-US-LN-SHELL: 6,793/6,801 LN rows are shell/evidence-only rows with
--     no structured level/size fields, the known USLN01 source limitation.
--
-- Final expected registry state:
--   canonical_us_nodule_v2:        53 verified + 4 na = 57/57 closed
--   canonical_us_thyroid_gland_v2: 28 verified + 4 na = 32/32 closed
--   canonical_us_lymph_node_v2:    23 verified + 6 na = 29/29 closed
-- =============================================================================

BEGIN TRANSACTION;

-- 117a: canonical_us_nodule_v2
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'multi_source_derivation_plus_domain_sanity',
    batch_id            = 'mig_117_us_v2_family_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_117: US v2 nodule Protocol v2 sign-off. '
                          || 'Build lineage: Scripts 362 + 374 + 377 + 378 '
                          || '(CUNC/CUNM/tirads_v2/LLM/raw absorption). Live '
                          || 'MotherDuck checks: 37,579 rows / 6,523 patients; '
                          || '0 duplicate natural keys for (research_id, '
                          || 'us_exam_id, nodule_id) and index key; exam_date '
                          || 'is DATE; TIRADS guard 0 band and 0 concordance '
                          || 'mismatches; TR1-TR5 and feature vocab clean. '
                          || 'canonical_fna_events_v1 has no nodule_id/US '
                          || 'cross-reference, so FNA orphan check not '
                          || 'applicable. Carry-forwards: CF-117-US-EXAM-ID-'
                          || 'PORTABILITY (97.99% multisource exam-id mismatch '
                          || 'by research_id+exam_date), CF-117-US-NODULE-RANGE '
                          || '(21 size_cm_max>20cm; 484 prior_size_mm_max>200mm), '
                          || 'CF-117-US-LATERALITY-RAW (laterality stores raw '
                          || 'location phrases).'
WHERE schema_name='main'
  AND table_name='canonical_us_nodule_v2'
  AND verification_status='not_started';

-- 117b: canonical_us_thyroid_gland_v2
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'rule_based_derivation_with_source_limited_nulls',
    batch_id            = 'mig_117_us_v2_family_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_117: US v2 thyroid gland Protocol v2 '
                          || 'sign-off. Build lineage: Script 364 from raw.'
                          || 'ultrasound_reports plus us_nodules_tirads shell '
                          || 'fallback rows. Live MotherDuck checks: 13,578 '
                          || 'rows / 10,859 patients; 0 duplicate natural keys '
                          || 'for (research_id, us_exam_id); exam_date is DATE; '
                          || '0 out-of-range lobe/isthmus/volume values; source '
                          || 'counts 6,793 ultrasound_reports + 6,785 fallback '
                          || 'shell rows. Parenchymal phenotype fields are all '
                          || 'NULL by known parser limitation (CF-117-US-GLAND-'
                          || 'PARENCHYMA / USGLAND02). Exam-id portability CF '
                          || 'shared with nodule/LN tables.'
WHERE schema_name='main'
  AND table_name='canonical_us_thyroid_gland_v2'
  AND verification_status='not_started';

-- 117c: canonical_us_lymph_node_v2
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'rule_based_derivation_with_source_limited_shell_rows',
    batch_id            = 'mig_117_us_v2_family_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_117: US v2 lymph-node Protocol v2 '
                          || 'sign-off. Build lineage: Script 364b from '
                          || 'ultrasound_reports.lymph_node_assessment plus '
                          || 'CPM lnus_* rows; source_modality CHECK scoped to '
                          || 'US. Live MotherDuck checks: 6,801 rows / 4,077 '
                          || 'patients; 0 duplicate natural keys for '
                          || '(research_id, us_exam_id, us_ln_id) and index '
                          || 'key; exam_date is DATE; source_modality and '
                          || 'suspicion_level vocab clean; size/axis ranges have '
                          || '0 out-of-range rows. 6,793 rows are expected '
                          || 'evidence-only shell rows (CF-117-US-LN-SHELL / '
                          || 'USLN01), with 8 CPM lnus_* structured rows.'
WHERE schema_name='main'
  AND table_name='canonical_us_lymph_node_v2'
  AND verification_status='not_started';

-- 117d: recompute sign-off counts for all three US v2 canonical tables.
UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total = 57,
    n_verified = 53,
    n_not_started = 0,
    n_failed = 0,
    n_na = 4,
    table_status = 'verified',
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/117_us_v2_family_signoff.sql',
    notes = 'US v2 nodule table closed under Protocol v2. 37,579 rows / 6,523 patients; '
            || '53 verified + 4 na. Natural keys unique; exam_date DATE; TIRADS guard '
            || '0 mismatches; sonographic feature vocab clean. Carry-forwards: exam-id '
            || 'portability mismatch across child tables, inherited size/prior-size '
            || 'outliers, and raw laterality/location phrases.'
WHERE schema_name='main'
  AND table_name='canonical_us_nodule_v2';

UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total = 32,
    n_verified = 28,
    n_not_started = 0,
    n_failed = 0,
    n_na = 4,
    table_status = 'verified',
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/117_us_v2_family_signoff.sql',
    notes = 'US v2 thyroid gland table closed under Protocol v2. 13,578 rows / 10,859 '
            || 'patients; 28 verified + 4 na. Natural key unique; exam_date DATE; '
            || 'dimension ranges clean. Known carry-forward: parenchymal phenotype fields '
            || 'are source-limited NULLs and 6,785 fallback shell rows remain documented.'
WHERE schema_name='main'
  AND table_name='canonical_us_thyroid_gland_v2';

UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total = 29,
    n_verified = 23,
    n_not_started = 0,
    n_failed = 0,
    n_na = 6,
    table_status = 'verified',
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/117_us_v2_family_signoff.sql',
    notes = 'US v2 lymph-node table closed under Protocol v2. 6,801 rows / 4,077 patients; '
            || '23 verified + 6 na. Natural keys unique; exam_date DATE; modality and '
            || 'suspicion vocab clean. Known carry-forward: 6,793 evidence-only shell rows '
            || 'await future US LN parser/LLM enrichment.'
WHERE schema_name='main'
  AND table_name='canonical_us_lymph_node_v2';

COMMIT;

-- =============================================================================
-- end of migration 117 -- US v2 family closed (3 Tier-1 source canonicals)
-- =============================================================================
