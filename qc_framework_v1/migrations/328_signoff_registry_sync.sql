-- =============================================================================
-- mig_328_signoff_registry_sync — sync pub_signoff.canonical_table_signoff_registry_v1
--                                   for tables loaded in mig_327
-- =============================================================================
-- Prerequisites:
--   * mig_327_bulk_md_to_bq_missing_tables.py completed successfully
--   * All pub_canonical tables referenced below must exist
--
-- Run via:
--   bq query --use_legacy_sql=false < qc_framework_v1/migrations/328_signoff_registry_sync.sql
--
-- Note: schema_name uses 'main' to match the MD-origin convention already
--       established in the existing 123 registry rows.  This makes the registry
--       a cross-system governance log rather than a BQ-schema-per-row index.
--
-- For each table: idempotent INSERT (skip if already registered).
-- =============================================================================

-- ── Helper macro pattern (BigQuery doesn't have EXECUTE macros, so we
--    INSERT each row explicitly with the same NOT EXISTS guard) ────────────────

-- 1. canonical_tumor_characteristics_v1 (rebuilt from STL+TEM sources in mig_327)
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  (schema_name,table_name,n_columns_total,n_verified,n_not_started,n_failed,n_na,
   table_status,signed_off_ts,signoff_migration,priority_tier,notes,registered_ts)
SELECT
  'main','canonical_tumor_characteristics_v1',
  (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
   WHERE table_name='canonical_tumor_characteristics_v1'),
  0, (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name='canonical_tumor_characteristics_v1'),
  0, 0, 'live', CURRENT_TIMESTAMP(),
  'qc_framework_v1/migrations/327_bulk_md_to_bq_missing_tables.py',
  'tier1_canonical_base',
  'THY-18 CTC inline-rebuilt from readonly_share.synoptic_tumor_long_v1+specimen_tumor_focus_v1+tumor_episode_master_v2 (mig_327). Original table dropped from MD main by script 361; source tables intact in readonly_share. Grain: research_id×surgery_episode_id×tumor_ordinal.',
  CURRENT_TIMESTAMP()
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1 FROM `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  WHERE schema_name='main' AND table_name='canonical_tumor_characteristics_v1');

-- 2. manuscript_cohort_v1
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  (schema_name,table_name,n_columns_total,n_verified,n_not_started,n_failed,n_na,
   table_status,signed_off_ts,signoff_migration,priority_tier,notes,registered_ts)
SELECT
  'main','manuscript_cohort_v1',
  (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
   WHERE table_name='manuscript_cohort_v1'),
  0, (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name='manuscript_cohort_v1'),
  0, 0, 'live', CURRENT_TIMESTAMP(),
  'qc_framework_v1/migrations/327_bulk_md_to_bq_missing_tables.py',
  'tier1_canonical_base',
  'mig_327: per-patient manuscript analysis cohort (scripts/57+48 lineage). 10,871 rows.',
  CURRENT_TIMESTAMP()
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1 FROM `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  WHERE schema_name='main' AND table_name='manuscript_cohort_v1');

-- 3. path_synoptics
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  (schema_name,table_name,n_columns_total,n_verified,n_not_started,n_failed,n_na,
   table_status,signed_off_ts,signoff_migration,priority_tier,notes,registered_ts)
SELECT
  'main','path_synoptics',
  (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
   WHERE table_name='path_synoptics'),
  0, (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name='path_synoptics'),
  0, 0, 'live', CURRENT_TIMESTAMP(),
  'qc_framework_v1/migrations/327_bulk_md_to_bq_missing_tables.py',
  'tier2_reference',
  'mig_327: primary pathology synoptic source (311 cols, 11,688 rows). PHI-safe — no MRN/name/full-DOB exported.',
  CURRENT_TIMESTAMP()
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1 FROM `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  WHERE schema_name='main' AND table_name='path_synoptics');

-- 4. tumor_stage_heterogeneity_v1
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  (schema_name,table_name,n_columns_total,n_verified,n_not_started,n_failed,n_na,
   table_status,signed_off_ts,signoff_migration,priority_tier,notes,registered_ts)
SELECT
  'main','tumor_stage_heterogeneity_v1',
  (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
   WHERE table_name='tumor_stage_heterogeneity_v1'),
  0, (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name='tumor_stage_heterogeneity_v1'),
  0, 0, 'live', CURRENT_TIMESTAMP(),
  'qc_framework_v1/migrations/327_bulk_md_to_bq_missing_tables.py',
  'tier1_canonical_base',
  'mig_327: per-patient stage heterogeneity (script 266b). 8,422 rows. heterogeneity_t~851 (~10% malignant).',
  CURRENT_TIMESTAMP()
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1 FROM `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  WHERE schema_name='main' AND table_name='tumor_stage_heterogeneity_v1');

-- 5. rai_treatment_episode_v2
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  (schema_name,table_name,n_columns_total,n_verified,n_not_started,n_failed,n_na,
   table_status,signed_off_ts,signoff_migration,priority_tier,notes,registered_ts)
SELECT
  'main','rai_treatment_episode_v2',
  (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
   WHERE table_name='rai_treatment_episode_v2'),
  0, (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name='rai_treatment_episode_v2'),
  0, 0, 'live', CURRENT_TIMESTAMP(),
  'qc_framework_v1/migrations/327_bulk_md_to_bq_missing_tables.py',
  'tier1_canonical_base',
  'mig_327: RAI treatment episode v2. 1,857 rows. 41% dose coverage post-mig_262.',
  CURRENT_TIMESTAMP()
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1 FROM `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  WHERE schema_name='main' AND table_name='rai_treatment_episode_v2');

-- 6. specimen_tumor_focus_v1
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  (schema_name,table_name,n_columns_total,n_verified,n_not_started,n_failed,n_na,
   table_status,signed_off_ts,signoff_migration,priority_tier,notes,registered_ts)
SELECT
  'main','specimen_tumor_focus_v1',
  (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
   WHERE table_name='specimen_tumor_focus_v1'),
  0, (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name='specimen_tumor_focus_v1'),
  0, 0, 'live', CURRENT_TIMESTAMP(),
  'qc_framework_v1/migrations/327_bulk_md_to_bq_missing_tables.py',
  'tier1_canonical_base',
  'mig_327: broker linking STL rows to surgery_episode_id. 11,103 rows. Sourced from readonly_share.',
  CURRENT_TIMESTAMP()
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1 FROM `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  WHERE schema_name='main' AND table_name='specimen_tumor_focus_v1');

-- 7–40: remaining tables (batch upsert via VALUES)
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1`
  (schema_name,table_name,n_columns_total,n_verified,n_not_started,n_failed,n_na,
   table_status,signed_off_ts,signoff_migration,priority_tier,notes,registered_ts)
SELECT schema_name, table_name, n_cols, 0, n_cols, 0, 0, 'live',
       CURRENT_TIMESTAMP(),
       'qc_framework_v1/migrations/327_bulk_md_to_bq_missing_tables.py',
       priority_tier,
       notes, CURRENT_TIMESTAMP()
FROM (
  SELECT
    'main' AS schema_name, t.table_name,
    (SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS` c
     WHERE c.table_name = t.table_name) AS n_cols,
    meta.pt AS priority_tier, meta.n AS notes
  FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.TABLES` t
  JOIN (
    SELECT * FROM UNNEST([
      STRUCT('specimen_master_v1'                    AS tbl,'tier1_canonical_base' AS pt,'mig_327: specimen master. 10,139 rows.' AS n),
      STRUCT('patient_cross_domain_timeline_v2'      ,'tier1_canonical_base','mig_327: patient cross-domain timeline v2. 61,055 rows.'),
      STRUCT('patient_completion_oed_path_linkage_v1','tier2_derived'        ,'mig_327: OED-path linkage for completion thyroidectomy patients. 11,506 rows.'),
      STRUCT('specimen_genomic_assay_v1'             ,'tier2_derived'        ,'mig_327: per-specimen genomic assay. 10,370 rows.'),
      STRUCT('specimen_source_xref_v1'               ,'tier2_derived'        ,'mig_327: specimen-source cross-reference. 11,273 rows.'),
      STRUCT('tg_postop_surveillance_windows_v1'     ,'tier2_derived'        ,'mig_327: Tg postop surveillance windows. 16,184 rows.'),
      STRUCT('tg_timeline_patient_summary_v1'        ,'tier2_derived'        ,'mig_327: Tg timeline per-patient summary. 3,258 rows.'),
      STRUCT('mri_imaging'                           ,'tier3_source'         ,'mig_327: MRI imaging source. 715 rows.'),
      STRUCT('nuclear_med'                           ,'tier3_source'         ,'mig_327: nuclear medicine source. 2,220 rows.'),
      STRUCT('nsqip_enrichment'                      ,'tier3_source'         ,'mig_327: NSQIP enrichment. 1,275 rows.'),
      STRUCT('nsqip_patient_summary'                 ,'tier3_source'         ,'mig_327: NSQIP patient summary. 1,261 rows.'),
      STRUCT('thyroid_sizes'                         ,'tier3_source'         ,'mig_327: thyroid sizes source. 11,675 rows.'),
      STRUCT('thyroid_weights'                       ,'tier3_source'         ,'mig_327: thyroid weights source. 10,001 rows.'),
      STRUCT('note_entities_llm_airway_invasion_v2'  ,'tier3_nlp_source'     ,'mig_327: NLP airway invasion entities v2. 6,054 rows.'),
      STRUCT('note_entities_llm_cervical_ln_detail'  ,'tier3_nlp_source'     ,'mig_327: NLP cervical LN detail. 10,084 rows.'),
      STRUCT('note_entities_llm_dynamic_risk_response','tier3_nlp_source'    ,'mig_327: NLP dynamic risk response. 11,037 rows.'),
      STRUCT('note_entities_llm_esophageal_invasion' ,'tier3_nlp_source'     ,'mig_327: NLP esophageal invasion. 4,409 rows.'),
      STRUCT('note_entities_llm_ete_subgrade_v1'     ,'tier3_nlp_source'     ,'mig_327: NLP ETE subgrade v1. 287 rows.'),
      STRUCT('note_entities_llm_frozen_section_detail','tier3_nlp_source'    ,'mig_327: NLP frozen section detail. 32,408 rows.'),
      STRUCT('note_entities_llm_parathyroid_detail_v1','tier3_nlp_source'    ,'mig_327: NLP parathyroid detail v1. 8,697 rows.'),
      STRUCT('note_entities_llm_past_medical_hx'     ,'tier3_nlp_source'     ,'mig_327: NLP PMH. 18,112 rows.'),
      STRUCT('note_entities_llm_past_surgical_hx'    ,'tier3_nlp_source'     ,'mig_327: NLP PSH. 11,037 rows.'),
      STRUCT('note_entities_llm_pathology'           ,'tier3_nlp_source'     ,'mig_327: NLP pathology. 10,084 rows.'),
      STRUCT('note_entities_llm_presenting_symptoms' ,'tier3_nlp_source'     ,'mig_327: NLP presenting symptoms. 11,037 rows.'),
      STRUCT('note_entities_llm_rai_detailed'        ,'tier3_nlp_source'     ,'mig_327: NLP RAI detailed. 11,037 rows.'),
      STRUCT('note_entities_llm_recurrence'          ,'tier3_nlp_source'     ,'mig_327: NLP recurrence. 11,037 rows.'),
      STRUCT('note_entities_llm_t4b_invasion_v1'     ,'tier3_nlp_source'     ,'mig_327: NLP T4b invasion v1. 944 rows.'),
      STRUCT('note_entities_llm_tirads_granular'     ,'tier3_nlp_source'     ,'mig_327: NLP TIRADS granular. 10,084 rows.'),
      STRUCT('note_entities_llm_vascular_invasion_v2','tier3_nlp_source'     ,'mig_327: NLP vascular invasion v2. 4,667 rows.'),
      STRUCT('note_entities_operative_detail'        ,'tier3_nlp_source'     ,'mig_327: NLP operative detail. 12,151 rows.'),
      STRUCT('note_entities_procedures'              ,'tier3_nlp_source'     ,'mig_327: NLP procedures. 21,942 rows.'),
      STRUCT('signoff_migration'                     ,'tier4_governance'     ,'mig_327: MD signoff_migration governance table. 78 rows.'),
      STRUCT('pub_release_manifest_v1_1'             ,'tier4_governance'     ,'mig_327: MD release manifest v1_1. 1 row.')
    ])
  ) meta ON meta.tbl = t.table_name
  WHERE t.table_type = 'BASE TABLE'
) src
WHERE NOT EXISTS (
  SELECT 1 FROM `thyroid-canonical-pub-2026.pub_signoff.canonical_table_signoff_registry_v1` z
  WHERE z.schema_name = 'main' AND z.table_name = src.table_name
);
