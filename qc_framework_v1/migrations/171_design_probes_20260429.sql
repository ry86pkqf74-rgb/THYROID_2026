-- mig_171 design probes (read-only replay pack)
-- Batch: mig_171_canonical_us_lymph_node_v2_build_20260429
-- Posture: read-only probes only. Do not mutate MotherDuck from this lane.

-- §2a Existing US-related tables in main; confirms no canonical_us_lymph_node_v1 exists.
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
  AND table_name LIKE 'canonical_us%'
ORDER BY table_name;

-- §2b canonical_patient_master columns carrying CF-mig150-TP-UPSTREAM-NOT-IN-MAIN.
SELECT column_name,
       COALESCE(verification_status, 'unknown') AS status,
       notes
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND notes ILIKE '%CF-mig150-TP-UPSTREAM-NOT-IN-MAIN%'
ORDER BY column_name;

-- §2c Candidate LN extraction / canonical sources in main.
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
  AND (
    table_name ILIKE '%lymph_node%'
    OR table_name ILIKE '%_ln_%'
    OR table_name ILIKE 'note_entities_%ln%'
  )
ORDER BY table_name;

-- §2d Path-malignant LN event columns available for overlap / TP bridge validation.
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'main'
  AND table_name = 'canonical_path_malignant_events_v1'
  AND (
    column_name ILIKE '%ln%'
    OR column_name ILIKE '%lymph%'
    OR column_name ILIKE '%nodal%'
  )
ORDER BY column_name;

-- §2e clinical_notes_long metadata columns. Do not SELECT note_text in design lanes.
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'main'
  AND table_name = 'clinical_notes_long'
ORDER BY column_name
LIMIT 200;

-- Existing canonical_us_lymph_node_v2 shell profile.
SELECT COUNT(*) AS n_rows,
       COUNT(DISTINCT research_id) AS n_patients,
       COUNT(*) FILTER (WHERE exam_date IS NULL) AS null_exam_date,
       COUNT(*) FILTER (WHERE suspicious_flag IS TRUE) AS suspicious_true,
       COUNT(*) FILTER (WHERE neck_level IS NOT NULL AND TRIM(CAST(neck_level AS VARCHAR)) <> '') AS neck_level_populated,
       COUNT(*) FILTER (WHERE laterality IS NOT NULL AND TRIM(CAST(laterality AS VARCHAR)) <> '') AS laterality_populated,
       COUNT(*) FILTER (WHERE short_axis_mm IS NOT NULL OR long_axis_mm IS NOT NULL OR size_cm_max IS NOT NULL) AS size_populated,
       COUNT(*) FILTER (WHERE nlp_backfill_pending IS TRUE) AS nlp_backfill_pending_true
FROM main.canonical_us_lymph_node_v2;

-- Existing US exam master profile.
SELECT COUNT(*) AS n_rows,
       COUNT(DISTINCT research_id) AS n_patients,
       COUNT(DISTINCT us_exam_id) AS distinct_exam_ids,
       COUNT(*) FILTER (WHERE exam_date IS NULL) AS null_exam_date,
       COUNT(*) FILTER (WHERE has_us_ln_findings IS TRUE) AS has_us_ln_findings_true,
       SUM(COALESCE(n_us_ln_total_on_exam, 0)) AS sum_ln_total,
       SUM(COALESCE(n_abnormal_us_ln_on_exam, 0)) AS sum_abnormal_ln
FROM main.canonical_us_exam_master_VIEW_v2;

-- Candidate source row/patient counts.
SELECT 'canonical_us_lymph_node_v2' AS source,
       COUNT(*) AS n_rows,
       COUNT(DISTINCT CAST(research_id AS VARCHAR)) AS n_patients
FROM main.canonical_us_lymph_node_v2
UNION ALL
SELECT 'canonical_cervical_ln_clinical_events_v1', COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR))
FROM main.canonical_cervical_ln_clinical_events_v1
UNION ALL
SELECT 'canonical_cervical_ln_clinical_patient_rollup_v1', COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR))
FROM main.canonical_cervical_ln_clinical_patient_rollup_v1
UNION ALL
SELECT 'clinical_note_ln_extracted_v1', COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR))
FROM main.clinical_note_ln_extracted_v1
UNION ALL
SELECT 'note_entities_llm_cervical_ln_detail', COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR))
FROM main.note_entities_llm_cervical_ln_detail
UNION ALL
SELECT 'canonical_path_malignant_events_v1', COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR))
FROM main.canonical_path_malignant_events_v1
UNION ALL
SELECT 'clinical_notes_long', COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR))
FROM main.clinical_notes_long
ORDER BY source;

-- Candidate source schemas.
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'main'
  AND table_name IN (
    'canonical_cervical_ln_clinical_events_v1',
    'canonical_cervical_ln_clinical_patient_rollup_v1',
    'clinical_note_ln_extracted_v1',
    'note_entities_llm_cervical_ln_detail',
    'canonical_path_malignant_events_v1',
    'canonical_us_lymph_node_v2'
  )
ORDER BY table_name, ordinal_position;

-- Exam-id portability: current USLN ids resolve poorly by direct ID but perfectly by (research_id, exam_date).
SELECT COUNT(*) AS n_rows,
       COUNT(*) FILTER (WHERE em.us_exam_id IS NOT NULL) AS rows_join_exam_master,
       COUNT(DISTINCT usn.us_exam_id) AS n_usln_exam_ids,
       COUNT(DISTINCT em.us_exam_id) AS n_joined_exam_ids
FROM main.canonical_us_lymph_node_v2 usn
LEFT JOIN main.canonical_us_exam_master_VIEW_v2 em
  ON usn.us_exam_id = em.us_exam_id;

SELECT COUNT(*) AS n_rows,
       COUNT(*) FILTER (WHERE em.us_exam_id IS NOT NULL) AS rows_join_by_rid_date,
       COUNT(*) FILTER (WHERE md5(CAST(usn.research_id AS VARCHAR) || '|' || CAST(usn.exam_date AS VARCHAR)) = em.us_exam_id) AS legacy_hash_matches_existing,
       COUNT(DISTINCT em.us_exam_id) AS joined_exam_ids
FROM main.canonical_us_lymph_node_v2 usn
LEFT JOIN main.canonical_us_exam_master_VIEW_v2 em
  ON CAST(usn.research_id AS VARCHAR) = CAST(em.research_id AS VARCHAR)
 AND usn.exam_date = em.exam_date;

-- Cohort coverage and PM CF non-null burden.
SELECT COUNT(*) AS cpm_rows,
       COUNT(*) FILTER (WHERE usn.research_id IS NOT NULL) AS cpm_with_usln_row,
       ROUND(100.0 * COUNT(*) FILTER (WHERE usn.research_id IS NOT NULL) / COUNT(*), 2) AS pct_cpm_with_usln_row
FROM main.canonical_patient_master pm
LEFT JOIN (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_us_lymph_node_v2
) usn
  ON CAST(pm.research_id AS VARCHAR) = usn.research_id;

SELECT COUNT(*) AS cpm_rows,
       COUNT(*) FILTER (WHERE tp_central_examined IS NOT NULL) AS tp_central_examined_nonnull,
       COUNT(*) FILTER (WHERE tp_central_positive_total IS NOT NULL) AS tp_central_positive_total_nonnull,
       COUNT(*) FILTER (WHERE tp_ln_central_positive IS NOT NULL) AS tp_ln_central_positive_nonnull,
       COUNT(*) FILTER (WHERE tp_ln_ene IS NOT NULL) AS tp_ln_ene_nonnull,
       COUNT(*) FILTER (WHERE tp_ln_examined IS NOT NULL) AS tp_ln_examined_nonnull,
       COUNT(*) FILTER (WHERE tp_ln_largest_deposit_cm IS NOT NULL) AS tp_ln_largest_deposit_cm_nonnull,
       COUNT(*) FILTER (WHERE tp_ln_lateral_positive IS NOT NULL) AS tp_ln_lateral_positive_nonnull,
       COUNT(*) FILTER (WHERE tp_ln_levels_involved IS NOT NULL) AS tp_ln_levels_involved_nonnull,
       COUNT(*) FILTER (WHERE tp_ln_positive IS NOT NULL) AS tp_ln_positive_nonnull
FROM main.canonical_patient_master;
