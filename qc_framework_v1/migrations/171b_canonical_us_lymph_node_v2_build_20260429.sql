-- mig_171b canonical_us_lymph_node_v2 BUILD SQL (DRAFT FOR RATIFICATION)
-- Batch_id: mig_171b_canonical_us_lymph_node_v2_build_20260429
-- Database: thyroid_canonical_publication_v1_0
-- Posture: AUTHORING ARTIFACT ONLY until Logan/Cowork ratification.
--
-- DO NOT EXECUTE AGAINST MOTHERDUCK UNTIL RATIFIED.
-- This file contains data-definition/data-write statements for the proposed build of:
--   main.canonical_us_lymph_node_events_v2
--   main.canonical_us_lymph_node_patient_rollup_v2
--   main.val_mig171b_canonical_us_ln_build_v1
--
-- Governance notes:
--   * Uses live thyroid_canonical_publication_v1_0.main sources only.
--   * Does not update canonical_patient_master.
--   * Does not replace the existing main.canonical_us_lymph_node_v2 shell table.
--   * Archives source shell / prior target snapshots before replacement.
--   * evidence_text is snippet-limited; raw clinical_notes_long.note_text is never selected.
--   * research_id is stored as VARCHAR to align with canonical_patient_master.
--   * clinical dates are DATE; build/provenance timestamps are TIMESTAMP.
--
-- Closes after ratified apply + green validation:
--   CF-mig171-DESIGN-RATIFICATION-PENDING
--   CF-mig171-EXAM-ID-RECIPE-LOCK
--   CF-mig171-SOURCE-COVERAGE-note_entities_llm_cervical_ln_detail
--   CF-mig171-SOURCE-COVERAGE-canonical_cervical_ln_clinical_events_v1
--   CF-mig171-SOURCE-COVERAGE-canonical_path_malignant_events_v1
--   CF-mig150-TP-UPSTREAM-NOT-IN-MAIN (bridge source, not CPM write)
--
-- Carry-forward opened by this draft:
--   CF-mig171b-EXAM-MASTER-REBUILD: fallback us_exam_id values need downstream exam-master rebuild/verification.
--   CF-mig171b-RAW-JSON-REPLAY-DEFERRED: note_entities_llm_cervical_ln_detail replay remains verification/audit input.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- Section 0 — REQUIRED preflight probes (run before any apply)
-- =============================================================================

-- 0a. CPM invariant.
-- SELECT COUNT(*) AS pm_rows, COUNT(DISTINCT research_id) AS pm_distinct_rids
-- FROM main.canonical_patient_master;
-- Expected: 10871 / 10871

-- 0b. Source shell profile.
-- SELECT COUNT(*) AS shell_rows,
--        COUNT(DISTINCT CAST(research_id AS VARCHAR)) AS shell_patients,
--        COUNT(*) FILTER (WHERE exam_date IS NULL) AS null_exam_date,
--        COUNT(*) FILTER (WHERE suspicious_flag IS TRUE) AS suspicious_true,
--        COUNT(*) FILTER (WHERE nlp_backfill_pending IS TRUE) AS nlp_backfill_pending_true
-- FROM main.canonical_us_lymph_node_v2;
-- Expected from design lane: 6801 / 4077 / 0 / 8 / 6793

-- 0c. Detail-source US-specific candidate coverage.
-- WITH clinical_us AS (
--   SELECT COALESCE(
--            CAST(TRY_STRPTIME(NULLIF(TRIM(entity_date), ''), '%m/%d/%Y') AS DATE),
--            TRY_CAST(NULLIF(TRIM(entity_date), '') AS DATE),
--            CAST(TRY_STRPTIME(NULLIF(TRIM(note_date), ''), '%m/%d/%Y') AS DATE),
--            TRY_CAST(NULLIF(TRIM(note_date), '') AS DATE)
--          ) AS exam_date,
--          *
--   FROM main.clinical_note_ln_extracted_v1
--   WHERE extraction_status = 'ok'
--     AND evidence_source_modality = 'imaging'
--     AND regexp_matches(
--           LOWER(COALESCE(evidence_text, '') || ' ' || COALESCE(entity_value, '') || ' ' || COALESCE(source_note_type, '')),
--           '(ultrasound|sonogram|sonographic)'
--         )
-- )
-- SELECT COUNT(*) AS us_candidate_rows,
--        COUNT(DISTINCT research_id) AS us_candidate_patients,
--        COUNT(*) FILTER (WHERE exam_date IS NOT NULL) AS date_parse_rows
-- FROM clinical_us;
-- Expected from authoring probe: 196 rows / 137 patients / 172 date-parse rows.

-- =============================================================================
-- Section A — pre-snapshots / archives
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_lymph_node_v2_shell_pre_mig171b_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig171b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_us_lymph_node_v2;

-- Prior target snapshots, if these tables already exist from an earlier dry-run/apply,
-- must be created manually before this file is run. Do not include conditional SELECTs
-- from possibly-missing objects here: DuckDB binds table references before WHERE EXISTS.
-- Suggested names:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_lymph_node_events_v2_pre_mig171b_20260429
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_lymph_node_patient_rollup_v2_pre_mig171b_20260429

-- =============================================================================
-- Section B — events table build
-- =============================================================================

CREATE OR REPLACE TABLE main.canonical_us_lymph_node_events_v2 AS
WITH exam_master_by_rid_date AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         exam_date,
         MIN(us_exam_id) AS us_exam_id,
         COUNT(*) AS n_exam_rows
  FROM main.canonical_us_exam_master_VIEW_v2
  WHERE exam_date IS NOT NULL
  GROUP BY 1, 2
),
clinical_us_raw AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         COALESCE(
           CAST(TRY_STRPTIME(NULLIF(TRIM(entity_date), ''), '%m/%d/%Y') AS DATE),
           TRY_CAST(NULLIF(TRIM(entity_date), '') AS DATE),
           CAST(TRY_STRPTIME(NULLIF(TRIM(note_date), ''), '%m/%d/%Y') AS DATE),
           TRY_CAST(NULLIF(TRIM(note_date), '') AS DATE)
         ) AS exam_date,
         CAST(ROUND(COALESCE(entity_index, 0)) AS INTEGER) AS ln_index,
         entity_type,
         entity_value,
         ln_level,
         laterality,
         size_cm,
         ln_status,
         extranodal_extension,
         present_or_negated,
         confidence,
         evidence_text,
         source_note_type,
         note_row_id,
         original_llm_model,
         date_confidence,
         date_source_keyword,
         entity_date,
         note_date
  FROM main.clinical_note_ln_extracted_v1
  WHERE extraction_status = 'ok'
    AND evidence_source_modality = 'imaging'
    AND regexp_matches(
          LOWER(COALESCE(evidence_text, '') || ' ' || COALESCE(entity_value, '') || ' ' || COALESCE(source_note_type, '')),
          '(ultrasound|sonogram|sonographic)'
        )
),
clinical_us_events AS (
  SELECT research_id,
         exam_date,
         ln_index,
         CASE
           WHEN LOWER(COALESCE(laterality, entity_value, '')) IN ('left', 'l', 'lt')
             OR LOWER(COALESCE(laterality, entity_value, '')) LIKE '%left%'
             THEN 'left'
           WHEN LOWER(COALESCE(laterality, entity_value, '')) IN ('right', 'r', 'rt')
             OR LOWER(COALESCE(laterality, entity_value, '')) LIKE '%right%'
             THEN 'right'
           WHEN LOWER(COALESCE(laterality, entity_value, '')) LIKE '%bilat%'
             THEN 'bilateral'
           WHEN LOWER(COALESCE(laterality, entity_value, '')) LIKE '%central%'
             THEN 'central'
           ELSE NULL
         END AS side,
         NULLIF(REGEXP_REPLACE(LOWER(TRIM(COALESCE(ln_level, ''))), '^level\s*', ''), '') AS neck_level,
         NULL::VARCHAR AS region,
         NULL::DOUBLE AS size_short_mm,
         NULL::DOUBLE AS size_long_mm,
         CASE WHEN size_cm BETWEEN 0.05 AND 15.0 THEN size_cm * 10.0 ELSE NULL END AS size_max_mm,
         CASE WHEN size_cm BETWEEN 0.05 AND 15.0 THEN size_cm ELSE NULL END AS size_max_cm,
         NULL::VARCHAR AS shape,
         NULL::VARCHAR AS echogenicity,
         NULL::BOOLEAN AS hilum_preserved,
         CASE
           WHEN regexp_matches(LOWER(COALESCE(evidence_text, '') || ' ' || COALESCE(entity_value, '')), 'calcification|microcalc')
             THEN 'mentioned'
           ELSE NULL
         END AS calcifications,
         CASE
           WHEN regexp_matches(LOWER(COALESCE(evidence_text, '') || ' ' || COALESCE(entity_value, '')), 'cystic') THEN TRUE
           ELSE NULL
         END AS cystic_component,
         NULL::VARCHAR AS vascularity_pattern,
         CASE WHEN extranodal_extension IS TRUE THEN TRUE ELSE NULL END AS extranodal_extension_on_us,
         CASE
           WHEN LOWER(COALESCE(present_or_negated, '')) = 'negated' THEN FALSE
           WHEN regexp_matches(LOWER(COALESCE(ln_status, entity_value, evidence_text, '')), '(suspicious|positive|metastatic|abnormal|malignant)')
             THEN TRUE
           WHEN regexp_matches(LOWER(COALESCE(ln_status, entity_value, evidence_text, '')), '(negative|benign|normal|no suspicious)')
             THEN FALSE
           ELSE NULL
         END AS suspicious_flag,
         CASE
           WHEN regexp_matches(LOWER(COALESCE(ln_status, entity_value, evidence_text, '')), '(metastatic|malignant|positive)') THEN 'high'
           WHEN regexp_matches(LOWER(COALESCE(ln_status, entity_value, evidence_text, '')), '(suspicious|abnormal)') THEN 'moderate'
           WHEN regexp_matches(LOWER(COALESCE(ln_status, entity_value, evidence_text, '')), '(negative|benign|normal|no suspicious)') THEN 'low'
           ELSE 'unknown'
         END AS suspicion_level,
         CASE WHEN regexp_matches(LOWER(COALESCE(evidence_text, '') || ' ' || COALESCE(entity_value, '')), '(biopsy|fna|aspiration)') THEN TRUE ELSE NULL END AS biopsy_recommended,
         CASE WHEN entity_type = 'fna_of_ln' OR regexp_matches(LOWER(COALESCE(evidence_text, '') || ' ' || COALESCE(entity_value, '')), '(fna|fine needle)') THEN TRUE ELSE NULL END AS fna_of_ln_mentioned,
         CASE WHEN entity_type = 'washout_tg' OR regexp_matches(LOWER(COALESCE(evidence_text, '') || ' ' || COALESCE(entity_value, '')), '(washout|thyroglobulin)') THEN TRUE ELSE NULL END AS washout_tg_mentioned,
         'US'::VARCHAR AS source_modality,
         'clinical_note_ln_extracted_v1'::VARCHAR AS source_table,
         md5('clinical_note_ln_extracted_v1|' || COALESCE(note_row_id, '') || '|' || COALESCE(CAST(ln_index AS VARCHAR), '') || '|' || COALESCE(entity_type, '') || '|' || COALESCE(entity_value, '')) AS source_row_id,
         source_note_type AS source_note_type,
         note_row_id AS source_report_id,
         LEFT(REGEXP_REPLACE(COALESCE(evidence_text, entity_value, ''), '\s+', ' ', 'g'), 240) AS evidence_text,
         confidence,
         original_llm_model AS llm_model,
         CAST(NULL AS TIMESTAMP) AS extracted_at,
         date_confidence,
         date_source_keyword,
         1 AS source_priority
  FROM clinical_us_raw
  WHERE exam_date IS NOT NULL
),
legacy_shell_events AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         exam_date,
         us_ln_index_within_exam AS ln_index,
         CASE
           WHEN LOWER(COALESCE(laterality, '')) IN ('left', 'l', 'lt') OR LOWER(COALESCE(laterality, '')) LIKE '%left%' THEN 'left'
           WHEN LOWER(COALESCE(laterality, '')) IN ('right', 'r', 'rt') OR LOWER(COALESCE(laterality, '')) LIKE '%right%' THEN 'right'
           WHEN LOWER(COALESCE(laterality, '')) LIKE '%bilat%' THEN 'bilateral'
           WHEN LOWER(COALESCE(laterality, '')) LIKE '%central%' THEN 'central'
           ELSE NULLIF(TRIM(laterality), '')
         END AS side,
         NULLIF(REGEXP_REPLACE(LOWER(TRIM(COALESCE(neck_level, ''))), '^level\s*', ''), '') AS neck_level,
         region,
         short_axis_mm AS size_short_mm,
         long_axis_mm AS size_long_mm,
         COALESCE(size_cm_max * 10.0, GREATEST(short_axis_mm, long_axis_mm)) AS size_max_mm,
         COALESCE(size_cm_max, GREATEST(short_axis_mm, long_axis_mm) / 10.0) AS size_max_cm,
         shape,
         echogenicity,
         hilum_preserved,
         calcifications,
         cystic_component,
         vascularity_pattern,
         extranodal_extension_on_us,
         suspicious_flag,
         COALESCE(NULLIF(TRIM(suspicion_level), ''), CASE WHEN suspicious_flag IS TRUE THEN 'moderate' WHEN suspicious_flag IS FALSE THEN 'low' ELSE 'unknown' END) AS suspicion_level,
         biopsy_recommended,
         NULL::BOOLEAN AS fna_of_ln_mentioned,
         NULL::BOOLEAN AS washout_tg_mentioned,
         'US'::VARCHAR AS source_modality,
         'canonical_us_lymph_node_v2'::VARCHAR AS source_table,
         COALESCE(us_ln_id, md5('legacy_usln|' || CAST(research_id AS VARCHAR) || '|' || CAST(exam_date AS VARCHAR) || '|' || CAST(us_ln_index_within_exam AS VARCHAR))) AS source_row_id,
         source_note_type,
         source_report_id,
         LEFT(REGEXP_REPLACE(COALESCE(evidence_text, ''), '\s+', ' ', 'g'), 240) AS evidence_text,
         confidence,
         llm_model,
         extracted_at,
         CASE WHEN exam_date IS NOT NULL THEN 1.0 ELSE NULL END AS date_confidence,
         'canonical_us_lymph_node_v2.exam_date'::VARCHAR AS date_source_keyword,
         2 AS source_priority
  FROM main.canonical_us_lymph_node_v2
  WHERE exam_date IS NOT NULL
),
all_candidates AS (
  SELECT * FROM clinical_us_events
  UNION ALL
  SELECT * FROM legacy_shell_events
),
with_exam_id AS (
  SELECT c.*,
         COALESCE(
           CASE WHEN em.n_exam_rows = 1 THEN em.us_exam_id ELSE NULL END,
           md5('US_EXAM_V2|' || c.research_id || '|' || CAST(c.exam_date AS VARCHAR))
         ) AS us_exam_id,
         CASE WHEN em.n_exam_rows = 1 THEN 'exam_master_reused' ELSE 'fallback_ln_only_exam_id' END AS exam_id_source
  FROM all_candidates c
  LEFT JOIN exam_master_by_rid_date em
    ON c.research_id = em.research_id
   AND c.exam_date = em.exam_date
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY research_id, us_exam_id, COALESCE(CAST(ln_index AS VARCHAR), 'textual'), COALESCE(side, ''), COALESCE(neck_level, ''), source_table, source_row_id
           ORDER BY source_priority, confidence DESC NULLS LAST, evidence_text DESC NULLS LAST
         ) AS rn
  FROM with_exam_id
),
final_events AS (
  SELECT research_id,
         us_exam_id,
         md5('US_LN_EVENT_V2|' || research_id || '|' || us_exam_id || '|' || COALESCE(CAST(ln_index AS VARCHAR), 'textual') || '|' || COALESCE(side, 'unspecified') || '|' || COALESCE(neck_level, 'unspecified') || '|' || source_table || '|' || source_row_id) AS ln_event_id,
         ln_index,
         side,
         neck_level,
         region,
         size_short_mm,
         size_long_mm,
         size_max_mm,
         size_max_cm,
         shape,
         echogenicity,
         hilum_preserved,
         calcifications,
         cystic_component,
         vascularity_pattern,
         extranodal_extension_on_us,
         suspicious_flag,
         suspicion_level,
         biopsy_recommended,
         fna_of_ln_mentioned,
         washout_tg_mentioned,
         source_modality,
         source_table,
         source_row_id,
         source_note_type,
         source_report_id,
         evidence_text,
         confidence,
         llm_model,
         exam_date,
         date_confidence,
         date_source_keyword,
         extracted_at,
         exam_id_source,
         CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts,
         'mig_171b'::VARCHAR AS build_migration
  FROM deduped
  WHERE rn = 1
)
SELECT *
FROM final_events;

-- =============================================================================
-- Section C — patient rollup build
-- =============================================================================

CREATE OR REPLACE TABLE main.canonical_us_lymph_node_patient_rollup_v2 AS
WITH pm_spine AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_patient_master
),
event_agg AS (
  SELECT research_id,
         COUNT(*)::INTEGER AS n_us_ln_events,
         COUNT(DISTINCT us_exam_id)::INTEGER AS n_us_ln_exams,
         MIN(exam_date) AS first_us_ln_exam_date,
         MAX(exam_date) AS last_us_ln_exam_date,
         BOOL_OR(suspicious_flag IS TRUE) AS any_us_ln_suspicious,
         COUNT(*) FILTER (WHERE suspicious_flag IS TRUE)::INTEGER AS n_us_ln_suspicious,
         MAX(size_short_mm) AS max_us_ln_short_axis_mm,
         MAX(size_long_mm) AS max_us_ln_long_axis_mm,
         MAX(size_max_mm) AS max_us_ln_size_mm,
         MAX(size_max_cm) AS max_us_ln_size_cm,
         BOOL_OR(extranodal_extension_on_us IS TRUE) AS any_us_ln_extranodal_extension,
         BOOL_OR(biopsy_recommended IS TRUE) AS any_us_ln_biopsy_recommended,
         BOOL_OR(fna_of_ln_mentioned IS TRUE) AS any_us_ln_fna_mentioned,
         BOOL_OR(washout_tg_mentioned IS TRUE) AS any_us_ln_washout_tg_mentioned,
         COUNT(*) FILTER (WHERE side IS NULL AND neck_level IS NULL AND size_max_mm IS NULL)::INTEGER AS n_us_ln_events_textual_only,
         COUNT(*) FILTER (WHERE confidence >= 0.80 OR (source_table = 'canonical_us_lymph_node_v2' AND confidence IS NULL))::INTEGER AS n_us_ln_events_high_confidence
  FROM main.canonical_us_lymph_node_events_v2
  GROUP BY research_id
),
side_tokens AS (
  SELECT research_id,
         STRING_AGG(side, '|' ORDER BY side) AS us_ln_sides_observed
  FROM (
    SELECT DISTINCT research_id, side
    FROM main.canonical_us_lymph_node_events_v2
    WHERE side IS NOT NULL AND TRIM(side) <> ''
  ) s
  GROUP BY research_id
),
level_tokens AS (
  SELECT research_id,
         STRING_AGG(neck_level, '|' ORDER BY neck_level) AS us_ln_levels_observed
  FROM (
    SELECT DISTINCT research_id, neck_level
    FROM main.canonical_us_lymph_node_events_v2
    WHERE neck_level IS NOT NULL AND TRIM(neck_level) <> ''
  ) l
  GROUP BY research_id
),
path_patient AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         MAX(COALESCE(ln_examined, CAST(nodal_disease_total_count AS DOUBLE))) AS tp_ln_examined,
         MAX(COALESCE(ln_involved, CAST(nodal_disease_positive_count AS DOUBLE))) AS tp_central_positive_total,
         BOOL_OR(COALESCE(ln_involved, CAST(nodal_disease_positive_count AS DOUBLE), 0) > 0) AS tp_ln_positive,
         BOOL_OR(
           COALESCE(ln_involved, CAST(nodal_disease_positive_count AS DOUBLE), 0) > 0
           AND UPPER(COALESCE(n_stage_ajcc8, n_stage_ajcc7, '')) IN ('N1A')
         ) AS tp_ln_central_positive,
         BOOL_OR(
           COALESCE(ln_involved, CAST(nodal_disease_positive_count AS DOUBLE), 0) > 0
           AND UPPER(COALESCE(n_stage_ajcc8, n_stage_ajcc7, '')) IN ('N1B')
         ) AS tp_ln_lateral_positive,
         BOOL_OR(LOWER(COALESCE(extranodal_extension, '')) IN ('x', 'yes', 'present', 'positive', 'focal', 'extensive', 'microscopic', 'gross')) AS tp_ln_ene,
         MAX(CASE
               WHEN COALESCE(ln_involved, CAST(nodal_disease_positive_count AS DOUBLE), 0) > 0
                AND UPPER(COALESCE(n_stage_ajcc8, n_stage_ajcc7, '')) = 'N1A'
                 THEN COALESCE(ln_examined, CAST(nodal_disease_total_count AS DOUBLE))
               ELSE NULL
             END) AS tp_central_examined,
         MAX(CASE
               WHEN COALESCE(ln_involved, CAST(nodal_disease_positive_count AS DOUBLE), 0) > 0
                 THEN size_greatest_dimension_cm
               ELSE NULL
             END) AS tp_ln_largest_deposit_cm
  FROM main.canonical_path_malignant_events_v1
  GROUP BY 1
),
path_level_tokens AS (
  SELECT research_id,
         STRING_AGG(n_stage_token, '|' ORDER BY n_stage_token) AS tp_ln_levels_involved
  FROM (
    SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id,
           UPPER(COALESCE(n_stage_ajcc8, n_stage_ajcc7)) AS n_stage_token
    FROM main.canonical_path_malignant_events_v1
    WHERE COALESCE(ln_involved, CAST(nodal_disease_positive_count AS DOUBLE), 0) > 0
      AND UPPER(COALESCE(n_stage_ajcc8, n_stage_ajcc7, '')) IN ('N1A', 'N1B', 'N1')
  ) p
  GROUP BY research_id
)
SELECT pm.research_id,
       (ea.research_id IS NOT NULL) AS has_us_ln_findings,
       COALESCE(ea.n_us_ln_events, 0) AS n_us_ln_events,
       COALESCE(ea.n_us_ln_exams, 0) AS n_us_ln_exams,
       ea.first_us_ln_exam_date,
       ea.last_us_ln_exam_date,
       COALESCE(ea.any_us_ln_suspicious, FALSE) AS any_us_ln_suspicious,
       COALESCE(ea.n_us_ln_suspicious, 0) AS n_us_ln_suspicious,
       ea.max_us_ln_short_axis_mm,
       ea.max_us_ln_long_axis_mm,
       ea.max_us_ln_size_mm,
       ea.max_us_ln_size_cm,
       st.us_ln_sides_observed,
       lt.us_ln_levels_observed,
       ea.any_us_ln_extranodal_extension,
       ea.any_us_ln_biopsy_recommended,
       ea.any_us_ln_fna_mentioned,
       ea.any_us_ln_washout_tg_mentioned,
       COALESCE(ea.n_us_ln_events_textual_only, 0) AS n_us_ln_events_textual_only,
       COALESCE(ea.n_us_ln_events_high_confidence, 0) AS n_us_ln_events_high_confidence,
       pp.tp_central_examined,
       pp.tp_central_positive_total,
       pp.tp_ln_central_positive,
       pp.tp_ln_ene,
       pp.tp_ln_examined,
       pp.tp_ln_largest_deposit_cm,
       pp.tp_ln_lateral_positive,
       plt.tp_ln_levels_involved,
       pp.tp_ln_positive,
       CASE
         WHEN ea.research_id IS NULL THEN 'no_us_ln_event_source_rows_after_us_modality_gate'
         WHEN ea.n_us_ln_events_textual_only = ea.n_us_ln_events THEN 'textual_only_us_ln_findings'
         ELSE 'us_ln_event_detail_available'
       END AS source_coverage_notes,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts,
       'mig_171b'::VARCHAR AS build_migration
FROM pm_spine pm
LEFT JOIN event_agg ea USING (research_id)
LEFT JOIN side_tokens st USING (research_id)
LEFT JOIN level_tokens lt USING (research_id)
LEFT JOIN path_patient pp USING (research_id)
LEFT JOIN path_level_tokens plt USING (research_id);

-- =============================================================================
-- Section D — validation table
-- =============================================================================

CREATE OR REPLACE TABLE main.val_mig171b_canonical_us_ln_build_v1 AS
WITH events AS (
  SELECT * FROM main.canonical_us_lymph_node_events_v2
),
rollup AS (
  SELECT * FROM main.canonical_us_lymph_node_patient_rollup_v2
),
pm AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id FROM main.canonical_patient_master
),
exam_master AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id, exam_date, us_exam_id
  FROM main.canonical_us_exam_master_VIEW_v2
),
rollup_event_recalc AS (
  SELECT research_id,
         COUNT(*) AS n_events_recalc,
         COUNT(*) FILTER (WHERE suspicious_flag IS TRUE) AS n_suspicious_recalc,
         BOOL_OR(suspicious_flag IS TRUE) AS any_suspicious_recalc
  FROM events
  GROUP BY research_id
),
validation_rows AS (
  SELECT 'G1_event_id_unique' AS check_id,
         CASE WHEN COUNT(*) = COUNT(DISTINCT ln_event_id) THEN 'PASS' ELSE 'FAIL' END AS status,
         CAST(COUNT(*) AS VARCHAR) AS observed_value,
         CAST(COUNT(DISTINCT ln_event_id) AS VARCHAR) AS expected_or_reference,
         'ln_event_id must be unique' AS notes
  FROM events
  UNION ALL
  SELECT 'G2_event_exam_date_nonnull',
         CASE WHEN COUNT(*) FILTER (WHERE exam_date IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) FILTER (WHERE exam_date IS NULL) AS VARCHAR),
         '0',
         'events require a parsed DATE exam_date'
  FROM events
  UNION ALL
  SELECT 'G3_source_modality_us_only',
         CASE WHEN COUNT(DISTINCT source_modality) = 1 AND MIN(source_modality) = 'US' THEN 'PASS' ELSE 'FAIL' END,
         STRING_AGG(DISTINCT source_modality, '|' ORDER BY source_modality),
         'US',
         'US lymph-node table cannot ingest non-US modalities'
  FROM events
  UNION ALL
  SELECT 'G4_evidence_snippet_limited',
         CASE WHEN COALESCE(MAX(LENGTH(evidence_text)), 0) <= 240 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COALESCE(MAX(LENGTH(evidence_text)), 0) AS VARCHAR),
         '<=240',
         'PHI safety: no full note text in evidence_text'
  FROM events
  UNION ALL
  SELECT 'G5_rollup_row_count',
         CASE WHEN COUNT(*) = 10871 AND COUNT(DISTINCT research_id) = 10871 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) AS VARCHAR) || '/' || CAST(COUNT(DISTINCT research_id) AS VARCHAR),
         '10871/10871',
         'patient rollup must align to CPM spine'
  FROM rollup
  UNION ALL
  SELECT 'G6_rollup_has_findings_bidirectional',
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) AS VARCHAR),
         '0',
         'rollup has_us_ln_findings must match event existence'
  FROM rollup r
  LEFT JOIN rollup_event_recalc e USING (research_id)
  WHERE r.has_us_ln_findings IS DISTINCT FROM (e.research_id IS NOT NULL)
  UNION ALL
  SELECT 'G7_rollup_event_counts_match',
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) AS VARCHAR),
         '0',
         'rollup n_us_ln_events / suspicious counts must match events'
  FROM rollup r
  LEFT JOIN rollup_event_recalc e USING (research_id)
  WHERE COALESCE(r.n_us_ln_events, 0) <> COALESCE(e.n_events_recalc, 0)
     OR COALESCE(r.n_us_ln_suspicious, 0) <> COALESCE(e.n_suspicious_recalc, 0)
     OR COALESCE(r.any_us_ln_suspicious, FALSE) <> COALESCE(e.any_suspicious_recalc, FALSE)
  UNION ALL
  SELECT 'G8_events_resolve_existing_exam_master',
         CASE WHEN COUNT(*) FILTER (WHERE em.us_exam_id IS NULL AND e.exam_id_source = 'exam_master_reused') = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) FILTER (WHERE em.us_exam_id IS NULL AND e.exam_id_source = 'exam_master_reused') AS VARCHAR),
         '0',
         'events that claim exam_master_reused must join exam master by us_exam_id'
  FROM events e
  LEFT JOIN exam_master em
    ON e.research_id = em.research_id
   AND e.exam_date = em.exam_date
   AND e.us_exam_id = em.us_exam_id
  UNION ALL
  SELECT 'G9_fallback_exam_ids_pending_rebuild',
         CASE WHEN COUNT(*) FILTER (WHERE exam_id_source = 'fallback_ln_only_exam_id') = 0 THEN 'PASS' ELSE 'WARN' END,
         CAST(COUNT(*) FILTER (WHERE exam_id_source = 'fallback_ln_only_exam_id') AS VARCHAR),
         '0 ideal before exam-master rebuild',
         'fallback IDs are expected only for LN-only dates and must be resolved by downstream exam-master rebuild'
  FROM events
  UNION ALL
  SELECT 'G10_pm_anti_join_rollup',
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) AS VARCHAR),
         '0',
         'every CPM patient must appear in rollup'
  FROM pm
  LEFT JOIN rollup USING (research_id)
  WHERE rollup.research_id IS NULL
)
SELECT check_id,
       status,
       observed_value,
       expected_or_reference,
       notes,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS validated_at,
       'mig_171b'::VARCHAR AS validation_migration
FROM validation_rows;

-- =============================================================================
-- Section E — post-apply review probes (run after validation table materializes)
-- =============================================================================

-- E1. Validation summary.
-- SELECT status, COUNT(*) AS n_checks
-- FROM main.val_mig171b_canonical_us_ln_build_v1
-- GROUP BY status
-- ORDER BY status;

-- E2. Event source mix.
-- SELECT source_table, exam_id_source, COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_patients
-- FROM main.canonical_us_lymph_node_events_v2
-- GROUP BY 1, 2
-- ORDER BY 1, 2;

-- E3. Rollup coverage.
-- SELECT COUNT(*) AS rollup_rows,
--        COUNT(*) FILTER (WHERE has_us_ln_findings IS TRUE) AS patients_with_us_ln_events,
--        SUM(n_us_ln_events) AS total_events,
--        SUM(n_us_ln_suspicious) AS total_suspicious_events,
--        COUNT(*) FILTER (WHERE tp_ln_positive IS TRUE) AS path_bridge_ln_positive_patients
-- FROM main.canonical_us_lymph_node_patient_rollup_v2;

-- E4. CPM remains untouched.
-- SELECT COUNT(*) AS pm_rows, COUNT(DISTINCT research_id) AS pm_distinct_rids
-- FROM main.canonical_patient_master;
