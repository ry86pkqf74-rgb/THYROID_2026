-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY
-- mig_189 canonical_us_thyroid_gland_v2 EVENTS BUILD (mirror mig_171b pattern)
-- Batch_id: mig_189_canonical_us_thyroid_gland_v2_build_ratified_20260430
-- Database: thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Preconditions (Path-C VERIFY before APPLY):
--   1. main.canonical_us_exam_master_VIEW_v2 readable (VIEW; mig_187 R-A extension recommended).
--   2. **NLP spine:** main.clinical_note_thyroid_us_extracted_v1 must exist & mirror
--      clinical_note_ln_extracted_v1 grain (excluding LN-only columns); produced by cloning
--      scripts/extract_clinical_note_ln_data.py → thyroid/US parenchyma entity types.
--      If NLP supplemental distinct (rid, exam_date) pairs < 100 BY PROBE §0d, STOP — see
--      qc_framework_v1/reports/mig_189_canonical_us_thyroid_gland_v2_build_ratified_20260430.md §1.
--   3. Does NOT REPLACE main.canonical_us_thyroid_gland_v2 (legacy shell/table SSOT unchanged).
--
-- Targets:
--   main.canonical_us_thyroid_gland_events_v2
--   main.canonical_us_thyroid_gland_patient_rollup_v2
--   main.val_mig189_canonical_us_thyroid_gland_build_v1
--
-- Closes CF-117-US-GLAND-PARENCHYMA (registry note appendix + new events/rollup SSOT).
-- Governance:
--   * research_id VARCHAR in events/rollup to align canonical_patient_master join pattern (mig_171b).
--   * PHI: evidence snippets LEFT(...,240); never select full clinical_notes_long.note_text here.
--   * CAST(CURRENT_TIMESTAMP AS TIMESTAMP) for all build timestamps.
--   * NO BEGIN TRANSACTION / COMMIT.
--
USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §0 Pre-flight probes (Cowork executes manually before DDL; comment-only here)
-- =============================================================================

-- 0a. CPM invariant.
-- SELECT COUNT(*) AS pm_rows, COUNT(DISTINCT research_id) AS pm_distinct_rids
-- FROM main.canonical_patient_master;
-- Expect: 10871 / 10871

-- 0b. Legacy shell baseline.
-- SELECT COUNT(*) AS shell_rows,
--        COUNT(DISTINCT CAST(research_id AS VARCHAR)) AS shell_patients,
--        COUNT(*) FILTER (WHERE exam_date IS NOT NULL
--                         AND exam_date <> CAST(NULL AS DATE)) AS dated_rows,
--        COUNT(*) FILTER (WHERE exam_date IS NULL) AS null_exam_date_rows
-- FROM main.canonical_us_thyroid_gland_v2;
-- Expect mig_117 lineage: ~13578 shell rows / ~10859 pts (confirm live drift).

-- 0c. Grain inventory (reported cohort).
-- SELECT COUNT(*) AS gland_rows_nonnull_date,
--        COUNT(DISTINCT (CAST(research_id AS VARCHAR), exam_date))
-- FROM main.canonical_us_thyroid_gland_v2
-- WHERE exam_date IS NOT NULL;

-- 0d. NLP supplemental — distinct (rid, exam_date) pairs in NLP spine NOT appearing in gland_v2
-- ON EXACT DATE MATCH (canonical gate for exam alignment).
/*
WITH gland_keys AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id, exam_date
  FROM main.canonical_us_thyroid_gland_v2
  WHERE exam_date IS NOT NULL
),
nlp_keys AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id,
                  COALESCE(
                    CAST(TRY_STRPTIME(NULLIF(TRIM(entity_date), ''), '%m/%d/%Y') AS DATE),
                    TRY_CAST(NULLIF(TRIM(entity_date), '') AS DATE),
                    CAST(TRY_STRPTIME(NULLIF(TRIM(note_date), ''), '%m/%d/%Y') AS DATE),
                    TRY_CAST(NULLIF(TRIM(note_date), '') AS DATE)
                  ) AS exam_date
  FROM main.clinical_note_thyroid_us_extracted_v1
  WHERE extraction_status = 'ok'
    AND evidence_source_modality = 'imaging'
    AND regexp_matches(
          LOWER(COALESCE(evidence_text,'')||' '||COALESCE(entity_value,'')||' '||COALESCE(source_note_type,'')),
          '(ultrasound|sonogram|sonographic)'
        )
)
SELECT COUNT(*) AS n_pairs_nlp_only
FROM nlp_keys n
WHERE n.exam_date IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM gland_keys g
    WHERE g.research_id = n.research_id AND g.exam_date = n.exam_date
  );
*/
-- If n_pairs_nlp_only < 100: abort full lane per ratification memo; escalate to Logan.

-- 0e. Registry CF rows (expected 28).
-- SELECT COUNT(*) AS n_cf_cols
-- FROM main.canonical_column_verification_registry_v1
-- WHERE schema_name='main' AND table_name='canonical_us_thyroid_gland_v2'
--   AND COALESCE(notes,'') ILIKE '%CF-117-US-GLAND-PARENCHYMA%';

-- =============================================================================
-- §A Pre-snapshots / archives
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_thyroid_gland_v2_shell_pre_mig189_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig189_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_us_thyroid_gland_v2;

-- If targets already existed from trial applies, executor must archive manually BEFORE rerun:
--   archive_pub_v1_0.canonical_us_thyroid_gland_events_v2_pre_mig189_20260430
--   archive_pub_v1_0.canonical_us_thyroid_gland_patient_rollup_v2_pre_mig189_20260430

-- =============================================================================
-- §B Events table — structured shell UNION NLP supplemental (exam_id deterministic)
-- =============================================================================
-- Notes:
-- * exam_id_source enum (this migration): structured | nlp_supplemental | fallback
-- * us_exam_id recipe aligned with mig_171b exam-master reuse when exactly one EM row/date.
-- * Legacy Script 364 us_exam_id uses md5(rid|'|'|datevarchar); retained on structured rows via
--   COALESCE(EM match, gland.us_exam_id, deterministic US_EXAM_V2 hash).

CREATE OR REPLACE TABLE main.canonical_us_thyroid_gland_events_v2 AS
WITH exam_master_by_rid_date AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         exam_date,
         MIN(us_exam_id) AS us_exam_id,
         COUNT(*) AS n_exam_rows
  FROM main.canonical_us_exam_master_VIEW_v2
  WHERE exam_date IS NOT NULL
  GROUP BY 1, 2
),
clinical_thy_us_raw AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         COALESCE(
           CAST(TRY_STRPTIME(NULLIF(TRIM(entity_date), ''), '%m/%d/%Y') AS DATE),
           TRY_CAST(NULLIF(TRIM(entity_date), '') AS DATE),
           CAST(TRY_STRPTIME(NULLIF(TRIM(note_date), ''), '%m/%d/%Y') AS DATE),
           TRY_CAST(NULLIF(TRIM(note_date), '') AS DATE)
         ) AS exam_date,
         CAST(ROUND(COALESCE(entity_index, 0)) AS INTEGER) AS entity_index,
         entity_type,
         entity_value,
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
  FROM main.clinical_note_thyroid_us_extracted_v1
  WHERE extraction_status = 'ok'
    AND evidence_source_modality = 'imaging'
    AND regexp_matches(
          LOWER(
            COALESCE(evidence_text, '')
            || ' '
            || COALESCE(entity_value, '')
            || ' '
            || COALESCE(source_note_type, '')
          ),
          '(ultrasound|sonogram|sonographic)'
        )
),
clinical_thy_events AS (
  SELECT research_id,
         exam_date,
         entity_index,
         NULLIF(TRIM(LOWER(CASE
           WHEN regexp_matches(COALESCE(entity_type,'')||' '||COALESCE(entity_value,''), '(hypoechoic|hyperechoic|isoechoic|anechoic|echogenicity)')
             THEN COALESCE(entity_value, regexp_extract(COALESCE(entity_value,evidence_text), '(hypoechoic|hyperechoic|isoechoic|anechoic)', 1))
           ELSE NULL END)), '') AS background_echogenicity_nlp,
         NULLIF(TRIM(CASE
           WHEN regexp_matches(LOWER(COALESCE(evidence_text,'')||COALESCE(entity_value,'')), '(heterogeneous|heterogeneity|heterogen)')
             THEN COALESCE(entity_value,'heterogeneity_mentioned')
           ELSE NULL END), '') AS heterogeneity_nlp,
         CASE WHEN regexp_matches(LOWER(COALESCE(evidence_text,'')||COALESCE(entity_value,'')), '(hashimoto|chronic.?lymphocytic.?thyroiditis)') THEN TRUE ELSE NULL END AS hashimoto_flag_nlp,
         NULLIF(TRIM(CASE
           WHEN regexp_matches(LOWER(COALESCE(evidence_text,'')||COALESCE(entity_value,'')), '(hypervascular|increased.?vascular|vascularity)')
             THEN COALESCE(entity_value,'vascularity_mentioned')
           ELSE NULL END), '') AS vascularity_overall_nlp,
         CASE WHEN regexp_matches(LOWER(COALESCE(evidence_text,'')||COALESCE(entity_value,'')), '(parenchymal.?calc|microcalc|coarse.?calc|thyroid.?calc)') THEN TRUE ELSE NULL END AS calcifications_parenchymal_flag_nlp,
         CASE WHEN regexp_matches(LOWER(COALESCE(evidence_text,'')||COALESCE(entity_value,'')), '(goiter|thyromegaly|enlarged.?thyroid|gland.?enlargement)') THEN TRUE ELSE NULL END AS goiter_flag_nlp,
         CASE WHEN regexp_matches(LOWER(COALESCE(evidence_text,'')||COALESCE(entity_value,'')), '(pyramid|pyramidal)') THEN TRUE ELSE NULL END AS pyramidal_present_flag_nlp,
         CASE WHEN regexp_matches(LOWER(COALESCE(evidence_text,'')||COALESCE(entity_value,'')), '(substernal|retrosternal|retro.?sternal)') THEN TRUE ELSE NULL END AS substernal_extension_flag_nlp,
         NULL::DOUBLE AS rl_length_cm,
         NULL::DOUBLE AS rl_width_cm,
         NULL::DOUBLE AS rl_depth_cm,
         NULL::DOUBLE AS rl_volume_ml,
         NULL::DOUBLE AS ll_length_cm,
         NULL::DOUBLE AS ll_width_cm,
         NULL::DOUBLE AS ll_depth_cm,
         NULL::DOUBLE AS ll_volume_ml,
         NULL::DOUBLE AS isthmus_thickness_mm,
         NULL::DOUBLE AS total_thyroid_volume_ml,
         'clinical_note_thyroid_us_extracted_v1'::VARCHAR AS source_table,
         md5(
           'clinical_note_thyroid_us_extracted_v1|'
           || COALESCE(note_row_id, '')
           || '|'
           || COALESCE(CAST(entity_index AS VARCHAR), '')
           || '|'
           || COALESCE(entity_type, '')
           || '|'
           || COALESCE(entity_value, '')
         ) AS source_row_id,
         source_note_type,
         note_row_id AS source_report_id,
         LEFT(REGEXP_REPLACE(COALESCE(evidence_text, entity_value, ''), '\s+', ' ', 'g'), 240) AS evidence_text,
         confidence,
         original_llm_model AS llm_model,
         CAST(NULL AS TIMESTAMP) AS extracted_at,
         date_confidence,
         date_source_keyword,
         1 AS source_priority,
         'nlp_supplemental'::VARCHAR AS exam_id_source_nominal,
         FALSE AS exam_date_unavailable_fallback_flag
  FROM clinical_thy_us_raw
  WHERE exam_date IS NOT NULL
),
structured_shell_events AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         exam_date,
         NULL::INTEGER AS entity_index,
         NULLIF(TRIM(LOWER(background_echogenicity)), '') AS background_echogenicity_nlp,
         NULLIF(TRIM(heterogeneity), '') AS heterogeneity_nlp,
         CASE
           WHEN regexp_matches(
                  LOWER(TRIM(COALESCE(CAST(hashimoto_pattern AS VARCHAR), ''))),
                  '^(mentioned|present|positive|yes|true|y|x|1)(\b|$)'
                )
           THEN TRUE
           ELSE NULL
         END AS hashimoto_flag_nlp,
         NULLIF(TRIM(vascularity_overall), '') AS vascularity_overall_nlp,
         CASE WHEN LOWER(TRIM(COALESCE(calcifications_parenchymal,''))) IN ('positive','mentioned','present','x') THEN TRUE ELSE NULL END AS calcifications_parenchymal_flag_nlp,
         goiter_flag AS goiter_flag_nlp,
         pyramidal_present_flag AS pyramidal_present_flag_nlp,
         substernal_extension_flag AS substernal_extension_flag_nlp,
         rl_length_cm,
         rl_width_cm,
         rl_depth_cm,
         rl_volume_ml,
         ll_length_cm,
         ll_width_cm,
         ll_depth_cm,
         ll_volume_ml,
         isthmus_thickness_mm,
         total_thyroid_volume_ml,
         'canonical_us_thyroid_gland_v2'::VARCHAR AS source_table,
         COALESCE(
           us_exam_id,
           md5('legacy_usgland|' || CAST(research_id AS VARCHAR) || '|' || COALESCE(CAST(exam_date AS VARCHAR), 'NO_DATE'))
         ) AS source_row_id,
         NULL::VARCHAR AS source_note_type,
         CAST(NULL AS VARCHAR) AS source_report_id,
         LEFT(
           REGEXP_REPLACE(
             COALESCE(clinical_impression_text, '')
             || ' '
             || COALESCE(source_us_impression_text, ''),
             '\s+', ' ', 'g'
           ),
           240
         ) AS evidence_text,
         NULL::DOUBLE AS confidence,
         NULL::VARCHAR AS llm_model,
         extracted_at AS extracted_at,
         CASE WHEN exam_date IS NOT NULL THEN 1.0 ELSE NULL END AS date_confidence,
         CASE WHEN exam_date IS NOT NULL
           THEN 'canonical_us_thyroid_gland_v2.exam_date'
           ELSE NULL
         END::VARCHAR AS date_source_keyword,
         2 AS source_priority,
         CASE WHEN exam_date IS NULL THEN 'fallback' ELSE 'structured' END AS exam_id_source_nominal,
         CASE WHEN exam_date IS NULL THEN TRUE ELSE FALSE END AS exam_date_unavailable_fallback_flag
  FROM main.canonical_us_thyroid_gland_v2
),
all_candidates AS (
  SELECT * FROM clinical_thy_events
  UNION ALL
  SELECT * FROM structured_shell_events
),
with_exam_keys AS (
  SELECT c.*,
         COALESCE(
           CASE WHEN em.n_exam_rows = 1 THEN em.us_exam_id ELSE NULL END,
           md5('US_EXAM_V2|' || c.research_id || '|' || COALESCE(CAST(c.exam_date AS VARCHAR), 'NULL_EXAM'))
         ) AS us_exam_id,
         CASE
           WHEN c.exam_id_source_nominal IN ('nlp_supplemental','structured') THEN c.exam_id_source_nominal
           WHEN c.exam_id_source_nominal = 'fallback' THEN 'fallback'
           ELSE 'fallback'
         END AS exam_id_source_raw
  FROM all_candidates c
  LEFT JOIN exam_master_by_rid_date em
    ON c.research_id = em.research_id
   AND c.exam_date IS NOT DISTINCT FROM em.exam_date
),
normalized_source AS (
  SELECT *,
         CASE
           WHEN exam_id_source_raw = 'nlp_supplemental' THEN 'nlp_supplemental'::VARCHAR
           WHEN exam_id_source_raw = 'structured' THEN 'structured'::VARCHAR
           ELSE 'fallback'::VARCHAR
         END AS exam_id_source
  FROM with_exam_keys
),
deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY research_id,
                        us_exam_id,
                        COALESCE(CAST(exam_date AS VARCHAR), 'NONE')
           ORDER BY source_priority ASC,
                    confidence DESC NULLS LAST,
                    evidence_text DESC NULLS LAST,
                    LENGTH(COALESCE(source_row_id,'')) DESC
         ) AS rn
  FROM normalized_source
),
final_events AS (
  SELECT research_id,
         us_exam_id,
         md5(
           'US_GLAND_EVENT_V2|'
           || research_id || '|'
           || us_exam_id || '|'
           || COALESCE(CAST(entity_index AS VARCHAR), 'exam_level') || '|'
           || COALESCE(source_table, '') || '|'
           || COALESCE(source_row_id, '')
         ) AS gland_event_id,
         entity_index AS gland_entity_index_within_exam,
         exam_date,
         exam_date_unavailable_fallback_flag,
         background_echogenicity_nlp AS background_echogenicity_raw,
         heterogeneity_nlp AS heterogeneity_raw,
         hashimoto_flag_nlp,
         vascularity_overall_nlp AS vascularity_overall_raw,
         calcifications_parenchymal_flag_nlp,
         goiter_flag_nlp AS goiter_flag,
         pyramidal_present_flag_nlp AS pyramidal_present_flag,
         substernal_extension_flag_nlp AS substernal_extension_flag,
         rl_length_cm,
         rl_width_cm,
         rl_depth_cm,
         rl_volume_ml,
         ll_length_cm,
         ll_width_cm,
         ll_depth_cm,
         ll_volume_ml,
         isthmus_thickness_mm,
         total_thyroid_volume_ml,
         source_modality_constants.source_modality,
         source_table,
         source_row_id,
         source_note_type,
         source_report_id,
         evidence_text,
         confidence,
         llm_model,
         date_confidence,
         date_source_keyword,
         extracted_at,
         exam_id_source,
         CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts,
         'mig_189'::VARCHAR AS build_migration
  FROM deduped
  CROSS JOIN (SELECT 'US'::VARCHAR AS source_modality) AS source_modality_constants
  WHERE rn = 1
)
SELECT *
FROM final_events;

-- =============================================================================
-- §C Patient rollup (CPM spine 10,871; gland metrics + NLP parenchyma flags)
-- =============================================================================

CREATE OR REPLACE TABLE main.canonical_us_thyroid_gland_patient_rollup_v2 AS
WITH pm_spine AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_patient_master
),
ev AS (
  SELECT * FROM main.canonical_us_thyroid_gland_events_v2
),
event_agg AS (
  SELECT research_id,
         COUNT(*)::BIGINT AS n_us_gland_events,
         COUNT(DISTINCT us_exam_id)::BIGINT AS n_us_gland_exams,
         MIN(exam_date) FILTER (WHERE exam_date IS NOT NULL) AS first_us_gland_exam_date,
         MAX(exam_date) FILTER (WHERE exam_date IS NOT NULL) AS last_us_gland_exam_date,
         BOOL_OR(COALESCE(goiter_flag, FALSE) IS TRUE)::BOOLEAN AS any_us_goiter_flag,
         BOOL_OR(COALESCE(hashimoto_flag_nlp, FALSE) IS TRUE)::BOOLEAN AS any_us_hashimoto_signal,
         BOOL_OR(COALESCE(calcifications_parenchymal_flag_nlp, FALSE) IS TRUE)::BOOLEAN AS any_us_parenchymal_calc_flag,
         BOOL_OR(COALESCE(pyramidal_present_flag, FALSE) IS TRUE)::BOOLEAN AS any_pyramidal_present,
         BOOL_OR(COALESCE(substernal_extension_flag, FALSE) IS TRUE)::BOOLEAN AS any_substernal_extension_signal,
         MAX(rl_volume_ml) AS max_us_rl_volume_ml,
         MAX(ll_volume_ml) AS max_us_ll_volume_ml,
         MAX(total_thyroid_volume_ml) AS max_us_total_thyroid_volume_ml,
         MAX(isthmus_thickness_mm) AS max_us_isthmus_thickness_mm,
         SUM(CASE WHEN source_table = 'clinical_note_thyroid_us_extracted_v1' THEN 1 ELSE 0 END)::BIGINT AS n_events_from_nlp,
         SUM(CASE WHEN source_table = 'canonical_us_thyroid_gland_v2' THEN 1 ELSE 0 END)::BIGINT AS n_events_from_structured_shell,
         SUM(CASE WHEN exam_id_source = 'nlp_supplemental' THEN 1 ELSE 0 END)::BIGINT AS n_events_by_source_nlp_supplemental,
         SUM(CASE WHEN exam_id_source = 'structured' THEN 1 ELSE 0 END)::BIGINT AS n_events_by_source_structured,
         SUM(CASE WHEN exam_id_source = 'fallback' THEN 1 ELSE 0 END)::BIGINT AS n_events_by_source_fallback
  FROM ev
  GROUP BY research_id
)
SELECT pm.research_id,
       (ea.research_id IS NOT NULL) AS has_us_gland_events,
       COALESCE(ea.n_us_gland_events, 0)::BIGINT AS n_us_gland_events,
       COALESCE(ea.n_us_gland_exams, 0)::BIGINT AS n_us_gland_exams,
       ea.first_us_gland_exam_date,
       ea.last_us_gland_exam_date,
       COALESCE(ea.any_us_goiter_flag, FALSE) AS any_us_goiter_flag,
       COALESCE(ea.any_us_hashimoto_signal, FALSE) AS any_us_hashimoto_signal,
       COALESCE(ea.any_us_parenchymal_calc_flag, FALSE) AS any_us_parenchymal_calc_flag,
       COALESCE(ea.any_pyramidal_present, FALSE) AS any_pyramidal_present,
       COALESCE(ea.any_substernal_extension_signal, FALSE) AS any_substernal_extension_signal,
       ea.max_us_rl_volume_ml,
       ea.max_us_ll_volume_ml,
       ea.max_us_total_thyroid_volume_ml,
       ea.max_us_isthmus_thickness_mm,
       COALESCE(ea.n_events_from_nlp, 0)::BIGINT AS n_events_from_clinical_note_thyroid_us,
       COALESCE(ea.n_events_from_structured_shell, 0)::BIGINT AS n_events_from_legacy_shell_table,
       COALESCE(ea.n_events_by_source_nlp_supplemental, 0)::BIGINT AS n_events_exam_source_nlp_supplemental,
       COALESCE(ea.n_events_by_source_structured, 0)::BIGINT AS n_events_exam_source_structured,
       COALESCE(ea.n_events_by_source_fallback, 0)::BIGINT AS n_events_exam_source_fallback,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts,
       'mig_189'::VARCHAR AS build_migration
FROM pm_spine pm
LEFT JOIN event_agg ea USING (research_id);

-- =============================================================================
-- §D 10-gate validation (parity mig_171b intent; mig_189 US-gland wording)
-- =============================================================================

CREATE OR REPLACE TABLE main.val_mig189_canonical_us_thyroid_gland_build_v1 AS
WITH shell AS (
  SELECT COUNT(*) AS n_shell FROM main.canonical_us_thyroid_gland_v2
),
ev AS (
  SELECT * FROM main.canonical_us_thyroid_gland_events_v2
),
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         exam_date,
         us_exam_id
  FROM main.canonical_us_exam_master_VIEW_v2
),
em_agg AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         exam_date,
         COUNT(*)::BIGINT AS n_em_rows,
         MIN(us_exam_id)::VARCHAR AS us_exam_id_em_single
  FROM main.canonical_us_exam_master_VIEW_v2
  WHERE exam_date IS NOT NULL
  GROUP BY 1, 2
),
validation_rows AS (
  SELECT 'G1_rid_with_events_has_true_rollup'::VARCHAR AS check_id,
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
         CAST(COUNT(*) AS VARCHAR) AS observed_value,
         '0'::VARCHAR AS expected_or_reference,
         'distinct event research_id must resolve to rollup rows with has_us_gland_events TRUE'::VARCHAR AS notes
  FROM (
         SELECT DISTINCT research_id FROM ev
       ) er
  JOIN rollup r USING (research_id)
  WHERE COALESCE(r.has_us_gland_events, FALSE) IS DISTINCT FROM TRUE

  UNION ALL
  SELECT 'G2_events_row_count_ge_shell'::VARCHAR,
         CASE WHEN (SELECT COUNT(*) FROM ev) >= (SELECT n_shell FROM shell) THEN 'PASS' ELSE 'WARN' END,
         CAST((SELECT COUNT(*) FROM ev) AS VARCHAR) || '>=' || CAST((SELECT n_shell FROM shell) AS VARCHAR),
         'shell rows treated as additive lower bound'::VARCHAR,
         'clinical NLP UNION may shrink after dedup; WARN investigates overlap vs shell baseline'::VARCHAR

  UNION ALL
  SELECT 'G3_exam_date_or_fallback_flag'::VARCHAR,
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) AS VARCHAR),
         '0'::VARCHAR,
         'every row requires exam_date OR exam_date_unavailable_fallback_flag'::VARCHAR
  FROM ev
  WHERE exam_date IS NULL AND COALESCE(exam_date_unavailable_fallback_flag, FALSE) IS NOT TRUE

  UNION ALL
  SELECT 'G4_us_exam_id_deterministic_recipe'::VARCHAR,
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) AS VARCHAR),
         '0'::VARCHAR,
         'Exam dates with singleton EM reuse MIN(us_exam_id); else md5 US_EXAM_V2 hash'::VARCHAR
  FROM ev e
  LEFT JOIN em_agg g
    ON e.research_id = g.research_id
   AND e.exam_date IS NOT DISTINCT FROM g.exam_date
  WHERE (COALESCE(g.n_em_rows, 0)::BIGINT = 1
         AND e.us_exam_id IS DISTINCT FROM g.us_exam_id_em_single)
     OR (
          COALESCE(g.n_em_rows, 0)::BIGINT <> 1
          AND e.us_exam_id IS DISTINCT FROM md5(
            'US_EXAM_V2|' || e.research_id || '|' || COALESCE(CAST(e.exam_date AS VARCHAR), 'NULL_EXAM')
          )
        )

  UNION ALL
  SELECT 'G5_no_duplicate_natural_tuple_rid_exam'::VARCHAR,
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) AS VARCHAR),
         '0'::VARCHAR,
         'no duplicate (research_id, us_exam_id, COALESCE(cast exam_date sentinel))'::VARCHAR
  FROM (
    SELECT research_id,
           us_exam_id,
           COUNT(*) AS c
    FROM ev
    GROUP BY 1, 2, CASE WHEN exam_date IS NULL THEN 'DATE_NULL'::VARCHAR ELSE CAST(exam_date AS VARCHAR) END
    HAVING COUNT(*) > 1
  ) d

  UNION ALL
  SELECT 'G6_rollup_schema_and_rid_parity'::VARCHAR,
         CASE WHEN
                  (SELECT COUNT(*) FROM rollup) = 10871
              AND (SELECT COUNT(DISTINCT research_id) FROM rollup) = 10871
              AND (SELECT COUNT(*) FROM rollup WHERE has_us_gland_events IS TRUE) =
                  (SELECT COUNT(DISTINCT research_id) FROM ev)
         THEN 'PASS' ELSE 'FAIL' END,
         CAST((SELECT COUNT(*) FROM rollup WHERE has_us_gland_events IS TRUE) AS VARCHAR) || '='
           || CAST((SELECT COUNT(DISTINCT research_id) FROM ev) AS VARCHAR) || '; rollup_total='
           || CAST((SELECT COUNT(*) FROM rollup) AS VARCHAR),
         'rollup10871_=dist_evt_rids'::VARCHAR,
         'rollup spans full CPM spine AND finding-flag cardinality matches gland-event cohort'::VARCHAR
  FROM shell

  UNION ALL
  SELECT 'G7_exam_source_distribution'::VARCHAR,
         CASE WHEN COUNT(*) FILTER (WHERE exam_id_source = 'structured') = 0 THEN 'FAIL'
              WHEN COUNT(*) FILTER (WHERE exam_date IS NULL) > 0
                   AND COUNT(*) FILTER (
                     WHERE exam_date IS NULL AND exam_id_source <> 'fallback'
                   ) > 0 THEN 'FAIL'
              WHEN COUNT(*) FILTER (WHERE exam_id_source = 'nlp_supplemental') = 0 THEN 'WARN'
              ELSE 'PASS'
          END AS status,
         CAST(SUM(CASE WHEN exam_id_source = 'structured' THEN 1 ELSE 0 END) AS VARCHAR)
           || '|f=' || CAST(SUM(CASE WHEN exam_id_source = 'fallback' THEN 1 ELSE 0 END) AS VARCHAR)
           || '|nlp=' || CAST(SUM(CASE WHEN exam_id_source = 'nlp_supplemental' THEN 1 ELSE 0 END) AS VARCHAR),
         'structured>0;nlp_ideal>0'::VARCHAR,
         'structured+fallback+NLP buckets; NLP=WARN only when NLP spine empty/low-yield per §0d'::VARCHAR
  FROM ev

  UNION ALL
  SELECT 'G8_nlp_supplemental_joins_exam_master'::VARCHAR,
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END,
         CAST(COUNT(*) AS VARCHAR),
         '0 ideal after mig_187 exam-master extension'::VARCHAR,
         'NLP supplemental rows SHOULD join canonical_us_exam_master_VIEW_v2 on (research_id, exam_date, us_exam_id); WARN until EM universe lands'::VARCHAR
  FROM ev e
  WHERE e.exam_id_source = 'nlp_supplemental'
    AND NOT EXISTS (
      SELECT 1 FROM em z
      WHERE z.research_id = e.research_id
        AND z.exam_date IS NOT DISTINCT FROM e.exam_date
        AND z.us_exam_id IS NOT DISTINCT FROM e.us_exam_id
    )

  UNION ALL
  SELECT 'G9_fallback_exam_alignment_probe'::VARCHAR,
         CASE WHEN COUNT(*) FILTER (WHERE exam_date IS NOT NULL AND exam_id_source <> 'fallback') > 0
              THEN 'PASS' ELSE 'WARN' END,
         CAST(COUNT(*) FILTER (WHERE exam_date IS NOT NULL AND exam_id_source <> 'fallback') AS VARCHAR),
         '>0'::VARCHAR,
         'mig171b-equivalent funnel: confirm dated non-fallback gland events exist'::VARCHAR
  FROM ev

  UNION ALL
  SELECT 'G10_measurement_plausibility'::VARCHAR,
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) AS VARCHAR),
         '0'::VARCHAR,
         'bounds: lobe cm <=25; isthmus mm <=40; total vol <=600ml conservative US guard'::VARCHAR
  FROM ev
  WHERE COALESCE(rl_length_cm, 0) > 25
     OR COALESCE(rl_width_cm, 0) > 25
     OR COALESCE(rl_depth_cm, 0) > 25
     OR COALESCE(ll_length_cm, 0) > 25
     OR COALESCE(ll_width_cm, 0) > 25
     OR COALESCE(ll_depth_cm, 0) > 25
     OR COALESCE(isthmus_thickness_mm, 0) > 40
     OR COALESCE(total_thyroid_volume_ml, 0) > 600
),
final_rows AS (
  SELECT *,
         CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS validated_at,
         'mig_189'::VARCHAR AS validation_migration
  FROM validation_rows
)
SELECT * FROM final_rows;

-- =============================================================================
-- §E canonical_column_verification_registry_v1 — close CF trace on flagged cols
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE
              WHEN COALESCE(notes,'') LIKE '%CF-117-US-GLAND-PARENCHYMA CLOSED (mig_189)%' THEN notes
              ELSE COALESCE(notes,'')
                   || ' | mig_189 canonical_us_thyroid_gland_events_v2 + rollup: CF-117-US-GLAND-PARENCHYMA CLOSED '
                   || '(Parenchyma phenotype now sourced from UNION of NLP clinical_note_thyroid_us_extracted_v1 '
                   || 'plus legacy gland shell; deterministic us_exam_id per mig_171b exam-master recipe).'
            END,
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verified_by = 'logan_ratified+mig189_path_c_executor',
    batch_id = 'mig_189_canonical_us_thyroid_gland_v2_build_ratified_20260430'::VARCHAR
WHERE schema_name='main'
  AND table_name='canonical_us_thyroid_gland_v2'
  AND COALESCE(notes,'') ILIKE '%CF-117-US-GLAND-PARENCHYMA%'
  AND COALESCE(notes,'') NOT ILIKE '%CF-117-US-GLAND-PARENCHYMA CLOSED (mig_189)%';

-- =============================================================================
-- §F manuscript_workspace.cpm_reconciliation_provenance_v1 (audit ledger)
-- =============================================================================

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1 (
  run_id,
  started_at,
  ended_at,
  phases_applied,
  critical_findings_cleared,
  high_findings_cleared,
  med_findings_cleared,
  held_for_adjudication
)
SELECT
  'mig189_canonical_us_thyroid_gland_v2_build_ratified_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'pre_snapshot_gland_shell_events_union_nlp_parenchyma_rollups_validation_registry_provenance_cf117_closure_post_probes',
  'CF-117-US-GLAND-PARENCHYMA',
  'canonical_us_thyroid_gland_events_v2_introduction',
  'canonical_us_thyroid_gland_patient_rollup_v2_introduction_val_mig189',
  'depends_on_clinical_note_thyroid_us_extracted_v1_materialization+mig187_exam_master_alignment'
FROM (SELECT 1 AS _anchor) AS _
WHERE NOT EXISTS (
  SELECT 1
  FROM manuscript_workspace.cpm_reconciliation_provenance_v1 p
  WHERE p.run_id = 'mig189_canonical_us_thyroid_gland_v2_build_ratified_20260430'
);

-- =============================================================================
-- §G Post-state probes (comment-only)
-- =============================================================================
-- SELECT status, COUNT(*) AS n_checks
-- FROM main.val_mig189_canonical_us_thyroid_gland_build_v1
-- GROUP BY 1 ORDER BY 1;

-- SELECT exam_id_source, source_table, COUNT(*) AS rows, COUNT(DISTINCT research_id) AS pts
-- FROM main.canonical_us_thyroid_gland_events_v2 GROUP BY 1,2 ORDER BY 1,2;

-- SELECT COUNT(*) AS rollup_pts_with_events FROM main.canonical_us_thyroid_gland_patient_rollup_v2 WHERE has_us_gland_events;
