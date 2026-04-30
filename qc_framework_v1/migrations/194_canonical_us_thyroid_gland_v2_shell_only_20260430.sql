-- mig_194 Option B — shell-only Us thyroid gland events + patient rollup (CF-117-US-GLAND-PARENCHYMA)
-- Batch_id: mig_194_option_b_shell_only_20260430
-- Ratified path: qc_framework_v1/reports/mig_194_thyroid_us_nlp_source_unblock_20260430.md §2 Option B.
-- Database: thyroid_canonical_publication_v1_0
--
-- Does NOT reference clinical_note_thyroid_us_extracted_v1 (absent on MD).
-- Builds canonical_us_thyroid_gland_events_v2 + patient_rollup_v2 ONLY from main.canonical_us_thyroid_gland_v2.
-- exam_id_source ∈ {structured, fallback} — structured when source_ultrasound_reports=TRUE AND exam_date present;
--   otherwise fallback (incl. null exam_date, us_nodules_tirads-only rows).
--
-- Leaves main.canonical_us_thyroid_gland_v2 unchanged (SSOT).
-- CAST(CURRENT_TIMESTAMP AS TIMESTAMP); no BEGIN TRANSACTION.
--
USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §A Archives
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_thyroid_gland_v2_shell_pre_mig194_optionB_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig194_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_us_thyroid_gland_v2;
-- Prior events_v2 / patient_rollup_v2 snapshots (if present) handled by apply runner — avoids first-apply DDL failure when targets absent.

-- =============================================================================
-- §B Events (shell-only)
-- =============================================================================

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
         CASE
           WHEN exam_date IS NULL THEN 'fallback'
           WHEN COALESCE(source_ultrasound_reports, FALSE) IS TRUE THEN 'structured'
           ELSE 'fallback'
         END::VARCHAR AS exam_id_source_nominal,
         CASE WHEN exam_date IS NULL THEN TRUE ELSE FALSE END AS exam_date_unavailable_fallback_flag
  FROM main.canonical_us_thyroid_gland_v2
),
with_exam_keys AS (
  SELECT c.*,
         COALESCE(
           CASE WHEN em.n_exam_rows = 1 THEN em.us_exam_id ELSE NULL END,
           md5('US_EXAM_V2|' || c.research_id || '|' || COALESCE(CAST(c.exam_date AS VARCHAR), 'NULL_EXAM'))
         ) AS us_exam_id,
         CASE
           WHEN c.exam_id_source_nominal IN ('structured','fallback') THEN c.exam_id_source_nominal
           ELSE 'fallback'
         END AS exam_id_source_raw
  FROM structured_shell_events c
  LEFT JOIN exam_master_by_rid_date em
    ON c.research_id = em.research_id
   AND c.exam_date IS NOT DISTINCT FROM em.exam_date
),
normalized_source AS (
  SELECT *,
         CASE
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
         'mig_194_option_b'::VARCHAR AS build_migration
  FROM deduped
  CROSS JOIN (SELECT 'US'::VARCHAR AS source_modality) AS source_modality_constants
  WHERE rn = 1
)
SELECT *
FROM final_events;

-- =============================================================================
-- §C Patient rollup (CPM spine 10,871)
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
         0::BIGINT AS n_events_from_nlp,
         SUM(CASE WHEN source_table = 'canonical_us_thyroid_gland_v2' THEN 1 ELSE 0 END)::BIGINT AS n_events_from_structured_shell,
         0::BIGINT AS n_events_by_source_nlp_supplemental,
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
       'mig_194_option_b'::VARCHAR AS build_migration
FROM pm_spine pm
LEFT JOIN event_agg ea USING (research_id);

-- =============================================================================
-- §D Validation (10 gates; G7/G8 adjusted for shell-only / no NLP spine)
-- =============================================================================

CREATE OR REPLACE TABLE main.val_mig194_canonical_us_thyroid_gland_shell_only_v1 AS
WITH shell AS (
  SELECT COUNT(*) AS n_shell FROM main.canonical_us_thyroid_gland_v2
),
ev AS (
  SELECT * FROM main.canonical_us_thyroid_gland_events_v2
),
rollup AS (
  SELECT * FROM main.canonical_us_thyroid_gland_patient_rollup_v2
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
         'shell rows additive lower bound'::VARCHAR,
         'dedup may equal shell; WARN if shrink'::VARCHAR

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
         'singleton EM reuse MIN(us_exam_id); else md5 US_EXAM_V2 hash'::VARCHAR
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
         'no duplicate (research_id, us_exam_id, exam_date sentinel)'::VARCHAR
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
         'rollup spans CPM spine; has_events matches distinct event rids'::VARCHAR
  FROM shell

  UNION ALL
  SELECT 'G7_shell_only_source_distribution'::VARCHAR,
         CASE WHEN COUNT(*) FILTER (WHERE exam_id_source NOT IN ('structured','fallback')) > 0 THEN 'FAIL'
              WHEN COUNT(*) FILTER (WHERE exam_id_source = 'structured') = 0 THEN 'WARN'
              WHEN COUNT(*) FILTER (WHERE exam_id_source = 'nlp_supplemental') > 0 THEN 'FAIL'
              ELSE 'PASS'
          END AS status,
         CAST(SUM(CASE WHEN exam_id_source = 'structured' THEN 1 ELSE 0 END) AS VARCHAR)
           || '|f=' || CAST(SUM(CASE WHEN exam_id_source = 'fallback' THEN 1 ELSE 0 END) AS VARCHAR)
           || '|nlp=' || CAST(SUM(CASE WHEN exam_id_source = 'nlp_supplemental' THEN 1 ELSE 0 END) AS VARCHAR),
         'structured>0; nlp=0'::VARCHAR,
         'mig_194 Option B: only structured+fallback; nlp_supplemental must be 0'::VARCHAR
  FROM ev

  UNION ALL
  SELECT 'G8_no_nlp_spine_SKIP'::VARCHAR,
         'SKIP'::VARCHAR,
         '0'::VARCHAR,
         'N/A'::VARCHAR,
         'mig_194: clinical_note_thyroid_us_extracted_v1 absent; NLP supplemental join NA — source limitation ratified'::VARCHAR

  UNION ALL
  SELECT 'G9_fallback_exam_alignment_probe'::VARCHAR,
         CASE WHEN COUNT(*) FILTER (WHERE exam_date IS NOT NULL AND exam_id_source = 'structured') > 0
              THEN 'PASS' ELSE 'WARN' END,
         CAST(COUNT(*) FILTER (WHERE exam_date IS NOT NULL AND exam_id_source = 'structured') AS VARCHAR),
         '>0'::VARCHAR,
         'confirm dated structured rows exist'::VARCHAR
  FROM ev

  UNION ALL
  SELECT 'G10_measurement_plausibility'::VARCHAR,
         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
         CAST(COUNT(*) AS VARCHAR),
         '0'::VARCHAR,
         'bounds: lobe cm <=25; isthmus mm <=40; total vol <=600ml'::VARCHAR
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
         'mig_194_option_b'::VARCHAR AS validation_migration
  FROM validation_rows
)
SELECT * FROM final_rows;

-- =============================================================================
-- §E Registry — CF-117 closure (shell-only lineage; supersedes mig_189 NLP wording)
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET notes = CASE
              WHEN COALESCE(notes,'') LIKE '%CF-117-US-GLAND-PARENCHYMA CLOSED (mig_194%' THEN notes
              ELSE COALESCE(notes,'')
                   || ' | CF-117-US-GLAND-PARENCHYMA CLOSED (mig_194 Option B shell-only): '
                   || 'events_v2 + patient_rollup_v2 from canonical_us_thyroid_gland_v2 only; '
                   || 'exam_id_source structured or fallback only; NLP parenchyma spine pending future extraction lane.'
            END,
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verified_by = 'mig194_option_B_shell_only_executor',
    batch_id = 'mig_194_option_b_shell_only_20260430'::VARCHAR
WHERE schema_name='main'
  AND table_name='canonical_us_thyroid_gland_v2'
  AND COALESCE(notes,'') ILIKE '%CF-117-US-GLAND-PARENCHYMA%'
  AND COALESCE(notes,'') NOT ILIKE '%CF-117-US-GLAND-PARENCHYMA CLOSED (mig_194%';

-- =============================================================================
-- §F Provenance ledger
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
  'mig194_us_thyroid_gland_shell_only_option_B_20260430',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'shell_only_events_rollup_validation_registry_cf117_closure_archive_pre_snapshots',
  'CF-117-US-GLAND-PARENCHYMA',
  'canonical_us_thyroid_gland_events_v2_mig194_option_b_rebuild',
  'canonical_us_thyroid_gland_patient_rollup_v2_val_mig194_shell_only',
  'NLP_parenchyma_future_lane; clinical_note_thyroid_us_extracted_v1_absent_as_of_mig_194'
FROM (SELECT 1 AS _anchor) AS _
WHERE NOT EXISTS (
  SELECT 1
  FROM manuscript_workspace.cpm_reconciliation_provenance_v1 p
  WHERE p.run_id = 'mig194_us_thyroid_gland_shell_only_option_B_20260430'
);
