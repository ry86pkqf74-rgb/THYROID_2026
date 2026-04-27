-- ============================================================================
-- Migration 62 — canonical_recurrence_resolved_v1 (strict dual-track)
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Author:        Logan Glosser (executed via Claude / Cowork)
-- Date:          2026-04-27
-- Issue ID:      MANUSCRIPT_RECURRENCE_DUAL_TRACK
-- ----------------------------------------------------------------------------
-- Problem:
--   The existing recurrence pipeline conflated path-proven recurrence with
--   imaging-suspicious findings. Specifically:
--   - canonical_recurrence_v1 had recurrence_confirmed=FALSE on all 10,871 patients
--     (broken — never populated).
--   - recurrence_event_clean_v1 had 1,818 'structural_recurrence' patients,
--     but 1,764 (97%) had recurrence_definition='structural_date_unknown' and
--     came from an old archived rule-based extractor (extracted_recurrence_refined_v1)
--     that was overly permissive and conflated all recurrence-adjacent mentions.
--   - Only 54 patients had structural_confirmed (dated path-proven).
--   - LLM (note_entities_llm_recurrence) confirmed only 109 of 1,818 — 94% disagreement.
--
-- Solution (this migration):
--   Build a strict dual-track patient-level canonical:
--     - recurrence_path_proven    BOOL  + date + source + evidence
--     - recurrence_imaging_suspicious BOOL + date + modality + finding text
--     - recurrence_status_final   ENUM  (path_proven|imaging_only_unconfirmed|none)
--     - recurrence_imaging_then_path_confirmed for time-to-confirmation analyses
--
-- Path-proven sources (priority order):
--   1. multi_malignant_surgery — patients with malignant path on 2+ distinct
--      surgery dates (reoperation for recurrent disease — user idea)
--   2. structural_confirmed     — recurrence_event_clean_v1 dated rows
--   3. post_op_fna_b56          — FNA Bethesda 5/6 done >30d after first surgery
--   4. llm_path_keyword         — LLM entity with path-keyword in evidence
--                                 (biopsy, FNA, cytology, op, specimen, pathology)
--
-- Imaging-suspicious sources:
--   1. ct_imaging.lymph_nodes_suspicious = TRUE (post-op)
--   2. mri_imaging.pathologic_lymph_nodes >= 1 (post-op)
--   3. nuclear_med.impression_text matches recurr|suspicious|residual|metasta
--   4. llm_imaging_keyword (LLM entity with imaging-keyword and NO path-keyword)
--
-- DROPPED (intentionally): 1,686 'structural_date_unknown' patients with NO
--   dated signal in any source. These are likely false positives from the older
--   permissive extractor and should not be carried forward.
-- ----------------------------------------------------------------------------
-- Reads:   main.canonical_path_malignant_events_v1, recurrence_event_clean_v1,
--          canonical_fna_events_v1, note_entities_llm_recurrence,
--          canonical_us_lymph_node_v2, ct_imaging, mri_imaging, nuclear_med,
--          canonical_patient_master
-- Writes:  manuscript_workspace.recurrence_path_proven_candidates_v1
--          manuscript_workspace.recurrence_imaging_suspicious_candidates_v1
--          main.canonical_recurrence_resolved_v1
--          manuscript_workspace.ete_manuscript_analytic_v7 (view layered on v6)
--          main.canonical_ete_event_resolved_v1 (refreshed to add recurrence cols)
-- ----------------------------------------------------------------------------
-- Acceptance probes:
--   SELECT recurrence_status_final, COUNT(*) FROM main.canonical_recurrence_resolved_v1 GROUP BY 1;
--     -- path_proven=191, imaging_only_unconfirmed=701, none=9979
--   SELECT COUNT(*) FROM main.canonical_recurrence_resolved_v1
--     WHERE recurrence_imaging_then_path_confirmed; -- 33
--   SELECT recurrence_status_final, COUNT(*) FROM main.canonical_ete_event_resolved_v1
--     WHERE cohort_ptc AND analytic_eligible GROUP BY 1;
--     -- path_proven=98 events, imaging_only_unconfirmed=120, none=3836
-- ============================================================================

-- 1. Path-proven candidate pool ---------------------------------------------

CREATE OR REPLACE TABLE manuscript_workspace.recurrence_path_proven_candidates_v1 AS
WITH multi_surg AS (
  SELECT CAST(research_id AS VARCHAR) AS rid,
         (ARRAY_AGG(DISTINCT CAST(surgery_date AS DATE) ORDER BY CAST(surgery_date AS DATE)))[2] AS rec_date
  FROM main.canonical_path_malignant_events_v1
  WHERE primary_histology IS NOT NULL
    AND primary_histology NOT IN ('NIFTP','FTUMP','follicular adenoma','atypical follicular / hurthle neoplasm','uncertain malignant potential (non-FTUMP)')
    AND surgery_date IS NOT NULL
  GROUP BY 1 HAVING COUNT(DISTINCT CAST(surgery_date AS DATE)) >= 2
),
struct_confirmed AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, CAST(MIN(recurrence_date) AS DATE) AS rec_date
  FROM main.recurrence_event_clean_v1 WHERE recurrence_definition='structural_confirmed' GROUP BY 1
),
post_op_fna_b56 AS (
  SELECT CAST(fr.research_id AS VARCHAR) AS rid,
         MIN(fr.fna_date_resolved) AS rec_date,
         MIN(LEFT(COALESCE(fr.pathology_diagnosis, fr.bethesda_2023_name, fr.bethesda_2015_name, ''), 200)) AS evidence
  FROM main.canonical_fna_events_v1 fr
  JOIN (SELECT CAST(research_id AS VARCHAR) AS rid, MIN(CAST(surgery_date AS DATE)) AS first_surg
        FROM main.canonical_path_malignant_events_v1 WHERE primary_histology IS NOT NULL GROUP BY 1) fs
    ON fs.rid = CAST(fr.research_id AS VARCHAR)
  WHERE fr.fna_date_resolved > fs.first_surg + INTERVAL '30 days'
    AND fr.bethesda_final_num IN (5,6)
  GROUP BY 1
),
llm_path AS (
  SELECT r.research_id AS rid,
         MIN(TRY_CAST(json_extract_string(e.value,'$.entity_date') AS DATE)) AS rec_date,
         MIN(LEFT(json_extract_string(e.value,'$.evidence_text'), 300)) AS evidence
  FROM main.note_entities_llm_recurrence r,
       LATERAL (SELECT unnest(CAST(json_extract(r.result_json, '$.entities') AS JSON[])) AS value) e
  WHERE r.result_json NOT LIKE '%"entities": []%' AND r.result_json NOT LIKE '%"entities":[]%'
    AND r.result_json NOT LIKE '%parse_error%'
    AND json_extract_string(e.value,'$.entity_type') IN ('structural_recurrence','distant_recurrence')
    AND json_extract_string(e.value,'$.present_or_negated') = 'present'
    AND (LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%biopsy%'
         OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%fna%'
         OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%fine needle%'
         OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%cytology%'
         OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%core bx%'
         OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%operative%'
         OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%specimen%'
         OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%pathology%'
         OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%lobectomy%'
         OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%thyroidectomy%')
  GROUP BY r.research_id
),
unioned AS (
  SELECT rid, 'multi_malignant_surgery' AS source, rec_date, NULL::VARCHAR AS evidence, 1 AS priority FROM multi_surg
  UNION ALL SELECT rid, 'structural_confirmed', rec_date, NULL, 2 FROM struct_confirmed
  UNION ALL SELECT rid, 'post_op_fna_b56', rec_date, evidence, 3 FROM post_op_fna_b56
  UNION ALL SELECT rid, 'llm_path_keyword', rec_date, evidence, 4 FROM llm_path
)
SELECT rid AS research_id, source AS path_proven_source, rec_date AS path_proven_date,
       evidence AS path_proven_evidence, priority
FROM unioned
QUALIFY ROW_NUMBER() OVER (PARTITION BY rid ORDER BY priority, rec_date NULLS LAST) = 1;

-- 2. Imaging-suspicious candidate pool --------------------------------------

CREATE OR REPLACE TABLE manuscript_workspace.recurrence_imaging_suspicious_candidates_v1 AS
WITH first_surg AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, MIN(CAST(surgery_date AS DATE)) AS first_surg_date
  FROM main.canonical_path_malignant_events_v1 WHERE primary_histology IS NOT NULL GROUP BY 1
),
llm_img AS (
  SELECT
    r.research_id AS rid, 'llm_imaging_keyword' AS img_source,
    TRY_CAST(json_extract_string(e.value,'$.entity_date') AS DATE) AS img_date,
    LEFT(json_extract_string(e.value,'$.evidence_text'), 400) AS img_finding,
    CASE
      WHEN LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%pet%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%positron%' THEN 'pet'
      WHEN LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%i-131%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%iodine%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%whole body scan%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%wbs%' THEN 'nucmed'
      WHEN LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%mri%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%magnetic resonance%' THEN 'mri'
      WHEN LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%ct scan%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '% ct %' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%computed tomograph%' THEN 'ct'
      WHEN LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%ultrasound%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%sonograph%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%hypoechoic%' THEN 'us'
      ELSE NULL END AS modality
  FROM main.note_entities_llm_recurrence r,
       LATERAL (SELECT unnest(CAST(json_extract(r.result_json, '$.entities') AS JSON[])) AS value) e
  WHERE r.result_json NOT LIKE '%"entities": []%' AND r.result_json NOT LIKE '%"entities":[]%' AND r.result_json NOT LIKE '%parse_error%'
    AND json_extract_string(e.value,'$.entity_type') IN ('structural_recurrence','distant_recurrence')
    AND json_extract_string(e.value,'$.present_or_negated') = 'present'
    AND (LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%ultrasound%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%sonograph%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%hypoechoic%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%hyperechoic%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%ct scan%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '% ct %' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%computed tomograph%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%mri%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%magnetic resonance%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%pet%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%positron%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%iodine%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%whole body scan%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%wbs%')
    AND NOT (LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%biopsy%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%fna%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%fine needle%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%cytology%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%pathology%' OR LOWER(json_extract_string(e.value,'$.evidence_text')) LIKE '%specimen%')
)
SELECT rid AS research_id, img_source, img_date, img_finding, modality, 1 AS img_priority
FROM llm_img WHERE rid IS NOT NULL;

INSERT INTO manuscript_workspace.recurrence_imaging_suspicious_candidates_v1
  (research_id, img_source, img_date, img_finding, modality, img_priority)
WITH first_surg AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, MIN(CAST(surgery_date AS DATE)) AS first_surg_date
  FROM main.canonical_path_malignant_events_v1 WHERE primary_histology IS NOT NULL GROUP BY 1
),
us_susp AS (
  SELECT CAST(usn.research_id AS VARCHAR) AS rid, 'us_ln_suspicious_flag' AS img_source,
         CAST(usn.exam_date AS DATE) AS img_date,
         LEFT(CAST(usn.suspicion_level AS VARCHAR), 200) AS img_finding, 'us' AS modality
  FROM main.canonical_us_lymph_node_v2 usn
  LEFT JOIN first_surg fs ON fs.rid = CAST(usn.research_id AS VARCHAR)
  WHERE usn.suspicious_flag = TRUE AND usn.exam_date IS NOT NULL
    AND (fs.first_surg_date IS NULL OR CAST(usn.exam_date AS DATE) > fs.first_surg_date + INTERVAL '30 days')
),
ct_susp AS (
  SELECT CAST(ct.research_id AS VARCHAR) AS rid, 'ct_ln_suspicious_flag' AS img_source,
         TRY_CAST(ct.date_of_exam AS DATE) AS img_date,
         LEFT(COALESCE(ct.lymph_node_findings, ''), 400) AS img_finding, 'ct' AS modality
  FROM main.ct_imaging ct
  LEFT JOIN first_surg fs ON fs.rid = CAST(ct.research_id AS VARCHAR)
  WHERE ct.lymph_nodes_suspicious = TRUE AND ct.date_of_exam IS NOT NULL
    AND (fs.first_surg_date IS NULL OR TRY_CAST(ct.date_of_exam AS DATE) > fs.first_surg_date + INTERVAL '30 days')
),
mri_susp AS (
  SELECT CAST(m.research_id AS VARCHAR) AS rid, 'mri_pathologic_ln' AS img_source,
         TRY_CAST(m.date_of_exam AS DATE) AS img_date,
         LEFT(COALESCE(m.lymph_node_details, m.lymph_node_locations, ''), 400) AS img_finding, 'mri' AS modality
  FROM main.mri_imaging m
  LEFT JOIN first_surg fs ON fs.rid = CAST(m.research_id AS VARCHAR)
  WHERE m.pathologic_lymph_nodes >= 1 AND m.date_of_exam IS NOT NULL
    AND (fs.first_surg_date IS NULL OR TRY_CAST(m.date_of_exam AS DATE) > fs.first_surg_date + INTERVAL '30 days')
),
nm_susp AS (
  SELECT CAST(n.research_id AS VARCHAR) AS rid, 'nucmed_impression_keyword' AS img_source,
         TRY_CAST(n.scandate AS DATE) AS img_date,
         LEFT(COALESCE(n.impression_text, n.findings_text, ''), 400) AS img_finding, 'nucmed' AS modality
  FROM main.nuclear_med n
  LEFT JOIN first_surg fs ON fs.rid = CAST(n.research_id AS VARCHAR)
  WHERE (LOWER(COALESCE(n.impression_text,'')) LIKE '%recurr%'
      OR LOWER(COALESCE(n.impression_text,'')) LIKE '%suspicious%'
      OR LOWER(COALESCE(n.impression_text,'')) LIKE '%residual%'
      OR LOWER(COALESCE(n.impression_text,'')) LIKE '%metasta%')
    AND n.scandate IS NOT NULL
    AND (fs.first_surg_date IS NULL OR TRY_CAST(n.scandate AS DATE) > fs.first_surg_date + INTERVAL '30 days')
)
SELECT rid, img_source, img_date, img_finding, modality, 2 AS img_priority FROM us_susp
UNION ALL SELECT rid, img_source, img_date, img_finding, modality, 2 FROM ct_susp
UNION ALL SELECT rid, img_source, img_date, img_finding, modality, 3 FROM mri_susp
UNION ALL SELECT rid, img_source, img_date, img_finding, modality, 4 FROM nm_susp;

-- 3. Patient-grain canonical_recurrence_resolved_v1 --------------------------

CREATE OR REPLACE TABLE main.canonical_recurrence_resolved_v1 AS
WITH first_surg AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, MIN(CAST(surgery_date AS DATE)) AS first_surg_date
  FROM main.canonical_path_malignant_events_v1 WHERE primary_histology IS NOT NULL GROUP BY 1
),
pp AS (
  SELECT research_id AS rid, MIN(path_proven_date) AS path_date,
         STRING_AGG(DISTINCT path_proven_source, ',' ORDER BY path_proven_source) AS path_sources,
         STRING_AGG(DISTINCT NULLIF(path_proven_evidence,''), ' | ') AS path_evidence
  FROM manuscript_workspace.recurrence_path_proven_candidates_v1 GROUP BY 1
),
imgp AS (
  SELECT research_id AS rid, MIN(img_date) AS img_date,
         STRING_AGG(DISTINCT modality, ',' ORDER BY modality) AS modalities,
         STRING_AGG(DISTINCT img_source, ',' ORDER BY img_source) AS img_sources,
         STRING_AGG(DISTINCT NULLIF(img_finding,''), ' | ') AS img_findings,
         COUNT(*) AS n_imaging_events
  FROM manuscript_workspace.recurrence_imaging_suspicious_candidates_v1 GROUP BY 1
),
all_pts AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_patient_master)
SELECT
  ap.rid AS research_id, fs.first_surg_date,
  (pp.rid IS NOT NULL) AS recurrence_path_proven,
  pp.path_date AS recurrence_path_proven_date,
  pp.path_sources AS recurrence_path_proven_source,
  pp.path_evidence AS recurrence_path_proven_evidence,
  CASE WHEN pp.path_date IS NOT NULL AND fs.first_surg_date IS NOT NULL
       THEN date_diff('day', fs.first_surg_date, pp.path_date) ELSE NULL END AS days_to_path_proven,
  (imgp.rid IS NOT NULL) AS recurrence_imaging_suspicious,
  imgp.img_date AS recurrence_imaging_suspicious_date,
  imgp.modalities AS recurrence_imaging_modality,
  CASE WHEN imgp.modalities LIKE '%,%' THEN 'multiple' ELSE imgp.modalities END AS recurrence_imaging_modality_summary,
  imgp.img_sources AS recurrence_imaging_source,
  imgp.img_findings AS recurrence_imaging_finding_text,
  imgp.n_imaging_events AS recurrence_imaging_n_events,
  CASE WHEN imgp.img_date IS NOT NULL AND fs.first_surg_date IS NOT NULL
       THEN date_diff('day', fs.first_surg_date, imgp.img_date) ELSE NULL END AS days_to_imaging_suspicious,
  CASE WHEN pp.path_date IS NOT NULL AND imgp.img_date IS NOT NULL
            AND imgp.img_date < pp.path_date - INTERVAL '7 days'
       THEN TRUE ELSE FALSE END AS recurrence_imaging_then_path_confirmed,
  CASE
    WHEN pp.rid IS NOT NULL                       THEN 'path_proven'
    WHEN imgp.rid IS NOT NULL                     THEN 'imaging_only_unconfirmed'
    ELSE 'none'
  END AS recurrence_status_final,
  'mig_62_canonical_recurrence_resolved_v1_20260427' AS build_script,
  CURRENT_TIMESTAMP AS build_ts
FROM all_pts ap
LEFT JOIN first_surg fs ON fs.rid = ap.rid
LEFT JOIN pp ON pp.rid = ap.rid
LEFT JOIN imgp ON imgp.rid = ap.rid;

-- 4. Layer recurrence into ETE manuscript view (v7) --------------------------
-- See repo: see also refresh of main.canonical_ete_event_resolved_v1 to add
-- the dual-track recurrence columns. The legacy 'recurrence_ever_trusted'
-- field on ete_manuscript_analytic_v4 is now SUPERSEDED by these explicit
-- BOOLs and the recurrence_status_final ENUM. Do not collapse.
