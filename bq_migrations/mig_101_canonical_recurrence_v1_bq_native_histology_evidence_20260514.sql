-- mig_101 — canonical_recurrence_v1: rebuild recurrence_histology +
-- recurrence_evidence_source BigQuery-native from Scripts 203 / 203b tier logic.
--
-- Inputs (all pub_canonical): canonical_operative_events_v1, path_synoptics,
-- canonical_fna_events_v1, thyroglobulin_lab_canonical_v1,
-- note_entities_llm_recurrence, recurrence_event_clean_v1.
--
-- Tier precedence matches 203b (priority order):
--   1 structural_confirmed (reoperation ±30d path_synoptics.histology)
--   2 fna_confirmed
--   3 structural_confirmed_legacy (recurrence_event_clean_v1)
--   4 biochemical_tg_rise
--   5 persistent_biochemical_disease
--   6 imaging_suspicious_unconfirmed (LLM)
--
-- recurrence_histology: populated ONLY for tier 1 (closest path_synoptics row
-- within ±30d of reoperation); NULL elsewhere (203b / user spec).
--
-- recurrence_evidence_source literals:
--   reoperation_pathology | fna_cytology | tg_time_series |
--   imaging_or_clinical_note | recurrence_event_clean_v1_legacy | NULL
--
-- Project: thyroid-canonical-pub-2026
-- Supersedes: mig_100 parquet archive feeder + mig_098 §1b CPM circular path.
--
-- Preconditions:
--   - pub_canonical.canonical_recurrence_v1 exists (10,871 rows) with the two columns.
--
-- Post-checks (run after):
--   SELECT recurrence_evidence_source, COUNT(*) FROM ...canonical_recurrence_v1 GROUP BY 1;
-- =============================================================================

CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.stg_cr_v1_mig101_pre_merge_snapshot`
AS
SELECT
  CAST(research_id AS STRING) AS research_id,
  recurrence_histology AS old_recurrence_histology,
  recurrence_evidence_source AS old_recurrence_evidence_source
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1`;

CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.stg_cr_v1_mig101_rebuilt_cols`
AS
WITH
-- First calendar surgery date: operative spine ∪ path_synoptics fallback (203b).
oe_dates AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    DATE(
      COALESCE(
        surgery_date_native,
        SAFE_CAST(resolved_surgery_date AS DATE)
      )
    ) AS surgery_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_events_v1`
  WHERE COALESCE(surgery_date_native, resolved_surgery_date) IS NOT NULL
),
first_surg_oe AS (
  SELECT research_id, MIN(surgery_date) AS first_surgery_date
  FROM oe_dates
  GROUP BY research_id
),
first_surg_ps_only AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    MIN(path_surg_date) AS first_surgery_date
  FROM (
    SELECT
      CAST(research_id AS STRING) AS research_id,
      COALESCE(
        SAFE_CAST(surg_date AS DATE),
        DATE(SAFE_CAST(surg_date AS TIMESTAMP))
      ) AS path_surg_date
    FROM `thyroid-canonical-pub-2026.pub_canonical.path_synoptics`
    WHERE surg_date IS NOT NULL
  ) ps
  WHERE path_surg_date IS NOT NULL
    AND ps.research_id NOT IN (SELECT research_id FROM first_surg_oe)
  GROUP BY research_id
),
first_surg_union AS (
  SELECT research_id, first_surgery_date FROM first_surg_oe
  UNION ALL
  SELECT research_id, first_surgery_date FROM first_surg_ps_only
),
first_surg AS (
  SELECT research_id, MIN(first_surgery_date) AS first_surgery_date
  FROM first_surg_union
  GROUP BY research_id
),
all_surgeries AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    DATE(
      COALESCE(
        surgery_date_native,
        SAFE_CAST(resolved_surgery_date AS DATE)
      )
    ) AS surgery_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_events_v1`
  WHERE COALESCE(surgery_date_native, resolved_surgery_date) IS NOT NULL
),
reoperations AS (
  SELECT a.research_id, a.surgery_date AS reop_date
  FROM all_surgeries a
  INNER JOIN first_surg f USING (research_id)
  WHERE a.surgery_date > f.first_surgery_date
),
path_syn_dates AS (
  SELECT
    CAST(ps.research_id AS STRING) AS research_id,
    ps.tumor_1_histologic_type,
    COALESCE(
      SAFE_CAST(ps.surg_date AS DATE),
      DATE(SAFE_CAST(ps.surg_date AS TIMESTAMP))
    ) AS path_surg_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.path_synoptics` ps
),
reop_path_join AS (
  SELECT
    r.research_id,
    r.reop_date,
    ps.tumor_1_histologic_type AS recurrence_histology,
    ABS(DATE_DIFF(ps.path_surg_date, r.reop_date, DAY)) AS day_gap
  FROM reoperations r
  INNER JOIN path_syn_dates ps USING (research_id)
  WHERE ps.tumor_1_histologic_type IS NOT NULL
    AND TRIM(CAST(ps.tumor_1_histologic_type AS STRING)) != ''
    AND ps.path_surg_date IS NOT NULL
    AND ABS(DATE_DIFF(ps.path_surg_date, r.reop_date, DAY)) <= 30
),
reop_ranked AS (
  SELECT
    research_id,
    recurrence_histology,
    ROW_NUMBER() OVER (
      PARTITION BY research_id
      ORDER BY reop_date ASC, day_gap ASC
    ) AS rn
  FROM reop_path_join
),
tier1 AS (
  SELECT
    research_id,
    CAST(1 AS INT64) AS priority,
    recurrence_histology,
    CAST('reoperation_pathology' AS STRING) AS recurrence_evidence_source
  FROM reop_ranked
  WHERE rn = 1
),
tier2_raw AS (
  SELECT DISTINCT CAST(f.research_id AS STRING) AS research_id
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_fna_events_v1` f
  INNER JOIN first_surg s USING (research_id)
  WHERE f.fna_date_resolved > s.first_surgery_date
    AND f.bethesda_final_num IN (5, 6)
),
tier2 AS (
  SELECT
    r.research_id,
    CAST(2 AS INT64) AS priority,
    CAST(NULL AS STRING) AS recurrence_histology,
    CAST('fna_cytology' AS STRING) AS recurrence_evidence_source
  FROM tier2_raw r
  WHERE NOT EXISTS (SELECT 1 FROM tier1 t WHERE t.research_id = r.research_id)
),
-- thyroglobulin_lab_canonical_v1.specimen_collect_dt on BigQuery is INTEGER epoch-ns (÷ 1e9 → TIMESTAMP_SECONDS).
tg_postop AS (
  SELECT
    CAST(t.research_id AS STRING) AS research_id,
    DATE(TIMESTAMP_SECONDS(DIV(t.specimen_collect_dt, 1000000000))) AS lab_date,
    t.result_numeric AS tg_value
  FROM `thyroid-canonical-pub-2026.pub_canonical.thyroglobulin_lab_canonical_v1` t
  INNER JOIN first_surg s USING (research_id)
  WHERE t.analyte = 'Tg'
    AND t.result_numeric IS NOT NULL
    AND t.specimen_collect_dt IS NOT NULL
    AND DATE(TIMESTAMP_SECONDS(DIV(t.specimen_collect_dt, 1000000000))) > s.first_surgery_date
),
tg_trajectory AS (
  SELECT
    research_id,
    MIN(IF(tg_value < 0.2, lab_date, NULL)) AS first_undetectable_date,
    LOGICAL_OR(tg_value < 0.2) AS ever_undetectable
  FROM tg_postop
  GROUP BY research_id
),
tier3_raw AS (
  SELECT tp.research_id, MIN(tp.lab_date) AS recurrence_date
  FROM tg_postop tp
  INNER JOIN tg_trajectory tt USING (research_id)
  WHERE tt.ever_undetectable
    AND tp.tg_value > 1.0
    AND tp.lab_date > tt.first_undetectable_date
  GROUP BY tp.research_id
),
tier3 AS (
  SELECT
    r.research_id,
    CAST(4 AS INT64) AS priority,
    CAST(NULL AS STRING) AS recurrence_histology,
    CAST('tg_time_series' AS STRING) AS recurrence_evidence_source
  FROM tier3_raw r
  WHERE NOT EXISTS (SELECT 1 FROM tier1 t WHERE t.research_id = r.research_id)
    AND NOT EXISTS (SELECT 1 FROM tier2 t WHERE t.research_id = r.research_id)
),
tier4_raw AS (
  SELECT research_id
  FROM tg_postop
  GROUP BY research_id
  HAVING MIN(tg_value) >= 0.2 AND MAX(tg_value) > 1.0
),
tier4 AS (
  SELECT
    r.research_id,
    CAST(5 AS INT64) AS priority,
    CAST(NULL AS STRING) AS recurrence_histology,
    CAST('tg_time_series' AS STRING) AS recurrence_evidence_source
  FROM tier4_raw r
  WHERE NOT EXISTS (SELECT 1 FROM tier1 t WHERE t.research_id = r.research_id)
    AND NOT EXISTS (SELECT 1 FROM tier2 t WHERE t.research_id = r.research_id)
    AND NOT EXISTS (SELECT 1 FROM tier3 t WHERE t.research_id = r.research_id)
),
llm_base AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    SAFE.PARSE_JSON(CAST(result_json AS STRING)) AS j
  FROM `thyroid-canonical-pub-2026.pub_canonical.note_entities_llm_recurrence`
  WHERE result_json IS NOT NULL
    AND NOT REGEXP_CONTAINS(CAST(result_json AS STRING), r'(?i)"entities"\s*:\s*\[\s*\]')
),
llm_exploded AS (
  SELECT
    research_id,
    JSON_VALUE(entity, '$.entity_type') AS entity_type,
    COALESCE(JSON_VALUE(entity, '$.present_or_negated'), 'present') AS present_or_negated,
    JSON_VALUE(entity, '$.entity_date') AS entity_date_raw
  FROM llm_base,
    UNNEST(JSON_QUERY_ARRAY(j, '$.entities')) AS entity
  WHERE j IS NOT NULL
),
llm_filt AS (
  SELECT
    research_id,
    SAFE_CAST(entity_date_raw AS DATE) AS entity_date,
    entity_date_raw
  FROM llm_exploded
  WHERE entity_type IN ('structural_recurrence', 'distant_recurrence')
    AND present_or_negated = 'present'
),
llm_ranked AS (
  SELECT
    research_id,
    ROW_NUMBER() OVER (
      PARTITION BY research_id
      ORDER BY entity_date NULLS LAST, entity_date_raw NULLS LAST
    ) AS rn
  FROM llm_filt
),
tier5_llm_pool AS (
  SELECT research_id FROM llm_ranked WHERE rn = 1
),
tier5_raw AS (
  SELECT l.research_id
  FROM tier5_llm_pool l
  WHERE NOT EXISTS (SELECT 1 FROM tier1 t WHERE t.research_id = l.research_id)
    AND NOT EXISTS (SELECT 1 FROM tier2 t WHERE t.research_id = l.research_id)
    AND NOT EXISTS (SELECT 1 FROM tier3 t WHERE t.research_id = l.research_id)
),
tier5 AS (
  SELECT
    r.research_id,
    CAST(6 AS INT64) AS priority,
    CAST(NULL AS STRING) AS recurrence_histology,
    CAST('imaging_or_clinical_note' AS STRING) AS recurrence_evidence_source
  FROM tier5_raw r
  WHERE NOT EXISTS (SELECT 1 FROM tier4 t WHERE t.research_id = r.research_id)
),
tier_excl_legacy AS (
  SELECT research_id FROM tier1
  UNION DISTINCT SELECT research_id FROM tier2
  UNION DISTINCT SELECT research_id FROM tier3
  UNION DISTINCT SELECT research_id FROM tier4
  UNION DISTINCT SELECT research_id FROM tier5
),
tier_legacy_raw AS (
  SELECT CAST(r.research_id AS STRING) AS research_id
  FROM `thyroid-canonical-pub-2026.pub_canonical.recurrence_event_clean_v1` r
  INNER JOIN first_surg fs ON CAST(r.research_id AS STRING) = fs.research_id
  WHERE r.recurrence_type = 'structural'
    AND r.recurrence_definition = 'structural_confirmed'
    AND COALESCE(
      SAFE_CAST(r.recurrence_date AS DATE),
      DATE(SAFE_CAST(r.recurrence_date AS TIMESTAMP))
    ) > fs.first_surgery_date
),
tier_legacy AS (
  SELECT
    x.research_id,
    CAST(3 AS INT64) AS priority,
    CAST(NULL AS STRING) AS recurrence_histology,
    CAST('recurrence_event_clean_v1_legacy' AS STRING) AS recurrence_evidence_source
  FROM tier_legacy_raw x
  WHERE NOT EXISTS (SELECT 1 FROM tier_excl_legacy e WHERE e.research_id = x.research_id)
),
unioned AS (
  SELECT research_id, priority, recurrence_histology, recurrence_evidence_source FROM tier1
  UNION ALL
  SELECT research_id, priority, recurrence_histology, recurrence_evidence_source FROM tier2
  UNION ALL
  SELECT research_id, priority, recurrence_histology, recurrence_evidence_source FROM tier_legacy
  UNION ALL
  SELECT research_id, priority, recurrence_histology, recurrence_evidence_source FROM tier3
  UNION ALL
  SELECT research_id, priority, recurrence_histology, recurrence_evidence_source FROM tier4
  UNION ALL
  SELECT research_id, priority, recurrence_histology, recurrence_evidence_source FROM tier5
),
winner AS (
  SELECT research_id, recurrence_histology, recurrence_evidence_source
  FROM unioned
  QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY priority ASC) = 1
)
SELECT
  CAST(c.research_id AS STRING) AS research_id,
  w.recurrence_histology,
  w.recurrence_evidence_source
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1` c
LEFT JOIN winner w ON CAST(c.research_id AS STRING) = w.research_id;

MERGE `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1` AS T
USING `thyroid-canonical-pub-2026.pub_workspace.stg_cr_v1_mig101_rebuilt_cols` AS S
ON CAST(T.research_id AS STRING) = S.research_id
WHEN MATCHED THEN UPDATE SET
  T.recurrence_histology = S.recurrence_histology,
  T.recurrence_evidence_source = S.recurrence_evidence_source;

-- Reconciliation vs pre-mig101 snapshot (interim legacy / archive-derived feeder).
SELECT
  COUNT(*) AS n_rows,
  COUNTIF(S.old_recurrence_histology IS NOT NULL AND TRIM(S.old_recurrence_histology) != '') AS old_hist_nonempty,
  COUNTIF(S.old_recurrence_evidence_source IS NOT NULL AND TRIM(S.old_recurrence_evidence_source) != '') AS old_evidence_nonempty,
  COUNTIF(
    T.recurrence_histology IS NOT DISTINCT FROM S.old_recurrence_histology
    AND T.recurrence_evidence_source IS NOT DISTINCT FROM S.old_recurrence_evidence_source
  ) AS rows_both_cols_match_snapshot,
  COUNTIF(
    NOT (
      T.recurrence_histology IS NOT DISTINCT FROM S.old_recurrence_histology
      AND T.recurrence_evidence_source IS NOT DISTINCT FROM S.old_recurrence_evidence_source
    )
  ) AS rows_any_col_mismatch_snapshot
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1` T
INNER JOIN `thyroid-canonical-pub-2026.pub_workspace.stg_cr_v1_mig101_pre_merge_snapshot` S
  ON CAST(T.research_id AS STRING) = S.research_id;
