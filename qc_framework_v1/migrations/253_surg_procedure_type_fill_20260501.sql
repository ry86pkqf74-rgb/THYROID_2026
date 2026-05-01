-- =============================================================================
-- Migration 253 -- CPM surgical procedure-type NULL fill
-- =============================================================================
-- Date:   2026-05-01
-- Lane:   mig_253 / CF-SURG-PROC-TYPE-NULL
-- Scope:  main.canonical_patient_master only; no table rebuild.
--
-- GOVERNANCE: Do not apply until Logan signs off on the dry-run artifact from
--   qc_framework_v1/scripts/build_mig253_surg_proc_type_dryrun.py.
--   Dry-run 20260501T103539Z produced:
--     all-three NULL surgical flags: 2,138 -> 2
--     M038 >=200g NULL procedure type: 121 -> 0
--     consistency defects: 0
--
-- Source precedence:
--   1. canonical_operative_events_v1.procedure_normalized/procedure_raw
--   2. CPM NSQIP CPT code/description
--   3. canonical_operative_procedure_codes_v1 curated operative procedure NLP
--   4. path_synoptics.thyroid_procedure/procedure_other_description residual fallback
--   5. note_entities_operative_detail direct text fallback
--
-- Mapping:
--   total / completion / substernal thyroidectomy -> total_thyroidectomy, TRUE/FALSE
--   hemi / lobectomy / partial thyroid           -> hemithyroidectomy, FALSE/TRUE
--   two distinct hemi episodes                   -> total_thyroidectomy, TRUE/FALSE
--   isthmusectomy                                -> isthmusectomy, FALSE/FALSE
--   non-thyroid operative evidence               -> other, FALSE/FALSE
--
-- The UPDATE is gated to rows where all three CPM surgical procedure fields are
-- currently NULL and where the dry-run derivation proposes a non-NULL value.
-- =============================================================================

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre253_surg_proc_type_20260501 AS
SELECT * FROM main.canonical_patient_master;

BEGIN TRANSACTION;

CREATE TEMP TABLE _mig253_null_pts AS
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  first_surgery_date,
  n_surgeries,
  gland_weight_final_g,
  histology_final,
  nsqip_thyroidectomy_has_data,
  nsqip_cpt_code,
  nsqip_cpt_description
FROM main.canonical_patient_master
WHERE surg_procedure_type IS NULL
  AND surg_total_thyroidectomy IS NULL
  AND surg_hemithyroidectomy IS NULL;

SELECT CASE WHEN (SELECT COUNT(*) FROM _mig253_null_pts) <> 2138
  THEN error('mig_253 abort: all-three NULL baseline no longer equals signed dry-run count 2138')
  ELSE 0 END;

CREATE TEMP TABLE _mig253_event_source AS
SELECT
  n.research_id,
  COALESCE(BOOL_OR(
    regexp_matches(LOWER(COALESCE(op.procedure_normalized, op.procedure_raw, '')),
      'total[^a-z0-9]*thyroidectomy|total or complete|completion|removal of all remaining|substernal thyroid|thyroidectomy including substernal')
  ), FALSE) AS has_total,
  COALESCE(BOOL_OR(
    regexp_matches(LOWER(COALESCE(op.procedure_normalized, op.procedure_raw, '')),
      'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')
  ), FALSE) AS has_hemi,
  COALESCE(BOOL_OR(
    regexp_matches(LOWER(COALESCE(op.procedure_normalized, op.procedure_raw, '')), 'isthmusectomy')
  ), FALSE) AS has_isthmus,
  COUNT(*) FILTER (WHERE COALESCE(op.procedure_normalized, op.procedure_raw) IS NOT NULL) > 0 AS has_other,
  COUNT(*) FILTER (WHERE COALESCE(op.procedure_normalized, op.procedure_raw) IS NOT NULL) AS n_evidence_rows,
  STRING_AGG(DISTINCT COALESCE(op.procedure_normalized, op.procedure_raw), ' | ')
    FILTER (WHERE COALESCE(op.procedure_normalized, op.procedure_raw) IS NOT NULL) AS evidence_values
FROM _mig253_null_pts n
LEFT JOIN main.canonical_operative_events_v1 op
  ON CAST(op.research_id AS VARCHAR) = n.research_id
GROUP BY n.research_id;

CREATE TEMP TABLE _mig253_cpt_source AS
SELECT
  research_id,
  CASE
    WHEN TRY_CAST(nsqip_cpt_code AS INTEGER) IN (60240,60252,60254,60260,60270,60271)
      OR regexp_matches(LOWER(COALESCE(nsqip_cpt_description, '')),
        'total or complete|total or subtotal|removal of all remaining|substernal thyroid')
      THEN TRUE ELSE FALSE END AS has_total,
  CASE
    WHEN TRY_CAST(nsqip_cpt_code AS INTEGER) IN (60210,60212,60220,60225)
      OR regexp_matches(LOWER(COALESCE(nsqip_cpt_description, '')),
        'lobectomy|hemithyroidectomy|partial thyroid')
      THEN TRUE ELSE FALSE END AS has_hemi,
  FALSE AS has_isthmus,
  CASE WHEN nsqip_cpt_code IS NOT NULL OR nsqip_cpt_description IS NOT NULL THEN 1 ELSE 0 END AS n_evidence_rows,
  FALSE AS has_other,
  CONCAT(COALESCE(CAST(nsqip_cpt_code AS VARCHAR), ''), ' ', COALESCE(nsqip_cpt_description, '')) AS evidence_values
FROM _mig253_null_pts;

CREATE TEMP TABLE _mig253_proc_code_source AS
WITH normalized AS (
  SELECT
    n.research_id,
    pc.linked_surgery_episode_id,
    LOWER(COALESCE(pc.procedure_normalized, pc.procedure_raw, '')) AS proc_text,
    COALESCE(pc.procedure_normalized, pc.procedure_raw) AS proc_value
  FROM _mig253_null_pts n
  JOIN main.canonical_operative_procedure_codes_v1 pc
    ON CAST(pc.research_id AS VARCHAR) = n.research_id
  WHERE COALESCE(pc.procedure_normalized, pc.procedure_raw) IS NOT NULL
), patient_counts AS (
  SELECT
    research_id,
    COUNT(*) AS n_evidence_rows,
    STRING_AGG(DISTINCT proc_value, ' | ') AS evidence_values,
    COALESCE(BOOL_OR(
      regexp_matches(proc_text,
        'total[^a-z0-9]*thyroidectomy|completion[^a-z0-9]*thyroidectomy|removal of all remaining|substernal thyroid')
    ), FALSE) AS has_total_text,
    COALESCE(BOOL_OR(regexp_matches(proc_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')), FALSE) AS has_hemi_text,
    COALESCE(BOOL_OR(regexp_matches(proc_text, 'isthmusectomy')), FALSE) AS has_isthmus_text,
    COUNT(DISTINCT linked_surgery_episode_id) FILTER (
      WHERE linked_surgery_episode_id IS NOT NULL
        AND regexp_matches(proc_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')
    ) AS n_distinct_hemi_episodes,
    COUNT(*) FILTER (
      WHERE regexp_matches(proc_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')
    ) AS n_hemi_mentions
  FROM normalized
  GROUP BY research_id
)
SELECT
  n.research_id,
  COALESCE(pc.has_total_text, FALSE)
    OR COALESCE(pc.n_distinct_hemi_episodes, 0) >= 2 AS has_total,
  COALESCE(pc.has_hemi_text, FALSE) AS has_hemi,
  COALESCE(pc.has_isthmus_text, FALSE) AS has_isthmus,
  COALESCE(pc.n_evidence_rows, 0) > 0 AS has_other,
  COALESCE(pc.n_evidence_rows, 0) AS n_evidence_rows,
  pc.evidence_values,
  COALESCE(pc.n_distinct_hemi_episodes, 0) AS n_distinct_hemi_episodes,
  COALESCE(pc.n_hemi_mentions, 0) AS n_hemi_mentions
FROM _mig253_null_pts n
LEFT JOIN patient_counts pc USING (research_id);

CREATE TEMP TABLE _mig253_path_source AS
WITH path_values AS (
  SELECT
    n.research_id,
    LOWER(COALESCE(ps.thyroid_procedure, '')) AS proc_text,
    LOWER(COALESCE(ps.procedure_other_description, '')) AS other_text,
    CONCAT(COALESCE(ps.thyroid_procedure, ''),
           CASE WHEN ps.procedure_other_description IS NOT NULL THEN CONCAT(' / ', ps.procedure_other_description) ELSE '' END) AS proc_value
  FROM _mig253_null_pts n
  JOIN main.path_synoptics ps
    ON CAST(ps.research_id AS VARCHAR) = n.research_id
  WHERE ps.thyroid_procedure IS NOT NULL
     OR ps.procedure_other_description IS NOT NULL
)
SELECT
  n.research_id,
  COALESCE(BOOL_OR(regexp_matches(proc_text, 'total[^a-z0-9]*thyroidectomy')), FALSE) AS has_total,
  COALESCE(BOOL_OR(regexp_matches(proc_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy')), FALSE) AS has_hemi,
  COALESCE(BOOL_OR(regexp_matches(proc_text, 'isthmusectomy')), FALSE) AS has_isthmus,
  COUNT(p.proc_value) > 0 AS has_other,
  COUNT(p.proc_value) AS n_evidence_rows,
  STRING_AGG(DISTINCT p.proc_value, ' | ') FILTER (WHERE p.proc_value IS NOT NULL AND p.proc_value <> '') AS evidence_values
FROM _mig253_null_pts n
LEFT JOIN path_values p USING (research_id)
GROUP BY n.research_id;

CREATE TEMP TABLE _mig253_op_detail_source AS
WITH op_detail_text AS (
  SELECT
    n.research_id,
    LOWER(COALESCE(d.entity_value_norm, d.entity_value_raw, '')) AS detail_text,
    COALESCE(d.entity_value_norm, d.entity_value_raw) AS detail_value
  FROM _mig253_null_pts n
  JOIN main.note_entities_operative_detail d
    ON CAST(d.research_id AS VARCHAR) = n.research_id
  WHERE COALESCE(d.present_or_negated, 'present') = 'present'
    AND COALESCE(d.entity_value_norm, d.entity_value_raw) IS NOT NULL
)
SELECT
  n.research_id,
  COALESCE(BOOL_OR(regexp_matches(detail_text,
    'total[^a-z0-9]*thyroidectomy|completion[^a-z0-9]*thyroidectomy|removal of all remaining|substernal thyroid')), FALSE) AS has_total,
  COALESCE(BOOL_OR(regexp_matches(detail_text, 'hemi[^a-z0-9]*thyroidectomy|lobectomy|partial[^a-z0-9]*thyroid')), FALSE) AS has_hemi,
  COALESCE(BOOL_OR(regexp_matches(detail_text, 'isthmusectomy')), FALSE) AS has_isthmus,
  FALSE AS has_other,
  COUNT(detail_value) AS n_evidence_rows,
  STRING_AGG(DISTINCT detail_value, ' | ') FILTER (WHERE detail_value IS NOT NULL) AS evidence_values
FROM _mig253_null_pts n
LEFT JOIN op_detail_text d USING (research_id)
GROUP BY n.research_id;

CREATE TEMP TABLE _mig253_resolution AS
WITH chosen AS (
  SELECT
    n.research_id,
    n.first_surgery_date,
    n.n_surgeries,
    n.gland_weight_final_g,
    n.histology_final,
    n.nsqip_cpt_code,
    n.nsqip_cpt_description,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN 'canonical_operative_events_v1'
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN 'nsqip_cpt'
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN 'canonical_operative_procedure_codes_v1'
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN 'path_synoptics'
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN 'note_entities_operative_detail'
      ELSE 'unresolved'
    END AS resolution_source,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.has_total
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.has_total
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.has_total
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.has_total
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.has_total
      ELSE NULL
    END AS src_has_total,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.has_hemi
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.has_hemi
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.has_hemi
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.has_hemi
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.has_hemi
      ELSE NULL
    END AS src_has_hemi,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.has_isthmus
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.has_isthmus
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.has_isthmus
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.has_isthmus
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.has_isthmus
      ELSE NULL
    END AS src_has_isthmus,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.has_other
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.has_other
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.has_other
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.has_other
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.has_other
      ELSE NULL
    END AS src_has_other,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.n_evidence_rows
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.n_evidence_rows
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.n_evidence_rows
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.n_evidence_rows
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.n_evidence_rows
      ELSE 0
    END AS chosen_evidence_rows,
    CASE
      WHEN ev.has_total OR ev.has_hemi OR ev.has_isthmus OR ev.has_other THEN ev.evidence_values
      WHEN cpt.has_total OR cpt.has_hemi OR cpt.has_isthmus OR cpt.has_other THEN cpt.evidence_values
      WHEN pc.has_total OR pc.has_hemi OR pc.has_isthmus OR pc.has_other THEN pc.evidence_values
      WHEN ps.has_total OR ps.has_hemi OR ps.has_isthmus OR ps.has_other THEN ps.evidence_values
      WHEN od.has_total OR od.has_hemi OR od.has_isthmus OR od.has_other THEN od.evidence_values
      ELSE NULL
    END AS chosen_evidence_values,
    pc.n_distinct_hemi_episodes,
    pc.n_hemi_mentions
  FROM _mig253_null_pts n
  LEFT JOIN _mig253_event_source ev USING (research_id)
  LEFT JOIN _mig253_cpt_source cpt USING (research_id)
  LEFT JOIN _mig253_proc_code_source pc USING (research_id)
  LEFT JOIN _mig253_path_source ps USING (research_id)
  LEFT JOIN _mig253_op_detail_source od USING (research_id)
)
SELECT
  *,
  CASE
    WHEN resolution_source = 'unresolved' THEN NULL
    WHEN src_has_total THEN 'total_thyroidectomy'
    WHEN src_has_hemi THEN 'hemithyroidectomy'
    WHEN src_has_isthmus THEN 'isthmusectomy'
    WHEN src_has_other THEN 'other'
    ELSE NULL
  END AS proposed_surg_procedure_type,
  CASE
    WHEN resolution_source = 'unresolved' THEN NULL
    WHEN src_has_total THEN TRUE
    ELSE FALSE
  END AS proposed_surg_total_thyroidectomy,
  CASE
    WHEN resolution_source = 'unresolved' THEN NULL
    WHEN src_has_total THEN FALSE
    WHEN src_has_hemi THEN TRUE
    ELSE FALSE
  END AS proposed_surg_hemithyroidectomy
FROM chosen;

SELECT CASE WHEN (SELECT COUNT(*) FROM _mig253_resolution) <> 2138
  THEN error('mig_253 abort: resolution table row count is not 2138')
  ELSE 0 END;

SELECT CASE WHEN (
  SELECT COUNT(*) FROM _mig253_resolution
  WHERE proposed_surg_procedure_type IS NULL
) > 50
  THEN error('mig_253 abort: residual NULL procedure count exceeds acceptance threshold 50')
  ELSE 0 END;

SELECT CASE WHEN (
  SELECT COUNT(*) FROM _mig253_resolution
  WHERE proposed_surg_procedure_type='total_thyroidectomy'
    AND proposed_surg_total_thyroidectomy IS NOT TRUE
) <> 0
  THEN error('mig_253 abort: total_thyroidectomy type/flag inconsistency')
  ELSE 0 END;

SELECT CASE WHEN (
  SELECT COUNT(*) FROM _mig253_resolution
  WHERE proposed_surg_procedure_type='hemithyroidectomy'
    AND proposed_surg_hemithyroidectomy IS NOT TRUE
) <> 0
  THEN error('mig_253 abort: hemithyroidectomy type/flag inconsistency')
  ELSE 0 END;

SELECT CASE WHEN (
  SELECT COUNT(*) FROM _mig253_resolution
  WHERE proposed_surg_total_thyroidectomy IS TRUE
    AND proposed_surg_hemithyroidectomy IS TRUE
) <> 0
  THEN error('mig_253 abort: total and hemi flags both TRUE')
  ELSE 0 END;

SELECT CASE WHEN (
  WITH simulated AS (
    SELECT
      COALESCE(r.proposed_surg_procedure_type, c.surg_procedure_type) AS proc_type
    FROM manuscript_workspace.cohort_m038_massive_goiter_v1 c
    LEFT JOIN _mig253_resolution r
      ON CAST(c.research_id AS VARCHAR) = r.research_id
    WHERE c.gland_weight_final_g >= 200
  )
  SELECT COUNT(*) FROM simulated WHERE proc_type IS NULL
) > 5
  THEN error('mig_253 abort: M038 >=200g simulated NULL procedure type exceeds acceptance threshold 5')
  ELSE 0 END;

UPDATE main.canonical_patient_master AS pm
SET
  surg_procedure_type = r.proposed_surg_procedure_type,
  surg_total_thyroidectomy = r.proposed_surg_total_thyroidectomy,
  surg_hemithyroidectomy = r.proposed_surg_hemithyroidectomy,
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM _mig253_resolution r
WHERE CAST(pm.research_id AS VARCHAR) = r.research_id
  AND pm.surg_procedure_type IS NULL
  AND pm.surg_total_thyroidectomy IS NULL
  AND pm.surg_hemithyroidectomy IS NULL
  AND r.proposed_surg_procedure_type IS NOT NULL;

SELECT CASE WHEN (
  SELECT COUNT(*) FROM main.canonical_patient_master
  WHERE surg_procedure_type IS NULL
    AND surg_total_thyroidectomy IS NULL
    AND surg_hemithyroidectomy IS NULL
) > 50
  THEN error('mig_253 abort: post-update residual NULL procedure count exceeds acceptance threshold 50')
  ELSE 0 END;

SELECT CASE WHEN (
  SELECT COUNT(*) FROM main.canonical_patient_master
  WHERE surg_procedure_type='total_thyroidectomy'
    AND surg_total_thyroidectomy IS NOT TRUE
) <> 0
  THEN error('mig_253 abort: post-update total_thyroidectomy type/flag inconsistency')
  ELSE 0 END;

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'mig_253_multi_source_surgical_procedure_type_fill',
    batch_id = 'mig_253_surg_procedure_type_fill_20260501',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
      || ' | mig_253: filled cohort-wide NULL surgical procedure type flags for 2,136/2,138 affected CPM rows using source precedence canonical_operative_events_v1, NSQIP CPT, canonical_operative_procedure_codes_v1, path_synoptics, note_entities_operative_detail. Dry-run artifact: exports/mig253_surg_proc_type_dryrun_20260501T103539Z.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('surg_procedure_type','surg_total_thyroidectomy','surg_hemithyroidectomy','cpm_built_at');

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified = subq.n_verified,
    n_not_started = subq.n_not_started,
    n_failed = COALESCE(subq.n_failed, 0),
    n_na = subq.n_na,
    table_status = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/253_surg_procedure_type_fill_20260501.sql',
    notes = COALESCE(ts.notes,'')
      || ' | mig_253: CPM surgical procedure type NULL repair; all-three NULL 2138->2, M038 >=200g NULL 121->0 in signed dry-run.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1 (
  run_id, started_at, ended_at, phases_applied,
  critical_findings_cleared, high_findings_cleared, med_findings_cleared,
  held_for_adjudication
)
VALUES (
  'mig_253_surg_procedure_type_fill_20260501',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
  'dry_run_signed_off_update_cpm_surg_procedure_type_flags_registry_refresh',
  '0',
  '1',
  '0',
  '2'
);

-- Post-apply expected checks:
--   SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
--   SELECT COUNT(*) FROM main.canonical_patient_master
--   WHERE surg_procedure_type IS NULL
--     AND surg_total_thyroidectomy IS NULL
--     AND surg_hemithyroidectomy IS NULL; -- expected 2 at dry-run time
--   SELECT surg_procedure_type, surg_total_thyroidectomy, surg_hemithyroidectomy, COUNT(*)
--   FROM manuscript_workspace.cohort_m038_massive_goiter_v1
--   WHERE gland_weight_final_g >= 200
--   GROUP BY 1,2,3 ORDER BY 4 DESC; -- expected no NULL procedure types

COMMIT;

-- =============================================================================
-- End mig_253
-- =============================================================================