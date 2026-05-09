-- Cross-source agreement audit for canonical_operative_patient_rollup v1 → v1_1 promotion
-- Run date: 2026-05-09
-- DFL: DFL-2026-05-09-operative-rollup-v1-1-cross-source-audit
-- Related: NF-2026-05-09-operative-rollup-surgery-type-undercount, THY-56
--
-- RESULT: Stop condition fired. Agreement = 92.3% (8,162/8,840) < 98% threshold.
-- 28 patients in 3-way disagreement queue (pub_workspace.qc_v1_1_three_way_disagreement_v1)
-- Canonical promotion BLOCKED. Awaiting Logan sign-off on disagreement review.
--
-- Disagreement breakdown summary (2026-05-09):
--   disagree_pg_op_events: 326 (254 multi-surgery = expected artifact, 72 single-surgery need review)
--   disagree_opc_pg: 315 (190 opc=total/pg=hemi; 59 opc=completion/pg=hemi; 51 opc=completion/pg=total; 15 staged-completion)
--   disagree_all_3: 28 (16 staged_completion_consistent, 6 completion_total_vs_hemi_pg, 6 completion_total_pg_vs_hemi_op)
--   disagree_opc_op_events: 9 (all single-surgery)

-- ============================================================
-- Table 1: Cross-source agreement audit
-- Output: pub_workspace.canonical_operative_patient_rollup_v1_1_audit
-- ============================================================
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.canonical_operative_patient_rollup_v1_1_audit` AS
WITH
opc AS (
  SELECT research_id,
    CASE
      WHEN COUNTIF(LOWER(procedure_normalized)='total_thyroidectomy') > 0 THEN 'total'
      WHEN COUNTIF(LOWER(procedure_normalized)='completion_thyroidectomy') > 0 THEN 'completion'
      WHEN COUNTIF(LOWER(procedure_normalized)='hemithyroidectomy') > 0 THEN 'hemi'
      ELSE NULL
    END AS opc_type
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_procedure_codes_v1`
  GROUP BY research_id
),
pg AS (
  SELECT research_id,
    CASE
      WHEN left_lobe_max_dim_cm IS NOT NULL AND right_lobe_max_dim_cm IS NOT NULL THEN 'total'
      WHEN left_lobe_max_dim_cm IS NOT NULL OR right_lobe_max_dim_cm IS NOT NULL THEN 'hemi'
      ELSE NULL
    END AS pg_type
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_gland_patient_rollup_v1`
),
op_events AS (
  SELECT research_id,
    CASE
      WHEN COUNTIF(LOWER(procedure_normalized)='total_thyroidectomy') > 0 THEN 'total'
      WHEN COUNTIF(LOWER(procedure_normalized)='completion_thyroidectomy') > 0 THEN 'completion'
      WHEN COUNTIF(LOWER(procedure_normalized)='hemithyroidectomy') > 0 THEN 'hemi'
      ELSE NULL
    END AS op_events_type
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_events_v1`
  WHERE procedure_normalized IS NOT NULL
  GROUP BY research_id
),
combined AS (
  SELECT
    COALESCE(opc.research_id, pg.research_id, op_events.research_id) AS research_id,
    opc.opc_type,
    pg.pg_type,
    op_events.op_events_type,
    (CASE WHEN opc.opc_type IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN pg.pg_type IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN op_events.op_events_type IS NOT NULL THEN 1 ELSE 0 END) AS n_sources_available
  FROM opc
  FULL OUTER JOIN pg USING(research_id)
  FULL OUTER JOIN op_events USING(research_id)
)
SELECT
  research_id,
  opc_type, pg_type, op_events_type,
  n_sources_available,
  CASE
    WHEN n_sources_available < 2 THEN 'single_source_only'
    WHEN n_sources_available = 2 THEN
      CASE
        WHEN opc_type IS NOT NULL AND pg_type IS NOT NULL THEN
          IF(opc_type = pg_type, 'agree_opc_pg', 'disagree_opc_pg')
        WHEN opc_type IS NOT NULL AND op_events_type IS NOT NULL THEN
          IF(opc_type = op_events_type, 'agree_opc_op_events', 'disagree_opc_op_events')
        WHEN pg_type IS NOT NULL AND op_events_type IS NOT NULL THEN
          IF(pg_type = op_events_type, 'agree_pg_op_events', 'disagree_pg_op_events')
        ELSE 'unknown_2src'
      END
    WHEN n_sources_available = 3 THEN
      CASE
        WHEN opc_type = pg_type AND pg_type = op_events_type THEN 'agree_all_3'
        WHEN opc_type = pg_type THEN 'agree_opc_pg_only'
        WHEN opc_type = op_events_type THEN 'agree_opc_op_only'
        WHEN pg_type = op_events_type THEN 'agree_pg_op_only'
        ELSE 'disagree_all_3'
      END
    ELSE 'no_sources'
  END AS agreement_status,
  (pg_type = 'total' AND opc_type = 'hemi') AS is_staged_completion_pattern
FROM combined
WHERE n_sources_available >= 2;

-- ============================================================
-- Table 2: 3-way disagreement manual-review queue
-- Output: pub_workspace.qc_v1_1_three_way_disagreement_v1
-- All 28 patients with 3-way disagreements must be reviewed before promotion
-- ============================================================
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.qc_v1_1_three_way_disagreement_v1` AS
SELECT
  a.research_id,
  a.opc_type,
  a.pg_type,
  a.op_events_type,
  a.is_staged_completion_pattern,
  op.n_surgeries,
  CASE
    WHEN a.opc_type = 'hemi' AND a.pg_type = 'total' AND a.op_events_type = 'completion'
      THEN 'staged_completion_consistent'
    WHEN a.opc_type = 'total' AND a.pg_type = 'hemi' AND a.op_events_type = 'completion'
      THEN 'completion_total_vs_hemi_path_gland'
    WHEN a.opc_type = 'completion' AND a.pg_type = 'total' AND a.op_events_type = 'hemi'
      THEN 'completion_total_path_gland_vs_hemi_op'
    ELSE 'other_3way'
  END AS pattern_label,
  CURRENT_TIMESTAMP() AS flagged_at,
  'qc_v1_1_three_way_disagreement_v1 (promotion stop condition 2026-05-09)' AS review_context
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_operative_patient_rollup_v1_1_audit` a
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_patient_rollup_v1` op
  ON a.research_id = op.research_id
WHERE a.agreement_status = 'disagree_all_3';

-- ============================================================
-- Summary query (run to verify)
-- ============================================================
-- SELECT agreement_status, COUNT(*) AS n
-- FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_operative_patient_rollup_v1_1_audit`
-- GROUP BY agreement_status ORDER BY n DESC;
--
-- Expected: agree_{all_3, opc_pg, pg_op_events, ...} total >= 92.3% of 8840 multi-source patients
-- 3-way disagreements: 28 patients in qc_v1_1_three_way_disagreement_v1
--   - 16 staged_completion_consistent (multi-surgery artifact, likely acceptable)
--   - 12 require Logan review before canonical promotion
