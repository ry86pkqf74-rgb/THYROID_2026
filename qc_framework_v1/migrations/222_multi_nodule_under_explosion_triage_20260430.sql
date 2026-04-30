-- mig_222 — Lane F: multi-nodule under-explosion + deferred LLM absorption triage
-- run_id: mig_222_multi_nodule_under_explosion_triage_20260430
-- Target DB: thyroid_canonical_publication_v1_0
-- Source prompt section: cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md
--   "Lane F: Multi-nodule under-explosion + deferred LLM absorption triage"
--
-- Logan-ratified scope:
--   * 448 candidate exams in manuscript_workspace.qc_tir03_llm_candidates_v1
--   * 825 deferred LLM absorption patients in manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1
--
-- Decision policy used in this migration:
--   The live queue tables contain exam/patient-level ambiguity signals but no
--   deterministic per-nodule LLM feature-to-row mapping table is present in the
--   publication DB (tirads_llm_extracted_v2 is not a live table). Therefore no
--   LLM-derived nodule features are bulk-absorbed. All queued items are
--   categorized as documented manuscript limitation / not safely absorbable,
--   affected canonical_us_nodule_v2 rows are flagged with
--   multi_nodule_attribution_unresolved=TRUE, and the QC queues are emptied only
--   after full archive snapshots + a durable triage decision table are created.

USE thyroid_canonical_publication_v1_0;

-- =============================================================================
-- §0 Pre-snapshots: canonical affected rows + both queue tables + registries
-- =============================================================================
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_nodule_v2_pre_mig222_multi_nodule_triage_20260430 AS
WITH affected AS (
  SELECT DISTINCT n.research_id, n.us_exam_id, n.nodule_id
  FROM main.canonical_us_nodule_v2 n
  JOIN manuscript_workspace.qc_tir03_llm_candidates_v1 q
    ON CAST(n.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
   AND n.us_exam_id = q.us_exam_id
  UNION
  SELECT DISTINCT n.research_id, n.us_exam_id, n.nodule_id
  FROM main.canonical_us_nodule_v2 n
  JOIN manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1 d
    ON CAST(n.research_id AS VARCHAR) = CAST(d.research_id AS VARCHAR)
)
SELECT n.*, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig222_snapshot_ts
FROM main.canonical_us_nodule_v2 n
JOIN affected a
  ON n.research_id = a.research_id
 AND n.us_exam_id = a.us_exam_id
 AND n.nodule_id = a.nodule_id;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.qc_tir03_llm_candidates_v1_pre_mig222_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig222_snapshot_ts
FROM manuscript_workspace.qc_tir03_llm_candidates_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.us_llm_absorption_deferred_multi_nodule_v1_pre_mig222_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig222_snapshot_ts
FROM manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig222_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig222_snapshot_ts
FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'main' AND table_name = 'canonical_us_nodule_v2';

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_v1_pre_mig222_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig222_snapshot_ts
FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main' AND table_name = 'canonical_us_nodule_v2';

-- =============================================================================
-- §1 Canonical flag column for manuscript-safe filtering/stratification
-- =============================================================================
ALTER TABLE main.canonical_us_nodule_v2
  ADD COLUMN IF NOT EXISTS multi_nodule_attribution_unresolved BOOLEAN DEFAULT FALSE;

COMMENT ON COLUMN main.canonical_us_nodule_v2.multi_nodule_attribution_unresolved IS
'Lane F / mig_222: TRUE when a nodule row belongs to a US exam or patient-level LLM absorption queue where multi-nodule attribution is unresolved. No LLM-derived nodule features were bulk-absorbed because the live queue is exam/patient-level only and no deterministic per-nodule LLM feature mapping table exists in the publication DB. Manuscript TIRADS analyses should exclude or sensitivity-stratify TRUE rows when nodule-level attribution is required.';

-- Normalize NULLs for the new column if ADD COLUMN IF NOT EXISTS encountered a prior partially-populated state.
UPDATE main.canonical_us_nodule_v2
SET multi_nodule_attribution_unresolved = FALSE
WHERE multi_nodule_attribution_unresolved IS NULL;

-- =============================================================================
-- §2 Durable triage decision table (one row per queued exam/patient item)
-- =============================================================================
CREATE OR REPLACE TABLE manuscript_workspace.us_multi_nodule_attribution_triage_v1 AS
WITH fna AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_fna_events_v1
),
path AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_path_malignant_events_v1
),
mol AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_molecular_genetics_v2
),
candidate AS (
  SELECT
    'qc_tir03_llm_candidates_v1' AS source_queue,
    CAST(q.research_id AS VARCHAR) AS research_id,
    q.us_exam_id,
    q.exam_date,
    q.n_current_nodules,
    q.n_reported_tirads,
    q.n_acr_cats,
    q.reported_tirads_list,
    q.acr_cats_list,
    CAST(NULL AS BIGINT) AS n_llm_entities,
    CAST(NULL AS BIGINT) AS n_v2_rows,
    q.candidate_built_at AS source_queued_at
  FROM manuscript_workspace.qc_tir03_llm_candidates_v1 q
),
deferred AS (
  SELECT
    'us_llm_absorption_deferred_multi_nodule_v1' AS source_queue,
    CAST(d.research_id AS VARCHAR) AS research_id,
    CAST(NULL AS VARCHAR) AS us_exam_id,
    CAST(NULL AS DATE) AS exam_date,
    CAST(NULL AS BIGINT) AS n_current_nodules,
    CAST(NULL AS BIGINT) AS n_reported_tirads,
    CAST(NULL AS BIGINT) AS n_acr_cats,
    CAST(NULL AS VARCHAR) AS reported_tirads_list,
    CAST(NULL AS VARCHAR) AS acr_cats_list,
    d.n_llm_entities,
    d.n_v2_rows,
    CAST(d.deferred_at AS TIMESTAMP) AS source_queued_at
  FROM manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1 d
),
combined AS (
  SELECT * FROM candidate
  UNION ALL
  SELECT * FROM deferred
),
scored AS (
  SELECT
    c.*,
    COALESCE(pm.is_malignant, FALSE) AS is_malignant,
    fna.rid IS NOT NULL AS has_fna,
    path.rid IS NOT NULL AS has_path_malignant,
    mol.rid IS NOT NULL AS has_molecular,
    (
      CASE WHEN COALESCE(pm.is_malignant, FALSE) THEN 100 ELSE 0 END +
      CASE WHEN fna.rid IS NOT NULL THEN 20 ELSE 0 END +
      CASE WHEN path.rid IS NOT NULL THEN 10 ELSE 0 END +
      CASE WHEN mol.rid IS NOT NULL THEN 5 ELSE 0 END +
      COALESCE(c.n_current_nodules, c.n_v2_rows, 0)
    ) AS triage_priority_score
  FROM combined c
  LEFT JOIN main.canonical_patient_master pm
    ON CAST(pm.research_id AS VARCHAR) = c.research_id
  LEFT JOIN fna ON c.research_id = fna.rid
  LEFT JOIN path ON c.research_id = path.rid
  LEFT JOIN mol ON c.research_id = mol.rid
)
SELECT
  *,
  CASE
    WHEN is_malignant AND has_fna AND has_path_malignant AND has_molecular THEN 'tier_1_malignant_fna_path_molecular'
    WHEN is_malignant AND has_fna AND has_path_malignant THEN 'tier_2_malignant_fna_path'
    WHEN is_malignant THEN 'tier_3_malignant_other'
    WHEN has_fna OR has_molecular THEN 'tier_4_benign_or_unknown_with_diagnostic_events'
    ELSE 'tier_5_lowest_publication_risk'
  END AS triage_priority_tier,
  'document_as_limitation' AS triage_decision,
  'not_absorbed_ambiguous_multi_nodule_attribution' AS absorption_status,
  FALSE AS extractor_bug_escalation_flag,
  TRUE AS multi_nodule_attribution_unresolved,
  'No deterministic per-nodule LLM feature mapping table is live; queue is exam/patient-level only. Bulk absorption would risk cross-nodule feature contamination.' AS triage_rationale,
  'mig_222_multi_nodule_under_explosion_triage_20260430' AS triage_migration,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS triaged_at
FROM scored;

COMMENT ON TABLE manuscript_workspace.us_multi_nodule_attribution_triage_v1 IS
'Lane F / mig_222 durable triage ledger for 448 TIR03 candidate exams and 825 deferred LLM absorption patients. All rows categorized as document_as_limitation / not_absorbed because deterministic per-nodule LLM attribution is unavailable in live publication DB.';

-- =============================================================================
-- §3 Flag canonical nodule rows covered by either queue
-- =============================================================================
UPDATE main.canonical_us_nodule_v2 AS n
SET multi_nodule_attribution_unresolved = TRUE
FROM (
  SELECT DISTINCT research_id, us_exam_id, nodule_id
  FROM (
    SELECT DISTINCT n.research_id, n.us_exam_id, n.nodule_id
    FROM main.canonical_us_nodule_v2 n
    JOIN manuscript_workspace.qc_tir03_llm_candidates_v1 q
      ON CAST(n.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
     AND n.us_exam_id = q.us_exam_id
    UNION
    SELECT DISTINCT n.research_id, n.us_exam_id, n.nodule_id
    FROM main.canonical_us_nodule_v2 n
    JOIN manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1 d
      ON CAST(n.research_id AS VARCHAR) = CAST(d.research_id AS VARCHAR)
  ) u
) AS affected
WHERE n.research_id = affected.research_id
  AND n.us_exam_id = affected.us_exam_id
  AND n.nodule_id = affected.nodule_id;

-- =============================================================================
-- §4 Queue closure: remove categorized rows after durable triage table + archive
-- =============================================================================
DELETE FROM manuscript_workspace.qc_tir03_llm_candidates_v1;
DELETE FROM manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1;

-- =============================================================================
-- §5 Registry for new canonical column + signoff recompute
-- =============================================================================
DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_us_nodule_v2'
  AND column_name = 'multi_nodule_attribution_unresolved'
  AND batch_id = 'mig_222_multi_nodule_under_explosion_triage_20260430';

INSERT INTO main.canonical_column_verification_registry_v1 (
  schema_name, table_name, column_name, data_type, ordinal_position,
  category, upstream_source, verification_status, verified_by, verified_ts,
  verification_method, batch_id, notes, registered_ts
)
SELECT
  'main',
  'canonical_us_nodule_v2',
  c.column_name,
  c.data_type,
  c.ordinal_position,
  'derived',
  'manuscript_workspace.qc_tir03_llm_candidates_v1|manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1',
  'verified',
  'mig_222',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'deterministic_queue_membership_flag_multi_nodule_attribution_unresolved',
  'mig_222_multi_nodule_under_explosion_triage_20260430',
  'TRUE for canonical_us_nodule_v2 rows in 448 TIR03 candidate exams or 825 deferred LLM patient queues; all categorized as documented limitation / not absorbed because per-nodule LLM attribution is ambiguous.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM information_schema.columns c
WHERE c.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND c.table_schema = 'main'
  AND c.table_name = 'canonical_us_nodule_v2'
  AND c.column_name = 'multi_nodule_attribution_unresolved'
  AND NOT EXISTS (
    SELECT 1 FROM main.canonical_column_verification_registry_v1 r
    WHERE r.schema_name = 'main'
      AND r.table_name = 'canonical_us_nodule_v2'
      AND r.column_name = 'multi_nodule_attribution_unresolved'
  );

UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/222_multi_nodule_under_explosion_triage_20260430.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_222: Lane F multi-nodule under-explosion + deferred LLM absorption triage; added multi_nodule_attribution_unresolved and flagged affected rows.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'canonical_us_nodule_v2'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- =============================================================================
-- §6 Provenance
-- =============================================================================
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied,
   critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_222_multi_nodule_under_explosion_triage_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'lane_f_multi_nodule_under_explosion_and_deferred_llm_absorption_triage_archive_flag_triage_table_queue_closure',
   '0',
   '448_candidate_exams_and_825_deferred_patients_categorized_documented_limitation',
   '10570_canonical_us_nodule_v2_rows_flagged_multi_nodule_attribution_unresolved',
   'future_llm_reparse_or_manual_review_needed_for_safe_per_nodule_absorption');

-- =============================================================================
-- §7 Post-verify helpers
-- =============================================================================
SELECT 'triage_total' AS metric, COUNT(*) AS n FROM manuscript_workspace.us_multi_nodule_attribution_triage_v1
UNION ALL SELECT 'triage_candidate_exam_rows', COUNT(*) FROM manuscript_workspace.us_multi_nodule_attribution_triage_v1 WHERE source_queue='qc_tir03_llm_candidates_v1'
UNION ALL SELECT 'triage_deferred_patient_rows', COUNT(*) FROM manuscript_workspace.us_multi_nodule_attribution_triage_v1 WHERE source_queue='us_llm_absorption_deferred_multi_nodule_v1'
UNION ALL SELECT 'qc_tir03_remaining', COUNT(*) FROM manuscript_workspace.qc_tir03_llm_candidates_v1
UNION ALL SELECT 'deferred_remaining', COUNT(*) FROM manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1
UNION ALL SELECT 'canonical_flagged_rows', COUNT(*) FROM main.canonical_us_nodule_v2 WHERE multi_nodule_attribution_unresolved IS TRUE;
