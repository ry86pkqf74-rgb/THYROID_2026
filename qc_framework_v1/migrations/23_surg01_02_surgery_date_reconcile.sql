-- ============================================================================
-- Migration 23 — SURG01/SURG02: reconcile three surgery-date columns
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue IDs:     SURG01 (three cols disagree) — 171 pts
--                SURG02 (three cols identical — schema smell) — 8,559 pts
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- main.manuscript_cohort_v1 columns surgery_date / first_surgery_date / surg_first_date:
--   all three identical:        8,559 (SURG02 schema smell)
--   2-of-3 consensus, one diss.:  171 (SURG01; all are sd=sfd, fsd dissents)
--   single-col populated:       2,140 (1 two-col)
--   true all-three-disagree:        0
--
-- Priority rules for surgery_date_canonical:
--   (a) any two of the three agree → use that value
--   (b) if all three disagree OR two disagree → first_surgery_date (SoT per op episodes)
--   (c) single column populated → use that value
--
-- Output:
--   manuscript_workspace.manuscript_cohort_v1_surgery_reconciled
--     + surgery_date_canonical
--     + surgery_date_source_rank ∈ {all_three_agree, consensus_2of3,
--         all_three_disagree_first_surgery_fallback, two_agree,
--         two_disagree_first_surgery_fallback, single_only, all_null}
--
-- Queue: SURG01 = all rows with internal disagreement (n=171).
-- Deprecation: surg_first_date scheduled for DROP at prompt 46.
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.manuscript_cohort_v1_surgery_reconciled AS
WITH base AS (
  SELECT
    c.*,
    ((c.surgery_date IS NOT NULL)::INT
     + (c.first_surgery_date IS NOT NULL)::INT
     + (c.surg_first_date IS NOT NULL)::INT) AS n_populated,
    (c.surgery_date = c.first_surgery_date) AS sd_eq_fsd,
    (c.surgery_date = c.surg_first_date)    AS sd_eq_sfd,
    (c.first_surgery_date = c.surg_first_date) AS fsd_eq_sfd
  FROM main.manuscript_cohort_v1 c
)
SELECT
  b.*,
  CASE
    WHEN n_populated = 0 THEN NULL
    WHEN n_populated = 3 AND sd_eq_fsd AND fsd_eq_sfd THEN surgery_date
    WHEN n_populated = 3 AND sd_eq_fsd  THEN surgery_date
    WHEN n_populated = 3 AND sd_eq_sfd  THEN surgery_date
    WHEN n_populated = 3 AND fsd_eq_sfd THEN first_surgery_date
    WHEN n_populated = 3 THEN first_surgery_date
    WHEN n_populated = 2 AND sd_eq_fsd     THEN surgery_date
    WHEN n_populated = 2 AND sd_eq_sfd     THEN surgery_date
    WHEN n_populated = 2 AND fsd_eq_sfd    THEN first_surgery_date
    WHEN n_populated = 2 AND first_surgery_date IS NOT NULL THEN first_surgery_date
    WHEN n_populated = 2 AND surgery_date IS NOT NULL       THEN surgery_date
    WHEN n_populated = 2                                    THEN surg_first_date
    WHEN surgery_date IS NOT NULL       THEN surgery_date
    WHEN first_surgery_date IS NOT NULL THEN first_surgery_date
    ELSE surg_first_date
  END AS surgery_date_canonical,
  CASE
    WHEN n_populated = 0 THEN 'all_null'
    WHEN n_populated = 3 AND sd_eq_fsd AND fsd_eq_sfd THEN 'all_three_agree'
    WHEN n_populated = 3 AND (sd_eq_fsd OR sd_eq_sfd OR fsd_eq_sfd) THEN 'consensus_2of3'
    WHEN n_populated = 3 THEN 'all_three_disagree_first_surgery_fallback'
    WHEN n_populated = 2 AND (sd_eq_fsd OR sd_eq_sfd OR fsd_eq_sfd) THEN 'two_agree'
    WHEN n_populated = 2 THEN 'two_disagree_first_surgery_fallback'
    WHEN n_populated = 1 THEN 'single_only'
    ELSE 'all_null'
  END AS surgery_date_source_rank
FROM base b;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='SURG01';

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'SURG01',
  TRY_CAST(research_id AS INTEGER),
  'main.manuscript_cohort_v1',
  CAST(research_id AS VARCHAR),
  TO_JSON(struct_pack(
    surgery_date := surgery_date,
    first_surgery_date := first_surgery_date,
    surg_first_date := surg_first_date,
    surgery_date_canonical := surgery_date_canonical,
    surgery_date_source_rank := surgery_date_source_rank
  )),
  CONCAT('3-col surgery-date disagreement — sd=', CAST(surgery_date AS VARCHAR),
         ', fsd=', CAST(first_surgery_date AS VARCHAR),
         ', sfd=', CAST(surg_first_date AS VARCHAR),
         ' (canonical='||CAST(surgery_date_canonical AS VARCHAR)||')'),
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.manuscript_cohort_v1_surgery_reconciled
WHERE surgery_date_source_rank IN ('consensus_2of3','all_three_disagree_first_surgery_fallback','two_disagree_first_surgery_fallback');

COMMENT ON COLUMN main.manuscript_cohort_v1.surgery_date IS
'One of three surgery-date columns. 171 patients have internal disagreement (SURG01); 8,559 have all three identical (SURG02). Use manuscript_workspace.manuscript_cohort_v1_surgery_reconciled.surgery_date_canonical.';

COMMENT ON COLUMN main.manuscript_cohort_v1.first_surgery_date IS
'One of three surgery-date columns. SoT per operative episodes. Use surgery_date_canonical in manuscript_cohort_v1_surgery_reconciled for downstream analytics.';

COMMENT ON COLUMN main.manuscript_cohort_v1.surg_first_date IS
'Deprecated duplicate of first_surgery_date. Scheduled for DROP in prompt 46. Use manuscript_workspace.manuscript_cohort_v1_surgery_reconciled.surgery_date_canonical.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_22';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1 VALUES
  ('main.manuscript_cohort_v1.(surgery_date,first_surgery_date,surg_first_date)','column_group',
   'manuscript_workspace.manuscript_cohort_v1_surgery_reconciled',
   'SURG01,SURG02','prompt_22','column_only',DATE '2026-04-23',
   '8,559 all-three-identical; 171 patients with 2-of-3 consensus (surgery_date=surg_first_date, first_surgery_date dissents); 2,140 single-column-populated; 1 two-column-populated.',
   NULL,
   'Consensus-of-2 rule with first_surgery_date fallback. surgery_date_canonical + surgery_date_source_rank in reconciled view. 171 rows queued under SURG01. surg_first_date scheduled for DROP at prompt 46.');
