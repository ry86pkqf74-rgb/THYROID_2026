-- ============================================================================
-- Migration 05 — GEN01: derive stable molecular_episode_uid
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      GEN01 (broken molecular_episode_id — only 3 distinct values across 1,384 rows)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Pre-state on main.canonical_molecular_genetics_v2:
--   1,384 rows / 1,151 patients
--   molecular_episode_id: 3 distinct values across 1,384 rows (broken key)
--   resolved_test_date  : 903 NULL (65%)
--   platform_version    : 1,384 NULL (100% — zero discriminative value)
--   report_text_ref     : 1,384 populated (4 distinct values)
--
-- Deviation from prompt 05 spec (documented per Logan's honest-engineering rule):
--   Prompt originally proposed:
--     md5(research_id | resolved_test_date | platform | platform_version)
--   Three observed problems:
--     1. platform_version is 100% NULL — contributes zero entropy, kept for
--        schema-faithfulness would just force every hash string to end in "||".
--     2. resolved_test_date is 65% NULL — the bare hash collapses 36 distinct
--        rows into 12 UIDs (date-NULL rows on the same platform get the same UID
--        even when they are clearly separate reports).
--     3. No existing linkage-ID field (fna_id, nodule_id, surgery_id, site_id)
--        is populated on the date-NULL rows, so none of those can serve as
--        tie-breakers.
--   Chosen hash:
--     md5(research_id | COALESCE(resolved_test_date,'') | COALESCE(platform,'')
--         | COALESCE(report_text_ref,''))
--   Rationale:
--     * report_text_ref is 100% populated and has 4 distinct values
--       ('molecular_test_episode_v2#1/#2/#3/#None') — it encodes which slot in
--       the source ingestion the row came from, which is exactly the axis
--       that separates otherwise-identical date-NULL rows.
--     * Dry-run result: 1,383 UIDs / 1,384 rows / 1 residual collision.
--     * The 1 collision is research_id=10771, platform=NGS_unspecified,
--       date NULL, two byte-identical rows — a legitimate duplicate, not
--       a failure of the hash.
--
-- Auxiliary column `molecular_episode_uid_source`:
--   * 'date_platform_report'    — row had a non-NULL resolved_test_date
--   * 'platform_report_no_date' — row had no date; UID is stitched on
--                                 (rid, platform, report_text_ref) only
--   This lets downstream queries and QC filter out low-confidence UIDs
--   without re-deriving the join logic.
--
-- Output: manuscript_workspace.molecular_episode_uid_v1 (view over main.*,
--         never mutates canonical_molecular_genetics_v2 directly).
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.molecular_episode_uid_v1 AS
WITH base AS (
    SELECT
        research_id,
        resolved_test_date,
        platform,
        platform_version,
        report_text_ref,
        molecular_episode_id AS legacy_molecular_episode_id,
        -- stable hash: rid | date | platform | report_text_ref
        md5(
            CAST(research_id AS VARCHAR) || '|' ||
            COALESCE(CAST(resolved_test_date AS VARCHAR), '') || '|' ||
            COALESCE(platform, '') || '|' ||
            COALESCE(report_text_ref, '')
        ) AS molecular_episode_uid,
        CASE
            WHEN resolved_test_date IS NOT NULL THEN 'date_platform_report'
            ELSE 'platform_report_no_date'
        END AS molecular_episode_uid_source
    FROM main.canonical_molecular_genetics_v2
)
SELECT
    research_id,
    resolved_test_date,
    platform,
    platform_version,
    report_text_ref,
    legacy_molecular_episode_id,
    molecular_episode_uid,
    molecular_episode_uid_source
FROM base;

-- ---------------------------------------------------------------------------
-- QC queue emission (idempotent) — GEN01
--   Queue rows are emitted ONLY for the residual true-duplicate collision, so
--   human review confirms that collapsing it is intended (vs. the 36
--   date-NULL rows that the hash now correctly separates).
-- ---------------------------------------------------------------------------

INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
    (issue_id, research_id, source_table, source_pk, context_json, reason)
SELECT
    'GEN01' AS issue_id,
    ANY_VALUE(research_id) AS research_id,
    'canonical_molecular_genetics_v2' AS source_table,
    CAST(molecular_episode_uid AS VARCHAR) AS source_pk,
    TO_JSON(struct_pack(
        collision_type := 'byte_identical_duplicate',
        platform := ANY_VALUE(platform),
        resolved_test_date := ANY_VALUE(CAST(resolved_test_date AS VARCHAR)),
        report_text_ref := ANY_VALUE(report_text_ref),
        n_rows := COUNT(*)
    )) AS context_json,
    'byte-identical duplicate row — confirm intended collapse' AS reason
FROM manuscript_workspace.molecular_episode_uid_v1 v
GROUP BY molecular_episode_uid
HAVING COUNT(*) > 1
AND NOT EXISTS (
    SELECT 1 FROM manuscript_workspace.qc_manual_review_queue_v1 q
    WHERE q.issue_id = 'GEN01'
    AND q.source_table = 'canonical_molecular_genetics_v2'
    AND q.source_pk = CAST(v.molecular_episode_uid AS VARCHAR)
);
