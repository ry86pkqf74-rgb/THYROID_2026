-- ============================================================================
-- Migration 39 — GEN02: canonical_molecular_genetics_v2 platform_version
--                         regex extraction from platform_raw
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue ID:      GEN02 — 1,384 of 1,384 rows have platform_version=NULL
--                (registry listed ~422 extractable: ThyroSeq 267 / Afirma 119 /
--                 NGS_unspecified 36)
-- Author:        Logan Glosser
-- Date:          2026-04-23
-- ----------------------------------------------------------------------------
-- Probe (2026-04-23):
--   ThyroSeq 885 rows → 238 with "v2"/"v3"/"version 3" tokens (227 v3 + 11 v2,
--   rounded — registry's 267 is stale; the version is genuinely absent from
--   most of the raw text, which is typically just report narrative).
--   Afirma 417 rows → 138 with classifier family (118 GSC + 20 GEC). GSC and
--   GEC are classifier products, not integer "versions" — we surface a
--   derived integer (3=GSC, 2=GEC) AND a subversion string.
--   NGS_unspecified 82 rows → 0 extractable (raw is literally "NGS" or
--   "NGS_or_unknown").
--
-- The canonical column `main.canonical_molecular_genetics_v2.platform_version`
-- is INTEGER but populated NULL for all 1,384 rows. Per pub-DB policy we do
-- NOT mutate the canonical table — we surface a derived view.
--
-- Subversion mapping:
--   ThyroSeq: 'v3' / 'v2' / 'unversioned'
--   Afirma:   'GSC' / 'GEC' / 'unversioned'
--   NGS_unspecified: 'ngs_unspecified'
-- Derived integer (only where semantically meaningful):
--   ThyroSeq 'v3' → 3, 'v2' → 2
--   Afirma 'GSC' → 3, 'GEC' → 2  (semantic: GSC is the newer generation)
--
-- Output:
--   manuscript_workspace.canonical_molecular_genetics_v2_platform_clean (VIEW)
--     + platform_subversion            VARCHAR
--     + platform_version_derived       INTEGER (NULL where unresolved)
--     + gen02_platform_version_flag    BOOLEAN (derived version present)
--     + gen02_platform_unresolved_flag BOOLEAN (no subversion + no version)
-- ============================================================================

CREATE OR REPLACE VIEW manuscript_workspace.canonical_molecular_genetics_v2_platform_clean AS
SELECT
  g.*,
  CASE
    -- ThyroSeq versioning
    WHEN g.platform='ThyroSeq'
         AND (g.platform_raw ILIKE '%ThyroSeq v3%' OR g.platform_raw ILIKE '%ThyroSeq version 3%')
         THEN 'v3'
    WHEN g.platform='ThyroSeq' AND g.platform_raw ILIKE '%ThyroSeq v2%' THEN 'v2'
    WHEN g.platform='ThyroSeq' THEN 'unversioned'
    -- Afirma classifier generation
    WHEN g.platform='Afirma'
         AND (g.platform_raw ILIKE '%Afirma GSC%' OR g.platform_raw ILIKE '%GSC %'
              OR g.platform_raw ILIKE '%Xpression Atlas%' OR g.platform_raw ILIKE '%XA %')
         THEN 'GSC'
    WHEN g.platform='Afirma' AND g.platform_raw ILIKE '%Afirma GEC%' THEN 'GEC'
    WHEN g.platform='Afirma' THEN 'unversioned'
    -- NGS
    WHEN g.platform='NGS_unspecified' THEN 'ngs_unspecified'
    ELSE NULL
  END AS platform_subversion,
  CASE
    WHEN g.platform='ThyroSeq'
         AND (g.platform_raw ILIKE '%ThyroSeq v3%' OR g.platform_raw ILIKE '%ThyroSeq version 3%')
         THEN 3
    WHEN g.platform='ThyroSeq' AND g.platform_raw ILIKE '%ThyroSeq v2%' THEN 2
    WHEN g.platform='Afirma'
         AND (g.platform_raw ILIKE '%Afirma GSC%' OR g.platform_raw ILIKE '%GSC %'
              OR g.platform_raw ILIKE '%Xpression Atlas%' OR g.platform_raw ILIKE '%XA %')
         THEN 3
    WHEN g.platform='Afirma' AND g.platform_raw ILIKE '%Afirma GEC%' THEN 2
    ELSE NULL
  END AS platform_version_derived,
  (CASE
    WHEN g.platform='ThyroSeq'
         AND (g.platform_raw ILIKE '%ThyroSeq v3%' OR g.platform_raw ILIKE '%ThyroSeq version 3%') THEN TRUE
    WHEN g.platform='ThyroSeq' AND g.platform_raw ILIKE '%ThyroSeq v2%' THEN TRUE
    WHEN g.platform='Afirma'
         AND (g.platform_raw ILIKE '%Afirma GSC%' OR g.platform_raw ILIKE '%GSC %'
              OR g.platform_raw ILIKE '%Xpression Atlas%' OR g.platform_raw ILIKE '%XA %') THEN TRUE
    WHEN g.platform='Afirma' AND g.platform_raw ILIKE '%Afirma GEC%' THEN TRUE
    ELSE FALSE
  END) AS gen02_platform_version_flag,
  (CASE
    WHEN g.platform='ThyroSeq'
         AND NOT (g.platform_raw ILIKE '%ThyroSeq v3%' OR g.platform_raw ILIKE '%ThyroSeq version 3%'
                  OR g.platform_raw ILIKE '%ThyroSeq v2%') THEN TRUE
    WHEN g.platform='Afirma'
         AND NOT (g.platform_raw ILIKE '%Afirma GSC%' OR g.platform_raw ILIKE '%GSC %'
                  OR g.platform_raw ILIKE '%Xpression Atlas%' OR g.platform_raw ILIKE '%XA %'
                  OR g.platform_raw ILIKE '%Afirma GEC%') THEN TRUE
    WHEN g.platform='NGS_unspecified' THEN TRUE
    ELSE FALSE
  END) AS gen02_platform_unresolved_flag
FROM main.canonical_molecular_genetics_v2 g;

DELETE FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='GEN02';

-- GEN02: rows where the regex chain couldn't identify a version/generation
INSERT INTO manuscript_workspace.qc_manual_review_queue_v1
  (issue_id, research_id, source_table, source_pk, context_json, reason, status, created_at)
SELECT
  'GEN02',
  TRY_CAST(research_id AS INTEGER),
  'main.canonical_molecular_genetics_v2',
  CAST(molecular_episode_id AS VARCHAR),
  TO_JSON(struct_pack(
    platform := platform,
    platform_subversion := platform_subversion,
    platform_raw_head := SUBSTRING(platform_raw, 1, 200)
  )),
  'GEN02 platform version unresolved after regex chain — raw text lacks explicit v2/v3 or GSC/GEC marker',
  'open',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM manuscript_workspace.canonical_molecular_genetics_v2_platform_clean
WHERE gen02_platform_unresolved_flag;

COMMENT ON TABLE main.canonical_molecular_genetics_v2 IS
'Molecular genetics master (1,384 rows). platform_version INTEGER is populated NULL for all rows in the raw canonical — use manuscript_workspace.canonical_molecular_genetics_v2_platform_clean.platform_version_derived (INTEGER) and .platform_subversion (VARCHAR) instead. Regex chain maps ThyroSeq v2/v3 and Afirma GSC/GEC → INT 2/3. Unversioned / NGS_unspecified rows queued under GEN02. 2026-04-23.';

DELETE FROM manuscript_workspace.canonical_deprecation_log_v1 WHERE closing_prompt='prompt_38';

INSERT INTO manuscript_workspace.canonical_deprecation_log_v1
  (deprecated_object, object_kind, superseding_object, issue_id, closing_prompt, deprecation_kind, deprecated_date, reason, hard_drop_gate, notes)
VALUES
  ('main.canonical_molecular_genetics_v2.platform_version','column',
   'manuscript_workspace.canonical_molecular_genetics_v2_platform_clean.platform_version_derived',
   'GEN02','prompt_38','column_only',DATE '2026-04-23',
   'Raw canonical column platform_version is NULL for all 1,384 rows. Regex chain over platform_raw extracts ThyroSeq v2/v3 and Afirma GSC/GEC → derived INT (2/3) on clean view. Unresolved rows (ThyroSeq ~647 unversioned + Afirma ~279 unversioned + NGS 82) queued.',
   NULL,
   'Both platform_subversion (VARCHAR) and platform_version_derived (INTEGER) exposed. Registry numbers were snapshot-stale; actual extractable pool is smaller than registry predicted because much of platform_raw is raw report narrative rather than versioned headers. Unresolved rows represent LLM-pass candidates alongside GEN03.');
