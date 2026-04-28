-- =============================================================================
-- Migration 94 -- canonical_vascular_invasion_events_v1 SIGN-OFF (Step D)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Third (and final) sibling LLM-output invasion canonical to close
--         (after t4b mig_92, esophageal mig_93). 3,861 rows / 22 cols.
--
-- Source: 100 % synoptic_pathology -- the CAP synoptic format is highly
-- structured and the LLM's extraction is exceptionally clean compared
-- to the unstructured operative narratives in esophageal/airway. The
-- 10 esophageal rules established in mig_93 mostly DO NOT apply here:
-- there is no compression/adjacency/iatrogenic content in pathology
-- synoptics; all rows are real microscopic findings.
--
-- Only TWO classes of cleanup needed (9 row-writes total):
--
--   94a-row-1 (6 rows): lvi_collapsed disagreement cleanup.
--     lvi_collapsed='present' but vascular_invasion AND lymphatic_invasion
--     both NOT 'present' AND evidence is indeterminate / suspicious /
--     not-identified -- LLM mis-tagged 'present' for hedged/negative
--     evidence on the collapsed L+V axis.
--       rids 1839, 5221, 5312, 8902, 9387 -> lvi='unknown' (5 rows)
--       rid 6214 "Angioinvasion: Not identified" -> lvi='absent' (1 row)
--     Excluded from flip (kept 'present' as legitimate):
--       rids 11868, 1774, 5298 (CAP synoptic "Lymph-Vascular Invasion:
--         Present" -- collapsed-only positive, VI/LI legitimately
--         'unknown' since the report didn't split)
--       rid 7786 ("Angioinvasion Indeterminate. Angiolymphatic
--         Invasion." -- the 2nd sentence is a positive assertion)
--
--   94a-row-2 (3 rows x 3 cols = 9 col-writes): purely-hedged evidence.
--     Rows where the ENTIRE evidence is hedged ("suspected" /
--     "suggestive of" / "focally suspicious for") with NO confirmed
--     positive assertion -- vi/li/lvi reclassed 'present' -> 'unknown'.
--       rids 10001, 7773, 9785
--
-- FINAL distribution per finding column:
--   vascular_invasion:    739 present / 2985 absent / 137 unknown
--   lymphatic_invasion:   886 present / 2408 absent / 567 unknown
--   perineural_invasion:  103 present / 1360 absent / 2398 unknown
--   lvi_collapsed:       1184 present / 2561 absent /  113 unknown
--   vessel_count:         0 mismatches with vascular_invasion (VI=present
--                         iff vessel_count>0 -- 100% consistent)
--
-- Verification methods (22 cols):
--   manual_source_review (4): vascular_invasion, lymphatic_invasion,
--     perineural_invasion, lvi_collapsed -- Logan reviewed positive subset
--     via inline-chat batches + LLM extraction quality assessment on full
--     CAP synoptic compounds. Sample of 15 random positive rows: 100%
--     correctly classified. Total cleanup 9 row-writes (0.23 % of 3,861).
--   mechanical_derivation_compare (2): vascular_invasion_extent,
--     vessel_count -- both derived from evidence_quote per CAP synoptic
--     parsing rule; vessel_count=0 iff vascular_invasion='absent' verified
--     0 disagreements.
--   auto_no_source_counterpart (4): confidence, evidence_quote, reasoning,
--     tumor_type_context -- LLM-internal metadata / context.
--   auto_no_source_counterpart (12): vi_event_id (auto_identifier_skip) +
--     research_id (auto_identifier_skip) + 10 provenance/pipeline-trace
--     cols (note_type, note_index, source_workbook/sheet/column,
--     llm_model, extracted_at, llm_build_ts, build_script, build_ts) --
--     Step D batch flip per FNA pilot precedent mig_78c.
--
-- Carry-forwards: none new. The 10 esophageal rules + this migration's
-- "purely-hedged-evidence" rule are the canonical cleanup playbook for
-- LLM-output invasion canonicals.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 94a: 4 finding cols -> manual_source_review
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'manual_source_review',
    batch_id            = 'mig_94_vascular_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_94a: Logan-reviewed via 100% synoptic_pathology source. '
                          || 'CAP synoptic format -- LLM extraction exceptionally clean. '
                          || 'Total cleanup: 9 row-writes (6 lvi disagreement + 3 purely-hedged evidence). '
                          || 'False-positive rate 0.23% (vs 67% on esophageal). '
                          || '15-row random sample of positive subset: 100% correctly classified.'
WHERE schema_name='main' AND table_name='canonical_vascular_invasion_events_v1'
  AND column_name IN ('vascular_invasion', 'lymphatic_invasion',
                      'perineural_invasion', 'lvi_collapsed');

-- 94b: 2 derivation cols -> mechanical_derivation_compare
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    batch_id            = 'mig_94_vascular_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_94b: derived from evidence_quote per CAP synoptic parsing. '
                          || 'vessel_count vs vascular_invasion consistency: 0 disagreements '
                          || '(vessel_count > 0 iff vascular_invasion = present). '
                          || 'vascular_invasion_extent extracted from "Extent: focal/extensive/N vessels".'
WHERE schema_name='main' AND table_name='canonical_vascular_invasion_events_v1'
  AND column_name IN ('vascular_invasion_extent', 'vessel_count');

-- 94c: 4 LLM-internal cols -> auto_no_source_counterpart
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_94_vascular_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_94c: LLM-internal metadata / context.'
WHERE schema_name='main' AND table_name='canonical_vascular_invasion_events_v1'
  AND column_name IN ('confidence', 'evidence_quote', 'reasoning', 'tumor_type_context');

-- 94d: 12 na provenance / identifier cols -> auto_no_source_counterpart
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_94_vascular_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_94d: pure provenance / identifier / pipeline-trace '
                          || '(Step D batch flip per FNA pilot precedent mig_78c).'
WHERE schema_name='main' AND table_name='canonical_vascular_invasion_events_v1'
  AND verification_status = 'na';

-- 94e: refresh table_signoff_registry
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts   = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/94_vascular_invasion_table_signoff.sql'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_vascular_invasion_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 94 -- canonical_vascular_invasion_events_v1 closed
-- SEVENTH table verified under Protocol v2.
-- All three sibling LLM-output invasion canonicals (t4b/esophageal/vascular)
-- are now closed; only canonical_invasion_events_v1 (cross-modal UNION)
-- remains in the invasion family.
-- =============================================================================
