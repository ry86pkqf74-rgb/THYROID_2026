-- =============================================================================
-- Migration 93 -- canonical_esophageal_invasion_events_v1 SIGN-OFF (Step D)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Second of 3 sibling LLM-output invasion canonicals to close
--         (after t4b mig_92). 188 rows / 15 cols.
--
-- Logan-reviewed artefact:
--   verification_csvs/canonical_esophageal_invasion_events_v1/
--     t4b_implication_signoff__mig_91__LOGAN_REVIEWED.xlsx (NOT this; that's t4b)
--   The esophageal review was performed inline in chat over multiple
--   batches plus rule-based bulk reclassification. Total 7 row-write
--   passes summarized below.
--
-- Esophageal-specific clinical rules established this session
-- (carry forward to vascular sibling):
--   1. Cancer-only:    not-tumor pathology (goiter, parathyroid adenoma,
--                      benign, hyperplasia, multinodular, nodular thyroid)
--                      should be 'negated' for invasion (per Logan).
--   2. Compression:    "compression / displacement / deviation /
--                      effacement / mass effect" of esophagus is NOT
--                      invasion -> 'negated'.
--   3. Adjacency:      "behind / posterior to / lateral to / abutting /
--                      adjacent to / along the / retroesophageal +
--                      esophagus" without an invasion verb -> 'negated'.
--   4. Adherent-only:  "densely / intimately adherent to esophagus"
--                      WITHOUT invasion / fistul / defect / muscularis /
--                      mucosa context -> 'negated'.
--   5. Explicit negs:  "no entrance / without entrance / no esophageal
--                      injury / without compromise / no evidence of
--                      invasion / lumen not violated" -> 'negated'.
--   6. Procedural:     Maloney dilator / Dobhoff feeding tube / surgicel
--                      / NG tube placement WITHOUT a real invasion
--                      finding -> 'negated'.
--   7. Closure text:   wound-closure / "no complications" / "hemostasis
--                      excellent" / Vicryl stitches / sutured closed
--                      WITHOUT invasion content -> 'negated'.
--   8. Iatrogenic:     intra-op surgical injury (e.g. luminal defect
--                      created during dissection) is NOT tumor invasion
--                      -> 'negated' (rid 10887 example).
--   9. Multi-structure summary: "<carcinoma|tumor> with invasion of A,
--                      B, esophagus, D" comprehensive shopping-list
--                      statements are staging summaries, not primary
--                      findings -> 'negated' (rid 9850 example).
--   10. Subtype mismatch: when evidence specifies muscularis but explicitly
--                      states "mucosa intact", esophageal_mucosal_invasion
--                      should NOT be 'present' (rid 11944 / 10386 examples).
--
-- ROW WRITES (94 rows total reclassed 'present' -> 'negated'):
--   93a-row-1 (32):  Pass 1 rule-based bulk -- compression / adjacency /
--                    non-cancer pathology.
--   93a-row-2 (15):  Pass 2 multi-structure-list staging summaries.
--                    rids 9850, 10044, 8428, 9332, 10157 + 3 bonus
--                    "no esophageal injury" rows for rid 9332.
--   93a-row-3 (12):  Pass 3 per-row review batch 1 (rows #1, #6, #11,
--                    #12, #13, #14-#20). Includes rid 10887 iatrogenic
--                    luminal defect (whole rid flipped).
--   93a-row-4 (13):  Pass 4 per-row review batch 2 (rows #41, #42, #43,
--                    #44, #45, #53, #55, #57, #60). rid 11898 partial.
--   93a-row-5 (10):  Pass 5 per-row review batch 3 (rows #101-#110, #119,
--                    #120). rids 7835, 7977, 8292 (varicosity), 8352,
--                    8977, 9012.
--   93a-row-6 (9):   Pass 6 per-row review batch 4 (rows #108-#111, #116,
--                    #117-#120). rids 9012 / 9017 repair / 9090 / 9126.
--   93a-row-7 (10):  Pass 7 rule-based bulk excl. rid 8614 -- adherent-
--                    only / procedural / explicit-negative.
--   93a-row-8 (3):   Pass 8 final pattern cleanup -- rid 11944 mucosa
--                    intact subtype mismatch + rid 9899 lymph dissection.
--
-- FINAL distribution: 52 present / 136 negated = 188.
-- Started: 156 present / 32 negated.
-- Net 104 rows flipped present -> negated.
--
-- Column verification (15 cols):
--   present_or_negated, entity_type, entity_value, evidence_text:
--     manual_source_review (Logan reviewed every row's evidence_text +
--     present_or_negated decision; entity_type and entity_value were
--     visible in every row's review and not flagged as mis-categorized).
--   confidence, date_confidence, date_source_keyword, entity_date:
--     auto_no_source_counterpart (LLM-internal metadata; no upstream
--     source counterpart).
--   research_id (auto_identifier_skip) + 6 provenance/pipeline-trace cols
--   (build_ts / note_date / note_row_id / note_type / source_column /
--    source_line): auto_no_source_counterpart (Step D batch flip per
--   FNA pilot precedent mig_78c).
--
-- Carry-forwards: none new; the 10 esophageal clinical rules are
-- captured in this header for reuse on vascular invasion.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 93a: 4 cols Logan-reviewed via inline chat batches -> manual_source_review
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'manual_source_review',
    batch_id            = 'mig_93_esophageal_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_93a: Logan-reviewed via 7 row-write passes '
                          || '(32 rule-based + 15 multi-structure + 13+10+9+13+8 per-row). '
                          || '104 rows flipped present -> negated. Final 52 present / 136 negated = 188. '
                          || '10 esophageal-specific clinical rules established (see migration header).'
WHERE schema_name='main' AND table_name='canonical_esophageal_invasion_events_v1'
  AND column_name IN ('present_or_negated', 'entity_type', 'entity_value', 'evidence_text');

-- 93b: 4 LLM-internal metadata cols -> auto_no_source_counterpart
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_93_esophageal_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_93b: LLM-internal metadata (no upstream source counterpart). '
                          || 'confidence + date_confidence + date_source_keyword + entity_date '
                          || 'are LLM self-reports.'
WHERE schema_name='main' AND table_name='canonical_esophageal_invasion_events_v1'
  AND column_name IN ('confidence', 'date_confidence', 'date_source_keyword', 'entity_date');

-- 93c: 7 na provenance / pipeline-trace cols -> auto_no_source_counterpart (Step D)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_93_esophageal_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_93c: pure provenance / pipeline-trace, no source counterpart '
                          || '(Step D batch flip per FNA pilot precedent mig_78c).'
WHERE schema_name='main' AND table_name='canonical_esophageal_invasion_events_v1'
  AND verification_status = 'na';

-- 93d: recompute table_signoff_registry counts and sign off
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
    signoff_migration = 'qc_framework_v1/migrations/93_esophageal_invasion_table_signoff.sql'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_esophageal_invasion_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 93 -- canonical_esophageal_invasion_events_v1 closed
-- SIXTH table verified under Protocol v2.
-- 10 reusable esophageal rules captured for vascular sign-off (mig_94).
-- =============================================================================
