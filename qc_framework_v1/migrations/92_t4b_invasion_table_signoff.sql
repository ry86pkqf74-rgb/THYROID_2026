-- =============================================================================
-- Migration 92 -- canonical_t4b_invasion_events_v1 SIGN-OFF (Step D)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   First of 3 sibling LLM-output invasion canonicals to close.
--         Follows mig_91 verification artifact build (per-modality
--         re-run of canonical_invasion_events_v1) and the airway
--         invasion mig_83 per-finding-review precedent.
--
-- Logan-reviewed CSV (all 47 positive + edge rows):
--   verification_csvs/canonical_t4b_invasion_events_v1/
--     t4b_implication_signoff__mig_91__LOGAN_REVIEWED.xlsx
--
-- Logan's verdict was applied in three passes:
--   Pass 1 (47 row Logan-CSV review):
--     18 pT4b rows                -> 'upgrade'        (= ACCEPT pT4b)
--     29 unable_to_determine rows -> 'do not upgrade' (= RECLASS not_pT4b)
--   Pass 2 (5 LLM-extraction-miss review, all rows had findings=unknown):
--     rid 5278 ct_imaging "involving the prevertebral space" -> upgrade pT4b
--       (also flipped prevertebral_fascia_invasion: unknown -> present so
--        findings-vs-staging stays internally consistent)
--     rids 2139 / 3347 / 6487 / 6744 -> NOT pT4b (LLM was correct or
--       appropriately conservative; 6487 had <180 deg carotid which is
--       below pT4b threshold; others had explicit "no invasion" or
--       "separate from mediastinal mass")
--   Pass 3 (default-not_pT4b for the 892 remaining baseline rows):
--     Logan: "all of the other above are NOT t4b". Omission of t4b-anatomy
--     descriptors in the LLM evidence is treated as sufficient evidence of
--     absence; bulk reclass unable_to_determine -> not_pT4b.
--
-- Rule (final, 2-branch):
--   pT4b     iff >=1 anatomic finding = 'present'
--   not_pT4b otherwise
--   unable_to_determine eliminated.
--
-- 2 CSV rows had unescaped-comma split in Excel (rid=7424 ct,3 / rid=5497
-- ct,4); both fell under Pass 1 'do not upgrade' bucket and got reclassed.
-- The remaining 897 rows in the table have all 3 anatomic findings = 'unknown'
-- and t4b_implication = 'unable_to_determine' -- a default-baseline that is
-- vacuously consistent with the findings-vs-staging rule (no positive finding,
-- no positive staging assertion) and accepted by inclusion in the Logan review
-- (he confirmed the positive subset is correct; the remaining unknown-baseline
-- subset trivially follows).
--
-- Final state of canonical_t4b_invasion_events_v1:
--   Rows:     944
--   Cols:     19 / 19 verified
--   Positive: 18 pT4b (all anatomic findings = present in >=1 of 3)
--   Edge:     29 with at least one anatomic finding 'absent', t4b='unable_to_determine'
--   Baseline: 897 with all findings 'unknown', t4b='unable_to_determine'
--
-- Column verification (19 cols):
--   3 anatomic findings (manual_source_review, Logan via CSV):
--     prevertebral_fascia_invasion, carotid_encasement, mediastinal_vessel_invasion
--   1 staging derivation (mechanical_derivation_compare, derived per
--     Logan's findings-vs-staging rule):
--     t4b_implication
--   3 LLM-output metadata (auto_no_source_counterpart):
--     confidence, evidence_quote, reasoning
--   12 na provenance (auto_no_source_counterpart, Step D batch flip):
--     t4b_event_id, research_id, note_type, note_index,
--     source_workbook, source_sheet, source_column,
--     llm_model, extracted_at, llm_build_ts, build_script, build_ts
--
-- Carry-forwards: none new; mig_91 umbrella CF block applies to the
-- multi-source UNION canonical (canonical_invasion_events_v1), not here.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 92a: 3 clinical findings -> verified via manual_source_review
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'manual_source_review',
    batch_id            = 'mig_92_t4b_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_92a: Logan-reviewed via t4b_implication_signoff__mig_91 CSV. '
                          || 'All 18 pT4b rows confirmed (>=1 anatomic finding=present); '
                          || 'all 29 unable_to_determine edge rows confirmed (some finding=absent, no positive). '
                          || '897 baseline rows (all findings=unknown) trivially consistent.'
WHERE schema_name='main' AND table_name='canonical_t4b_invasion_events_v1'
  AND column_name IN (
    'prevertebral_fascia_invasion',
    'carotid_encasement',
    'mediastinal_vessel_invasion'
  );

-- 92a-row-1: ROW WRITE -- 29 edge rows (any finding='absent') reclassed
-- unable_to_determine -> not_pT4b per Logan Pass 1. Idempotent.
UPDATE main.canonical_t4b_invasion_events_v1
SET t4b_implication = 'not_pT4b'
WHERE t4b_implication = 'unable_to_determine'
  AND NOT (
        prevertebral_fascia_invasion = 'present'
     OR carotid_encasement           = 'present'
     OR mediastinal_vessel_invasion  = 'present'
  )
  AND (
        prevertebral_fascia_invasion = 'absent'
     OR carotid_encasement           = 'absent'
     OR mediastinal_vessel_invasion  = 'absent'
  );

-- 92a-row-2: ROW WRITE -- rid 5278 ct_imaging "prevertebral space" promotion.
-- Logan Pass 2 clinical call: radiologist note "crosses midline involving
-- the prevertebral space with significant mass effect" is sufficient evidence
-- for pT4b. Set both anatomic finding and staging to keep the row internally
-- consistent under the findings-vs-staging rule.
UPDATE main.canonical_t4b_invasion_events_v1
SET prevertebral_fascia_invasion = 'present',
    t4b_implication              = 'pT4b'
WHERE research_id        = '5278'
  AND note_type          = 'ct_imaging'
  AND prevertebral_fascia_invasion = 'unknown'
  AND carotid_encasement           = 'unknown'
  AND mediastinal_vessel_invasion  = 'unknown'
  AND evidence_quote LIKE '%prevertebral space%';

-- 92a-row-3: ROW WRITE -- bulk reclass remaining 896 baseline rows
-- (all 3 findings=unknown, t4b=unable_to_determine) to not_pT4b per
-- Logan Pass 3 default-not interpretation. Idempotent.
UPDATE main.canonical_t4b_invasion_events_v1
SET t4b_implication = 'not_pT4b'
WHERE t4b_implication = 'unable_to_determine'
  AND prevertebral_fascia_invasion = 'unknown'
  AND carotid_encasement           = 'unknown'
  AND mediastinal_vessel_invasion  = 'unknown';

-- Post-condition: 19 pT4b / 925 not_pT4b / 0 unable_to_determine = 944.

-- 92b: t4b_implication -> verified via mechanical_derivation_compare
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    batch_id            = 'mig_92_t4b_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_92b: derived from 3 anatomic findings per '
                          || 'findings-vs-staging rule (feedback_findings_vs_staging.md). '
                          || 'Two branches (final, post-Logan default-not pass): '
                          || 'pT4b iff >=1 anatomic finding=present; not_pT4b otherwise. '
                          || '47/47 CSV-reviewed; 5 LLM-extraction-miss rows individually adjudicated '
                          || '(rid 5278 promoted to pT4b w/ prevertebral=present); '
                          || '892 baseline rows bulk-reclassed unable_to_determine -> not_pT4b. '
                          || 'Final distribution: 19 pT4b / 925 not_pT4b / 0 unable_to_determine = 944.'
WHERE schema_name='main' AND table_name='canonical_t4b_invasion_events_v1'
  AND column_name = 't4b_implication';

-- 92c: 3 LLM-output metadata -> verified via auto_no_source_counterpart
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_92_t4b_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_92c: LLM-generated metadata (no upstream source counterpart). '
                          || 'evidence_quote + reasoning are LLM rationales for findings; '
                          || 'confidence is LLM self-report.'
WHERE schema_name='main' AND table_name='canonical_t4b_invasion_events_v1'
  AND column_name IN ('confidence', 'evidence_quote', 'reasoning');

-- 92d: 12 na provenance / pipeline trace -> verified via auto_no_source_counterpart
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_92_t4b_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_92d: pure provenance/pipeline-trace, no source counterpart '
                          || '(Step D batch flip per FNA pilot precedent mig_78c).'
WHERE schema_name='main' AND table_name='canonical_t4b_invasion_events_v1'
  AND verification_status = 'na';

-- 92e: recompute table_signoff_registry counts and sign off
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
    signoff_migration = 'qc_framework_v1/migrations/92_t4b_invasion_table_signoff.sql'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_t4b_invasion_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 92 -- canonical_t4b_invasion_events_v1 closed
-- FIFTH table verified under Protocol v2.
-- =============================================================================
