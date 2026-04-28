-- =============================================================================
-- Migration 83 -- canonical_airway_invasion_events_v1 SIGN-OFF (Step D)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Final sign-off of canonical_airway_invasion_events_v1 under
--         Protocol v2 Step D. Second table verified after the FNA pilot.
--
-- Logan directives (verbatim across mig_80-82):
--   - "this should only be from operative reports/synoptic/gross
--      pathology/micro pathology. Not from any imaging." (mig_80)
--   - 9-row sample review: each marked "do not upstage" (Rule A confirmed)
--   - "The airway invasion and staging guidelines should be separate, we
--      are determining if airway invasion is present in one section, then
--      staging guidelines are separate." (mig_81)
--   - 11077: pathologist staging assertion does not override findings
--   - 8614: RLN sacrifice = full-thickness invasion = pT4a (mig_81)
--   - "unless the pathologist called these pT4a in the synoptic then that
--      is just a description of what pT4a is rather than evidence" (mig_82)
--   - "1. agree" (clean 18 template-echo rows)
--   - "2. confirm the pathologists call rather than just text" (keep
--      t4a_implication=pT4a; reset finding columns to unknown on the 18)
--   - "accept all" (138 remaining pT4a candidates accepted as-is)
--
-- Net session arc (mig_80 -> mig_83):
--   mig_80 : scope filter (drop ct_imaging/HP/DC_SUM + 2 phantoms; -2,899)
--   mig_81 : t4a logic round 1 (Rule A: 56 -> not_pT4a; 11077; 8614)
--   mig_82 : CAP template-echo cleanup (18 synoptic rows)
--   mig_83 : table sign-off (this file)
--
-- Final state of canonical_airway_invasion_events_v1:
--   Rows:     3,155 (started 6,054 -> -2,899 mig_80 scope filter)
--   Patients: 2,622
--   Note types: OPNOTE 1,936 + synoptic_pathology 1,219
--   Positive (any of 7 finding columns positive): 196
--     -> 138 pT4a / 58 not_pT4a / 0 unable_to_determine
--   Negative / all-unknown: 2,959
--
-- Column verification (23 columns):
--
--   7 clinical findings (manual_source_review via Logan):
--     tracheal_invasion, tracheal_invasion_depth, laryngeal_invasion,
--     cricoid_invasion, rln_invasion, rln_paralysis_preop, esophageal_invasion
--
--   1 staging derivation (mechanical_derivation_compare, derived per
--     Logan's findings-vs-staging rule):
--     t4a_implication
--
--   3 LLM-output provenance (auto_no_source_counterpart):
--     confidence, evidence_quote, reasoning
--     [registry had these mis-categorized as 'adjudicated'/'source'; they
--      are in fact LLM-generated metadata fields with no upstream source
--      counterpart. Reclassified to no_source_counterpart at sign-off.]
--
--   13 na provenance / pipeline trace (auto_no_source_counterpart, Step D
--   batch flip per FNA pilot precedent mig_78c):
--     airway_event_id, research_id, note_type, note_index,
--     source_workbook, source_sheet, source_column,
--     llm_model, extracted_at, llm_build_ts, build_script, build_ts
--     (13 rows in registry)
--
-- Carry-forward / open items at sign-off:
--   CF-1: 6017 synoptic — pT4a anchored on non-airway "extrathyroidal
--         extension into fat" rather than airway findings. Logan kept
--         pT4a (mig_81). Future call: should airway invasion table
--         exclude rows whose only pT4a evidence is non-airway? Defer to
--         downstream cohort builds.
--   CF-2: t4a_implication is currently a stored LLM column, but per the
--         findings-vs-staging rule it should arguably be a deterministic
--         post-derivation. Defer; current values are Logan-ratified.
--   CF-3: 17 'pathologist_call_only' rows have all anatomic findings =
--         unknown after mig_82 but t4a=pT4a. Downstream views may want
--         an evidence_grade flag (anatomic vs stage-only). Defer.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 83a: 7 clinical findings -> verified via manual_source_review
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'manual_source_review',
    batch_id            = 'mig_83_airway_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_83a: Logan-reviewed across mig_80-82. Scope-filtered to '
                          || 'OPNOTE+synoptic only; 196 positive rows surfaced for review; '
                          || '56 not-full-thickness flipped to not_pT4a (Rule A); 18 CAP '
                          || 'template-echo rows cleaned to unknown; 138 pT4a accepted.'
WHERE schema_name='main' AND table_name='canonical_airway_invasion_events_v1'
  AND column_name IN (
    'tracheal_invasion', 'tracheal_invasion_depth',
    'laryngeal_invasion', 'cricoid_invasion',
    'rln_invasion', 'rln_paralysis_preop', 'esophageal_invasion'
  );

-- 83b: t4a_implication -> verified via mechanical_derivation_compare
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'mechanical_derivation_compare',
    batch_id            = 'mig_83_airway_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_83b: derived from finding columns per Logan'
                          || CHR(39) || 's findings-vs-staging rule. '
                          || 'pT4a iff at least one of (laryngeal/cricoid/rln/full-thickness '
                          || 'tracheal/esophageal)=present, OR pathologist explicitly staged at '
                          || 'pT4a in CAP synoptic; otherwise not_pT4a. unable_to_determine '
                          || 'eliminated from positive subset.'
WHERE schema_name='main' AND table_name='canonical_airway_invasion_events_v1'
  AND column_name = 't4a_implication';

-- 83c: 3 LLM-output metadata -> verified via auto_no_source_counterpart
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_83_airway_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_83c: LLM-generated metadata (no upstream source counterpart). '
                          || 'Reasoning column has been augmented across mig_80-82 with audit '
                          || 'trail of Logan rule applications.'
WHERE schema_name='main' AND table_name='canonical_airway_invasion_events_v1'
  AND column_name IN ('confidence', 'evidence_quote', 'reasoning');

-- 83d: 13 na provenance / pipeline trace -> verified via auto_no_source_counterpart
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_no_source_counterpart',
    batch_id            = 'mig_83_airway_invasion_signoff',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_83d: pure provenance/pipeline-trace, no source counterpart '
                          || '(Step D batch flip per FNA pilot precedent mig_78c).'
WHERE schema_name='main' AND table_name='canonical_airway_invasion_events_v1'
  AND verification_status = 'na';

-- 83e: recompute table_signoff_registry counts and sign off
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started = 0 AND COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts   = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/83_airway_invasion_table_signoff.sql'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_airway_invasion_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 83 -- canonical_airway_invasion_events_v1 closed
-- Second table verified under Protocol v2.
-- =============================================================================
