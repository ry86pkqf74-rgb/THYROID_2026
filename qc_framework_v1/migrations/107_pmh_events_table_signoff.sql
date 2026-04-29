-- =============================================================================
-- Migration 107 -- canonical_pmh_events_v1 SIGN-OFF (Protocol v2)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Scope: canonical_pmh_events_v1 (Past Medical History), 19 columns.
--   15 not_started columns are verified here; 4 identifier/provenance columns
--   remain na (research_id, source_table, source_row_id, build_ts).
--
-- Current table state at verification:
--   Rows     : 12,696
--   Patients : 4,157
--   Sources  :
--      - note_entities_problem_list              11,579 rows / 4,037 patients
--      - note_entities_llm_past_medical_hx          865 rows /   295 patients
--      - mig_98*_pmh_synthetic                      246 rows /   221 patients
--      - mig_103_pmh_synthetic                        6 rows /     4 patients
--
-- Note on source count: the handoff expected three logical source families and
-- 246 synthetic mig_98 rows. Live MotherDuck also contains six post-handoff
-- mig_103 medication-to-PMH synthetic rows from the already-verified mig_103
-- medications closure. These were preserved and verified-as-injected with the
-- same synthetic-row sanity checks rather than deleted or modified.
--
-- Methodology by source family:
--
-- 1) Legacy non-LLM problem-list source
--    Source table main.note_entities_problem_list was archived/dropped by
--    Script 365 Phase 7. Verification used the archived source-of-truth copy:
--      "Thyroid 2026 UPdated".archive_pub_v1_0.
--        note_entities_problem_list_pre365_20260422_064230
--    The 365 build SQL was regenerated from scripts/365_psh_pmh_meds_consolidation.py,
--    pointed at that archived source, and compared row-wise to canonical rows
--    with source_table='note_entities_problem_list'. Results:
--      11,579 expected rows; 11,579 canonical rows; 11,579 joined rows;
--      0 missing rows; 0 extra rows; 0 column mismatches across all 15
--      adjudicated columns under IS DISTINCT FROM comparison.
--
-- 2) LLM PMH source
--    The same regenerated 365 build SQL re-derived PMH events from live
--    main.note_entities_llm_past_medical_hx parsed result_json entities.
--    Results:
--      865 expected rows; 865 canonical rows; 865 joined rows;
--      0 missing rows; 0 extra rows; 0 column mismatches across all 15
--      adjudicated columns under IS DISTINCT FROM comparison.
--
-- 3) Logan-curated synthetic attribution rows
--    The 246 mig_98 rows (98b/98c/98d/98e/98f) were not re-derived. They were
--    verified-as-injected from the Logan-curated classifier migrations. Sanity
--    checks passed for every row:
--      is_preexisting=TRUE, traceable anchor_source containing mig/logan_curated,
--      64-char sha256-like evidence_span_hash, and expected complication-domain
--      finding_value_norm values (chyle_leak, rln_injury,
--      vocal_cord_paralysis, seroma, hematoma, hypoparathyroidism).
--    The six mig_103 PMH synthetic medication rows also passed the same checks
--    (calcitriol/calcium_supplement), and are documented as a post-handoff
--    fourth synthetic source family.
--
-- Cross-source carry-forwards (not blocking):
--   CF-PMH-MULTISOURCE-DISAGREEMENT: 4 patient/finding keys where multiple PMH
--      sources record the same normalized finding with different statuses
--      (legacy present vs LLM absent). Sample: rids 4573 diabetes_mellitus,
--      4844 hypertension, 7555 obesity, 8002 hypertension. These require
--      clinical/source adjudication if those individual PMH phenotypes are used.
--   CF-PMH-COMPLICATION-MISS: 0 missing pairs. Verified preexisting/prior/not-op
--      complication rows all have corresponding present PMH rows under the
--      available complications schema proxy (onset_class + complication_type).
--
-- Verification results are recorded in memory/project_pmh_events_mig_107_closeout.md.
-- =============================================================================

-- 107a-d: flip 15 adjudicated columns after source-stratified verification.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_legacy_llm_plus_verify_as_injected_synthetic',
    batch_id            = 'mig_107_pmh_events_signoff_20260428',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_107: Protocol v2 PMH verification. '
                          || 'Legacy source note_entities_problem_list was '
                          || 're-derived from archived source '
                          || 'archive_pub_v1_0.note_entities_problem_list_pre365_20260422_064230; '
                          || 'LLM source note_entities_llm_past_medical_hx was '
                          || 're-derived from parsed result_json entities using '
                          || 'script 365 SQL. Legacy+LLM: 12,444 expected rows, '
                          || '12,444 canonical rows, 12,444 joined, 0 missing, '
                          || '0 extra, 0 mismatches across adjudicated columns. '
                          || 'Synthetic rows verified-as-injected: 246 mig_98 '
                          || 'Logan-curated rows plus 6 mig_103 medication-PMH '
                          || 'rows; all have is_preexisting TRUE, traceable '
                          || 'anchor_source, and sha256-like evidence_span_hash. '
                          || 'Carry-forwards: CF-PMH-MULTISOURCE-DISAGREEMENT '
                          || '(4 patient/finding keys); CF-PMH-COMPLICATION-MISS '
                          || '=0.'
WHERE schema_name='main'
  AND table_name='canonical_pmh_events_v1'
  AND verification_status='not_started';

-- 107e: recompute table_signoff_registry counts and sign off.
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed,0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/107_pmh_events_table_signoff.sql',
    notes             = 'mig_107: Protocol v2 PMH table sign-off. Legacy problem-list '
                        || '+ LLM result_json sources were re-derived with Script 365 '
                        || 'build logic and matched canonical bit-for-bit for 12,444 '
                        || 'base rows. Synthetic rows preserved unchanged and verified '
                        || 'as injected: 246 mig_98 Logan-curated complication-attribution '
                        || 'rows plus 6 post-handoff mig_103 medication-to-PMH rows. '
                        || 'Cross-source carry-forwards: 4 same-PMH status disagreements; '
                        || '0 preexisting complication PMH misses.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_pmh_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 107 -- canonical_pmh_events_v1 closed under Protocol v2
-- =============================================================================
