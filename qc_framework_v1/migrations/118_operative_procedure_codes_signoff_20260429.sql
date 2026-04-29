-- =============================================================================
-- Migration 118 -- canonical_operative_procedure_codes_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (UTC, Cowork session)
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Verify and sign off canonical_operative_procedure_codes_v1 under
--         Protocol v2. Closes the operative family alongside the already-
--         verified canonical_operative_events_v1 (mig_362 via Script 362)
--         and canonical_operative_patient_rollup_v1.
--
-- Build origin: Script 362 (operative consolidation) — STEP 3 builds this
--   table from main.note_entities_procedures (filtered to
--   present_or_negated='present') with temporal-linkage rule against
--   canonical_operative_events_v1 (±30d on surgery_date_native).
--
-- Methodology — hybrid:
--   * SOURCE CLUSTER (4 cols: procedure_raw, procedure_normalized,
--     confidence, evidence_span): extraction-faithfulness vs
--     note_entities_procedures filtered to present-only.
--   * LINKAGE CLUSTER (3 cols: linkage_method, n_candidate_episodes,
--     linkage_ambiguous_multi_episode): internal-consistency verification
--     vs canonical's own linked_surgery_episode_id + note_date and
--     canonical_operative_events_v1.surgery_date_native (rather than
--     re-deriving from upstream, because upstream note_date has drifted in
--     format since canonical build — see CF-118 below).
--
-- Verification probe results (run 2026-04-29 via Cowork query):
--
--   Row counts:
--     - canonical                                   : 21,691 / 4,712 patients
--     - upstream note_entities_procedures present   : 21,691 (exact match)
--
--   Source cluster (extraction-faithfulness):
--     - 21,691 / 21,691 rows match upstream multiset on
--       (research_id, note_row_id, procedure_raw, procedure_normalized,
--       confidence, evidence_span). EXCEPT ALL both directions = 0.
--     - Distinct value counts: 154 raw / 9 normalized / 154 evidence_span /
--       1 confidence — all identical between canonical and upstream.
--     - Confidence is uniformly 0.9 — confirmed extraction-faithful from
--       upstream LLM extraction (NOT a Script 362 default).
--
--   Linkage cluster (internal consistency):
--     - 0 rows where linkage_method='unlinked' but linked_surgery_episode_id
--       IS NOT NULL.
--     - 0 rows where linkage_method!='unlinked' but linked_surgery_episode_id
--       IS NULL.
--     - 0 rows where linkage_method='same_day' but day_diff_actual!=0
--       (recomputed from canonical's note_date vs operative event surg_date).
--     - 0 rows where linkage_method='temporal_30d*' but day_diff NOT in [1,30].
--     - 0 rows where linkage_ambiguous_multi_episode != (n_cand>1 AND day_diff>0).
--     - 0 rows where ambig flag mismatches the temporal_30d_ambiguous label.
--
--   n_candidate_episodes consistency:
--     - 0 unlinked rows with non-zero n_candidate_episodes.
--     - 0 temporal_30d_ambiguous rows with n_candidate_episodes < 2.
--     - 0 linked rows (same_day/temporal_30d) with n_candidate_episodes < 1.
--
--   Identity:
--     - procedure_mention_id (sha256 hash) is unique across all 21,691 rows
--       (0 dups). Stable mention identity confirmed.
--
--   Vocabulary:
--     - procedure_normalized has 9 distinct values:
--       hemithyroidectomy 9,323 / total_thyroidectomy 8,556 /
--       central_neck_dissection 1,000 / completion_thyroidectomy 949 /
--       tracheostomy 610 / laryngoscopy 575 /
--       modified_radical_neck_dissection 439 / lateral_neck_dissection 183 /
--       parathyroid_autotransplant 56. Vocab clean.
--     - linkage_method has 4 distinct: unlinked 11,134 / same_day 9,575 /
--       temporal_30d_ambiguous 904 / temporal_30d 78. All 4 enums covered.
--
-- Sign-off scope:
--   7 not_started cols flipped to verified:
--     procedure_raw, procedure_normalized, confidence, evidence_span via
--       'extraction_faithfulness_vs_note_entities_procedures_present'
--     linkage_method, n_candidate_episodes, linkage_ambiguous_multi_episode via
--       'internal_consistency_vs_linked_episode_id_and_operative_events'
--   9 already-na cols carry over:
--     procedure_mention_id (na_provenance/identifier),
--     research_id (identifier), note_row_id (provenance),
--     note_date (provenance — DATE), note_type (provenance),
--     extraction_run_id (identifier), linked_surgery_episode_id (identifier),
--     build_script, build_ts (provenance).
--   table_status: not_started -> verified.
--
-- Carry-forward:
--   CF-118-UPSTREAM-DATE-FORMAT-DRIFT: main.note_entities_procedures.note_date
--     is currently stored as VARCHAR MM/DD/YYYY (e.g., "12/10/2008") which
--     does NOT parse via TRY_CAST(VARCHAR AS DATE) in current MotherDuck (only
--     ISO-8601 parses natively). 12,952 of 21,691 upstream rows fail TRY_CAST.
--     Yet canonical_operative_procedure_codes_v1.note_date (DATE-typed) has
--     valid populated values for those rows — meaning the canonical was
--     built from a previous upstream state where note_date was in a different
--     format (likely ISO or pandas-parsed). Build is faithful, re-derivation
--     from current upstream is blocked.
--     Recommend in next op-procedure rebuild: switch from TRY_CAST to
--     TRY_STRPTIME(note_date,'%m/%d/%Y'), or normalize upstream note_date
--     format upstream of Script 362.
--     Logan-ratified disposition: verify-with-note + open this CF; do not
--     block table sign-off; future repair migration to address.
--
-- Final state of canonical_operative_procedure_codes_v1 (post-mig_118):
--   Rows     : 21,691
--   Patients : 4,712
--   Cols     : 16
--   Verified : 7 + 9 na = 16 / 16 closed
--
-- This is the 55th canonical table closed under Protocol v2 (after the
-- 54-table state captured in mig_117 audit reconciliation; mig_117_us_v2 +
-- mig_117_audit also brought tally up).
-- =============================================================================

-- 118a: flip 4 source-cluster cols via extraction-faithfulness
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_procedures_present',
    batch_id            = 'mig_118_operative_procedure_codes_signoff_20260429',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_118: extraction-faithfulness probe vs '
                          || 'main.note_entities_procedures filtered to '
                          || 'present_or_negated=''present''. EXCEPT ALL '
                          || 'multiset on (research_id, note_row_id, '
                          || 'procedure_raw, procedure_normalized, confidence, '
                          || 'evidence_span) returned 0 drift in both '
                          || 'directions across 21,691 rows. Source values '
                          || 'are bit-for-bit faithful to upstream LLM '
                          || 'extractor output (Script 362 STEP 3 build).'
WHERE schema_name='main'
  AND table_name='canonical_operative_procedure_codes_v1'
  AND verification_status='not_started'
  AND column_name IN ('procedure_raw','procedure_normalized','confidence','evidence_span');

-- 118b: flip 3 linkage-cluster cols via internal-consistency verification
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'internal_consistency_vs_linked_episode_id_and_operative_events',
    batch_id            = 'mig_118_operative_procedure_codes_signoff_20260429',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_118: internal-consistency probe joining '
                          || 'canonical row to canonical_operative_events_v1 '
                          || 'via linked_surgery_episode_id and recomputing '
                          || 'day_diff from canonical-stored note_date (DATE). '
                          || '7 consistency checks (linkage_method/'
                          || 'linked_id alignment, same_day day_diff=0, '
                          || 'temporal_30d range [1,30], ambig flag/label '
                          || 'consistency, n_candidate_episodes plausibility) '
                          || 'all returned 0 errors across 21,691 rows. '
                          || 'Re-derivation from upstream is blocked (see '
                          || 'CF-118-UPSTREAM-DATE-FORMAT-DRIFT) — internal '
                          || 'consistency is the appropriate verification.'
WHERE schema_name='main'
  AND table_name='canonical_operative_procedure_codes_v1'
  AND verification_status='not_started'
  AND column_name IN ('linkage_method','n_candidate_episodes','linkage_ambiguous_multi_episode');

-- 118c: append CF-118 note to upstream-source col evidence_span (pivots on
-- the date-drift finding; documented at the table level via verification_note
-- on the source-cluster cols in 118a above, but keeping a pointer here for
-- discovery).
-- (No additional col-row update needed — CF text already in 118a notes.)

-- 118d: flip table_status to verified
UPDATE main.canonical_table_signoff_registry_v1
SET table_status      = 'verified',
    n_verified        = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1
                          WHERE schema_name='main'
                            AND table_name='canonical_operative_procedure_codes_v1'
                            AND verification_status='verified'),
    n_not_started     = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1
                          WHERE schema_name='main'
                            AND table_name='canonical_operative_procedure_codes_v1'
                            AND verification_status='not_started'),
    n_failed          = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1
                          WHERE schema_name='main'
                            AND table_name='canonical_operative_procedure_codes_v1'
                            AND verification_status='failed'),
    signoff_migration = 'qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql',
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes             = COALESCE(notes,'')
                        || ' | mig_118: closed via hybrid extraction-'
                        || 'faithfulness (4 source cols) + internal-'
                        || 'consistency (3 linkage cols). 21,691 rows / '
                        || '4,712 patients. CF-118-UPSTREAM-DATE-FORMAT-DRIFT '
                        || 'open for upstream note_date VARCHAR MM/DD/YYYY '
                        || 'parsing repair.'
WHERE schema_name='main'
  AND table_name='canonical_operative_procedure_codes_v1';

-- =============================================================================
-- end of migration 118 -- canonical_operative_procedure_codes_v1 verified
-- 55th table closed under Protocol v2.
-- =============================================================================
