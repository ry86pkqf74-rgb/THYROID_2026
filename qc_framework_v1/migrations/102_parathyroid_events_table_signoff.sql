-- =============================================================================
-- Migration 102 -- canonical_parathyroid_events_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   20th canonical table closed under Protocol v2 (post path_gland family
--         + frozen_section). Verifies LLM-extracted parathyroid detail canonical
--         via extraction-faithfulness against upstream parsed_json JSON.
--
-- Methodology: Extraction-faithfulness vs upstream JSON
--   The build (mig 59 SQL, build_script='mig_58_parathyroid_detail_20260424')
--   is a deterministic SELECT * + json_extract_string from
--   main.note_entities_llm_parathyroid_detail_v1 WHERE error=0. Verification
--   re-derives every col fresh from the same upstream and compares per-row.
--   This is a CTC-equivalence variant adapted for LLM-output canonicals where
--   the upstream is the JSON source-of-truth (rather than a pre-Script-N
--   archive snapshot).
--
-- Verification probe results (run 2026-04-28):
--   - 8,697 rows in canonical, 8,697 error=0 rows in upstream, 8,697 joined on
--     parathyroid_event_id (= note_row_id).
--   - 12 of 13 not_started cols: 100% bit-for-bit identical to fresh
--     re-derivation from upstream parsed_json.
--   - 1 col (glands_identified_count): 8,696/8,697 identical. The 1 drift is
--     research_id 9371 / parathyroid_event_id '9371|OPNOTE|1' where upstream
--     JSON='5' (LLM contract violation, field spec is 0-4) but canonical=4
--     (post-build manual cleanup per CF-58-1 closure). Drift represents the
--     intentional cleanup, not extraction infidelity.
--
-- Sign-off scope:
--   13 not_started cols flipped to 'verified':
--     12 via extraction_faithfulness_vs_upstream_json (mass-equivalence)
--      1 (glands_identified_count) via extraction_faithfulness_with_clipped_outlier
--   12 already-na cols carry over: parathyroid_event_id, research_id, note_type,
--     note_index, source_workbook, source_sheet, source_column, llm_model,
--     extracted_at, llm_build_ts, build_script, build_ts (auto provenance/identifier)
--
-- Final state of canonical_parathyroid_events_v1 (post-mig_102):
--   Rows     : 8,697 (one per error=0 note_row_id)
--   Patients : 4,443
--   Cols     : 25
--   Verified : 13 / 25 + 12 na = 25 / 25 closed
--
-- Carry-forwards (deferred, not blocking):
--   CF-58-1 (CLOSED): 1 pt glands_identified=5 contract violation
--                     - canonical clipped to 4; upstream JSON unchanged. If
--                       upstream is ever re-derived, must re-apply the clip.
--   CF-58-2 (open) : 2,219/4,443 = 50% incidental_parathyroidectomy rate is
--                    higher than published rates (10-30%). LLM is liberal when
--                    synoptic lists "parathyroid tissue identified in specimen".
--                    Specificity pass against evidence_quote recommended before
--                    using this flag as outcome.
--   CF-58-3 (open) : 85 pts any_autotransplant=TRUE with NULL
--                    autotransplant_location. Re-prompt these 85 notes if
--                    location is needed for analysis.
--   CF-102-HYPOPT-MISS (NEW): 290 patients have hypoparathyroidism present per
--                    verified canonical_complications_events_v1 (mig_98f) but
--                    hypoparathyroidism_permanent='absent/unknown' in
--                    parathyroid_events. Systematic LLM under-extraction at the
--                    parathyroid-extractor level; complications mig_98f is
--                    source-of-truth for patient-level hypoparathyroidism.
--                    Cross-table consumers should JOIN to complications, not
--                    rely on parathyroid_events for patient-level rollup of
--                    this label.
--   CF-102-HYPOCAL-FP (NEW): 58 patients have hypocalcemia_postop='present' in
--                    parathyroid_events but NOT 'present' in
--                    canonical_complications_events_v1.complication_type=
--                    'hypocalcemia_clinical' (verified mig_98g). Likely
--                    parathyroid LLM over-attributing transient lab dips as
--                    clinical hypocalcemia. Specificity pass via evidence_quote
--                    review recommended.
--
-- This is the 20th canonical table closed under Protocol v2.
-- (Note: NOT including canonical_parathyroid_patient_rollup_v1 in this scope —
-- separate sign-off required, similar pattern.)
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 102a: flip 12 mass-equivalent not_started cols to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_upstream_json',
    batch_id            = 'mig_102_parathyroid_events_signoff_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_102: extraction-faithfulness vs '
                          || 'main.note_entities_llm_parathyroid_detail_v1 '
                          || '(error=0). Bit-for-bit identical 8,697/8,697 '
                          || 'rows under IS-DISTINCT-FROM compare against fresh '
                          || 'json_extract_string re-derivation. Build SQL: '
                          || 'qc_framework_v1/migrations/59_parathyroid_detail'
                          || '_canonical_tier2_v1.sql.'
WHERE schema_name='main'
  AND table_name='canonical_parathyroid_events_v1'
  AND verification_status='not_started'
  AND column_name <> 'glands_identified_count';

-- 102b: flip glands_identified_count with note about CF-58-1 cleanup
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_with_clipped_outlier',
    batch_id            = 'mig_102_parathyroid_events_signoff_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_102: 8,696/8,697 rows match upstream JSON '
                          || 'extraction. 1 row drift (research_id 9371, '
                          || 'parathyroid_event_id 9371|OPNOTE|1): upstream '
                          || 'JSON=5 (LLM contract violation, spec is 0-4); '
                          || 'canonical clipped to 4 in a prior cleanup pass '
                          || '(CF-58-1 closure). Drift is intentional. CF-58-1 '
                          || 'is now CLOSED; if upstream is ever re-extracted, '
                          || 'reapply the 5->4 clip.'
WHERE schema_name='main'
  AND table_name='canonical_parathyroid_events_v1'
  AND column_name='glands_identified_count'
  AND verification_status='not_started';

-- 102c: recompute table_signoff_registry counts and sign off
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
    signed_off_ts     = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/102_parathyroid_events_table_signoff.sql',
    notes             = 'Extraction-faithfulness vs upstream JSON ('
                        || 'main.note_entities_llm_parathyroid_detail_v1, '
                        || 'error=0). 12/13 cols 100% identical to fresh '
                        || 'json_extract_string re-derivation; '
                        || 'glands_identified_count has 1 intentional clip '
                        || '(CF-58-1 cleanup). 5 carry-forwards: CF-58-1 '
                        || 'CLOSED (clipped 5->4); CF-58-2/CF-58-3 (LLM '
                        || 'specificity issues from build); CF-102-HYPOPT-MISS '
                        || '(290 pts missed vs verified mig_98f); CF-102-'
                        || 'HYPOCAL-FP (58 pts over-attributed vs mig_98g).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_parathyroid_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 102 -- canonical_parathyroid_events_v1 closed
-- 20th table verified under Protocol v2.
-- =============================================================================
