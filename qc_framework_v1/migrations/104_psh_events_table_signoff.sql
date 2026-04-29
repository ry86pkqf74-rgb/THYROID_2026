-- =============================================================================
-- Migration 104 -- canonical_psh_events_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser <logan.glosser@gmail.com> (drafted by Cursor/Copilot)
-- Plan:   Verify canonical_psh_events_v1 under Protocol v2 using the LLM
--         extraction-faithfulness pattern adapted to Script 365's deterministic
--         JSON-unnest + transform builder.
--
-- Build SQL reviewed:
--   scripts/365_psh_pmh_meds_consolidation.py::_build_events_sql_for_domain('psh')
--
-- Source/build shape:
--   - Source table: main.note_entities_llm_past_surgical_hx
--   - Upstream source is a single LLM JSON note table with 11,037 note rows.
--   - Canonical build unnests result_json $.entities into 3,919 entity rows,
--     then applies deterministic Script 365 transforms:
--       source_row_id = note_row_id || ':' || ordinality
--       finding_value/finding_value_norm mapping
--       finding_status from present_or_negated/evidence_text
--       evidence_strength from LLM/source specificity rule
--       thyroidectomy anchor/date-derived fields from canonical_operative_events_v1
--       and canonical_patient_master fallback
--       evidence_span_hash = SHA256(evidence_text)
--   - No PSH post-build UPDATE chain was found; the live table should equal a
--     fresh re-execution of the Script 365 PSH event builder, ignoring build_ts.
--
-- Verification probes (MotherDuck, 2026-04-28):
--   - Live canonical: 3,919 rows / 1,878 patients.
--   - Source distribution: 3,919/3,919 rows from
--     note_entities_llm_past_surgical_hx.
--   - Fresh unnest from upstream result_json: 3,919 entity rows.
--   - Natural key: source_row_id; canonical COUNT = COUNT(DISTINCT source_row_id)
--     = 3,919 and fresh COUNT = COUNT(DISTINCT source_row_id) = 3,919.
--   - Fresh Script 365 temp rebuild vs live canonical, joined on source_row_id:
--     3,919/3,919 joined, 0 anti-join rows both directions.
--   - All 18 non-build_ts columns matched exactly under IS DISTINCT FROM:
--     research_id, source_table, source_row_id, source_note_type, llm_confidence,
--     extractor_name, finding_text, finding_value, finding_value_norm,
--     finding_date, mention_note_date, finding_status, evidence_strength,
--     days_from_first_thyroidectomy, is_preexisting, anchor_source, med_status,
--     evidence_span_hash.
--
-- Sign-off scope:
--   15 not_started columns flipped to verified with method
--   'extraction_faithfulness_plus_script365_transforms'. Four auto cols were
--   already NA and carry forward: research_id, source_table, source_row_id,
--   build_ts.
--
-- Cross-validation / carry-forward:
--   CF-104-PSH-OP-DRIFT (NEW): 17 patients have verified operative thyroid/
--   neck surgery evidence in canonical_operative_events_v1 while PSH has an
--   absent prior-thyroid/neck-surgery finding. This is an LLM-quality/context
--   carry-forward, not extraction/build drift, and does not block sign-off.
--   No clinical note text was printed during review.
-- =============================================================================

-- 104a: flip all remaining PSH event columns to verified.
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_plus_script365_transforms',
    batch_id            = 'mig_104_psh_events_signoff_20260428',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_104: verified against fresh Script 365 '
                          || 'PSH event rebuild from main.note_entities_llm_'
                          || 'past_surgical_hx result_json. Live canonical '
                          || '3,919 rows; fresh upstream entity unnest 3,919 '
                          || 'rows; 3,919/3,919 joined on source_row_id; '
                          || '0 anti-join rows both directions; this column '
                          || 'matched exactly under IS DISTINCT FROM. Build '
                          || 'SQL: scripts/365_psh_pmh_meds_consolidation.py '
                          || '_build_events_sql_for_domain(psh). Method covers '
                          || 'LLM JSON extraction plus deterministic Script 365 '
                          || 'transforms (status, evidence_strength, anchors, '
                          || 'date-derived fields, med_status, hash).'
WHERE schema_name='main'
  AND table_name='canonical_psh_events_v1'
  AND verification_status='not_started';

-- 104b: recompute table_signoff_registry counts and sign off.
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
    signoff_migration = 'qc_framework_v1/migrations/104_psh_events_table_signoff.sql',
    notes             = 'PSH events verified by extraction-faithfulness plus '
                        || 'deterministic Script 365 transform rebuild from '
                        || 'main.note_entities_llm_past_surgical_hx result_json. '
                        || 'Live canonical 3,919 rows / 1,878 patients; fresh '
                        || 'upstream entity unnest 3,919 rows; natural key '
                        || 'source_row_id unique on both sides; fresh Script 365 '
                        || 'temp rebuild joined 3,919/3,919 rows with zero '
                        || 'column mismatches across all 18 non-build_ts columns. '
                        || '15 columns verified, 4 auto identifier/provenance '
                        || 'columns remain NA. Carry-forward: CF-104-PSH-OP-'
                        || 'DRIFT = 17 patients with operative thyroid/neck '
                        || 'surgery evidence but PSH absent prior-surgery '
                        || 'finding; LLM-quality/context issue, not sign-off '
                        || 'blocker.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_psh_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 104 -- canonical_psh_events_v1 closed under Protocol v2.
-- =============================================================================
