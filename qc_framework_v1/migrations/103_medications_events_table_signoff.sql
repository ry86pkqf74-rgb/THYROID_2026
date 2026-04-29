-- =============================================================================
-- Migration 103 -- canonical_medications_events_v1 SIGN-OFF (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Methodology: Note-text REAL/TEMPLATE classifier (mig_98 family), applied via
--   qc_framework_v1/scripts/build_medications_review.py
--   qc_framework_v1/scripts/apply_mig_103_medications_decisions.py
--
-- Pre-apply state (2026-04-28 baseline):
--   7,501 rows / 2,070 patients / 19 cols
-- Post-apply row count: 6,473 (1,028 rows removed: absent + template +
--   negation + 6 pre-surgery supplements moved to PMH)
--
-- Sign-off scope:
--   15 not_started → verified (classifier + bulk disposition)
--   4 na unchanged (research_id, source_row_id, source_table, build_ts)
--
-- NOTE: Registry UPDATEs were executed by apply_mig_103_medications_decisions.py
-- alongside MotherDuck mutations. This file documents the intended final state and
-- can be re-run only if the table is still in pre-signoff column states (idempotent
-- WHERE verification_status = 'not_started' on columns).
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by = 'logan',
    verification_method = 'note_text_real_template_classifier_mig103',
    batch_id = 'mig_103_medications_signoff_20260428',
    verified_ts = CURRENT_TIMESTAMP,
    notes = COALESCE(notes,'')
      || ' | mig_103: note-text REAL/TEMPLATE classifier on clinical_notes_long; '
      || 'bulk disposition Logan-ratified (mig_98 family pattern).'
WHERE schema_name='main'
  AND table_name='canonical_medications_events_v1'
  AND verification_status = 'not_started';

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
    signed_off_ts     = CURRENT_TIMESTAMP,
    signoff_migration = 'qc_framework_v1/migrations/103_medications_events_table_signoff.sql',
    notes             = 'mig_103: Protocol v2 note-text classifier verification; '
                      || 'pre-surgery supplement moves to PMH; template/negation/absent DELETE.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_medications_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;
