-- =============================================================================
-- Migration 100 -- canonical_frozen_section_events_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   Final sign-off of canonical_frozen_section_events_v1 under Protocol v2
--         18th canonical table closed under v2 (after the 17 already verified
--         through mig_99 complications close-out earlier today).
--
-- Methodology: CTC-equivalence verification (pattern established mig_87 / mig_90 /
--   mig_91, see project_ctc_equivalence_verification_pattern.md).
--   Source-of-truth archive: archive_pub_v1_0.tier2_frozen_section_event_v1_pre
--   CANONICALPROMOTION_20260421T162256Z (the immediate pre-Script-360-promotion
--   snapshot, hosted in the "Thyroid 2026 UPdated" database).
--
-- Verification probe results (run 2026-04-28 under Cowork query_rw):
--   - Both tables: 31 cols / 7,081 rows / 7,081 distinct (research_id, frozen_event_index)
--     → natural key intact on both sides.
--   - Bidirectional EXCEPT across all 31 cols: 7,080 / 7,081 rows differ.
--   - Per-column IS-DISTINCT-FROM diff counts: ONLY frozen_section_date drifts
--     (7,080 / 7,081 rows). All other 30 cols are bit-for-bit identical.
--   - frozen_section_date drift characterization: format reshape ONLY.
--     Archive : ISO-8601 strings, mix of 'YYYY-MM-DD' (10-char) and
--               'YYYY-MM-DD 00:00:00' (19-char).
--     Current : US-locale 'MM/DD/YYYY' (10-char).
--     Calendar-day equality check via TRY_STRPTIME parse: 7,080 / 7,080 SAME DAY.
--     Zero semantic drift.
--
-- Sign-off scope:
--   25 not_started cols flipped to 'verified':
--     24 via CTC-equivalence (mechanical bit-for-bit match vs pre-promotion archive)
--      1 (frozen_section_date) via CTC-equivalence-with-format-note
--   6 already-na cols carry over: research_id, operative_episode_id, note_type,
--     source_sheet, source_column, source_priority (auto_provenance/identifier skips)
--
-- Final state of canonical_frozen_section_events_v1 (post-mig_100):
--   Rows     : 7,081
--   Patients : 4,116
--   Cols     : 31
--   Verified : 25 / 31 + 6 na = 31 / 31 closed
--   Carry-forwards (deferred, not blocking):
--     CF-100-DATE-RETYPE : frozen_section_date is VARCHAR storing US-locale
--       'MM/DD/YYYY' strings. Every other verified Tier 1 events table stores
--       date cols as DATE or TIMESTAMP types (e.g.
--       canonical_fna_events_v1.fna_date_resolved DATE,
--       canonical_complications_events_v1.surgery_date TIMESTAMP). This col
--       was ISO-8601 string in the pre-promotion archive — both shapes are
--       VARCHAR, so retyping was never explicitly handled by Script 360.
--       Recommend a future repair migration to:
--         1) ALTER TABLE main.canonical_frozen_section_events_v1
--            ADD COLUMN frozen_section_date_dt DATE;
--         2) UPDATE … SET frozen_section_date_dt =
--               TRY_STRPTIME(frozen_section_date,'%m/%d/%Y')::DATE;
--         3) Drop VARCHAR variant, RENAME _dt → frozen_section_date.
--       Logan-ratified disposition (Cowork session 2026-04-28): verify-with-note
--       + open this CF; do not block table sign-off on type repair.
--
-- This is the 18th table closed under Protocol v2.
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 100a: flip 24 mechanically-equivalent not_started cols to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'ctc_equivalence_vs_pre_promotion_archive',
    batch_id            = 'mig_100_frozen_section_signoff_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_100: CTC-equivalence vs '
                          || 'archive_pub_v1_0.tier2_frozen_section_event_v1_'
                          || 'preCANONICALPROMOTION_20260421T162256Z. Bit-for-bit '
                          || 'identical 7,081/7,081 rows on this column under '
                          || 'IS-DISTINCT-FROM compare. Pre-Script-360 promotion '
                          || 'baseline is the value-source-of-truth.'
WHERE schema_name='main'
  AND table_name='canonical_frozen_section_events_v1'
  AND verification_status = 'not_started'
  AND column_name <> 'frozen_section_date';

-- 100b: flip frozen_section_date to verified with format-reshape note + CF
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'ctc_equivalence_format_reshape_calendar_day_preserved',
    batch_id            = 'mig_100_frozen_section_signoff_20260428',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_100: format reshape ISO-8601 -> US-locale '
                          || 'MM/DD/YYYY post-Script-360. TRY_STRPTIME parse on '
                          || 'both sides: 7,080/7,080 drifted rows = same calendar '
                          || 'day. Zero semantic drift. CF-100-DATE-RETYPE: VARCHAR '
                          || 'shape divergent from project DATE/TIMESTAMP norm; '
                          || 'open future repair migration to retype.'
WHERE schema_name='main'
  AND table_name='canonical_frozen_section_events_v1'
  AND column_name='frozen_section_date'
  AND verification_status = 'not_started';

-- 100c: recompute canonical_table_signoff_registry_v1 counts and sign off
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
    signoff_migration = 'qc_framework_v1/migrations/100_frozen_section_table_signoff.sql',
    notes             = 'CTC-equivalence vs pre-Script-360 promotion archive '
                        || '(archive_pub_v1_0.tier2_frozen_section_event_v1_'
                        || 'preCANONICALPROMOTION_20260421T162256Z). 30/31 cols '
                        || 'bit-for-bit identical; frozen_section_date format '
                        || 'reshape (ISO->US-locale, calendar-day preserved). '
                        || 'CF-100-DATE-RETYPE open: retype VARCHAR -> DATE in '
                        || 'a future repair migration to align with Tier 1 norm.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_frozen_section_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 100 -- canonical_frozen_section_events_v1 closed
-- 18th table verified under Protocol v2.
-- =============================================================================
