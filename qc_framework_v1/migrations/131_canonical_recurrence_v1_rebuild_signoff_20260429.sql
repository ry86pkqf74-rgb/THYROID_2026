-- =============================================================================
-- Migration 131 — canonical_recurrence_v1 REBUILD SIGN-OFF (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC — Cursor Lane 19 RESUME — spot-check fixes + RW 203b)
-- Author: Logan Glosser (drafted / applied with Claude)
--
-- File numbering: `123_*.sql` is already taken by mig_123 survival follow-up
-- (Lane 15). Lane 19 batch_id remains **mig_123_canonical_recurrence_v1_rebuild_signoff_20260429**
-- per protocol v2 handoff (logical sign-off id).
--
-- Pre-requisites (MotherDuck live):
--   * scripts/203b_canonical_recurrence_harmonized_20260429.py --write completed
--   * Pre-snapshot: "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig123_20260429
--
-- Repo SSOT builder:
--   scripts/203b_canonical_recurrence_harmonized_20260429.py
--   Harmonized spine: canonical_operative_events_v1 + canonical_patient_master
--
-- Spot-check-derived filters (2026-04-29):
--   * Legacy fallback (recurrence_event_clean_v1): recurrence_date MUST be strictly
--     AFTER first_surgery_date (join canonical first_surg rollup) —
--     excludes initial-dx/completion-Thy mismaps (~15 structural_confirmed_legacy rows dropped).
--   * path_proven_candidates defensive date predicate (CONSTANT in Script 203b):
--     CAST(path_proven_date AS DATE) BETWEEN 1990-01-01 AND 2027-01-01
--     — audit confirms 2 upstream outliers (rids 12057 yr-0202, 10622 yr-1950) outside band.
--     Tier-1 UNION from manuscript_workspace table deferred to CF-mig124 lineage work.
--
-- Live verification probes (MotherDuck, post-write):
--   * COUNT(*) = COUNT(DISTINCT research_id) = 10,871 = canonical_patient_master
--   * recurrence_confirmed = TRUE ⇒ ~514 (spot-check tier mix)
--
-- CLOSED carry-forward:
--   * CF-mig122-RECURRENCE-203-REBUILD-PENDING — Script 203b harmonized rebuild landed.
--
-- OPEN carry-forwards (append to verification notes discovery):
--   * CF-mig123-UPSTREAM-DATE-202-TYPO — path_proven outliers at rids 12057, 10622
--     excluded from Tier-1 path UNION predicated on date band until upstream QC.
--   * CF-mig123-NEGATIVE-TTR-9-PATIENTS — residual first_surgery semantics review
--     where legacy negatives occurred pre-filter (nine index cases retained in lineage notes).
--   * CF-mig123-LEGACY-COMPLETION-CHECK-6674 — dual-surgery ±34d interim (completion pattern).
--   * CF-mig124-RECURRENCE-PATH-CANONICAL-LINEAGE — phase-2: Tier-1 path-proven UNION from
--     curated canonical_path_malignant_events_v1 + canonical_pathology_clinical_events_v1.
--   * Existing first_surgery TIMESTAMP retype drift — CF-mig122-RECURRENCE-FIRST-SURGERY-DATE-RETYPE /
--     CF-mig122-RECURRENCE-FIRST-SURGERY-OPERATIVE-PROXY-DRIFT (unchanged substantive).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 131a — Flip 11 derivation columns (replaces mig_122 shell-degenerate verification)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_method = 'derivation_re_derivation_post_script_203b_harmonized_rebuild',
    batch_id            = 'mig_123_canonical_recurrence_v1_rebuild_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_123 (SQL file 131): Script 203b harmonized RW + spot-check filters '
                          || '(2026-04-29). Legacy structural rec_date>first_surgery_date; '
                          || 'path_proven date-band predicate constant for future Tier-1 blend. '
                          || 'CLOSED CF-mig122-RECURRENCE-203-REBUILD-PENDING. '
                          || 'OPEN: CF-mig123-UPSTREAM-DATE-202-TYPO; CF-mig123-NEGATIVE-TTR-9-PATIENTS; '
                          || 'CF-mig123-LEGACY-COMPLETION-CHECK-6674; CF-mig124-RECURRENCE-PATH-CANONICAL-LINEAGE.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_v1'
  AND verification_status = 'verified';

-- first_surgery_date column: append mig_123 batch pointer (first_surgery CF continuity)
UPDATE main.canonical_column_verification_registry_v1
SET notes               = COALESCE(notes,'')
                          || ' | mig_123 (file 131): first_surgery_date semantics unchanged vs '
                          || 'mig_122 proxy-drift CFs — see CF-mig122-RECURRENCE-FIRST-SURGERY-* ; '
                          || 'recurrence rebuild does not retype TIMESTAMP storage.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_v1'
  AND column_name = 'first_surgery_date'
  AND verification_status = 'verified';

-- -----------------------------------------------------------------------------
-- 131b — Table signoff registry refresh
-- -----------------------------------------------------------------------------
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
    signoff_migration = 'qc_framework_v1/migrations/131_canonical_recurrence_v1_rebuild_signoff_20260429.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_131 (batch mig_123): Protocol v2 — Script 203b populated rebuild '
                        || '(confirmed TRUE ~514); replaces mig_122 degenerate-shell verification. '
                        || 'Spot-check QA 2026-04-29; carry-forwards mig_123+mig124 path lineage.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_recurrence_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end migration 131 — canonical_recurrence_v1 populated rebuild verified
-- (logical batch_id mig_123 per Lane 19)
-- =============================================================================
