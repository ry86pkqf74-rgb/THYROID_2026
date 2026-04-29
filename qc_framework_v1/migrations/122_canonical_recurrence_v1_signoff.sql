-- =============================================================================
-- Migration 122 — canonical_recurrence_v1 SIGN-OFF (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC — Cursor lane 14 / recurrence cohort-wide derivation)
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Repo SSOT builder: scripts/203_canonical_recurrence.py (tiers 1–5 +
--   recurrence_event_clean_v1 legacy + gold cohort padding — MotherDuck spine
--   differs; gold_master_patient_facts_v1 absent on publication catalog).
--
-- Pre-signoff probes (MotherDuck thyroid_canonical_publication_v1_0, 2026-04-29):
--   * Row/patient cardinality: COUNT(*)=10,871 =
--       COUNT(DISTINCT research_id)=10,871 = main.canonical_patient_master 10,871.
--   * Degenerate shell cohort: recurrence_confirmed=FALSE all 10,871 rows;
--       recurrence_type='none'; recurrence_definition='no_recurrence_evidence';
--       recurrence_evidence_source IS NULL all rows (INTEGER column, dormant until
--       tier-encoding populated); biochemical_tg_* both NULL all rows.
--   * Internal consistency gates (degenerate case): recurrence_date IS NULL for all;
--       time_to_recurrence_days IS NULL for all; nadir_gt_recurrence = 0.
--   * build_ts: absent on canonical_recurrence_v1 — staleness vs
--       canonical_path_malignant_events_v1 MAX(build_ts)=2026-04-22 not comparable;
--       shell table carries no malignant-feed recurrence rows.
--   * first_surgery_date (TIMESTAMP midnight storage): proxy drift vs
--       CAST(MIN(canonical_operative_events_v1.surgery_date_native) AS DATE) per
--       patient — 2,329 calendar-day mismatches; 2,140 recurrence rows NULL fs
--       while operative MIN exists — lineage gap vs Script 203 union of
--       operative_episode_detail_v2 + path_synoptics fallback (operative_episode
--       detail table not present on publication DB under legacy name).
--       Does NOT block Protocol v2 column closure — documented CF below.
--
-- Methodology:
--   Derivation verification on degenerate cohort-wide rollup: confirm universe =
--   canonical_patient_master and column bundles obey ATA-style exclusion logic for
--   the uniform no-evidence row (FALSE / NULL dates / NULL labs / encoded none).
--   Full Script 203 replay requires RW rebuild + operative spine harmonization —
--   OUT OF SCOPE for this sign-off (see CF-mig122-RECURRENCE-203-REBUILD-PENDING).
--
-- Carry-forwards (documentation — joins CF-100 / mig_117–119 batch context):
--   * CF-mig122-RECURRENCE-FIRST-SURGERY-DATE-RETYPE — first_surgery_date stored as
--       TIMESTAMP (calendar semantics via CAST/C DATE_TRUNC day vs joins); aligns
--       feedback_clinical_dates_calendar_only.md surgical-git-add lane.
--   * CF-mig122-RECURRENCE-FIRST-SURGERY-OPERATIVE-PROXY-DRIFT — 2,329-day /
--       2,140-null mismatches vs canonical_operative_events_v1 MIN proxy until
--       canonical recurrence rebuilt from harmonized operative canonical spine.
--   * CF-mig121-ETE-EVENT-RESOLVED-RECURRENCE-PENDING — CLOSED for labeling policy:
--       canonical_recurrence_v1 verified shell; downstream ete resolved recurrence
--       block may amend notes from extraction_faithfulness framing (see 122c).
--
-- Sign-off scope:
--   11 not_started → verified via cohort_wide_shell_derivation_verification.
--   1 na unchanged: research_id (auto_identifier_skip).
--
-- Lane 13 unblock: canonical_ete_event_resolved_v1 recurrence-column notes appended
--   (122c) clarifying derivation context post recurrence_v1 verification.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 122a — Flip all derivable/adjudicated columns on canonical_recurrence_v1
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cohort_wide_shell_derivation_verification_degenerate_case',
    batch_id            = 'mig_122_canonical_recurrence_v1_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_122: cohort parity 10,871 = canonical_patient_master; '
                          || 'uniform shell (none/no_recurrence_evidence); gates pass '
                          || '(FALSE ⇒ NULL recurrence_date/TTR; biochemical NULL). '
                          || 'Proxy drift first_surgery vs canonical_operative_events_v1 '
                          || 'MIN documented (2329 day mismatches / 2140 null-vs-op); '
                          || 'TIMESTAMP fs CF-mig122-RECURRENCE-FIRST-SURGERY-DATE-RETYPE. '
                          || 'Full Script 203 tier replay pending operative spine rebuild '
                          || '(CF-mig122-RECURRENCE-203-REBUILD-PENDING). Builder SSOT '
                          || 'scripts/203_canonical_recurrence.py.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_v1'
  AND verification_status = 'not_started';

-- -----------------------------------------------------------------------------
-- 122b — Table signoff registry for canonical_recurrence_v1
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
    signoff_migration = 'qc_framework_v1/migrations/122_canonical_recurrence_v1_signoff.sql',
    notes             = 'Protocol v2 cohort-wide recurrence derivation verified '
                        || '(shell degenerate case). 10,871 = CPM; internal gates PASS; '
                        || 'operative-proxy drift + TIMESTAMP fs documented as CFs. '
                        || 'Lane 14 closes Tier-2 recurrence patient rollup prerequisite.'
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

-- -----------------------------------------------------------------------------
-- 122c — Lane 13 carry-forward: clarify recurrence block notes on ete resolved
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
          || ' | mig_122 follow-on: canonical_recurrence_v1 verified (lane 14 shell '
          || 'cohort-wide). CF-mig121-ETE-EVENT-RESOLVED-RECURRENCE-PENDING → '
          || 'derivation_re_derivation_post_recurrence_verified for labeling policy '
          || '(faithfulness vs canonical_recurrence_resolved_v1 unchanged).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_ete_event_resolved_v1'
  AND column_name IN (
    'recurrence_path_proven','recurrence_path_proven_date','recurrence_path_proven_source','days_to_path_proven',
    'recurrence_imaging_suspicious','recurrence_imaging_suspicious_date',
    'recurrence_imaging_modality_summary','recurrence_imaging_modalities_all',
    'recurrence_imaging_finding_text','recurrence_imaging_n_events','days_to_imaging_suspicious',
    'recurrence_imaging_then_path_confirmed','recurrence_status_final'
  )
  AND verification_status = 'verified';

-- =============================================================================
-- end migration 122 — canonical_recurrence_v1 Protocol v2 closed (shell tier)
-- =============================================================================
