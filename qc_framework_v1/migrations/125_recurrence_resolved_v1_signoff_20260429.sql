-- =============================================================================
-- Migration 125 — canonical_recurrence_resolved_v1 SIGN-OFF (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC — Lane 17 / Tier-2 recurrence resolved)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Target: main.canonical_recurrence_resolved_v1 (19 cols, 10,871 rows, 10,871 patients)
-- Builder SSOT: qc_framework_v1/migrations/62_canonical_recurrence_resolved_v1.sql (mig_62)
--
-- Hybrid methodology (cf. qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql):
--   * SOURCE CLUSTERS — extraction-faithfulness vs manuscript_workspace candidate pools
--     produced by the same mig_62 DDL + first_surgery spine from
--     canonical_path_malignant_events_v1 (malignant histology filter per mig_62).
--   * DERIVED CLUSTER — internal consistency vs canonical column algebra in mig_62 §3
--     (days_to_*, n_events, imaging_then_path, recurrence_status_final CASE).
--
-- Live MotherDuck probes (thyroid_canonical_publication_v1_0, 2026-04-29):
--   * Cohort: COUNT(*)=10,871 = COUNT(DISTINCT research_id) = canonical_patient_master.
--   * Signals: recurrence_path_proven TRUE = 191; recurrence_imaging_suspicious TRUE = 768;
--       recurrence_imaging_then_path_confirmed = 33.
--   * recurrence_status_final (3 values): none 9,979 | imaging_only_unconfirmed 701 |
--       path_proven 191 — matches CASE WHEN path THEN path_proven WHEN img THEN imaging_only_unconfirmed
--       ELSE none (0 encoding-violation rows).
--   * Path cluster vs manuscript_workspace.recurrence_path_proven_candidates_v1 rollups:
--       0 drift on (BOOL presence, MIN date, STRING_AGG source, STRING_AGG evidence).
--   * Imaging cluster vs manuscript_workspace.recurrence_imaging_suspicious_candidates_v1:
--       0 drift on (MIN date, modalities, sources, findings); modality_summary 'multiple'
--       rule vs comma-in-modalities verified 0 drift;
--       recurrence_imaging_n_events vs per-patient COUNT(*) with COALESCE(NULL,0)=0 drift.
--   * first_surg_date vs MIN(surgery_date) from canonical_path_malignant_events_v1 (mig_62
--       primary_histology filter): 0 drift.
--   * Derived: days_to_* = date_diff(day, first_surg_date, respective date) where flags TRUE —
--       0 errors; recurrence_imaging_then_path_confirmed matches mig_62 predicate
--       (img_date < path_date - 7 days) with COALESCE — 0 errors; 0 temporal-inversion rows
--       on TRUE then_path rows.
--   * Staleness: MAX(rr.build_ts)=2026-04-27; MAX(canonical_path_malignant_events_v1.build_ts)
--       and MAX(canonical_pathology_clinical_events_v1.build_ts)=2026-04-22 (rr newer —
--       acceptable; path candidates rebuilt with PME snapshot at mig_62 apply).
--
-- Lane 13 cross-check (canonical_ete_event_resolved_v1 mig_121):
--   * Do NOT outer-join all 10,871 CPM patients to per-patient agg from ete_event_resolved:
--       ete layer is path-malignant event grain only (~4,137 patients / 6,689 events) — 6,734
--       rr rows have no ete rows, producing spurious IS DISTINCT FROM drift if per_pt is NULL.
--   * Correct probe: INNER JOIN events to rr on CAST(er.research_id AS VARCHAR)=rr.research_id;
--       COALESCE(er.recurrence_path_proven,FALSE) IS DISTINCT FROM rr.recurrence_path_proven → 0;
--       recurrence_path_proven_date IS DISTINCT FROM rr → 0 across all joined events.
--   * Confirms mig_121 extraction_faithfulness_against_canonical_recurrence_resolved_v1_mig62
--       remains grounded after this table’s verify pass.
--
-- Sign-off scope:
--   16 not_started → verified (below); 3 na unchanged: research_id, build_script, build_ts.
--   table_status → verified.
--
-- Note: recurrence_status_final enum uses imaging_only_unconfirmed (mig_62), not
--   imaging_only_suspicious — naming reflects unconfirmed imaging-only track vs path_proven.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 125a — Path-proven cluster (extraction-faithfulness vs path-proven candidates v1)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_recurrence_path_proven_candidates_v1_mig62',
    batch_id            = 'mig125_recurrence_resolved_v1_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig125: 0 drift vs manuscript_workspace.recurrence_path_proven_candidates_v1 '
                          || 'rollups for BOOL+MIN(date)+STRING_AGG(source)+STRING_AGG(evidence) on 10,871 '
                          || 'patient join. SSOT qc_framework_v1/migrations/62_canonical_recurrence_resolved_v1.sql. '
                          || 'n_path_proven=191 (2026-04-29 probe).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_resolved_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'recurrence_path_proven',
    'recurrence_path_proven_date',
    'recurrence_path_proven_source',
    'recurrence_path_proven_evidence'
  );

-- -----------------------------------------------------------------------------
-- 125b — Imaging-suspicious cluster (extraction-faithfulness vs imaging candidates v1)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_recurrence_imaging_suspicious_candidates_v1_mig62',
    batch_id            = 'mig125_recurrence_resolved_v1_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig125: 0 drift vs manuscript_workspace.recurrence_imaging_suspicious_candidates_v1 '
                          || 'for MIN(date), modalities, sources, finding text; modality_summary comma rule verified. '
                          || 'n_imaging_suspicious=768 (2026-04-29 probe).'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_resolved_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'recurrence_imaging_suspicious',
    'recurrence_imaging_suspicious_date',
    'recurrence_imaging_modality',
    'recurrence_imaging_modality_summary',
    'recurrence_imaging_source',
    'recurrence_imaging_finding_text'
  );

-- -----------------------------------------------------------------------------
-- 125c — first_surgery spine (extraction-faithfulness vs PME first malignant surgery)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_canonical_path_malignant_events_v1_first_surgery_mig62',
    batch_id            = 'mig125_recurrence_resolved_v1_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig125: first_surg_date equals MIN(cast(surgery_date AS DATE)) from '
                          || 'canonical_path_malignant_events_v1 with mig_62 primary_histology IS NOT NULL '
                          || 'exclusions; 0 patient-level drift vs canonical_recurrence_resolved_v1.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_resolved_v1'
  AND verification_status = 'not_started'
  AND column_name = 'first_surg_date';

-- -----------------------------------------------------------------------------
-- 125d — Derived + final status (internal consistency vs mig_62 SELECT algebra)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'internal_consistency_mig62_derived_recurrence_fields',
    batch_id            = 'mig125_recurrence_resolved_v1_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig125: days_to_* match date_diff from first_surg_date; '
                          || 'recurrence_imaging_n_events matches candidate COUNT with COALESCE(NULL,0); '
                          || 'recurrence_imaging_then_path matches img < path - 7d predicate (n_then=33); '
                          || 'recurrence_status_final matches CASE(path|imaging|none) with 0 manual violations.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_recurrence_resolved_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'days_to_path_proven',
    'days_to_imaging_suspicious',
    'recurrence_imaging_n_events',
    'recurrence_imaging_then_path_confirmed',
    'recurrence_status_final'
  );

-- -----------------------------------------------------------------------------
-- 125e — Table signoff registry
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/125_recurrence_resolved_v1_signoff_20260429.sql',
    notes             = COALESCE(ts.notes, '')
                        || ' | mig125: Tier-2 dual-track recurrence canonical verified (hybrid faithfulness '
                        || '+ internal consistency). 10,871 patients; path=191 / imaging_suspicious=768 / '
                        || 'status imaging_only_unconfirmed=701. Lane 13 ete_event INNER-join recurrence parity 0 drift.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'canonical_recurrence_resolved_v1'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- =============================================================================
-- end migration 125 — canonical_recurrence_resolved_v1 verified (Protocol v2)
-- =============================================================================
