-- =============================================================================
-- Migration 123 — canonical_survival_followup_v1 SIGN-OFF (Protocol v2)
-- =============================================================================
-- Date:   2026-04-29 (UTC — Cursor lane 15 / cohort-wide survival derivation)
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Plan:   Close patient-grain survival/follow-up SSOT built by scripts/364B_survival_followup_consolidation.py
--         (main.note_entities_llm_survival_followup + 5 canonical_labs_* + canonical_operative_events_v1 +
--          canonical_patient_master anchor).
--
-- Pre-signoff probes (MotherDuck thyroid_canonical_publication_v1_0, local MotherDuck client
-- staging note_entities_llm_survival_followup from scripts/output/parquet/main/
-- note_entities_llm_survival_followup.parquet — publication catalog currently has no main.*
-- survival_followup LLM staging table; full SQL replay requires parquet stage for re-derivation):
--   * Cohort parity: COUNT(*)=10,871 = COUNT(DISTINCT research_id)=DISTINCT canonical_patient_master.
--   * MAX(build_ts) survival = MAX(build_ts) canonical_operative_events_v1 = 2026-04-22 (aligned).
--   * Fresh re-derivation via imported _build_sql() from Script 364B + staged parquet vs live table:
--       9 adjudicated semantic cols compared; 8/9 cols 0 drift (10,871/10,871 patients).
--       last_followup_source: 2/10,871 label drift (research_id 7775, 8222) —
--       BOTH cases last_known_alive_date identical; tie on MAX(lab contact_date) across
--       canonical_labs_pth_v1 vs canonical_labs_vitamin_d_v1 same calendar day; Script 364B uses
--       ANY_VALUE(source) in latest_lab_date CTE (non-deterministic among tied MAX sources).
--       Date + all other columns match — value-correct, label cosmetic.
--   * Internal consistency (live): deceased_no_death_date=0; alive_no_lka=0; bad_vital=0;
--       followup_complete_at_{5,10}yr TRUE rows all have days_from_first_surgery_to_last_contact
--       >= YEAR_{5,10}_DAYS (1820 / 3645 with 5-day slack vs anniversaries per 364B).
--   * first_surgery_date vs MIN(canonical_operative_events_v1.surgery_date_native) DATE: 0 mismatch
--       (10,871 patients; 0 null-on-one-side anomalies).
--   * last_followup_source vocabulary: canonical_labs_* (5) + operative_events_{surgery,note}_date +
--       llm_last_followup_date_entity only.
--
-- Sign-off scope:
--   8 not_started → verified: derivation_re_derivation_against_script_364B_staged_upstream
--   1 not_started → verified: last_followup_source via
--       derivation_re_derivation_with_ANY_VALUE_lab_source_tiebreak (2 label-only drifts; dates 0 drift)
--   4 na unchanged: research_id, build_ts, build_script, extraction_run_id
--
-- Carry-forward:
--   CF-mig123-SURVIVAL-LLM-STAGING-ABSENT-ON-MD — main.note_entities_llm_survival_followup is not
--       present in current publication catalog; verification replay used repo parquet stage. Next 364B
--       --commit should restore LLM table before rebuild, or promote parquet in attach pattern.
--   CF-mig123-LAST-FOLLOWUP-SOURCE-ANY-VALUE — When two+ lab canonicals share the same MAX(contact_date),
--       ANY_VALUE(source) picks an arbitrary label; prefer ARG_MAX(contact_date, source) in a future
--       Script 364B patch for deterministic source attribution (cosmetic only).
--
-- Lane 13 follow-on: CF-mig121-ETE-EVENT-RESOLVED-SURVIVAL-PENDING — append closed on
-- canonical_ete_event_resolved_v1 survival columns (123d); TIMESTAMP/DATE bridge for last_known_alive_date
-- on ete layer remains under CF-100 / mig_121 header until view rebuild.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 123a — Eight columns: full match on staged Script 364B re-derivation (2026-04-29)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_against_script_364B_staged_upstream',
    batch_id            = 'mig_123_canonical_survival_followup_v1_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_123: Script 364B SQL replay with staged '
                          || 'note_entities_llm_survival_followup (repo parquet) + live '
                          || 'labs/op/CPM; 10,871/10,871 IS NOT DISTINCT FROM on these cols. '
                          || 'Builder: scripts/364B_survival_followup_consolidation.py.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_survival_followup_v1'
  AND verification_status = 'not_started'
  AND column_name IN (
    'vital_status_current',
    'death_date',
    'death_date_source',
    'last_known_alive_date',
    'days_from_first_surgery_to_last_contact',
    'followup_complete_at_5yr',
    'followup_complete_at_10yr',
    'first_surgery_date'
  );

-- -----------------------------------------------------------------------------
-- 123b — last_followup_source: deterministic date; ANY_VALUE label tiebreak on tied lab MAX
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_with_ANY_VALUE_lab_source_tiebreak',
    batch_id            = 'mig_123_canonical_survival_followup_v1_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_123: Re-derivation 2/10,871 label-only drift (rids 7775, 8222) '
                          || 'when PTH vs vitamin D labs tie on same MAX(contact_date); '
                          || 'last_known_alive_date 0 drift. ANY_VALUE(source) in 364B latest_lab_date. '
                          || 'CF-mig123-LAST-FOLLOWUP-SOURCE-ANY-VALUE open for ARG_MAX hardening.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_survival_followup_v1'
  AND column_name = 'last_followup_source'
  AND verification_status = 'not_started';

-- -----------------------------------------------------------------------------
-- 123c — Table signoff registry
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
    signoff_migration = 'qc_framework_v1/migrations/123_canonical_survival_followup_v1_signoff.sql',
    notes             = 'Protocol v2 survival follow-up SSOT: Script 364B derivation replay '
                        || '(staged LLM parquet) 8/9 cols exact; last_followup_source tiebreak only. '
                        || 'Cohort 10,871 = CPM; internal QA gates pass; first_surgery = MIN(op). '
                        || 'CF LLM staging absent on MD catalog; CF ANY_VALUE source label.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_survival_followup_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- -----------------------------------------------------------------------------
-- 123d — Lane 13: clarify ETE resolved survival layering post survival SSOT verification
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_method = 'derivation_re_derivation_post_survival_followup_v1_verified',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'')
          || ' | mig_123 follow-on: canonical_survival_followup_v1 Protocol v2 closed (lane 15). '
          || 'CF-mig121-ETE-EVENT-RESOLVED-SURVIVAL-PENDING → derivation_re_derivation_post_survival '
          || 'verified; TIMESTAMP last_alive on ete envelope still bridged vs DATE SSOT per CF-100.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_ete_event_resolved_v1'
  AND column_name IN ('last_known_alive_date','vital_status')
  AND verification_status = 'verified';

-- =============================================================================
-- end migration 123 — canonical_survival_followup_v1 verified (Protocol v2)
-- =============================================================================
