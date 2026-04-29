-- Migration: 142b_patient_master_rai_cohort_uniformity_cleanup_20260429.sql
-- Purpose: Cohort-uniformity sweep on canonical_patient_master mig_142 RAI cluster (51 cols)
--          surfaced 8 cols the agent didn't fully address:
--
--          (A) Type-B placeholder propagation -> reclassify verified -> na (2 cols):
--              - rai_avid_flag    : 0 TRUE / 862 FALSE / 10009 NULL
--              - rai_avidity      : 0 TRUE / 862 FALSE / 10009 NULL  (100% identical to rai_avid_flag)
--              Both directly propagate from upstream rai_treatment_episode_v2.iodine_avidity_flag
--              (1857/1857 FALSE) which was already reclassified to na in mig_148b. Following the
--              same Type-B "upstream not extracted" pattern. Will re-flip to verified after V2
--              RAI NLP backfill populates real iodine-avidity signal.
--
--          (B) Type-A presence-flag invariance -> CF-COHORT-NEAR-UNIFORM-TRUE appendix (4 cols):
--              - nlp_raidetail_has_data    : 620 TRUE / 0 FALSE / 10251 NULL
--              - rai_has_adjudication      : 862 TRUE / 0 FALSE / 10009 NULL
--              - rai_has_completion_status : 862 TRUE / 0 FALSE / 10009 NULL
--              - rai_received_reconciled   : 862 TRUE / 0 FALSE / 10009 NULL
--              These are presence flags by design — FALSE bucket is structurally impossible
--              (no-signal patients land as NULL, not FALSE). Same agent miss pattern as mig_141.
--              Keep verified informational.
--
--          (C) Single-value-VARCHAR upstream degeneracy -> CF-VALUE-DEGENERATE-UPSTREAM (2 cols):
--              - rai_intent_list           : 1 distinct ("unknown") across 862 non-null
--                                            -> propagation of upstream rai_intent (1857/1857 "unknown")
--                                            -> rai_intent_v9 is the analytic SSOT (5 distinct: ablation,
--                                               adjuvant, remnant_ablation, therapeutic, unknown)
--              - rai_assertion_statuses    : 1 distinct ("likely_received") across 35 non-null
--                                            -> column rolled out post-episode-promotion;
--                                               distribution will broaden after Script-22+ backfill
--              Keep verified informational.
--
--          Cross-source spot-check (5 random RAI-positive rids) was perfectly clean:
--          PM rai_total_cumulative_dose_mci matched SSOT SUM exactly for 5/5 patients
--          (310/310, 328/328, 750/750, 3204/3204, 396/396 mCi); first/last episode dates exact.
--          Cross-validation `rai_total_cumulative_dose_mci` ↔ rai_treatment_episode_v2 SUM:
--            n_pm=10871, drift_rows=0, ssot_only=4 (= existing CF-mig142-RAI-DOSE-WITHHOLD-4PT).
--
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 31b (mig_142 cleanup; registry-only)
-- Effect : canonical_patient_master n_verified -1140 + new lane landings, n_na +2; mig_142 n_cols 51 -> 49 verified + 2 na

-- (A) Reclassify rai_avid_flag verified -> na
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verification_method = 'placeholder_pending_real_extraction_propagation_from_rai_treatment_episode_v2_iodine_avidity_flag_na_per_mig148b',
    batch_id = 'mig_142b_rai_cohort_uniformity_20260429',
    verified_by = 'cowork_mig_142b',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'') ||
            ' | mig_142b: Reclassified verified->na. Upstream rai_treatment_episode_v2.iodine_avidity_flag is 1857/1857 FALSE placeholder (na per mig_148b). PM rai_avid_flag direct propagation: 0 TRUE / 862 FALSE / 10009 NULL — Type-B upstream not extracted. Also 100% identical to rai_avidity (10871/10871). Will return to verified after V2 RAI NLP backfill populates real avidity signal.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'rai_avid_flag';

-- (A) Reclassify rai_avidity verified -> na
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verification_method = 'placeholder_pending_real_extraction_propagation_from_rai_treatment_episode_v2_iodine_avidity_flag_na_per_mig148b',
    batch_id = 'mig_142b_rai_cohort_uniformity_20260429',
    verified_by = 'cowork_mig_142b',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(notes,'') ||
            ' | mig_142b: Reclassified verified->na. Upstream rai_treatment_episode_v2.iodine_avidity_flag is 1857/1857 FALSE placeholder (na per mig_148b). PM rai_avidity direct propagation: 0 TRUE / 862 FALSE / 10009 NULL — Type-B upstream not extracted. Also 100% identical to rai_avid_flag (10871/10871) — col-level redundancy. Will return to verified after V2 RAI NLP backfill populates real avidity signal.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'rai_avidity';

-- (B) CF-COHORT-NEAR-UNIFORM-TRUE appendix on 4 presence-flag Type-A cols
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_142b: CF-mig142-COHORT-NEAR-UNIFORM-TRUE-nlp_raidetail_has_data — sweep showed 620 TRUE / 0 FALSE / 10251 NULL. Type-A presence flag: when LLM ran on a note with entity data, it always found something. FALSE bucket structurally impossible by design (NULL = no LLM run / no notes). Keep verified informational. Sweep miss in agent QA — same pattern as mig_141.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'nlp_raidetail_has_data';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_142b: CF-mig142-COHORT-NEAR-UNIFORM-TRUE-rai_has_adjudication — sweep showed 862 TRUE / 0 FALSE / 10009 NULL. Type-A presence flag: when patient has any RAI signal (862 = 583 received + 279 discordant), adjudication was always performed. FALSE bucket structurally impossible by design. Keep verified informational.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'rai_has_adjudication';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_142b: CF-mig142-COHORT-NEAR-UNIFORM-TRUE-rai_has_completion_status — sweep showed 862 TRUE / 0 FALSE / 10009 NULL. Type-A presence flag: when patient has any RAI signal (862), completion_status was always assigned by Script-22 episode workflow. FALSE bucket structurally impossible by design. Keep verified informational.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'rai_has_completion_status';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_142b: CF-mig142-COHORT-NEAR-UNIFORM-TRUE-rai_received_reconciled — sweep showed 862 TRUE / 0 FALSE / 10009 NULL. Type-A presence flag: reconciled = OR rule across rai_received_flag (583) and rai_flag_discordant (279) = 862 with-any-signal. FALSE bucket structurally impossible (those land as NULL). Keep verified informational.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'rai_received_reconciled';

-- (C) CF-VALUE-DEGENERATE-UPSTREAM appendix on 2 single-value VARCHARs
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_142b: CF-mig142-VALUE-DEGENERATE-UPSTREAM-rai_intent_list — 1 distinct value ("unknown") across all 862 non-null. Extraction-faithful from upstream rai_treatment_episode_v2.rai_intent which is 100% "unknown" (1857/1857) — derived col rai_intent_v9 has 5 distinct values (ablation, adjuvant, remnant_ablation, therapeutic, unknown) and is the analytic SSOT. rai_intent_list useful as raw-aggregate provenance only. Keep verified informational.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'rai_intent_list';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_142b: CF-mig142-VALUE-DEGENERATE-UPSTREAM-rai_assertion_statuses — 1 distinct value ("likely_received") across 35 non-null / 10836 NULL. Extraction-faithful upstream STRING_AGG of episode-level assertion_status — only "likely_received" surfaces because the column was rolled out post-episode-promotion and only newer episodes carry it. Real cohort distribution will broaden after Script-22+ assertion-status backfill. Keep verified informational.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'rai_assertion_statuses';

-- Resync canonical_patient_master signoff registry counts from live registry
UPDATE main.canonical_table_signoff_registry_v1
SET n_verified = (
      SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1
      WHERE table_name='canonical_patient_master' AND verification_status='verified'
    ),
    n_na = (
      SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1
      WHERE table_name='canonical_patient_master' AND verification_status='na'
    ),
    n_not_started = (
      SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1
      WHERE table_name='canonical_patient_master' AND verification_status='not_started'
    ),
    n_failed = (
      SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1
      WHERE table_name='canonical_patient_master' AND verification_status='failed'
    )
WHERE table_name = 'canonical_patient_master';

-- End of mig_142b. Already applied via query_rw 2026-04-29.
-- Pre-snapshots:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig142b_rai_cohort_uniformity_20260429
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_pre_mig142b_pm_20260429
