-- Migration: 148b_rai_treatment_episode_v2_placeholder_reclassify_20260429.sql
-- Purpose: Reclassify 5 placeholder cols on rai_treatment_episode_v2 from `verified` -> `na`.
--          Cohort-uniformity sweep confirmed:
--            - iodine_avidity_flag, post_therapy_scan_flag, pre_scan_flag : 0 TRUE / 1857 FALSE
--              (script 22 default placeholders; agent CF-mig148-RAI-SCAN-FLAGS-SCRIPT22-DEFAULT)
--            - stimulated_tg, stimulated_tsh : 0 nonnull / 1857 NULL
--              (lab linkage backlog; agent CF-mig148-STIM-LAB-LINKAGE)
--          Reclassifying because cohort-uniform-FALSE / all-NULL provides zero analytic signal.
--          Will re-flip to verified when real extraction / lab linkage is wired.
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 38b (mig_148 cleanup; registry-only)
-- Effect : table_status stays 'verified', n_verified 25 -> 20, n_na 7 -> 12, total 32

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verification_method = 'helper_script_22_default_placeholder_pending_real_extraction',
    notes = COALESCE(notes,'') ||
            ' | mig_148b (2026-04-29): cohort-uniformity sweep confirmed all-FALSE (1,857/1,857) ' ||
            'across all 3 BOOLEANs; agent CF-mig148-RAI-SCAN-FLAGS-SCRIPT22-DEFAULT marked these as ' ||
            'script-22-default placeholders. Reclassifying verified -> na since helper-script-invariant ' ||
            'cols carry zero analytic signal. Will re-flip to verified when real iodine_avidity / scan_flag ' ||
            'extraction is wired into the build pipeline.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'rai_treatment_episode_v2'
  AND column_name IN ('iodine_avidity_flag', 'post_therapy_scan_flag', 'pre_scan_flag');

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verification_method = 'lab_linkage_pending_canonical_labs_join',
    notes = COALESCE(notes,'') ||
            ' | mig_148b (2026-04-29): all 1,857 episodes have stim lab IS NULL; ' ||
            'agent CF-mig148-STIM-LAB-LINKAGE marked as linkage-backlog. Reclassifying ' ||
            'verified -> na since the linkage layer has not been built. Will re-flip when ' ||
            'canonical_labs_tg_v1 / canonical_labs_tsh_v1 join into rai episodes is implemented.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'rai_treatment_episode_v2'
  AND column_name IN ('stimulated_tg', 'stimulated_tsh');

UPDATE main.canonical_table_signoff_registry_v1
SET n_verified = 20, n_na = 12,
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'rai_treatment_episode_v2';

-- End of mig_148b. Already applied via query_rw 2026-04-29.
