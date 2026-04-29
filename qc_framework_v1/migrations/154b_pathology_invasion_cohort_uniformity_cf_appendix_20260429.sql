-- Migration: 154b_pathology_invasion_cohort_uniformity_cf_appendix_20260429.sql
-- Purpose: Cohort-uniformity sweep on canonical_patient_master mig_154 pathology-invasion cluster (38 cols)
--          surfaced 2 additional Type-A presence-flag BOOLEANs the agent did not enumerate as such
--          (TRUE-only / explicit-FALSE-very-rare / NULL pattern). Same pattern agent caught for
--          pni_positive (1487/0/9384) but missed here. Append CF-mig154-COHORT-NEAR-UNIFORM-TRUE
--          informational note. Keep verified.
--
--          Sweep results (live MD post-mig_154 apply):
--          col                       T      F     NULL    Verdict
--          lvi_any_present_path      3392    57   7422    Near-Type-A (1.7% explicit FALSE)
--          vi_any_present_path       3698    55   7118    Near-Type-A (1.5% explicit FALSE)
--          pni_positive (already CF) 1487    0    9384    Type-A (agent caught)
--
--          Cowork independent-verification spot-checks were perfectly clean before apply:
--            - Cohort sweep matches agent claims exactly across all 7 BOOLEANs
--            - SSOT canonical_invasion_events_v1, canonical_path_malignant_*, canonical_molecular_genetics_v2
--              all confirmed live in main schema
--            - closest_margin_mm* DOUBLE confirmed (no VARCHAR-with-units retype needed)
--
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 43b (mig_154 cleanup; registry-only, notes only)
-- Effect : 2 cols get CF appendix note; verification_status unchanged (still verified)

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_154b: CF-mig154-COHORT-NEAR-UNIFORM-TRUE-lvi_any_present_path — Cowork sweep showed ' ||
            '3392 T / 57 F / 7422 N (1.7% explicit FALSE in non-null subset). Same Type-A presence-flag ' ||
            'pattern as pni_positive: FALSE primarily structural-absent (path event grain records ' ||
            'present-only; absence projects as NULL). Keep verified informational. Same agent miss ' ||
            'pattern as mig_141/142.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'lvi_any_present_path';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_154b: CF-mig154-COHORT-NEAR-UNIFORM-TRUE-vi_any_present_path — Cowork sweep showed ' ||
            '3698 T / 55 F / 7118 N (1.5% explicit FALSE in non-null subset). Same Type-A presence-flag ' ||
            'pattern. Keep verified informational.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'vi_any_present_path';

-- End of mig_154b. Already applied via query_rw 2026-04-29.
-- Pre-snapshot:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig154_20260429
