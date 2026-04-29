-- Migration: 141b_survival_cluster_cohort_uniformity_cf_appendix_20260429.sql
-- Purpose: Append CF tags to 2 mig_141 BOOLEAN cols that escaped the agent's cohort-uniformity
--          sweep with near-uniform-TRUE values (would mislead downstream analysts).
-- Trigger: Cowork verification of mig_141 found:
--          - survival_eligible_flag       = 10,870 / 10,871 TRUE (99.99%)
--          - prm_followup_has_complications = 10,862 / 10,871 TRUE (99.9%)
--          Both BOOLEANs essentially constant across the cohort. Same shape problem as
--          the mig_135 21 degenerate-FALSE cols, just inverted (cohort-uniform-TRUE).
--          Verification stays valid (definitions are correct). The CF tags are for
--          analyst-facing clarity: these cols should not be used for cohort-stratification
--          because they provide ~zero discriminative signal.
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Date  : 2026-04-29
-- Lane  : 30b (mig_141 cleanup; registry-only, no PM data writes)

-- ============================================================
-- Append CF-mig141-COHORT-NEAR-UNIFORM-TRUE-survival_eligible_flag
-- ============================================================
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_141b (2026-04-29): cohort-uniformity post-flip sweep flagged near-uniform-TRUE; ' ||
            'CF-mig141-COHORT-NEAR-UNIFORM-TRUE-survival_eligible_flag (10,870/10,871 = 99.99% TRUE; ' ||
            '1 ineligible patient = rid 5012). Type-A near-cohort-invariant — analytically non-discriminative; ' ||
            'BOOLEAN provides ~zero signal for cohort-stratification. Verification stays valid (definition correct), ' ||
            'but downstream analysts should NOT rely on this col for filtering survival-analytic subsets.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'canonical_patient_master'
  AND column_name = 'survival_eligible_flag';

-- ============================================================
-- Append CF-mig141-COHORT-NEAR-UNIFORM-TRUE-prm_followup_has_complications
-- ============================================================
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_141b (2026-04-29): cohort-uniformity post-flip sweep flagged near-uniform-TRUE; ' ||
            'CF-mig141-COHORT-NEAR-UNIFORM-TRUE-prm_followup_has_complications (10,862/10,871 = 99.9% TRUE). ' ||
            'Probable definition: "any followup contact occurred where complications were assessed", ' ||
            'NOT "patient has any complication" (canonical_complications_events_v1 has 2,481 distinct rids; ' ||
            '455 with finding_status=present). Type-A near-cohort-invariant; analytically non-discriminative. ' ||
            'For "patient HAS a complication", use canonical_complications_events_v1 finding_status=present, ' ||
            'not this PM col.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'canonical_patient_master'
  AND column_name = 'prm_followup_has_complications';

-- End of mig_141b. Already applied via query_rw at 2026-04-29; this file is for repo traceability.
