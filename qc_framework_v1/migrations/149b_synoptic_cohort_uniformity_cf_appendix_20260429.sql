-- Migration: 149b_synoptic_cohort_uniformity_cf_appendix_20260429.sql
-- Purpose: Cohort-uniformity sweep on canonical_patient_master mig_149 syn_* cluster (32 cols)
--          surfaced 12 Type-A presence-flag BOOLEANs the agent did not enumerate in its QA
--          (TRUE-only / 0 FALSE / NULL pattern — FALSE structurally impossible by design).
--          Same agent-QA miss pattern as mig_141 / mig_142.
--          Append CF-mig149-COHORT-NEAR-UNIFORM-TRUE informational note. Keep verified.
--
--          Sweep results (live MD, post-mig_149 apply):
--          col                              T    F    NULL
--          syn_adenomatoid_nodules         1172   0   9699
--          syn_chronic_thyroiditis         1096   0   9775
--          syn_colloid_nodule               416   0  10455
--          syn_follicular_adenoma           924   0   9947
--          syn_graves                       574   0  10297
--          syn_hashimoto                    248   0  10623   (also CF-mig149-SYN-HASHIMOTO-LOW-TEMPLATE-YIELD)
--          syn_hyperplastic_nodules         491   0  10380
--          syn_multinodular_goiter         6075   0   4796
--          syn_hurthle_cell_change          643   0  10228
--          syn_c_cell_hyperplasia            52   0  10819
--          syn_central_dissection           655   0  10216
--          syn_io_rln_monitoring            120   0  10751   (also CF-mig149-SYN-IO-RLN-VS-OPERATIVE-DISTINCT)
--
--          Cowork independent-verification spot-checks were perfectly clean before apply:
--            - syn_hashimoto BOOL_OR derivation: 248 PM TRUE = 248 expected TRUE (10,871/10,871 agree)
--            - syn_carcinoma_on_frozen Script-360 BOOL_OR + COALESCE FALSE: 581 = 581 (10,871 agree)
--            - syn_total_weight_g: 5/5 random rids match exactly (25.2, 25.6, 15.4, 88, 158)
--          Data types verified: 11 VARCHAR sizes are 3-D dimensions ("4.5 x 2.5 x 2.0"), correctly NOT retypable.
--
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 39b (mig_149 cleanup; registry-only)
-- Effect : 12 cols get CF appendix note; verification_status unchanged (still verified)

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_149b: CF-mig149-COHORT-NEAR-UNIFORM-TRUE — sweep showed presence-flag pattern ' ||
            '(TRUE-only / 0 FALSE / NULL). FALSE bucket structurally impossible (NULL = no signal in ' ||
            'patient synoptic). Keep verified informational. Same agent miss pattern as mig_141/142.'
WHERE table_name = 'canonical_patient_master'
  AND column_name IN (
    'syn_adenomatoid_nodules','syn_chronic_thyroiditis','syn_colloid_nodule','syn_follicular_adenoma',
    'syn_graves','syn_hashimoto','syn_hyperplastic_nodules','syn_multinodular_goiter',
    'syn_hurthle_cell_change','syn_c_cell_hyperplasia',
    'syn_central_dissection','syn_io_rln_monitoring'
  );

-- End of mig_149b. Already applied via query_rw 2026-04-29.
-- Pre-snapshot:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig149_20260429
