-- Migration: 150b_para_postop_tp_cleanup_appendix_20260429.sql
-- Purpose: Cleanup notes on canonical_patient_master mig_150 cluster (37 cols) post-Cowork
--          independent verification (Path C):
--
--          (A) Type-A presence-flag CF on 1 BOOLEAN missed in agent QA:
--              postop_labs_has_data — sweep: 1051 TRUE / 0 FALSE / 9820 NULL
--              FALSE structurally impossible (NULL = no postop labs); keep verified informational.
--
--          (B) Multi-source PTH derivation note on 7 PTH cols (verification_method partial-truth):
--              Cowork spot-check 10 random rids: pth_nadir = postop_pth_min_value (PM internal consistent).
--              canonical_labs_pth_v1.value_numeric matches PM where lab-feed data exists (e.g. 38=38, 21=21)
--              but 8/10 rids have non-null pth_nadir with NULL row in canonical_labs_pth_v1.
--              Multi-source resolution layer: structured lab feed (canonical_labs_pth_v1, ~20% pts) +
--              NLP-extracted PTH from notes (~80% pts, source not in main schema — likely deprecated
--              post-script-347 ingestion). Values are correct where checked; methodology label
--              "derivation_vs_canonical_labs_pth_v1" is partial-truth. Re-derive lineage when notes-PTH
--              source is restored or canonicalized.
--
--          (C) tumor_pathology not-in-main note on 9 tp_* cols:
--              verification_method "tumor_pathology_primary_ln_205_consolidation" references a
--              non-live SSOT (no `tumor_pathology` table in main; only canonical_us_lymph_node_v2 for LN).
--              The 205 consolidation script is the authoritative provenance label, and the methodology
--              string is script-based (acceptable per Cowork rules). Values are intact; live LN
--              canonical (tier-2 lymph_node) for tp_* re-derivation pending downstream build.
--
--          Cowork independent verification spot-checks pre-apply:
--            - postop_low_pth_flag vs hypopara complication: 97 PM biochem TRUE / 406 clinical TRUE.
--              Overlap=10, biochem-only=87, clinical-only=24. Agent CF-mig150-PARA-BIOCHEM-VS-COMPL-TIER
--              correctly documents these as different tiers (biochem <15 pg/mL lab vs clinical Dx).
--            - 5 BOOLEAN sweep: 4/5 healthy real distributions; 1 Type-A (postop_labs_has_data).
--            - Script-53 thresholds (PTH<15, Ca<8.0) confirmed in CPM build per agent header.
--
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 40b (mig_150 cleanup; registry-only)
-- Effect : 17 distinct cols get CF appendix notes; verification_status unchanged

-- (A) Type-A presence flag
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_150b: CF-mig150-COHORT-NEAR-UNIFORM-TRUE-postop_labs_has_data — sweep showed ' ||
            '1051 TRUE / 0 FALSE / 9820 NULL. Type-A presence flag: when patient has any postop labs ' ||
            '(1051 = postop_pth_n + postop_calcium_n > 0), this flag = TRUE; else NULL. ' ||
            'FALSE structurally impossible. Keep verified informational.'
WHERE table_name = 'canonical_patient_master' AND column_name = 'postop_labs_has_data';

-- (B) Multi-source PTH derivation note
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_150b: CF-mig150-PTH-MULTI-SOURCE-DERIVATION — Cowork spot-check 10 random rids ' ||
            'found canonical_labs_pth_v1.value_numeric MATCHES PM where lab-feed data exists ' ||
            '(e.g. 38=38, 21=21) BUT 8/10 rids have non-null pth_nadir with NULL canonical_labs_pth_v1 ' ||
            'row. Multi-source resolution layer: canonical_labs_pth_v1 (structured lab feed, ~20% pts) + ' ||
            'NLP-extracted PTH from notes (~80% pts, source not in main schema — likely deprecated ' ||
            'post-script-347 ingestion). Values are correct where checked; methodology label is ' ||
            'partial-truth. Re-derive lineage when notes-PTH source is restored or canonicalized.'
WHERE table_name = 'canonical_patient_master'
  AND column_name IN (
    'pth_nadir','pth_nadir_30d','pth_nadir_days_postop',
    'postop_pth_min_value','postop_pth_n_measurements','postop_pth_min_days_postop','postop_pth_source_reliability'
  );

-- (C) tumor_pathology not-in-main note
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_150b: CF-mig150-TP-UPSTREAM-NOT-IN-MAIN — verification_method names ' ||
            '"tumor_pathology_primary_ln_205_consolidation" but no `tumor_pathology` table lives in ' ||
            'main schema (only canonical_us_lymph_node_v2 for LN). The 205 consolidation script is the ' ||
            'authoritative provenance; live LN canonicals (tier-2 lymph_node) for tp_* re-derivation ' ||
            'pending. Values are intact; lineage check pending downstream LN canonical build.'
WHERE table_name = 'canonical_patient_master'
  AND column_name IN (
    'tp_central_examined','tp_central_positive_total','tp_ln_central_positive','tp_ln_ene',
    'tp_ln_examined','tp_ln_largest_deposit_cm','tp_ln_lateral_positive','tp_ln_levels_involved','tp_ln_positive'
  );

-- End of mig_150b. Already applied via query_rw 2026-04-29.
-- Pre-snapshots:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig150_20260429
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_pre_mig150_pm_20260429
