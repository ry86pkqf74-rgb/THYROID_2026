-- mig_264b — Bethesda-2 obvious-fix cohort (AUTHORITATIVE RUNNER IS PYTHON).
-- Applies DML via scripts/mig_264b_bethesda2_obvious_fix.py --apply
-- (MotherDuck: connect_locked → thyroid_canonical_publication_v1_0.main)
--
-- Sub-cohorts:
--   1) Bethesda-2 ∩ malignant ∩ histology_final = 'NIFTP' → is_malignant=FALSE
--   2) Bethesda-2 ∩ malignant ∩ histology ILIKE '%follicular adenoma%' → is_malignant=FALSE
--   3) Bethesda-2 ∩ malignant ∩ MIN(days_fna_to_surgery) < 0 → bethesda_final / bethesda_final_name
--      from latest preop FNA (fna_date_resolved < first_surgery_date ORDER BY date DESC LIMIT 1)
--
-- Archive: "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig264b_20260502
--
-- Post-verify (expect): NIFTP residual 0; FA residual 0; B2∩malig count ≈ 342 (385−43 nominal if no overlap).

SELECT 1 WHERE FALSE;
