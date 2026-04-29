-- =============================================================================
-- Migration 161b — mig_155 ATA-INITIAL-DUP carry-forward (Cowork addendum)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Cowork (auto-applied via Path C; gap-fill for mig_161 §B)
--
-- Lane:    mig_161b — registry-note appendix only
-- batch_id:                                                  -- inherits mig_155 batch lineage via WHERE clause; no new batch id needed
-- Pre-flight:
--   * Cowork live 2026-04-29: ata_initial_risk IS NOT DISTINCT FROM ata_risk_category
--     on **10,871 / 10,871** rows (n_identical=10871, n_different=0).
--   * Both cols nonnull on 3,144 / 10,871 (eligible cohort).
--   * Both currently `verified` under
--     batch_id `mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429`.
--
-- Why this exists (gap from mig_161 §B): mig_161's read-only audit identified the
-- 100% duplication in §2h but no per-col CF appendix landed in mig_161 §B (B-blocks
-- B0–B5 cover other findings). This file closes that gap.
--
-- EFFECT: 1 UPDATE on `canonical_column_verification_registry_v1` touching 2 rows
-- (`ata_initial_risk` + `ata_risk_category`). No verification_status mutation.
-- No PM data writes. No other tables touched.
--
-- Rollback: not needed (notes-only). To restore prior notes, query
--   archive_pub_v1_0.canonical_column_verification_registry_pre_mig161_20260429
--   and re-write the notes column for those two rows.
--
-- Apply order: Step 2 of apply queue (between mig_161 and mig_159).
-- =============================================================================

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_161b: CF-mig161-MIG155-ATA-INITIAL-RISK-DUP — '
            || 'ata_initial_risk IS NOT DISTINCT FROM ata_risk_category on 10,871/10,871 rows '
            || '(Cowork live verified 2026-04-29). ata_initial_risk is a redundant alias of '
            || 'ata_risk_category in the mig_155 build. Manuscript pipeline should pick one '
            || 'canonical name (recommend ata_risk_category since "category" is more descriptive); '
            || 'consider deprecating ata_initial_risk in a future build, but not blocking. '
            || 'Keep both verified informational.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND column_name IN ('ata_initial_risk', 'ata_risk_category');

-- End mig_161b. 1 query_rw call. 2 rows touched. Notes-only.
