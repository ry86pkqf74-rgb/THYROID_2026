-- Migration: 158_rid68_ln_integrity_fix_20260429.sql
-- Purpose: Single-row fix for canonical_patient_master rid 68 LN-arithmetic violation.
--          Pre-state: ln_total_examined=0 AND ln_total_positive=1 (impossible — examined ≥ positive).
--          rid 68 is_malignant=FALSE (benign-cohort patient); the positive=1 likely reflects an
--          FNA-cytology-positive node without a neck dissection (so no surgical examined count).
--          Setting examined=NULL preserves the positive signal while removing the arithmetic
--          violation. Carry-forward originally tracked as CF-mig133-PM-LN-COUNT-INTEGRITY in the
--          handoff doc §8.1.
--
--          Pre-state probe (live MD 2026-04-29):
--            - rid='68' is the ONLY patient with ln_total_examined=0 AND ln_total_positive>0
--            - ln_total_positive=1 patients with ln_total_examined IS NULL: 6 (siblings — leave alone)
--          Post-fix state: rid 68 has ln_total_examined=NULL, ln_total_positive=1 (matches sibling pattern)
--
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 47 (single-row CPM data write)
-- Effect : 1 row updated; Cowork PM signoff registry counts unchanged

UPDATE main.canonical_patient_master
SET ln_total_examined = NULL
WHERE research_id = '68'
  AND ln_total_examined = 0
  AND ln_total_positive = 1;

-- End of mig_158. Already applied via query_rw 2026-04-29.
-- Pre-snapshot:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig158_rid68_ln_20260429
