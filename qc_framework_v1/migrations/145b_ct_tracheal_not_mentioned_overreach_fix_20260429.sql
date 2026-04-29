-- Migration: 145b_ct_tracheal_not_mentioned_overreach_fix_20260429.sql
-- Purpose: Re-derive ct_tracheal_deviation_any and ct_tracheal_narrowing_any with correct
--          enum-filter logic. Original PM build was treating ct_imaging.tracheal_*='not_mentioned'
--          as TRUE -> 85% TRUE rate in CT subset (clinically implausible).
-- Trigger: Cowork verification of mig_145 confirmed agent's CF-mig145-CT-TRACHEAL-NOTMENTIONED-OVERREACH
--          via direct cohort-uniformity sweep:
--            ct_tracheal_deviation_any: pre 2,623 TRUE / 0 FALSE / 7,785 NULL  (85% TRUE in CT subset)
--            ct_tracheal_narrowing_any: pre 2,670 TRUE / 0 FALSE / 7,785 NULL  (86% TRUE in CT subset)
--          Underlying ct_imaging values: 'present' (32%), 'none' (24%), 'not_mentioned' (44%).
--          Old logic counted 'not_mentioned' as TRUE; new logic NULLs them out.
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 35b (mig_145 cleanup; PM data write)

-- ============================================================
-- STEP 1. Pre-snapshot
-- ============================================================
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig145b_ct_tracheal_20260429 AS
SELECT research_id,
       ct_tracheal_deviation_any,
       ct_tracheal_narrowing_any,
       ct_airway_compromise_any,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig145b_snapshot_ts
FROM main.canonical_patient_master;

-- ============================================================
-- STEP 2. Re-derive ct_tracheal_deviation_any
-- TRUE iff any episode has tracheal_deviation='present'
-- FALSE iff at least one 'none' AND no 'present'
-- NULL otherwise (all 'not_mentioned' / NULL / no-CT)
-- ============================================================
WITH dev_per_pt AS (
  SELECT CAST(research_id AS VARCHAR) AS rid,
         BOOL_OR(tracheal_deviation = 'present') AS any_present,
         BOOL_OR(tracheal_deviation = 'none') AS any_none
  FROM main.ct_imaging
  WHERE research_id IS NOT NULL
  GROUP BY 1
)
UPDATE main.canonical_patient_master pm
SET ct_tracheal_deviation_any = CASE
  WHEN d.any_present THEN TRUE
  WHEN d.any_none AND NOT COALESCE(d.any_present,FALSE) THEN FALSE
  ELSE NULL
END
FROM dev_per_pt d
WHERE CAST(pm.research_id AS VARCHAR) = d.rid;

-- ============================================================
-- STEP 3. Re-derive ct_tracheal_narrowing_any
-- TRUE for any clinically positive severity (mild/moderate/severe/present_unspecified/etc.)
-- FALSE for none/patent
-- NULL otherwise
-- ============================================================
WITH narr_per_pt AS (
  SELECT CAST(research_id AS VARCHAR) AS rid,
         BOOL_OR(tracheal_narrowing IN ('mild','moderate','severe','present_unspecified','less than 50%','mild_moderate')) AS any_positive,
         BOOL_OR(tracheal_narrowing IN ('none','patent')) AS any_negative
  FROM main.ct_imaging
  WHERE research_id IS NOT NULL
  GROUP BY 1
)
UPDATE main.canonical_patient_master pm
SET ct_tracheal_narrowing_any = CASE
  WHEN n.any_positive THEN TRUE
  WHEN n.any_negative AND NOT COALESCE(n.any_positive,FALSE) THEN FALSE
  ELSE NULL
END
FROM narr_per_pt n
WHERE CAST(pm.research_id AS VARCHAR) = n.rid;

-- ============================================================
-- STEP 4. Post-verify (expected new distributions)
-- ct_tracheal_deviation_any: 1,398 TRUE / 931 FALSE / 8,542 NULL  (45/30/25% in CT subset)
-- ct_tracheal_narrowing_any: 1,262 TRUE / 880 FALSE / 8,729 NULL  (41/29/30% in CT subset)
-- ============================================================
SELECT
  SUM(CASE WHEN ct_tracheal_deviation_any THEN 1 ELSE 0 END) AS dev_TRUE,
  SUM(CASE WHEN ct_tracheal_deviation_any=FALSE THEN 1 ELSE 0 END) AS dev_FALSE,
  SUM(CASE WHEN ct_tracheal_deviation_any IS NULL THEN 1 ELSE 0 END) AS dev_NULL,
  SUM(CASE WHEN ct_tracheal_narrowing_any THEN 1 ELSE 0 END) AS narr_TRUE,
  SUM(CASE WHEN ct_tracheal_narrowing_any=FALSE THEN 1 ELSE 0 END) AS narr_FALSE,
  SUM(CASE WHEN ct_tracheal_narrowing_any IS NULL THEN 1 ELSE 0 END) AS narr_NULL
FROM main.canonical_patient_master;

-- ============================================================
-- STEP 5. Append CF closure on the 2 retyped cols
-- ============================================================
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_145b (2026-04-29): re-derived after CF-mig145-CT-TRACHEAL-NOTMENTIONED-OVERREACH ' ||
            'discovered the original PM build counted ct_imaging.tracheal_deviation=''not_mentioned'' as TRUE ' ||
            '(85% TRUE rate in CT subset). New logic: TRUE iff any episode=''present''; FALSE iff any=''none'' ' ||
            'and no ''present''; NULL iff all ''not_mentioned''/NULL/ambiguous. CF-mig145-CT-TRACHEAL-NOTMENTIONED-OVERREACH CLOSED.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'derivation_vs_canonical_ct_imaging_v1_corrected_enum_filter'
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('ct_tracheal_deviation_any', 'ct_tracheal_narrowing_any');

-- End of mig_145b. Already applied via query_rw 2026-04-29.
-- Note: ct_airway_compromise_any retains its CF-mig145-CT-AIRWAY-COMMENT-PROXY tag (long-string proxy)
--       since rewriting the "compromise" definition needs Logan SSOT input. Not blocking.
