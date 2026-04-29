-- Migration: 153_cr_vs_crr_path_proven_reconcile_20260429.sql
-- Purpose: Reconcile 46-rid drift between two verified upstream canonicals:
--            canonical_recurrence_v1.recurrence_confirmed = FALSE   (mig_123 SSOT)
--            canonical_recurrence_resolved_v1.recurrence_status_final = 'path_proven'  (mig_125 SSOT)
--          Cowork verification of mig_144 surfaced the gap. Per-rid investigation showed:
--            24 / 46 with recurrence_path_proven_source='structural_confirmed' (no biopsy evidence)
--            22 / 46 with recurrence_path_proven_source='llm_path_keyword' but evidence text shows
--              imaging-only signals (unsuccessful biopsies, LNs not amenable to biopsy, scan uptake).
--          Conclusion: CRR over-classified imaging-only / structural-only signals as path_proven.
--          Fix direction (Option B from mig_153 prompt): demote CRR -> imaging_only_unconfirmed.
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 33c (cross-SSOT reconcile)
-- Effect : 46 CRR rows demoted; CR.recurrence_confirmed unchanged (514 TRUE);
--          PM imaging_suspicious_* cols unchanged (79=79 paired — independent of CRR enum).

-- ============================================================
-- STEP 1. Pre-snapshot CRR for the 46 affected rids
-- ============================================================
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_resolved_v1_pre_mig153_crr_demote_20260429 AS
SELECT * FROM main.canonical_recurrence_resolved_v1
WHERE CAST(research_id AS VARCHAR) IN (
  SELECT CAST(cr.research_id AS VARCHAR)
  FROM main.canonical_recurrence_v1 cr
  JOIN main.canonical_recurrence_resolved_v1 crr ON CAST(cr.research_id AS VARCHAR)=CAST(crr.research_id AS VARCHAR)
  WHERE COALESCE(cr.recurrence_confirmed,FALSE)=FALSE AND crr.recurrence_status_final='path_proven'
);

-- ============================================================
-- STEP 2. Demote 46 CRR rows from path_proven -> imaging_only_unconfirmed
--          Preserve recurrence_path_proven_evidence/_source for traceability.
-- ============================================================
UPDATE main.canonical_recurrence_resolved_v1 AS crr
SET recurrence_status_final = 'imaging_only_unconfirmed',
    recurrence_path_proven = FALSE
WHERE CAST(crr.research_id AS VARCHAR) IN (
  SELECT CAST(cr.research_id AS VARCHAR)
  FROM main.canonical_recurrence_v1 cr
  WHERE COALESCE(cr.recurrence_confirmed,FALSE)=FALSE
)
AND crr.recurrence_status_final='path_proven';

-- ============================================================
-- STEP 3. Post-verify drift = 0
-- ============================================================
SELECT COUNT(*) AS still_drifting
FROM main.canonical_recurrence_v1 cr
JOIN main.canonical_recurrence_resolved_v1 crr ON CAST(cr.research_id AS VARCHAR)=CAST(crr.research_id AS VARCHAR)
WHERE COALESCE(cr.recurrence_confirmed,FALSE)=FALSE AND crr.recurrence_status_final='path_proven';
-- Expect: 0

-- Pre-fix CRR: 9979 none / 701 imaging_only_unconfirmed / 191 path_proven
-- Post-fix CRR: 9979 none / 747 imaging_only_unconfirmed (+46) / 145 path_proven (-46)

-- ============================================================
-- STEP 4. Append CF closure on the 5 affected CRR cols
-- ============================================================
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_153 (2026-04-29): cr-vs-crr path_proven reconcile via Option B (demote). ' ||
            '46 rids drifted: cr.recurrence_confirmed=FALSE but crr.recurrence_status_final=path_proven. ' ||
            'Source breakdown: 24 structural_confirmed (no biopsy evidence) + 22 llm_path_keyword ' ||
            '(sample showed unsuccessful biopsies + imaging-only LNs not amenable to biopsy). ' ||
            'Demoted to imaging_only_unconfirmed, recurrence_path_proven=FALSE. Evidence/source ' ||
            'columns preserved for traceability. Post-fix CRR distribution: 9979 none / 747 ' ||
            'imaging_only_unconfirmed (was 701, +46) / 145 path_proven (was 191, -46). ' ||
            'CR.recurrence_confirmed unchanged at 514 TRUE. Cross-SSOT drift = 0.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'canonical_recurrence_resolved_v1'
  AND column_name IN (
    'recurrence_status_final',
    'recurrence_path_proven',
    'recurrence_path_proven_date',
    'recurrence_path_proven_evidence',
    'recurrence_path_proven_source'
  );

-- End of mig_153. Already applied via query_rw 2026-04-29.
-- Future work: 22 llm_path_keyword rids may have legitimate path-proven content not captured by
-- mig_123 CR rebuild. If manuscript prep raises priority, re-investigate per-rid evidence and
-- consider promoting some back to CR.recurrence_confirmed=TRUE in a focused follow-up lane.
