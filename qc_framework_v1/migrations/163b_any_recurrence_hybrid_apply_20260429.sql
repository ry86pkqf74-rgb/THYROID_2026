-- mig_163b ANY-RECURRENCE HYBRID apply (Logan-ratified 2026-04-29)
-- Batch_id: mig_163b_any_recurrence_hybrid_apply_20260429
-- Database: thyroid_canonical_publication_v1_0
-- Type: Path C data write to canonical_patient_master.any_recurrence_flag
-- Source prompt: cursor_prompts/CURSOR_PROMPT_mig163b_any_recurrence_strict_apply_20260429.md
--
-- Logan-ratified definition:
--   HYBRID = canonical_recurrence_v1.recurrence_confirmed=TRUE
--         OR canonical_recurrence_resolved_v1.recurrence_status_final='path_proven'
--
-- Cowork live 2026-04-29 preflight expectations:
--   HYBRID cardinality: strict_n=514, path_proven_n=145, hybrid_union_n=514,
--   path_proven_added_by_hybrid=0 (path_proven is currently a subset of recurrence_confirmed).
--   PM vs HYBRID 2x2 before apply: both=165, pm_only_dropped=219,
--   hybrid_only_added=349, neither=10138.
--   PM row parity: 10871 rows / 10871 distinct research_id.
--   Pre-state any_recurrence_flag distribution: 384 TRUE / 10487 FALSE / 0 NULL.
--   Post-state should be: 514 TRUE / 10357 FALSE / 0 NULL and 0 mismatch vs HYBRID.
--
-- Closes:
--   CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT
-- Adds informational carry-forwards:
--   CF-mig163b-HYBRID-UNION-EQUALS-STRICT-TODAY
--   CF-mig163b-WIDE-DEFINITION-DEFERRED
--
-- Required pre-flight probes (run before Section A if applying interactively):
-- §2a HYBRID cardinality reconcile — UNION of recurrence_confirmed + path_proven
-- WITH cr_conf AS (
--   SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
--   FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed = TRUE
-- ),
-- crr_path AS (
--   SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
--   FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final = 'path_proven'
-- ),
-- hybrid AS (
--   SELECT rid FROM cr_conf UNION SELECT rid FROM crr_path
-- )
-- SELECT
--   (SELECT COUNT(*) FROM cr_conf)   AS strict_n,
--   (SELECT COUNT(*) FROM crr_path)  AS path_proven_n,
--   (SELECT COUNT(*) FROM hybrid)    AS hybrid_union_n,
--   (SELECT COUNT(*) FROM crr_path WHERE rid NOT IN (SELECT rid FROM cr_conf)) AS path_proven_added_by_hybrid;
--
-- §2b 2x2 reconcile vs current PM any_recurrence_flag (vs HYBRID)
-- WITH hybrid AS (
--   SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE
--   UNION
--   SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='path_proven'
-- ),
-- pm AS (SELECT CAST(research_id AS VARCHAR) AS rid, any_recurrence_flag FROM main.canonical_patient_master)
-- SELECT
--   SUM(CASE WHEN pm.any_recurrence_flag AND hybrid.rid IS NOT NULL THEN 1 ELSE 0 END) AS both,
--   SUM(CASE WHEN pm.any_recurrence_flag AND hybrid.rid IS NULL THEN 1 ELSE 0 END) AS pm_only_dropped,
--   SUM(CASE WHEN NOT pm.any_recurrence_flag AND hybrid.rid IS NOT NULL THEN 1 ELSE 0 END) AS hybrid_only_added,
--   SUM(CASE WHEN NOT pm.any_recurrence_flag AND hybrid.rid IS NULL THEN 1 ELSE 0 END) AS neither
-- FROM pm LEFT JOIN hybrid USING (rid);
--
-- §2c Cohort parity invariant
-- SELECT COUNT(*) AS pm_rows, COUNT(DISTINCT research_id) AS pm_distinct_rids FROM main.canonical_patient_master;
--
-- §2d Pre-state distribution of any_recurrence_flag
-- SELECT SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS pre_t,
--        SUM(CASE WHEN NOT any_recurrence_flag THEN 1 ELSE 0 END) AS pre_f,
--        SUM(CASE WHEN any_recurrence_flag IS NULL THEN 1 ELSE 0 END) AS pre_n
-- FROM main.canonical_patient_master;

USE thyroid_canonical_publication_v1_0;

-- Section A — pre-snapshots

-- A1: Snapshot the column slice from PM before mutation.
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_any_recurrence_flag_pre_mig163b_20260429 AS
SELECT
  research_id,
  any_recurrence_flag,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig163b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- A2: Snapshot the registry row for any_recurrence_flag before mutation.
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_any_recurrence_flag_pre_mig163b_20260429 AS
SELECT
  *,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig163b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'any_recurrence_flag';

-- Section B — HYBRID redefinition (single transaction)

BEGIN TRANSACTION;

-- HYBRID = (canonical_recurrence_v1.recurrence_confirmed=TRUE)
--       OR (canonical_recurrence_resolved_v1.recurrence_status_final='path_proven')
UPDATE main.canonical_patient_master AS pm
SET any_recurrence_flag = (
  CAST(pm.research_id AS VARCHAR) IN (
    SELECT DISTINCT CAST(research_id AS VARCHAR)
    FROM main.canonical_recurrence_v1
    WHERE recurrence_confirmed = TRUE
    UNION
    SELECT DISTINCT CAST(research_id AS VARCHAR)
    FROM main.canonical_recurrence_resolved_v1
    WHERE recurrence_status_final = 'path_proven'
  )
);

-- B1: Update the column registry note with the redefinition lineage.
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
            || ' | mig_163b: any_recurrence_flag REDEFINED (HYBRID) = '
            || 'canonical_recurrence_v1.recurrence_confirmed=TRUE'
            || ' UNION canonical_recurrence_resolved_v1.recurrence_status_final=''path_proven''. '
            || 'Pre-snapshot canonical_patient_master_any_recurrence_flag_pre_mig163b_20260429. '
            || 'Today path_proven subset of recurrence_confirmed (UNION=514); definition is HYBRID for resilience '
            || 'against future recurrence_v1 vs recurrence_resolved_v1 drift. '
            || 'Drops 219 PM-only patients; adds 349 canon-only patients (Logan-ratified manuscript definition 2026-04-29). '
            || 'Closes CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT. '
            || 'CF-mig163b-HYBRID-UNION-EQUALS-STRICT-TODAY: informational; HYBRID equals STRICT in current data. '
            || 'CF-mig163b-WIDE-DEFINITION-DEFERRED: WIDE proxy-inclusive definition rejected for primary endpoint.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = 'any_recurrence_flag';

COMMIT;

-- Section C — post-state verification (run after apply)

-- C1: Confirm new TRUE count matches HYBRID UNION cardinality (514 today).
-- SELECT SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS post_t,
--        SUM(CASE WHEN NOT any_recurrence_flag THEN 1 ELSE 0 END) AS post_f,
--        SUM(CASE WHEN any_recurrence_flag IS NULL THEN 1 ELSE 0 END) AS post_n
-- FROM main.canonical_patient_master;
-- Expected today: 514 / 10357 / 0

-- C2: Confirm 0 row mismatch vs HYBRID union.
-- WITH pm AS (
--        SELECT CAST(research_id AS VARCHAR) AS rid, any_recurrence_flag
--        FROM main.canonical_patient_master
--      ),
--      hybrid AS (
--        SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
--        FROM main.canonical_recurrence_v1
--        WHERE recurrence_confirmed = TRUE
--        UNION
--        SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
--        FROM main.canonical_recurrence_resolved_v1
--        WHERE recurrence_status_final = 'path_proven'
--      )
-- SELECT
--   SUM(CASE WHEN pm.any_recurrence_flag AND hybrid.rid IS NULL THEN 1 ELSE 0 END) AS pm_t_hybrid_f,
--   SUM(CASE WHEN NOT pm.any_recurrence_flag AND hybrid.rid IS NOT NULL THEN 1 ELSE 0 END) AS pm_f_hybrid_t
-- FROM pm LEFT JOIN hybrid USING (rid);
-- Expected today: 0 / 0

-- C3: Confirm CPM invariant remains intact.
-- SELECT COUNT(*) AS pm_rows, COUNT(DISTINCT research_id) AS pm_distinct_rids
-- FROM main.canonical_patient_master;
-- Expected: 10871 / 10871