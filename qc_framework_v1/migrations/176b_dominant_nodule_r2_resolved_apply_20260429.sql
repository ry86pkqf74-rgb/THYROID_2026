-- mig_176b — dominant_nodule R2 resolved apply (Cowork-direct)
-- Closes CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT (1,065 both-non-null differ + 166 v2-only).
-- R2 ratified per exports/mig176_177_174_review_20260429/README.md after raw-US-Reports review of 19 extreme outliers
-- showed v2 has OCR/extraction inflation bug; COALESCE(v1, v2) correctly picks v1 for those.
--
-- Posture: Cowork applies. Schema additions (2 new DOUBLE/VARCHAR cols) + UPDATE.
-- Pre-snapshot affected slice. Cohort parity 10,871 invariant.
-- Target DB: thyroid_canonical_publication_v1_0
--
-- Live MD distribution (Cowork-probed 2026-04-29):
--   both v1+v2 non-null: 3,439 (of which 1,065 values differ)
--   v1-only:                 0
--   v2-only:               166
--   both null:           7,266
--   total:              10,871 ✓
--
-- Resolution rule:
--   v1_path_first_winner — 3,439 patients (v1 non-null; preferred over v2 due to v2 OCR bug)
--   v2_us_only           —   166 patients (v1 null but v2 non-null)
--   NULL                 — 7,266 patients

USE thyroid_canonical_publication_v1_0;

-- §A pre-snapshot of affected slice
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_dominant_nodule_pre_mig176b_20260429 AS
SELECT
    research_id,
    dominant_nodule_size_cm,
    dominant_nodule_size_cm_v2,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig176b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- §B add 2 new cols
ALTER TABLE main.canonical_patient_master
ADD COLUMN dominant_nodule_size_cm_resolved DOUBLE;

ALTER TABLE main.canonical_patient_master
ADD COLUMN dominant_nodule_resolution_rule VARCHAR;

-- §C populate resolved value via COALESCE(v1, v2)
UPDATE main.canonical_patient_master
SET dominant_nodule_size_cm_resolved = COALESCE(dominant_nodule_size_cm, dominant_nodule_size_cm_v2);

-- §D populate audit col
UPDATE main.canonical_patient_master
SET dominant_nodule_resolution_rule = CASE
    WHEN dominant_nodule_size_cm IS NOT NULL THEN 'v1_path_first_winner'
    WHEN dominant_nodule_size_cm_v2 IS NOT NULL THEN 'v2_us_only'
    ELSE NULL
  END;

-- §E register the 2 new cols in registry
INSERT INTO main.canonical_column_verification_registry_v1 BY NAME
SELECT
    'main' AS schema_name,
    'canonical_patient_master' AS table_name,
    'dominant_nodule_size_cm_resolved' AS column_name,
    'verified' AS verification_status,
    'Cowork' AS verified_by,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS verified_ts,
    'derivation_coalesce_v1_v2_with_audit_rule' AS verification_method,
    'mig_176b_dominant_nodule_r2_resolved_apply_20260429' AS batch_id,
    'mig_176b: COALESCE(dominant_nodule_size_cm, dominant_nodule_size_cm_v2) resolved value. R2 ratified after raw-US-Reports review showed v2 has OCR-inflation bug for 19 extreme outliers (>10cm). v1_path_first_winner=3439 / v2_us_only=166 / NULL=7266. Closes CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT (1065 both-non-null differ + 166 v2-only). Open: CF-mig176b-V2-UPSTREAM-EXTRACTION-INFLATION-19PTS informational for future canonical_us_nodule_v2 rebuild.' AS notes;

INSERT INTO main.canonical_column_verification_registry_v1 BY NAME
SELECT
    'main' AS schema_name,
    'canonical_patient_master' AS table_name,
    'dominant_nodule_resolution_rule' AS column_name,
    'verified' AS verification_status,
    'Cowork' AS verified_by,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS verified_ts,
    'audit_companion_to_resolved_col' AS verification_method,
    'mig_176b_dominant_nodule_r2_resolved_apply_20260429' AS batch_id,
    'mig_176b: audit companion to dominant_nodule_size_cm_resolved. Values: v1_path_first_winner / v2_us_only / NULL.' AS notes;

-- §F append CF-mig157 closure note on the 2 source cols
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_176b: CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT closed via R2 (COALESCE(v1, v2)) -- new resolved col is canonical_patient_master.dominant_nodule_size_cm_resolved with audit companion dominant_nodule_resolution_rule. v1_path_first_winner=3439 / v2_us_only=166 / NULL=7266.'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name IN ('dominant_nodule_size_cm','dominant_nodule_size_cm_v2');

-- §G resync table_signoff_registry counts (since we added 2 new verified cols)
UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total = (SELECT COUNT(*) FROM information_schema.columns WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main' AND table_name='canonical_patient_master'),
    n_verified = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='verified')
WHERE table_name='canonical_patient_master';

-- §H post-state verification
SELECT
  'POST_RESOLVED_NONNULL_COUNT' AS gate,
  COUNT(*) FILTER (WHERE dominant_nodule_size_cm_resolved IS NOT NULL) AS metric,
  CASE WHEN COUNT(*) FILTER (WHERE dominant_nodule_size_cm_resolved IS NOT NULL) = 3605 THEN 'PASS' ELSE 'FAIL' END AS status
FROM main.canonical_patient_master
UNION ALL
SELECT 'POST_RULE_V1_COUNT', COUNT(*) FILTER (WHERE dominant_nodule_resolution_rule='v1_path_first_winner'),
  CASE WHEN COUNT(*) FILTER (WHERE dominant_nodule_resolution_rule='v1_path_first_winner') = 3439 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master
UNION ALL
SELECT 'POST_RULE_V2_COUNT', COUNT(*) FILTER (WHERE dominant_nodule_resolution_rule='v2_us_only'),
  CASE WHEN COUNT(*) FILTER (WHERE dominant_nodule_resolution_rule='v2_us_only') = 166 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master
UNION ALL
SELECT 'POST_RULE_NULL_COUNT', COUNT(*) FILTER (WHERE dominant_nodule_resolution_rule IS NULL),
  CASE WHEN COUNT(*) FILTER (WHERE dominant_nodule_resolution_rule IS NULL) = 7266 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master
UNION ALL
SELECT 'POST_COHORT_INVARIANT', COUNT(*),
  CASE WHEN COUNT(*) = 10871 AND COUNT(DISTINCT research_id) = 10871 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master
UNION ALL
SELECT 'POST_COALESCE_PARITY',
  COUNT(*) FILTER (WHERE dominant_nodule_size_cm_resolved IS DISTINCT FROM COALESCE(dominant_nodule_size_cm, dominant_nodule_size_cm_v2)),
  CASE WHEN COUNT(*) FILTER (WHERE dominant_nodule_size_cm_resolved IS DISTINCT FROM COALESCE(dominant_nodule_size_cm, dominant_nodule_size_cm_v2)) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM main.canonical_patient_master;
