-- mig_215 — US nodule size outlier fix/quarantine (8931, 8613) + ACR 2017 points=1 category clear
-- Target: thyroid_canonical_publication_v1_0
-- Investigation: qc_framework_v1/reports/mig_215_investigation_tirads_8931_20260430.md
--
USE thyroid_canonical_publication_v1_0;

-- §0 Pre-flight counts
SELECT 'pre_total_rows' AS k, COUNT(*) AS n FROM main.canonical_us_nodule_v2;
SELECT 'pre_size_gt_20' AS k, COUNT(*) AS n
FROM main.canonical_us_nodule_v2 WHERE size_cm_max > 20 OR (size_cm_max IS NOT NULL AND size_cm_max <= 0);
SELECT 'pre_pts1_with_cat' AS k, COUNT(*) AS n
FROM main.canonical_us_nodule_v2
WHERE acr2017_tirads_points = 1 AND acr2017_tirads_category IS NOT NULL;

-- §A Pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_us_nodule_v2_pre_mig215_tirads_size_acr_fix_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig215_snapshot_ts
FROM main.canonical_us_nodule_v2;

-- §B Quarantine flag (size implausible / lost provenance)
ALTER TABLE main.canonical_us_nodule_v2
  ADD COLUMN IF NOT EXISTS is_size_outlier_quarantine BOOLEAN DEFAULT FALSE;

COMMENT ON COLUMN main.canonical_us_nodule_v2.is_size_outlier_quarantine IS
'TRUE when size_cm_max was cleared as implausible (e.g. rid 8931 placeholder 48 cm with no mm axes or extracted_size_cm). Manuscript size analyses SHOULD exclude OR stratify on this flag. Added mig_215 2026-04-30.';

UPDATE main.canonical_us_nodule_v2 SET is_size_outlier_quarantine = FALSE;

-- §C Size corrections — rid 8931: prefer extracted_size_cm when both 48 and extracted present
UPDATE main.canonical_us_nodule_v2
SET size_cm_max = extracted_size_cm,
    is_size_outlier_quarantine = FALSE
WHERE research_id = 8931
  AND size_cm_max = 48
  AND extracted_size_cm IS NOT NULL;

-- §D Size quarantine — rid 8931: NULL implausible 48 when no extracted correction
UPDATE main.canonical_us_nodule_v2
SET size_cm_max = NULL,
    is_size_outlier_quarantine = TRUE
WHERE research_id = 8931
  AND size_cm_max = 48
  AND extracted_size_cm IS NULL;

-- §E Size correction — rid 8613: 21 → 2.1 cm (mm entered as cm)
UPDATE main.canonical_us_nodule_v2
SET size_cm_max = 2.1,
    is_size_outlier_quarantine = FALSE
WHERE research_id = 8613
  AND size_cm_max = 21;

-- §F ACR 2017: no band for total points = 1 — clear stale legacy category
UPDATE main.canonical_us_nodule_v2
SET acr2017_tirads_category = NULL
WHERE acr2017_tirads_points = 1
  AND acr2017_tirads_category IS NOT NULL;

-- §G Recompute concordance for rows touched in §F (points=1 category now NULL)
UPDATE main.canonical_us_nodule_v2
SET acr2017_vs_updated_concordant = CASE
  WHEN acr2017_tirads_category IS NOT NULL
   AND updated_tirads_category IS NOT NULL
  THEN (acr2017_tirads_category = updated_tirads_category)
  ELSE NULL
END
WHERE acr2017_tirads_points = 1;

-- §H Column registry — new column (ordinal 58)
INSERT INTO main.canonical_column_verification_registry_v1 (
  schema_name, table_name, column_name, data_type, ordinal_position,
  category, upstream_source, verification_status, verified_by, verified_ts,
  verification_method, batch_id, notes, registered_ts
)
SELECT
  'main',
  'canonical_us_nodule_v2',
  'is_size_outlier_quarantine',
  'BOOLEAN',
  58,
  'derived',
  NULL,
  'verified',
  'cursor_agent',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'derivation_implausible_size_cm_max_cleared_mig215_8931_placeholder_48',
  'mig_215_tirads_outliers_acr_band_fix_20260430',
  'TRUE for rid 8931 rows where size_cm_max set to NULL (48 cm placeholder without extracted_size_cm). FALSE elsewhere after mig_215.',
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE NOT EXISTS (
  SELECT 1 FROM main.canonical_column_verification_registry_v1 r
  WHERE r.schema_name = 'main'
    AND r.table_name = 'canonical_us_nodule_v2'
    AND r.column_name = 'is_size_outlier_quarantine'
);

-- §I Table signoff registry — recompute from column registry
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/215_tirads_outliers_acr_band_fix_20260430.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_215: size_cm_max fixes (8931/8613), is_size_outlier_quarantine BOOLEAN, '
                        || 'ACR2017 clear category where points=1 (23 rows).'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'canonical_us_nodule_v2'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- §J Provenance
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_215_tirads_outliers_acr_band_fix_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'canonical_us_nodule_v2_size_fix_quarantine_acr2017_pts1_category_null',
   'SIZE_OUTLIER_8931_8613_AND_ACR_PTS1_BAND',
   'archive_snapshot_pre_mig215_archive_pub_v1_0',
   'column_registry_is_size_outlier_quarantine_signoff_recomputed',
   'none');

-- §K Post-verify
SELECT 'post_size_gt_20' AS k, COUNT(*) AS n
FROM main.canonical_us_nodule_v2 WHERE size_cm_max > 20 OR (size_cm_max IS NOT NULL AND size_cm_max <= 0);
SELECT 'post_pts1_with_cat' AS k, COUNT(*) AS n
FROM main.canonical_us_nodule_v2
WHERE acr2017_tirads_points = 1 AND acr2017_tirads_category IS NOT NULL;
SELECT 'post_quarantine_true' AS k, COUNT(*) AS n
FROM main.canonical_us_nodule_v2 WHERE is_size_outlier_quarantine IS TRUE;
