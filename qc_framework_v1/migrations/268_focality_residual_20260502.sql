-- mig_268 — residual tumor_focality cleanup after mig_261 (long-tail literals).
-- Database: thyroid_canonical_publication_v1_0.main.path_synoptics
-- Apply via scripts/mig_268_focality_residual_drift.py --apply

-- Pre-snapshot (archive read-only attach DB)
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.path_synoptics_pre_mig268_20260502 AS
SELECT research_id, tumor_focality
FROM main.path_synoptics
WHERE tumor_focality IN (
  'Multifocal', 'Unifocal', 'unifocal*', 'unifocal ', 'multifocal ',
  'multifocal' || CHR(10)
);

UPDATE main.path_synoptics
SET tumor_focality = LOWER(TRIM(REPLACE(REPLACE(tumor_focality, '*', ''), CHR(10), '')))
WHERE tumor_focality IN (
  'Multifocal', 'Unifocal', 'unifocal*', 'unifocal ', 'multifocal ',
  'multifocal' || CHR(10)
);

-- Post-verify (expect 0 rows)
-- SELECT tumor_focality, COUNT(*) FROM main.path_synoptics
-- WHERE tumor_focality IN (...) GROUP BY 1;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_268',
  CURRENT_TIMESTAMP::TIMESTAMP,
  'logan_via_cursor',
  'Cleared residual focality drift values from mig_261 long tail (asterisk + case + whitespace).'
);
