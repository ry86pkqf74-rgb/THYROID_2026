-- Migration: 144b_us_measurement_varchar_to_double_retype_20260429.sql
-- Purpose: Retype 4 PM US measurement cols VARCHAR -> DOUBLE.
--          Strip embedded unit suffix (' mm' / ' mL') so analysts can run statistics
--          directly. Upstream canonical_us_thyroid_gland_v2 already stores these as
--          DOUBLE; PM stringified them with units appended, breaking direct stats use.
-- Trigger: Cowork verification of mig_144 found 4 VARCHAR cols that should be DOUBLE:
--          - us_isthmus_thickness_mm    (4,074 non-null, all parseable, suffix ' mm')
--          - us_left_lobe_volume_ml     (suffix ' mL')
--          - us_right_lobe_volume_ml    (suffix ' mL')
--          - us_total_volume_ml         (suffix ' mL')
--          Probe confirmed: 4,074 / 4,074 non-null values are TRY_CAST-able to DOUBLE
--          after stripping the suffix. Zero unparseable.
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 34b (post-mig_144 cleanup; data write — pre-snapshot first)
--
-- Strategy: Add new DOUBLE col, copy parsed values, drop VARCHAR col, rename DOUBLE->orig name.
-- (DuckDB ALTER COLUMN SET DATA TYPE supports USING expression but we explicit-pre-snapshot
-- via a side table to enable rollback.)

-- ============================================================
-- STEP 1. Pre-snapshot the 4 cols + research_id
-- ============================================================
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig144b_us_measurements_20260429 AS
SELECT research_id,
       us_isthmus_thickness_mm,
       us_left_lobe_volume_ml,
       us_right_lobe_volume_ml,
       us_total_volume_ml,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig144b_snapshot_ts
FROM main.canonical_patient_master;

-- ============================================================
-- STEP 2. Retype each col VARCHAR -> DOUBLE in-place
--          (DuckDB SET DATA TYPE with USING expression)
-- ============================================================
ALTER TABLE main.canonical_patient_master
  ALTER COLUMN us_isthmus_thickness_mm
  SET DATA TYPE DOUBLE
  USING TRY_CAST(REGEXP_REPLACE(us_isthmus_thickness_mm, ' mm$', '') AS DOUBLE);

ALTER TABLE main.canonical_patient_master
  ALTER COLUMN us_left_lobe_volume_ml
  SET DATA TYPE DOUBLE
  USING TRY_CAST(REGEXP_REPLACE(us_left_lobe_volume_ml, ' mL$', '') AS DOUBLE);

ALTER TABLE main.canonical_patient_master
  ALTER COLUMN us_right_lobe_volume_ml
  SET DATA TYPE DOUBLE
  USING TRY_CAST(REGEXP_REPLACE(us_right_lobe_volume_ml, ' mL$', '') AS DOUBLE);

ALTER TABLE main.canonical_patient_master
  ALTER COLUMN us_total_volume_ml
  SET DATA TYPE DOUBLE
  USING TRY_CAST(REGEXP_REPLACE(us_total_volume_ml, ' mL$', '') AS DOUBLE);

-- ============================================================
-- STEP 3. Post-verify NULL counts unchanged + new types are DOUBLE
-- ============================================================
SELECT
  'post_mig144b' AS phase,
  (SELECT COUNT(*) FROM main.canonical_patient_master WHERE us_isthmus_thickness_mm IS NOT NULL) AS isthmus_nonnull,
  (SELECT COUNT(*) FROM main.canonical_patient_master WHERE us_left_lobe_volume_ml IS NOT NULL) AS ll_nonnull,
  (SELECT COUNT(*) FROM main.canonical_patient_master WHERE us_right_lobe_volume_ml IS NOT NULL) AS rl_nonnull,
  (SELECT COUNT(*) FROM main.canonical_patient_master WHERE us_total_volume_ml IS NOT NULL) AS total_nonnull,
  (SELECT data_type FROM information_schema.columns WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main' AND table_name='canonical_patient_master' AND column_name='us_isthmus_thickness_mm') AS isthmus_type,
  (SELECT data_type FROM information_schema.columns WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main' AND table_name='canonical_patient_master' AND column_name='us_total_volume_ml') AS total_type;
-- Expect isthmus_nonnull = 4074, all DOUBLE.

-- ============================================================
-- STEP 4. Append CF closure note + refresh verified_ts on the 4 cols
-- ============================================================
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_144b (2026-04-29): VARCHAR -> DOUBLE retype completed. ' ||
            'Pre-mig_144b values were like "3.6 mm" / "7.9 mL" with embedded unit suffix; ' ||
            'analytically broken (analysts had to parse text to compute stats). ' ||
            'Retyped via TRY_CAST(REGEXP_REPLACE(val, '' mm/mL$'', '''') AS DOUBLE). ' ||
            'All 4,074 non-null values parseable; 0 unparseable. NULL counts preserved. ' ||
            'Upstream canonical_us_thyroid_gland_v2 stores these as DOUBLE natively. ' ||
            'CF-mig144-US-MEASUREMENT-VARCHAR-RETYPE CLOSED.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    data_type = 'DOUBLE'
WHERE table_name = 'canonical_patient_master'
  AND column_name IN (
    'us_isthmus_thickness_mm',
    'us_left_lobe_volume_ml',
    'us_right_lobe_volume_ml',
    'us_total_volume_ml'
  );

-- End of mig_144b. Same shape as mig_139 (resync) — registry-stays-flagged-verified pattern.
