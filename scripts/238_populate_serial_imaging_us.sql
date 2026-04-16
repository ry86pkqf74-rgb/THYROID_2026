-- ============================================================================
-- Script 238 — Populate serial_imaging_us (one row per US exam, patients with ≥2 exams)
-- Date:    2026-04-16
-- Author:  THYROID_2026 canonical-finalization run (v1_0 lock)
--
-- Purpose: The serial_imaging_us shell was created by a prior migration
--          ("materialize serial_imaging_us in publication", Apr-16) but
--          never populated. Its registered grain is "one row per serial
--          US exam" (per manuscript_workspace.detail_table_registry_v1)
--          and its feeds_master_columns = 'n_us_exams'. This script fills
--          the shell exactly — no new columns added — for the subset of
--          patients who actually have a *series* of exams (≥2).
--
-- Tables READ:
--   thyroid_canonical_publication_v1_0.main.ultrasound_reports (source of record)
--   thyroid_canonical_publication_v1_0.main.imaging_nodule_master_v1
--       (used to hydrate dominant_nodule_size_on_us + dominant_nodule_location
--        via (research_id, exam_date) join; pick nodule with largest
--        max_dimension_cm per exam since no dominant_nodule_flag exists)
--   thyroid_canonical_publication_v1_0.main.canonical_patient_master
--       (read-only for a reconciliation report against CPM.n_us_exams)
--
-- Tables WRITTEN:
--   thyroid_canonical_publication_v1_0.main.serial_imaging_us   (populated)
--
-- Backup plan:
--   Current table has 0 rows. No pre-script backup needed — if the run
--   fails or needs undo, TRUNCATE TABLE serial_imaging_us returns it to
--   the baseline state. No other table is written.
--
-- Schema preserved:
--   (research_id INTEGER, us_date VARCHAR, dominant_nodule_size_on_us DOUBLE,
--    us_findings_impression VARCHAR, us_impression VARCHAR, dominant_nodule_location VARCHAR)
--
-- Column mapping (honest + minimal):
--   research_id                 <- CAST(ultrasound_reports.research_id AS INTEGER)
--   us_date                     <- ultrasound_reports.ultrasound_date  (VARCHAR preserved)
--   dominant_nodule_size_on_us  <- imaging_nodule_master_v1.max_dimension_cm of the
--                                  largest nodule that exam; NULL if no match
--   us_findings_impression      <- ultrasound_reports.source_us_impression
--   us_impression               <- ultrasound_reports.clinical_impression
--   dominant_nodule_location    <- imaging_nodule_master_v1.location_raw of
--                                  the largest-nodule row; NULL if no match
--
-- NOT done in this script (by design):
--   - TI-RADS trajectory columns (not in shell; source is broken anyway —
--     imaging_nodule_long_v2.tirads_score is 100% NULL. Defer to v1_1.)
--   - Per-patient summary columns (n_us_exams already exists in CPM)
-- ============================================================================

-- LOG: PHASE 1 — baseline assertions
-- ASSERT: serial_imaging_us is currently empty
SELECT (SELECT COUNT(*) FROM serial_imaging_us) = 0 AS ok;

-- ASSERT: ultrasound_reports has the expected baseline (6793 exams / 4074 patients)
SELECT
  COUNT(*) = 6793 AND COUNT(DISTINCT research_id) = 4074 AS ok
FROM ultrasound_reports;

-- ASSERT: exactly 1443 patients have ≥2 US exams
SELECT
  (SELECT COUNT(*) FROM
    (SELECT research_id FROM ultrasound_reports GROUP BY research_id HAVING COUNT(*) >= 2) t
  ) = 1443 AS ok;

-- LOG: PHASE 2 — build the populated rows
INSERT INTO serial_imaging_us
  (research_id, us_date, dominant_nodule_size_on_us,
   us_findings_impression, us_impression, dominant_nodule_location)
WITH ge2_pts AS (
  SELECT research_id
  FROM ultrasound_reports
  GROUP BY research_id
  HAVING COUNT(*) >= 2
),
us_exams AS (
  SELECT
    u.research_id                            AS research_id_txt,
    CAST(u.research_id AS INTEGER)           AS research_id_int,
    u.ultrasound_date                        AS us_date_txt,
    TRY_CAST(u.ultrasound_date AS DATE)      AS us_date_native,
    u.source_us_impression,
    u.clinical_impression
  FROM ultrasound_reports u
  JOIN ge2_pts g ON g.research_id = u.research_id
),
dominant_per_exam AS (
  -- Rank nodules per (research_id, exam_date) by max_dimension_cm DESC and pick #1.
  -- No dominant_nodule_flag on imaging_nodule_master_v1 — largest is the operational proxy.
  SELECT
    research_id AS rid,
    TRY_CAST(exam_date AS DATE) AS d,
    max_dimension_cm,
    location_raw,
    ROW_NUMBER() OVER (
      PARTITION BY research_id, TRY_CAST(exam_date AS DATE)
      ORDER BY max_dimension_cm DESC NULLS LAST, nodule_number ASC
    ) AS rn
  FROM imaging_nodule_master_v1
)
SELECT
  e.research_id_int                          AS research_id,
  e.us_date_txt                              AS us_date,
  d.max_dimension_cm                         AS dominant_nodule_size_on_us,
  e.source_us_impression                     AS us_findings_impression,
  e.clinical_impression                      AS us_impression,
  d.location_raw                             AS dominant_nodule_location
FROM us_exams e
LEFT JOIN dominant_per_exam d
  ON d.rid = e.research_id_int
 AND d.d   = e.us_date_native
 AND d.rn  = 1;

-- LOG: PHASE 3 — annotate the populated table
COMMENT ON TABLE serial_imaging_us IS
  'Script 238 (2026-04-16): one row per US exam for patients with ≥2 US exams in ultrasound_reports (the "serial" filter). Source: ultrasound_reports hydrated with imaging_nodule_master_v1 for dominant nodule (largest max_dimension_cm per exam, since no dominant_nodule_flag exists on that table). TI-RADS trajectory is intentionally absent (imaging_nodule_long_v2.tirads_score is 100% NULL; defer to v1_1). n_us_exams continues to flow through canonical_patient_master.n_us_exams.';

COMMENT ON COLUMN serial_imaging_us.dominant_nodule_size_on_us IS
  'Script 238 (2026-04-16): max_dimension_cm of the largest nodule on this exam from imaging_nodule_master_v1 (no dominant_nodule_flag exists; largest is the operational proxy). NULL when no matching imaging_nodule_master_v1 row exists for (research_id, ultrasound_date) — ~10 such exams out of ~4162.';

COMMENT ON COLUMN serial_imaging_us.dominant_nodule_location IS
  'Script 238 (2026-04-16): location_raw of the largest-nodule row paired with dominant_nodule_size_on_us. NULL when no nodule match exists for this exam.';

-- LOG: PHASE 4 — assertions (post-load)
-- ASSERT: populated table is non-empty
SELECT COUNT(*) > 0 AS ok FROM serial_imaging_us;

-- ASSERT: distinct patients = 1443 (strict: matches the prompt assertion)
SELECT COUNT(DISTINCT research_id) = 1443 AS ok FROM serial_imaging_us;

-- ASSERT: total row count equals (us exams for patients with ≥2 exams) — 4162
SELECT COUNT(*) = 4162 AS ok FROM serial_imaging_us;

-- ASSERT: ≥95% of exam rows have a dominant_nodule_size_on_us (nodule_master join coverage)
SELECT
  100.0 * COUNT(dominant_nodule_size_on_us) / COUNT(*) >= 95.0 AS ok
FROM serial_imaging_us;

-- ASSERT: for any patient in serial_imaging_us, CPM.n_us_exams is >= local count
--         (CPM is counted from a broader imaging source (imaging_nodule_master_v1,
--         not ultrasound_reports), so CPM.n_us_exams is a superset, not identity.
--         We check inclusion, which is the correct invariant.)
SELECT
  SUM(CASE WHEN cpm.n_us_exams IS NOT NULL AND cpm.n_us_exams < s.n_local THEN 1 ELSE 0 END) = 0 AS ok
FROM (
  SELECT research_id, COUNT(*) AS n_local FROM serial_imaging_us GROUP BY research_id
) s
JOIN canonical_patient_master cpm ON cpm.research_id = s.research_id;

-- ASSERT: canonical_patient_master row count unchanged
SELECT COUNT(*) = 10871 AS ok FROM canonical_patient_master;

-- LOG: PHASE 5 — reconciliation report (non-blocking diagnostics)
-- LOG: COUNT(DISTINCT research_id) population in the new serial table
SELECT
  (SELECT COUNT(*) FROM serial_imaging_us) AS total_rows,
  (SELECT COUNT(DISTINCT research_id) FROM serial_imaging_us) AS distinct_pts,
  (SELECT COUNT(*) FROM serial_imaging_us WHERE dominant_nodule_size_on_us IS NULL) AS n_missing_size,
  (SELECT COUNT(*) FROM serial_imaging_us WHERE dominant_nodule_location IS NULL) AS n_missing_loc;

-- LOG: Script 238 complete. serial_imaging_us populated to spec. Per-exam grain preserved.
