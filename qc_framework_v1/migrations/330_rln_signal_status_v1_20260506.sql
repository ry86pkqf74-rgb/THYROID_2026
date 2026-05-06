-- =============================================================================
-- mig_330 — rln_signal_status canonical view (Migration D)
--           pub_canonical.rln_signal_status_v1
--
-- Date:    2026-05-06
-- Source:  M038 canonical-data audit 2026-05-06.
--          Data Feedback Log: M038-AUDIT-F5-NerveSignal-AbnormalVsVerified
--
-- CONTEXT:
--   Derives a canonical signal-quality column from the structured
--   intraoperative nerve-stimulation amplitudes captured in
--   canonical_patient_master.ops_nerve_stim_left/_right (μV values for
--   ~118 cases). Loss-of-signal (LOS) threshold: <100 μV on either side.
--
-- VERIFY (post-apply):
--   SELECT rln_signal_status, COUNT(*) FROM `pub_canonical.rln_signal_status_v1`
--     GROUP BY rln_signal_status;
--   -- Expect: 'signal_verified' ~98, 'loss_of_signal_los' ~15, 'unknown' ~10,758
-- =============================================================================

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.rln_signal_status_v1` AS
SELECT
  research_id,
  ops_nerve_stim_left,
  ops_nerve_stim_right,
  SAFE_CAST(REGEXP_EXTRACT(ops_nerve_stim_left,  r'^(\d+)') AS INT64) AS stim_left_uV,
  SAFE_CAST(REGEXP_EXTRACT(ops_nerve_stim_right, r'^(\d+)') AS INT64) AS stim_right_uV,
  CASE
    WHEN ops_nerve_stim_left IS NULL AND ops_nerve_stim_right IS NULL
      THEN 'unknown'
    WHEN (SAFE_CAST(REGEXP_EXTRACT(ops_nerve_stim_left,  r'^(\d+)') AS INT64) < 100)
      OR (SAFE_CAST(REGEXP_EXTRACT(ops_nerve_stim_right, r'^(\d+)') AS INT64) < 100)
      THEN 'loss_of_signal_los'
    WHEN (SAFE_CAST(REGEXP_EXTRACT(ops_nerve_stim_left,  r'^(\d+)') AS INT64) >= 100
          OR ops_nerve_stim_left IS NULL)
      AND (SAFE_CAST(REGEXP_EXTRACT(ops_nerve_stim_right, r'^(\d+)') AS INT64) >= 100
           OR ops_nerve_stim_right IS NULL)
      AND (ops_nerve_stim_left IS NOT NULL OR ops_nerve_stim_right IS NOT NULL)
      THEN 'signal_verified'
    ELSE 'unknown'
  END AS rln_signal_status,
  CASE
    WHEN (SAFE_CAST(REGEXP_EXTRACT(ops_nerve_stim_left,  r'^(\d+)') AS INT64) < 100)
      OR (SAFE_CAST(REGEXP_EXTRACT(ops_nerve_stim_right, r'^(\d+)') AS INT64) < 100)
      THEN TRUE
    WHEN ops_nerve_stim_left IS NULL AND ops_nerve_stim_right IS NULL
      THEN NULL
    ELSE FALSE
  END AS rln_loss_of_signal_flag,
  'operative_details.final_nerve_stim (structured amplitudes; LOS threshold <100 microV)' AS rln_signal_status_source
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`;

-- mig_331 (Migration B, NLP enrichment spec) extends rln_signal_status to
-- supplement the structured-amplitude derivation with operative-note phrase
-- matching for cases where amplitudes are not captured.
