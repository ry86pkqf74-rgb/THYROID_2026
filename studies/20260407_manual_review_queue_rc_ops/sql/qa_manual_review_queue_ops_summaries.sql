-- Operational summaries for qa.manual_review_queue (DuckDB / MotherDuck)
-- Run in-context after attaching the Thyroid 2026 database.
-- Does not export review_reason or note text (PHI boundary).

-- ── 1) Blocking vs cleared (119 --release-mode structural gate) ─────────────
-- Strict validator fails when ANY row has verification_status IS NULL.

SELECT 'blocking_release_pending_verification' AS category,
       COUNT(*) AS n_rows
FROM qa.manual_review_queue
WHERE verification_status IS NULL
UNION ALL
SELECT 'cleared_has_verification_status', COUNT(*)
FROM qa.manual_review_queue
WHERE verification_status IS NOT NULL
UNION ALL
SELECT 'pending_discordant_existing', COUNT(*)
FROM qa.manual_review_queue
WHERE verification_status IS NULL
  AND algorithm_status = 'discordant_existing'
UNION ALL
SELECT 'pending_fill_candidate', COUNT(*)
FROM qa.manual_review_queue
WHERE verification_status IS NULL
  AND algorithm_status = 'existing_missing_fill_candidate'
ORDER BY category;

-- ── 2) Manuscript-grade vs placeholder automation (NOT enforced by 119 today) ─
-- Replace or adjudicate rows with synthetic placeholder before publication RC.

SELECT CASE
         WHEN verification_status = 'SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF'
           THEN 'placeholder_synthetic_non_manuscript'
         WHEN verification_status IS NULL THEN 'pending_null'
         WHEN verification_status ILIKE 'auto_accepted%' OR verification_status = 'confirmed_correct'
           THEN 'rubric_or_automation_track'
         ELSE 'other_status'
       END AS manuscript_readiness_bucket,
       COUNT(*) AS n_rows
FROM qa.manual_review_queue
GROUP BY 1
ORDER BY n_rows DESC;

-- ── 3) Priority work ordering (operational burn-down) ─────────────────────────
-- Order: discordant_existing pending first, then fill-candidate volume by domain.

SELECT domain,
       algorithm_status,
       COUNT(*) FILTER (WHERE verification_status IS NULL) AS n_pending,
       COUNT(*) AS n_total
FROM qa.manual_review_queue
GROUP BY 1, 2
ORDER BY CASE WHEN algorithm_status = 'discordant_existing' THEN 0 ELSE 1 END,
         n_pending DESC,
         n_total DESC;

-- ── 4) Reason codes + decision histogram ───────────────────────────────────

SELECT COALESCE(NULLIF(TRIM(reason_code), ''), '(none)') AS reason_code,
       COUNT(*) AS n_rows
FROM qa.manual_review_queue
GROUP BY 1
ORDER BY n_rows DESC;

SELECT COALESCE(verification_status, '(null)') AS verification_status,
       COUNT(*) AS n_rows
FROM qa.manual_review_queue
GROUP BY 1
ORDER BY n_rows DESC;

-- ── 5) Audit completeness (no PHI — flags only) ─────────────────────────────

SELECT
  COUNT(*) AS n_total,
  COUNT(*) FILTER (
    WHERE verification_status IS NOT NULL
      AND reviewer IS NOT NULL
      AND TRIM(CAST(reviewer AS VARCHAR)) <> ''
  ) AS n_with_reviewer,
  COUNT(*) FILTER (
    WHERE reviewer_evidence_span IS NOT NULL
      AND TRIM(CAST(reviewer_evidence_span AS VARCHAR)) <> ''
  ) AS n_with_reviewer_evidence_span,
  COUNT(*) FILTER (
    WHERE reviewer_comment IS NOT NULL
      AND TRIM(CAST(reviewer_comment AS VARCHAR)) <> ''
  ) AS n_with_reviewer_comment
FROM qa.manual_review_queue;
