-- =============================================================================
-- mig_337 — pub_canonical.canonical_path_benign_overrides_v1
--
-- Date:       2026-05-08
-- Lane:       H2 manuscript — pathology benign override decisions (Phase 2a)
-- Author:     Claude-Sonnet-4.6 (adjudication) + Cursor Agent (pipeline)
--
-- AUDIT ANCHORS:
--   VC-H2-ATYPICAL-ADENOMA-NLP-ANOMALY  (Verification Check, THYROID_DATA_REGISTRY)
--   VC-H2-SUBSTERNAL-PATH-RECONCILE      (Verification Check, THYROID_DATA_REGISTRY)
--   THY-34 (Linear, team Thyroid Database THY)
--
-- Rows included: NLP_FALSE_POSITIVE + MANUAL_TRUE_POSITIVE adjudications only.
-- Excluded: NLP_TRUE_POSITIVE (NLP correct, no override needed).
-- AMBIGUOUS: 18 case(s) withheld for human review.
-- =============================================================================

CREATE TABLE IF NOT EXISTS `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_overrides_v1` (
  research_id              STRING    NOT NULL,
  category                 STRING    NOT NULL,
  override_flag            BOOL      NOT NULL,
  original_nlp_flag        BOOL      NOT NULL,
  manual_flag              BOOL      NOT NULL,
  adjudication_outcome     STRING    NOT NULL,
  rationale_summary        STRING,
  reviewer                 STRING    NOT NULL,
  decision_date            DATE      NOT NULL
);

-- No override rows generated (all AMBIGUOUS or NLP_TRUE_POSITIVE).