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
-- AMBIGUOUS: 0 case(s) withheld for human review.
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

INSERT INTO `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_overrides_v1`
  (research_id, category, override_flag, original_nlp_flag, manual_flag,
   adjudication_outcome, rationale_summary, reviewer, decision_date)
VALUES
  ('10087', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes an atypical follicular nodule with papillary features and variable nuclear characteristics that do not meet criteria for papillary thyroid carcinoma, but this represents a distinct diagnostic entity from an atypical adenoma, which is not the terminology used in this thyroid specimen.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('10573', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes an atypical oncocytic nodule of the thyroid with capsular irregularities but no convincing invasion, which represents a distinct entity from an atypical adenoma and is more consistent with a follicular neoplasm of uncertain malignant potential rather than a parathyroid atypical adenoma.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('10596', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes macrofollicular thyroid tissue consistent with follicular nodular disease (previously termed nodular hyperplasia or adenomatoid nodule), which is a benign hyperplastic process rather than an atypical adenoma, and the comment explicitly notes absence of papillary-like nuclear features and high-grade features.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('11151', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes a low-risk atypical follicular neoplasm with uncertain malignant potential, which is a distinct diagnostic entity from an atypical adenoma and represents a lesion where capsular invasion cannot be definitively excluded rather than a benign adenoma with atypical features.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('11314', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes a follicular tumor of uncertain malignant potential, which is a distinct diagnostic entity from atypical adenoma and represents a borderline thyroid lesion without definitive features of malignancy or benignity.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('11693', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes atypical follicular neoplasms of uncertain malignant potential in the thyroid, which are distinct from atypical adenomas; these are follicular-pattern thyroid lesions with uncertain behavior rather than adenomas with atypical features.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('11760', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes a follicular adenoma with capsular irregularities attributed to prior biopsy effects, explicitly characterizing it as benign rather than atypical, despite initial mention of atypical features that were later reinterpreted as artifact.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('12008', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes an oncocytic adenoma characterized as a well-differentiated tumor with uncertain malignant potential, which is a distinct diagnostic entity from atypical adenoma; the term atypical modifies the classification category rather than representing a diagnosis of atypical adenoma itself.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('12082', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes an atypical oncocytic nodule characterized as a well-differentiated tumor of uncertain malignant potential, which is a distinct diagnostic entity from atypical adenoma and represents a thyroid-specific borderline lesion rather than a true adenoma with atypia.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('7108', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes benign thyroid tissue showing nodular hyperplasia without any mention of adenoma or atypical features, representing residual or ectopic thyroid rather than a neoplastic process.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('9705', 'atypical_adenoma', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology describes a follicular lesion with atypical features including increased mitotic activity and disrupted borders, but explicitly states uncertainty about whether the lesion is neoplastic or non-neoplastic, characterizing it as an atypical follicular lesion rather than definitively diagnosing it as an adenoma.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('10098', 'thymic_tissue', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The specimen labeled as thymic tissue contains only a benign lymph node and adipose tissue with scattered lymphoid foci, with no confirmation that actual thymic tissue is present in the specimen; the label appears to be an anatomical site descriptor rather than a pathological finding of intrathyroidal or ectopic thymus.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('11709', 'thymic_tissue', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The microscopic description explicitly states that immunohistochemistry was performed to distinguish between lymph nodes and thymic tissue in specimen B, with the stain being negative, thereby confirming the tissue as lymph nodes rather than thymus, meaning no thymic tissue was identified in the specimen.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('2517', 'thymic_tissue', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The pathology report explicitly states that no thymic tissue was identified in the left thymus excision specimen, representing a clear negation rather than confirmation of thymic tissue presence.', 'Claude-Sonnet-4.6', '2026-05-08'),
  ('2993', 'thymic_tissue', FALSE, TRUE, FALSE, 'NLP_FALSE_POSITIVE', 'The specimen labeled as right thymus biopsy contained only benign lymph nodes with explicit documentation that no parathyroid tissue was identified, but no mention confirming actual thymic tissue was present in any of the specimens examined.', 'Claude-Sonnet-4.6', '2026-05-08');