-- mig_339: H2 v3.2 Phase 2b — NLP_FALSE_NEGATIVE reverse overrides (thymic_tissue)
-- Lane: THY-40 / THY-32. Append-only INSERT into canonical_path_benign_overrides_v1.
-- Pre-log: DFL-20260508-H2-NLP-FN-REVERSE-OVERRIDES (rec5maEpmDMXyJKxx)
-- Rationale paraphrased from llm_extraction/runs/h2_path_reconciliation_20260508/per_patient_adjudications.jsonl
--   (no raw synoptic text).
--
INSERT INTO `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_overrides_v1`
  (research_id, category, override_flag, original_nlp_flag, manual_flag,
   adjudication_outcome, rationale_summary, reviewer, decision_date)
VALUES
  ('236', 'thymic_tissue', TRUE, FALSE, TRUE, 'NLP_FALSE_NEGATIVE',
   'Pathology narrative documents heterotopic thymic rest in the thyroid lobectomy specimen, consistent with ectopic thymic tissue; manual structured flag marked thymic_tissue positive while NLP did not — NLP_FALSE_NEGATIVE for cohort propagation.',
   'extract_h2_path_reconciliation.py / Anthropic Sonnet 4.6 (2026-05-08)',
   DATE '2026-05-08'),
  ('8589', 'thymic_tissue', TRUE, FALSE, TRUE, 'NLP_FALSE_NEGATIVE',
   'Surgical specimen narrative describes benign thymic tissue alongside parathyroid and thyroid elements; manual review flagged thymic_tissue while NLP missed — NLP_FALSE_NEGATIVE for cohort correction.',
   'extract_h2_path_reconciliation.py / Anthropic Sonnet 4.6 (2026-05-08)',
   DATE '2026-05-08');

-- Post-insert verification:
-- SELECT category, adjudication_outcome, COUNT(*) AS n
-- FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_overrides_v1`
-- WHERE decision_date = '2026-05-08'
-- GROUP BY 1,2 ORDER BY 1,2;
-- Expected total rows for that date: 17 (11 atyp FP + 4 thym FP + 2 thym FN).
