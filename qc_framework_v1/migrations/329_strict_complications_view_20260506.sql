-- =============================================================================
-- mig_329 — Strict-definition complications view (Migrations A + C)
--           pub_canonical.complications_strict_v1
--
-- Date:    2026-05-06
-- Source:  M038 canonical-data audit 2026-05-06 (M038_canonical_data_audit_20260506.docx)
--          Data Feedback Log rows: M038-AUDIT-F1-HypoCa-StrictFlag,
--                                   M038-AUDIT-F2-Tracheostomy-Perioperative
--
-- CONTEXT:
--   The canonical_patient_master.comp_hypoparathyroidism_transient and _permanent
--   flags do NOT enforce comp_hypoparathyroidism_confirmed = TRUE as a precondition,
--   in violation of the standing rule (memory/feedback_complications_transient_vs_permanent.md
--   ratified 2026-05-01). Cohort-wide impact: 41 unconfirmed-included transient cases
--   and 25 unconfirmed-included permanent cases.
--
--   The proc_nlp_tracheostomy flag does NOT enforce a perioperative temporal filter.
--   384 events flagged cohort-wide; only 2 met strict 0–30 day perioperative window.
--
-- SCOPE:
--   CREATE OR REPLACE VIEW only — non-destructive. Existing canonical_patient_master
--   columns preserved for backward compatibility. New strict-definition columns
--   exposed via pub_canonical.complications_strict_v1 view, joined by research_id.
--
-- VERIFY (post-apply):
--   SELECT COUNT(*) n FROM `thyroid-canonical-pub-2026.pub_canonical.complications_strict_v1`;
--   -- Expect: matches canonical_patient_master row count.
--   SELECT
--     COUNTIF(comp_hypoparathyroidism_transient_strict)   AS n_strict_transient,    -- Expect 280
--     COUNTIF(comp_hypoparathyroidism_permanent_strict)   AS n_strict_permanent,    -- Expect 16
--     COUNTIF(comp_perioperative_tracheostomy_strict)     AS n_strict_perioperative_trach -- Expect 2
--   FROM `thyroid-canonical-pub-2026.pub_canonical.complications_strict_v1`
--   JOIN `thyroid-canonical-pub-2026.pub_canonical.cohort_m038_massive_goiter_v1` USING (research_id);
-- =============================================================================

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.complications_strict_v1` AS
SELECT
  research_id,

  -- ----- Hypoparathyroidism (Migration C) -----
  -- Strict standing-rule definitions (require comp_hypoparathyroidism_confirmed = TRUE):
  (comp_hypoparathyroidism_confirmed AND comp_hypoparathyroidism_transient) AS comp_hypoparathyroidism_transient_strict,
  (comp_hypoparathyroidism_confirmed AND comp_hypoparathyroidism_permanent) AS comp_hypoparathyroidism_permanent_strict,

  -- Edge-case flag: cases with temporality flag but NOT confirmed status
  -- (timing_window evidence exists but no biochem/treatment confirmation, or
  --  duration was reset to unknown post audit).
  ((comp_hypoparathyroidism_transient OR comp_hypoparathyroidism_permanent)
    AND NOT comp_hypoparathyroidism_confirmed) AS comp_hypoparathyroidism_unconfirmed_evidence,

  -- ----- Perioperative tracheostomy (Migration A) -----
  -- Strict perioperative window: 0–30 days from index thyroidectomy
  (proc_nlp_tracheostomy AND proc_nlp_tracheostomy_days_from_surg BETWEEN 0 AND 30)
    AS comp_perioperative_tracheostomy_strict,

  -- Preexisting (history) tracheostomy: documented before index surgery
  (proc_nlp_tracheostomy AND proc_nlp_tracheostomy_days_from_surg < 0)
    AS comp_preexisting_tracheostomy,

  -- Late postoperative tracheostomy: > 30 days from index surgery
  -- (excluded from perioperative-strict count; clinical relevance varies by gap)
  (proc_nlp_tracheostomy AND proc_nlp_tracheostomy_days_from_surg > 30)
    AS comp_late_postop_tracheostomy,

  -- Tracheostomy event with no temporal anchor — currently 97.7% of NLP-flagged
  -- events. Canonical NLP enhancement target (Migration B part 5).
  (proc_nlp_tracheostomy AND proc_nlp_tracheostomy_days_from_surg IS NULL)
    AS comp_unanchored_tracheostomy_mention,

  -- Original derivation columns retained for backward compatibility / audit
  comp_hypoparathyroidism_confirmed,
  comp_hypoparathyroidism_transient AS comp_hypoparathyroidism_transient_loose,
  comp_hypoparathyroidism_permanent AS comp_hypoparathyroidism_permanent_loose,
  comp_hypoparathyroidism_timing_window,
  comp_hypopara_permanent_limitation_note,
  proc_nlp_tracheostomy AS proc_nlp_tracheostomy_loose,
  proc_nlp_tracheostomy_days_from_surg

FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`;
