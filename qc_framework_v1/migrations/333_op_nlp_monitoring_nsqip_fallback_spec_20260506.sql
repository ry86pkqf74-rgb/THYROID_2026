-- =============================================================================
-- mig_333 — op_nlp_nerve_monitoring_used NSQIP fallback view (SPEC)
--           pub_canonical.op_nlp_nerve_monitoring_v2 (provenance-aware)
--
-- Date:    2026-05-06
-- Source:  M038 audit Finding 3 (M038-AUDIT-F3-RLNMonitor-NLPGap) — Cowork
--          session continuation 2026-05-06.
-- Status:  SPEC ONLY. Apply AFTER mig_331 (extract_operative_v2.py @ v2.2)
--          propagates to canonical_patient_master via canonical rebuild.
--
-- CONTEXT:
--   F3 regex expansion (v2.2) reaches 96.0% sensitivity / 98.1% specificity
--   *among patients with an op-note in clinical_notes_long*, but only 58.9%
--   absolute sensitivity vs NSQIP n=730. The structural gap is 282 NSQIP-Yes
--   patients (38.6% of NSQIP-Yes ground truth) whose OPNote-1..4 cells in
--   raw/Notes 12_1_25.xlsx are blank — the op-notes were never transcribed
--   and cannot be recovered by NLP from the current notes file.
--
--   Per PI direction (Cowork session 2026-05-06): use NSQIP as fallback for
--   the canonical monitoring flag, BUT denote whether the value reflects
--   actual nerve-monitoring findings reported in the op-note or is a
--   structured-source fallback. Downstream papers (M038, M039, M040, M042,
--   M044, M082) need to know which patients have op-note-confirmed
--   monitoring vs NSQIP-only attestation.
--
-- DESIGN:
--   View pub_canonical.op_nlp_nerve_monitoring_v2 exposes:
--     - op_nlp_nerve_monitoring_used (BOOL)              — combined truth
--     - op_nlp_nerve_monitoring_source (STRING)          — provenance tier
--     - op_nlp_nerve_monitoring_op_note_evidence (BOOL)  — op-note regex hit
--     - op_nlp_nerve_monitoring_nsqip_evidence (BOOL)    — NSQIP structured
--     - op_nlp_nerve_monitoring_used_op_note_only (BOOL) — restrict for
--                                                          analyses requiring
--                                                          op-note-confirmed
--                                                          findings
--
--   Source-tier hierarchy:
--     1. op_note_nlp_v22_positive — op-note regex matched (n=3,551)
--     2. op_note_nlp_v22_negative — op-note exists, no monitoring evidence,
--                                    not negated (n≈900-1,000 patients)
--     3. nsqip_structured_fallback_positive — no op-note OR op-note silent;
--                                              nsqip_rln_monitoring='Yes'
--                                              (n≈282)
--     4. nsqip_structured_fallback_negative — no op-note OR op-note silent;
--                                              nsqip_rln_monitoring='No'
--     5. no_evidence_unknown — no op-note AND no NSQIP value
--
-- VERIFY (post-apply):
--   SELECT op_nlp_nerve_monitoring_source, COUNT(*)
--   FROM `thyroid-canonical-pub-2026.pub_canonical.op_nlp_nerve_monitoring_v2`
--   GROUP BY op_nlp_nerve_monitoring_source ORDER BY 2 DESC;
--   -- Expect:
--   --   op_note_nlp_v22_positive          ~3,551
--   --   no_evidence_unknown               ~5,386 minus NSQIP-restricted
--   --   nsqip_structured_fallback_positive ~282
--   --   op_note_nlp_v22_negative          ~900-1,000
--   --   nsqip_structured_fallback_negative ~varies
-- =============================================================================

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.op_nlp_nerve_monitoring_v2` AS
WITH op_note_present AS (
  SELECT DISTINCT research_id
  FROM `thyroid-canonical-pub-2026.pub_canonical.clinical_notes_long`
  WHERE LOWER(note_type) IN ('op_note','opnote') AND note_text IS NOT NULL
),
cpm AS (
  SELECT
    research_id,
    op_nlp_nerve_monitoring_used        AS op_note_evidence,
    op_nlp_nerve_monitoring_type,
    nsqip_rln_monitoring,
    -- once mig_331 / v2.2 ships, op_nlp_nerve_monitoring_used itself will be
    -- the v2.2-derived flag; until then this view will reflect v2.0 truth.
    UPPER(nsqip_rln_monitoring) = 'YES' AS nsqip_pos,
    UPPER(nsqip_rln_monitoring) = 'NO'  AS nsqip_neg
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
)
SELECT
  cpm.research_id,
  CASE
    WHEN cpm.op_note_evidence IS TRUE  THEN TRUE
    WHEN cpm.op_note_evidence IS FALSE THEN FALSE
    WHEN cpm.nsqip_pos                THEN TRUE
    WHEN cpm.nsqip_neg                THEN FALSE
    ELSE NULL
  END AS op_nlp_nerve_monitoring_used,
  CASE
    WHEN cpm.op_note_evidence IS TRUE  THEN 'op_note_nlp_v22_positive'
    WHEN cpm.op_note_evidence IS FALSE THEN 'op_note_nlp_v22_negative'
    WHEN cpm.nsqip_pos                THEN 'nsqip_structured_fallback_positive'
    WHEN cpm.nsqip_neg                THEN 'nsqip_structured_fallback_negative'
    ELSE 'no_evidence_unknown'
  END AS op_nlp_nerve_monitoring_source,
  cpm.op_note_evidence                  AS op_nlp_nerve_monitoring_op_note_evidence,
  CASE WHEN cpm.nsqip_pos THEN TRUE
       WHEN cpm.nsqip_neg THEN FALSE
       ELSE NULL END                    AS op_nlp_nerve_monitoring_nsqip_evidence,
  -- For analyses requiring op-note-confirmed findings ONLY (downstream
  -- papers asking 'what did monitoring show'), restrict to op-note tier.
  CASE WHEN cpm.op_note_evidence IS NOT NULL
       THEN cpm.op_note_evidence
       ELSE NULL END                    AS op_nlp_nerve_monitoring_used_op_note_only,
  cpm.op_nlp_nerve_monitoring_type,
  cpm.nsqip_rln_monitoring              AS nsqip_rln_monitoring_raw,
  CASE WHEN op_note_present.research_id IS NOT NULL THEN TRUE ELSE FALSE END
                                        AS has_any_op_note_in_clinical_notes_long
FROM cpm
LEFT JOIN op_note_present USING (research_id);
