-- =============================================================================
-- Migration 80 -- airway invasion scope filter (Step A)
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   First cleanup pass on canonical_airway_invasion_events_v1 under
--         Protocol v2.
--
-- Logan directive (verbatim):
--   "this should only be from operative reports/synoptic/gross pathology/
--    micro pathology. Not from any imaging."
--
-- Rationale:
--   Airway invasion (tracheal/laryngeal/cricoid/RLN/esophageal) is a
--   pT-staging assertion. Imaging notes describe radiographic suspicion
--   only and cannot anchor pT4a; they introduced noise (false positives,
--   speculative wording) into LLM extractions. HP and DC_SUM notes are
--   summary-of-care narratives that re-state what the operative/path notes
--   already capture. Restricting the table to OPNOTE + synoptic_pathology
--   (gross + micro) matches the staging-evidence grain the downstream
--   cohort needs.
--
-- Pre-state (canonical_airway_invasion_events_v1):
--   6,054 rows across 5 note_types
--     ct_imaging          : 1,458   (dropped)
--     HP                  : 1,401   (dropped)
--     DC_SUM              :    38   (dropped)
--     OPNOTE              : 1,936   (kept)
--     synoptic_pathology  : 1,219   (kept)
--   Phantom rows (all 7 clinical findings = NULL): 2 (also dropped)
--
-- Post-state:
--   3,155 rows / 2,622 patients
--   note_type IN ('OPNOTE', 'synoptic_pathology') only
--   196 rows with at least one positive finding (review CSV in
--   verification_csvs/canonical_airway_invasion_events_v1/
--   positive_findings_review.csv)
--
-- Net delete:
--   2,899 rows (1,458 + 1,401 + 38 + 2 phantom)
--
-- Audit log: not written. Per Logan's standing rule on no-change /
-- structural-filter rows ("no need for an audit trail, just get rid of
-- those values"), the LLM extractions on imaging/HP/DC_SUM are out-of-scope
-- by definition rather than corrections. The deletion is reproducible from
-- the raw CPM via the airway-extraction pipeline + this filter.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 80a: drop imaging + summary note_types (out of scope for pT staging)
DELETE FROM main.canonical_airway_invasion_events_v1
WHERE note_type IN ('ct_imaging', 'HP', 'DC_SUM');

-- 80b: drop 2 phantom rows (all 7 clinical findings NULL; no extracted signal)
DELETE FROM main.canonical_airway_invasion_events_v1
WHERE tracheal_invasion   IS NULL
  AND laryngeal_invasion  IS NULL
  AND cricoid_invasion    IS NULL
  AND rln_invasion        IS NULL
  AND rln_paralysis_preop IS NULL
  AND esophageal_invasion IS NULL
  AND t4a_implication     IS NULL;

-- =============================================================================
-- end of migration 80
-- Next: Logan reviews positive_findings_review.csv (196 rows) -> mig_81
--       applies decisions; then mechanical evidence_quote source-compare
--       (sample) -> mig_82 sign-off.
-- =============================================================================
