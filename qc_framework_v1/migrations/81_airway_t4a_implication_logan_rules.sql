-- =============================================================================
-- Migration 81 -- airway invasion t4a_implication, Logan rules round 1
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   First Logan-review pass on canonical_airway_invasion_events_v1's
--         positive subset (196 rows from mig_80). LLM was systematically
--         too cautious on t4a_implication, calling 'unable_to_determine'
--         whenever any uncertainty was present (shave margins, pre-op RLN
--         paralysis, no explicit operative statement). Logan's clinical
--         rule: those scenarios are NOT pT4a.
--
-- Logan directives (verbatim):
--   - 9 sample rows reviewed: each marked "do not upstage"
--   - 1. confirm  (Rule A: 56 rows → not_pT4a)
--   - 2. "The airway invasion and staging guidelines should be separate, we
--        are determining if airway invasion is present in one section, then
--        staging guidelines are separate." -> for 11077 specifically:
--        pathologist's 'pT Category pT4a' assertion does NOT override the
--        actual finding (shave margin); staging follows findings.
--   - 3. pT4a (8614 RLN sacrifice = real invasion).
--
-- Rule A (mig_81a) — bulk:
--   Within the 196 positive rows, where the row has NO full-thickness
--   invasion present anywhere AND t4a_implication is currently
--   'unable_to_determine', flip to 'not_pT4a'. The "positive" hits in
--   these rows are limited to:
--     - tracheal_invasion = 'shaved'  (adherence, not full-thickness)
--     - esophageal_invasion = 'shaved' (adherence, not full-thickness)
--     - rln_paralysis_preop = 'present' (pre-op vocal cord palsy is not
--                                        an operative invasion finding)
--   None of those upstage to pT4a per AJCC8.
--   Affected: 56 rows.
--
-- Edge case 11077 (mig_81b):
--   synoptic_pathology row, tracheal_invasion='shaved', t4a_implication=
--   'pT4a' because the pathologist literally wrote 'pT Category pT4a' in
--   the report. Per Logan, findings vs. staging are separate concerns.
--   The shave finding is recorded faithfully; the t4a column must follow
--   the finding-derived rule, so → not_pT4a.
--   Affected: 1 row.
--
-- Edge case 8614 (mig_81c):
--   OPNOTE row, rln_invasion='present' (left RLN sacrificed during
--   resection). LLM marked t4a as 'unable_to_determine' but RLN sacrifice
--   IS full-thickness RLN invasion = pT4a per AJCC8.
--   Affected: 1 row.
--
-- 6017 (no migration row needed):
--   Logan: pT4a. Already labeled pT4a, no change. Note: airway findings
--   all 'unknown'; pT4a is anchored by 'extrathyroidal extension into fat'
--   from the same synoptic note. Per directive #2 above, this raises a
--   future-cleanup question of whether a row whose only pT4a evidence is
--   non-airway belongs in the airway invasion table at all (deferred).
--
-- Net effect on the 196 positives:
--   pre-mig_81  : 138 pT4a / 1 not_pT4a / 57 unable_to_determine
--   post-mig_81 : 138 pT4a / 58 not_pT4a / 0 unable_to_determine
--                  (-1 pT4a from 11077 leaving, +1 pT4a from 8614 entering;
--                   net 0; +57 not_pT4a)
--
-- Carry-forward open questions:
--   CF-1: 6017-style rows (airway-table row whose pT4a is sourced from
--         non-airway ETE) — drop or keep? Decide at sign-off.
--   CF-2: t4a_implication column itself — should it be a deterministic
--         derivation rather than an LLM-extracted column? See memory:
--         feedback_findings_vs_staging.md.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 81a: Rule A — 56 rows, no full-thickness anywhere → not_pT4a
UPDATE main.canonical_airway_invasion_events_v1
SET t4a_implication = 'not_pT4a',
    reasoning = COALESCE(reasoning,'')
                || ' | mig_81a (Logan Rule A 2026-04-28): no full-thickness airway invasion documented; '
                || 'shave-only and/or pre-op RLN paralysis without explicit operative invasion do not upstage to pT4a.'
WHERE
  (tracheal_invasion IN ('present','shaved')
   OR laryngeal_invasion = 'present'
   OR cricoid_invasion = 'present'
   OR rln_invasion = 'present'
   OR rln_paralysis_preop = 'present'
   OR esophageal_invasion IN ('present','shaved')
   OR t4a_implication = 'pT4a')
  AND t4a_implication = 'unable_to_determine'
  AND (tracheal_invasion IS NULL OR tracheal_invasion IN ('absent','shaved','unknown'))
  AND (laryngeal_invasion IS NULL OR laryngeal_invasion IN ('absent','unknown'))
  AND (cricoid_invasion IS NULL OR cricoid_invasion IN ('absent','unknown'))
  AND (rln_invasion IS NULL OR rln_invasion IN ('absent','unknown'))
  AND (esophageal_invasion IS NULL OR esophageal_invasion IN ('absent','shaved','unknown'));

-- 81b: 11077 synoptic — pathologist's pT4a assertion overridden; finding is shave only
UPDATE main.canonical_airway_invasion_events_v1
SET t4a_implication = 'not_pT4a',
    reasoning = COALESCE(reasoning,'')
                || ' | mig_81b (Logan 2026-04-28): pathologist asserted pT4a on shave-margin '
                || 'finding only; per findings-vs-staging separation rule, staging follows finding '
                || '(shaved tracheal margin = not pT4a).'
WHERE research_id = '11077' AND note_type = 'synoptic_pathology' AND note_index = '1';

-- 81c: 8614 OPNOTE — RLN sacrificed = full-thickness RLN invasion → pT4a
UPDATE main.canonical_airway_invasion_events_v1
SET t4a_implication = 'pT4a',
    reasoning = COALESCE(reasoning,'')
                || ' | mig_81c (Logan 2026-04-28): RLN sacrificed during operation = '
                || 'full-thickness RLN invasion → pT4a per AJCC8.'
WHERE research_id = '8614' AND note_type = 'OPNOTE' AND note_index = '1';

-- =============================================================================
-- end of migration 81
-- Next: mig_82 = per-row Logan review of remaining 138 pT4a candidates,
--       then table sign-off. Build script:
--       qc_framework_v1/scripts/build_airway_pT4a_candidates_review.py
-- =============================================================================
