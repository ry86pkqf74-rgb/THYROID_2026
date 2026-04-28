-- =============================================================================
-- Migration 82 -- airway invasion CAP template-echo cleanup
-- =============================================================================
-- Date:   2026-04-28
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   The LLM (round-2) over-extracted on synoptic_pathology rows where
--         the only evidence was the CAP synoptic checklist OPTION TEXT for
--         pT4a. That option's label literally lists every airway/aerodigestive
--         structure ("subcutaneous soft tissues, larynx, trachea, esophagus
--         OR recurrent laryngeal nerve (i.e., pT4a)") because that IS the
--         AJCC pT4a definition. The "or" is disjunctive — the pathologist
--         checking that option means "at least one of these is invaded =
--         pT4a", not "all of these are invaded". The LLM saw the anatomic
--         words and lit up tracheal/laryngeal/rln/esophageal = 'present' on
--         every row.
--
-- Logan directives (verbatim 2026-04-28):
--   - "Looking at the first few examples; it looks like text from a
--      description of what pT4a would be are written as the reasoning from
--      the synoptic. So unless the pathologist called these pT4a in the
--      synoptic then that is just a description of what pT4a is rather than
--      evidence."
--   - "1. agree" (clean the 18 template-echo rows)
--   - "2. confirm the pathologists call rather than just text" (= keep
--      t4a_implication=pT4a, since the pathologist DID select that option;
--      the selection IS the staging call. Just don't pretend the option
--      label is anatomic-finding evidence.)
--
-- Affected rows (18 total, all synoptic_pathology):
--   16 pure template echo (mig_82a): reset all 4 findings to 'unknown'
--     research_ids: 6679, 7153, 7652, 8159, 8311, 8692, 9012, 10044, 10153,
--                   10386, 10475, 10711, 10949, 11118, 11241, 11944
--     (note: 10475 = abbreviated template "Invading subcutaneous soft
--      tissues, larynx, trachea" — same option, truncated)
--     (note: 9012 + 10386 also have OPNOTE rows with REAL findings; those
--      OPNOTE rows are untouched by this migration)
--
--   1 mixed real+template (mig_82b): 8449 — KEEP rln_invasion='present'
--     (real "Encases the left recurrent laryngeal nerve") + reset
--     trach/laryn/esoph to 'unknown' (template echo)
--
--   1 already-correct (no migration row needed): 9999 had real "Right
--     recurrent laryngeal nerve sacrificed" prefix; LLM already correctly
--     captured only rln_invasion='present'. Listed here for audit.
--
-- t4a_implication unchanged on all 18 rows: pathologist's checked-box
-- selection IS the pT4a call.
--
-- Net effect on 138-pT4a positive subset:
--   pre-mig_82  : 138 rows; 16 with all 4 findings='present' (template echo)
--   post-mig_82 : 138 rows; n_full_thickness=4 cluster reduced from 16 -> 0
--
-- Carry-forward / open question:
--   CF-3: After this cleanup, the 138-pT4a subset contains 17 rows whose
--         only real signal is the pathologist's pT4a stage selection
--         (no specific anatomic finding). Should those rows be marked
--         differently in downstream views (e.g. an anatomic_evidence flag)?
--         Defer to sign-off discussion.
--
-- Executed via Cowork query_rw 2026-04-28.
-- =============================================================================

-- 82a: 16 pure template-echo rows
UPDATE main.canonical_airway_invasion_events_v1
SET tracheal_invasion = 'unknown',
    laryngeal_invasion = 'unknown',
    rln_invasion = 'unknown',
    esophageal_invasion = 'unknown',
    reasoning = COALESCE(reasoning,'')
                || ' | mig_82a (Logan 2026-04-28): individual finding columns were reset to '
                || '''unknown'' because the LLM evidence quote was CAP synoptic checklist option '
                || 'text only ("Invading subcutaneous soft tissues, larynx, trachea, esophagus or '
                || 'recurrent laryngeal nerve (i.e., pT4a)" or close variant). That phrase is the '
                || 'AJCC pT4a definition tag — checking the option signals at-least-one structure '
                || 'invaded (= pT4a) but does not enumerate which specific structures. '
                || 't4a_implication=pT4a retained: confirming pathologist'
                || CHR(39) || 's call rather than just template text.'
WHERE note_type = 'synoptic_pathology'
  AND t4a_implication = 'pT4a'
  AND (research_id IN ('6679','7153','7652','8159','8311','8692','9012','10044','10153',
                       '10386','10711','10949','11118','11241','11944')
       OR research_id = '10475');

-- 82b: 8449 — keep real rln_invasion='present', reset other 3
UPDATE main.canonical_airway_invasion_events_v1
SET tracheal_invasion = 'unknown',
    laryngeal_invasion = 'unknown',
    esophageal_invasion = 'unknown',
    reasoning = COALESCE(reasoning,'')
                || ' | mig_82b (Logan 2026-04-28): rln_invasion=present is real ("Encases the '
                || 'left recurrent laryngeal nerve"); tracheal/laryngeal/esophageal=present were '
                || 'CAP template echo and reset to unknown. t4a_implication=pT4a retained.'
WHERE research_id = '8449' AND note_type = 'synoptic_pathology' AND note_index = '1';

-- =============================================================================
-- end of migration 82
-- Next: regenerate pT4a_candidates_review.csv (138 - 17 cleaned + 8449 + 9999
--       still need review = 121 substantive + 18 'pathologist call only'
--       rows). Logan reviews per-row -> mig_83 applies decisions.
-- =============================================================================
