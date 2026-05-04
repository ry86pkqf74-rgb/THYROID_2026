-- ============================================================================
-- mig_295 — M044 manuscript .docx v1.0 -> v1.1 numerical patch
-- Author : cursor_composer_mig295 (Cowork dispatch)
-- Date   : 2026-05-04
-- Closes : CF-M044-DOCX-V11-PATCH (newly opened by Cowork at HEAD 7279f23)
-- ============================================================================
--
-- Background
-- ----------
-- M044_submission_package_v1_0/02_manuscript.docx and 03_supplement.docx were
-- generated against frozen v1.0 numbers (cohort 4,128 / aOR 1.80 / Cox HR 2.34).
-- Cowork's v1.1 regenerate (round 11b at HEAD c3ef965) updated 04_tables.xlsx,
-- 06_figures/*, and 08_analysis_outputs/* to v1.1 numbers (cohort 4,012 /
-- aOR 2.08 / Cox HR 0.91, FLIPPED) per
-- `studies/m044_validation/m044_validation_summary.md` and
-- `manuscript_outputs/v1_0_20260501/M044_READY_FOR_WRITING_BRIEF_v1_1.md`.
-- The .docx body had only been partially re-patched (logistic + Cox numbers in
-- the Multivariable section were already at v1.1, but the cohort denominator
-- and Figure 1 / Figure 5 captions still cited v1.0 cells).
--
-- mig_295 patches the residual v1.0 cells via python-docx find/replace and
-- inserts a Discussion paragraph documenting the Cox-vs-logistic spec
-- sensitivity per CF-M044-COX-HR-FLIPPED Logan Option A disposition.
--
-- Files patched (Surgical git add):
--   M044_submission_package_v1_0/02_manuscript.docx
--   03_supplement.docx — verified no v1.0 tokens remain (revert if editor re-saved zip metadata only)
--
-- Per-cell diff (see scripts/output/mig_295_diff_table.md for the full table):
--   * Total cohort denominator             4,128 -> 4,012   (manuscript x9, supplement x1)
--   * Surgery-date completeness ratio      4,128/4,128 -> 4,012/4,012  (supplement x1)
--   * Figure 1 caption strict-DTC          (n = 3,789) -> (n = 3,750)
--   * Figure 1 caption 3-level analytic    (n = 3,756) -> (n = 3,750)
--   * Figure 1 caption Cox subset          Cox subset n = 2,025. -> Cox subset n = 2,511 (events = 178).
--   * Figure 5 caption logistic n/events   (n = 3,756; events = 139) -> (n = 3,750; events = 193)
--   * Discussion paragraph inserted at index 84 (after the existing paragraph
--     that mentions "the gross-vs-microscopic association moves toward the
--     crude gradient") with HR 0.91 / aOR 2.08 / 178 events / 2,511 n + Logan
--     Option A spec-sensitivity narrative.
--
-- Cells already at v1.1 (no edit needed; verified during apply):
--   * aOR 2.08 (95% CI 1.48-2.91; p=2.458e-05)             primary
--   * adjusted OR 0.67 (95% CI 0.32-1.40; p=0.2842)        no/neg vs micro
--   * HR 0.91 (95% CI 0.48-1.73; p=0.7741)                 Cox primary
--   * pseudo-R^2 0.1404; n=3750; events=193; LR chi^2 213.65
--   * RAI-retained sensitivity OR 1.59 (95% CI 1.12-2.26; p=0.009911)
--
-- Post-apply residual scan (scripts/output/mig_295_apply_log.txt):
--   none of {4,128 / 4128 / 1.80 / 2.34 / 3,789 / 3,756 / 2,025 / 139 events}
--   remain in either .docx (paragraphs + tables).
--   2026-05-04 follow-on: unformatted 4128 denominators required a second replace pass
--   ((n=4128) / 4128/4128 / n=0/4128) — see scripts/mig_295_apply_docx_patches.py.
--
-- Known prose residuals (NOT in mig_295 scope; routed to writing chat per
-- M044_READY_FOR_WRITING_BRIEF_v1_1.md):
--   * Per-ETE-group counts and percentages in body prose are still rendered
--     against the 4,128 v1.0 denominator (e.g. "microscopic ETE 2,576
--     (62.4%)", "73.4% female", "1,400 (33.9%) zero follow-up", crude
--     per-100-PY rates, mean ages, mean tumor sizes).  The writing chat
--     re-derives these from the now-v1.1 04_tables.xlsx in a follow-on pass;
--     mig_295 only patches the headline numerical cells listed in the
--     dispatch.
--   * Surgery-date stratum counts in the supplement (4,090 in 1999-2024;
--     3,717 strict-DTC) may also have shifted under v1.1 but are out of
--     mig_295 scope.
-- ----------------------------------------------------------------------------

USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
    'mig_295',
    CURRENT_TIMESTAMP,
    'cursor_composer_mig295',
    'mig_295: Patched M044_submission_package_v1_0/02_manuscript.docx (and verified 03_supplement.docx) '
    || 'from v1.0 to v1.1 headline numbers. Follow-on pass closed residual unformatted denominators '
    || '(n=4128 -> n=4,012; 4128/4128 -> 4,012/4,012; n=0/4128 -> n=0/4,012) missed when only '
    || '"4,128" (with comma) was replaced. Discussion sensitivity paragraph updated with '
    || 'median follow-up 3.2 years (Cox-eligible subset; from analytic parquet via build_cox_analytic_frame). '
    || 'Repro: scripts/mig_295_apply_docx_patches.py --apply. '
    || 'Apply log: scripts/output/mig_295_apply_log.txt; diff: scripts/output/mig_295_diff_table.md. '
    || 'Closes CF-M044-DOCX-V11-PATCH.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_295');

SELECT mig_id, signed_off_at, by_actor, substring(summary, 1, 140) AS summary_head
FROM main.signoff_migration
WHERE mig_id = 'mig_295';
