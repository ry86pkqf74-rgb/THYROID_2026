-- =============================================================================
-- Migration 175b — mig_136 DAYS-SEMANTIC OPTION B RATIFICATION + CF CLEANUP
-- =============================================================================
-- Date: 2026-04-29
-- Batch: mig_175b_mig136_days_semantic_option_b_ratification_20260429
-- Posture: registry-notes only. No data writes; no schema changes; no status flips.
-- Author: Cowork (Path-C-applied directly per §6.3 — Cowork-authored registry-only).
-- Source decision package: qc_framework_v1/reports/mig_175_mig136_days_semantic_adjudication_20260429.md
-- Source probe SQL: qc_framework_v1/migrations/175_days_semantic_probes_20260429.sql
-- Logan ratified Option B at: 2026-04-29 (Cowork chat post-mig_175 verification)
-- Closes: CF-mig136-DAYS-SEMANTIC (58 col-impact) by sub-type disposition.
-- Target DB: thyroid_canonical_publication_v1_0
-- Target table: main.canonical_column_verification_registry_v1
-- =============================================================================
--
-- mig_175 verified-against-live-MD findings (Cowork Path-C, 2026-04-29):
--   * 58 PM cols carry CF-mig136-DAYS-SEMANTIC; all verification_status='verified'.
--   * Sub-type breakdown:
--       21 BOOLEAN PMH/family-history flag    (no date anchor — CF misclassified)
--       20 count metric (no date anchor)      (no date anchor — CF misclassified)
--        4 categorical/provenance text        (no date anchor — CF misclassified)
--        1 confidence score                   (no date anchor — CF misclassified)
--        6 source first/event date            (paired source date, not an offset)
--        6 date-derived *_days_from_surg col  (actual day-offset metric)
--   * Across the 6 day-offset cols, current values match
--       DATE_DIFF('day', first_surgery_date, event_date)
--     with 0 patient-level and 0 patient-column-cell mismatches (1,062 patients).
--   * Option A (event-start anchor) would change 914 pts / 1,668 cells.
--   * Option C (LKA anchor)         would change 877 pts / 1,609 cells.
--
-- Logan's ratification (2026-04-29):
--   "Option B / recommended: Preserve current first_surgery_date anchoring for
--    the 6 actual *_days_from_surg metrics; reclass non-anchor fields out of
--    CF-mig136-DAYS-SEMANTIC in mig_175b."
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Section A — pre-snapshot the 58 affected col-registry rows.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig175b_20260429 AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig175b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND notes ILIKE '%CF-mig136-DAYS-SEMANTIC%';
-- Expected snapshot row count: 58.


-- -----------------------------------------------------------------------------
-- Section B1 — 6 day-offset cols: ratify Option B formula and close the CF.
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_175b: Option B ratified by Logan 2026-04-29 — '
            || 'formula = DATE_DIFF(''day'', CAST(first_surgery_date AS DATE), CAST(event_date AS DATE)). '
            || 'Live MD recompute showed 0 patient-cell mismatches vs current stored values across all 6 *_days_from_surg cols '
            || '(914 pts / 1,668 cells would have changed under Option A event-start; 877 pts / 1,609 cells under Option C LKA). '
            || 'Convention: negative = pre-operative PMH mention; positive = post-operative. '
            || 'CF-mig136-DAYS-SEMANTIC CLOSED for this col (anchor convention ratified, current values preserved as-is).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    'pmhx_nlp_diabetes_first_days_from_surg',
    'pmhx_nlp_hypertension_first_days_from_surg',
    'pmhx_nlp_hyperthyroidism_first_days_from_surg',
    'pmhx_nlp_hypothyroidism_first_days_from_surg',
    'pmhx_nlp_obesity_first_days_from_surg',
    'pmhx_nlp_radiation_exposure_days_from_surg'
  )
  AND notes ILIKE '%CF-mig136-DAYS-SEMANTIC%';
-- Expected rows touched: 6.


-- -----------------------------------------------------------------------------
-- Section B2 — 6 source first/event date cols: paired-source disposition.
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_175b: Source first/event date column (paired with the corresponding *_days_from_surg col). '
            || 'Anchor decision is not applicable — the column IS the event date, not an offset from one. '
            || 'CF-mig136-DAYS-SEMANTIC CLOSED for this col (misclassification: source date, not a day-offset metric).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    'pmhx_nlp_diabetes_first_date',
    'pmhx_nlp_hypertension_first_date',
    'pmhx_nlp_hyperthyroidism_first_date',
    'pmhx_nlp_hypothyroidism_first_date',
    'pmhx_nlp_obesity_first_date',
    'pmhx_nlp_radiation_exposure_date'
  )
  AND notes ILIKE '%CF-mig136-DAYS-SEMANTIC%';
-- Expected rows touched: 6.


-- -----------------------------------------------------------------------------
-- Section B3 — 46 non-anchor cols: CF reclassification (misclassified by mig_136).
-- Sub-types: 21 BOOLEAN PMH/family-history flag + 20 count metric +
--             4 categorical/provenance text +  1 confidence score = 46.
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_175b: Non-anchor column — sub-type is one of {boolean PMH/family-history flag, '
            || 'count metric, categorical/provenance text, confidence score}. '
            || 'Date anchor decision is not applicable to this column. '
            || 'CF-mig136-DAYS-SEMANTIC CLOSED for this col '
            || '(misclassification by mig_136 cluster batch — the cluster note tagged all 58 PMH-NLP cols with the '
            || 'days-semantic CF, but only 6 are actual *_days_from_surg metrics; see mig_175 report).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND notes ILIKE '%CF-mig136-DAYS-SEMANTIC%'
  AND column_name NOT IN (
    -- 6 day-offset cols (handled by B1)
    'pmhx_nlp_diabetes_first_days_from_surg',
    'pmhx_nlp_hypertension_first_days_from_surg',
    'pmhx_nlp_hyperthyroidism_first_days_from_surg',
    'pmhx_nlp_hypothyroidism_first_days_from_surg',
    'pmhx_nlp_obesity_first_days_from_surg',
    'pmhx_nlp_radiation_exposure_days_from_surg',
    -- 6 source first/event date cols (handled by B2)
    'pmhx_nlp_diabetes_first_date',
    'pmhx_nlp_hypertension_first_date',
    'pmhx_nlp_hyperthyroidism_first_date',
    'pmhx_nlp_hypothyroidism_first_date',
    'pmhx_nlp_obesity_first_date',
    'pmhx_nlp_radiation_exposure_date'
  );
-- Expected rows touched: 46.


-- -----------------------------------------------------------------------------
-- Section C — PM table-level signoff_migration pointer: NO CHANGE.
-- -----------------------------------------------------------------------------
-- mig_175b does not flip any column verification_status; n_verified/n_na/n_not_started
-- on canonical_patient_master are unchanged at 1441/13/144 (1598 total). PM remains
-- in_progress per its existing signoff_migration pointer. No table-registry write.


-- -----------------------------------------------------------------------------
-- Section D — post-state verification probes (commented; run via query).
-- -----------------------------------------------------------------------------
-- D1: 58 rows now carry both the original CF tag and the mig_175b closure note.
-- SELECT COUNT(*) AS n_with_175b_closure
-- FROM main.canonical_column_verification_registry_v1
-- WHERE schema_name='main' AND table_name='canonical_patient_master'
--   AND notes ILIKE '%CF-mig136-DAYS-SEMANTIC%'
--   AND notes ILIKE '%mig_175b%';
-- Expect: 58.

-- D2: Sub-type-specific closure-note text presence.
-- SELECT
--   SUM(CASE WHEN notes ILIKE '%mig_175b: Option B ratified%' THEN 1 ELSE 0 END) AS n_b1_day_offset,
--   SUM(CASE WHEN notes ILIKE '%mig_175b: Source first/event date%' THEN 1 ELSE 0 END) AS n_b2_source_date,
--   SUM(CASE WHEN notes ILIKE '%mig_175b: Non-anchor column%' THEN 1 ELSE 0 END) AS n_b3_non_anchor
-- FROM main.canonical_column_verification_registry_v1
-- WHERE schema_name='main' AND table_name='canonical_patient_master'
--   AND notes ILIKE '%CF-mig136-DAYS-SEMANTIC%';
-- Expect: 6 / 6 / 46.

-- D3: 5-gate audit unchanged.
-- (See qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql.)
-- Expect gate1=165 / 0 / 0 / 0 / 21 (no change vs pre-mig_175b state).

-- D4: PM signoff registry row unchanged.
-- SELECT n_verified, n_na, n_not_started, n_failed, n_columns_total, table_status
-- FROM main.canonical_table_signoff_registry_v1
-- WHERE table_name='canonical_patient_master';
-- Expect: 1441 / 13 / 144 / 0 / 1598 / in_progress.

-- End mig_175b.
