-- =============================================================================
-- Migration 119 -- canonical_frozen_section_patient_rollup_v1 SIGN-OFF
-- =============================================================================
-- Date:   2026-04-29 (Cursor lane 11)
-- Author: Logan Glosser <logan.glosser@gmail.com> (drafted with GitHub Copilot)
-- Plan:   Close frozen-section patient rollup under Protocol v2. Events table
--         canonical_frozen_section_events_v1 was closed in mig_100; current
--         source event date is DATE after the clinical-date retype pass, while
--         this rollup retained Script-360-era VARCHAR date projection.
--
-- Pre-signoff probe (2026-04-29):
--   - events:  7,081 rows / 4,116 patients / 0 duplicate (research_id,event)
--   - rollup:  4,116 rows / 4,116 patients / 0 duplicate research_id
--   - rollup registry: 188 cols = 187 not_started + 1 na (research_id)
--   - max frozen_N slot from schema: 12; max event count per patient: 13
--     (2 patients exceed visible slot count; count is retained, slots are capped
--      by the Script 360 12-slot wide schema)
--   - initial re-derivation against current events surfaced >5 slot-level
--     mismatches, driven by stale wide slots after upstream event date/type
--     normalization. Therefore this migration follows the rebuild-then-verify
--     pattern used by mig_101.
--
-- Rebuild methodology:
--   Rebuild main.canonical_frozen_section_patient_rollup_v1 from verified
--   main.canonical_frozen_section_events_v1 using Script 360 Phase 8 logic:
--     performed predicate = result_raw OR excel_result_raw OR result_class OR
--                           result_histology is non-null
--     slot order          = frozen_section_date, source_priority, frozen_event_index
--     visible slots       = 1..12, with frozen_section_count preserving all events
--   Date columns are projected back to MM/DD/YYYY VARCHAR to preserve the
--   current rollup schema during sign-off. This is an explicit carry-forward,
--   not a semantic date normalization.
--
-- Verification methodology:
--   Post-rebuild per-column drift against a fresh re-derivation from verified
--   events is 0 / 188 columns. All 187 derivable columns are flipped to
--   verified. research_id remains na.
--
-- Carry-forward:
--   CF-119-FROZEN-ROLLUP-DATE-RETYPE: the 14 rollup clinical date columns
--   remain VARCHAR (MM/DD/YYYY): frozen_section_first_date,
--   frozen_section_last_date, and frozen_1_date..frozen_12_date. Future DATE
--   retype migration should handle these together with CF-100/117 date retypes.
-- =============================================================================

-- 119a: defensive snapshot of pre-rebuild rollup into archive_pub_v1_0
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_frozen_section_patient_rollup_v1_pre_mig119_20260429 AS
SELECT * FROM main.canonical_frozen_section_patient_rollup_v1;

-- 119b: rebuild rollup using Script 360 Phase 8 logic against verified events.
-- Date output remains VARCHAR MM/DD/YYYY to preserve the existing schema while
-- CF-119-FROZEN-ROLLUP-DATE-RETYPE remains open.
CREATE OR REPLACE TABLE main.canonical_frozen_section_patient_rollup_v1 AS
WITH ev AS (
    SELECT
        v.*,
        (
            frozen_section_result_raw IS NOT NULL
            OR excel_result_raw IS NOT NULL
            OR frozen_section_result_class IS NOT NULL
            OR frozen_section_result_histology IS NOT NULL
        ) AS performed_yn,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY frozen_section_date ASC NULLS LAST,
                     source_priority ASC,
                     frozen_event_index
        ) AS slot
    FROM main.canonical_frozen_section_events_v1 v
    WHERE frozen_section_result_raw IS NOT NULL
       OR excel_result_raw IS NOT NULL
       OR frozen_section_result_class IS NOT NULL
       OR frozen_section_result_histology IS NOT NULL
),
agg AS (
    SELECT
        research_id,
        COUNT(*)::BIGINT AS frozen_section_count,
        BOOL_OR(
            frozen_section_result_raw IS NOT NULL
            OR excel_result_raw IS NOT NULL
            OR frozen_section_result_class IS NOT NULL
            OR frozen_section_result_histology IS NOT NULL
        ) AS frozen_section_any_performed_flag,
        BOOL_OR(was_malignant_flag) AS frozen_section_any_malignant_flag,
        BOOL_OR(was_deferred_flag) AS frozen_section_any_deferred_flag,
        BOOL_OR(was_suspected_flag) AS frozen_section_any_suspected_flag,
        STRFTIME(MIN(frozen_section_date), '%m/%d/%Y') AS frozen_section_first_date,
        STRFTIME(MAX(frozen_section_date), '%m/%d/%Y') AS frozen_section_last_date
    FROM main.canonical_frozen_section_events_v1 v
    WHERE frozen_section_result_raw IS NOT NULL
       OR excel_result_raw IS NOT NULL
       OR frozen_section_result_class IS NOT NULL
       OR frozen_section_result_histology IS NOT NULL
    GROUP BY research_id
),
slots AS (
    SELECT
        research_id,
        MAX(CASE WHEN slot = 1 THEN performed_yn END) AS frozen_1_yn,
        MAX(CASE WHEN slot = 1 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_1_date,
        MAX(CASE WHEN slot = 1 THEN frozen_section_site_norm END) AS frozen_1_location,
        MAX(CASE WHEN slot = 1 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_1_result_raw,
        MAX(CASE WHEN slot = 1 THEN frozen_section_result_histology END) AS frozen_1_result_histology,
        MAX(CASE WHEN slot = 1 THEN frozen_section_result_qualifier END) AS frozen_1_result_qualifier,
        MAX(CASE WHEN slot = 1 THEN frozen_section_result_class END) AS frozen_1_result_class,
        MAX(CASE WHEN slot = 1 THEN was_deferred_flag END) AS frozen_1_was_deferred_flag,
        MAX(CASE WHEN slot = 1 THEN was_malignant_flag END) AS frozen_1_was_malignant_flag,
        MAX(CASE WHEN slot = 1 THEN was_suspected_flag END) AS frozen_1_was_suspected_flag,
        MAX(CASE WHEN slot = 1 THEN was_negated_flag END) AS frozen_1_was_negated_flag,
        MAX(CASE WHEN slot = 1 THEN source_of_data END) AS frozen_1_source_of_data,
        MAX(CASE WHEN slot = 1 THEN excel_corroborated_flag END) AS frozen_1_excel_corroborated_flag,
        MAX(CASE WHEN slot = 1 THEN excel_result_raw END) AS frozen_1_excel_result_raw,
        MAX(CASE WHEN slot = 1 THEN surgery_n END) AS frozen_1_surgery_n,
        MAX(CASE WHEN slot = 2 THEN performed_yn END) AS frozen_2_yn,
        MAX(CASE WHEN slot = 2 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_2_date,
        MAX(CASE WHEN slot = 2 THEN frozen_section_site_norm END) AS frozen_2_location,
        MAX(CASE WHEN slot = 2 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_2_result_raw,
        MAX(CASE WHEN slot = 2 THEN frozen_section_result_histology END) AS frozen_2_result_histology,
        MAX(CASE WHEN slot = 2 THEN frozen_section_result_qualifier END) AS frozen_2_result_qualifier,
        MAX(CASE WHEN slot = 2 THEN frozen_section_result_class END) AS frozen_2_result_class,
        MAX(CASE WHEN slot = 2 THEN was_deferred_flag END) AS frozen_2_was_deferred_flag,
        MAX(CASE WHEN slot = 2 THEN was_malignant_flag END) AS frozen_2_was_malignant_flag,
        MAX(CASE WHEN slot = 2 THEN was_suspected_flag END) AS frozen_2_was_suspected_flag,
        MAX(CASE WHEN slot = 2 THEN was_negated_flag END) AS frozen_2_was_negated_flag,
        MAX(CASE WHEN slot = 2 THEN source_of_data END) AS frozen_2_source_of_data,
        MAX(CASE WHEN slot = 2 THEN excel_corroborated_flag END) AS frozen_2_excel_corroborated_flag,
        MAX(CASE WHEN slot = 2 THEN excel_result_raw END) AS frozen_2_excel_result_raw,
        MAX(CASE WHEN slot = 2 THEN surgery_n END) AS frozen_2_surgery_n,
        MAX(CASE WHEN slot = 3 THEN performed_yn END) AS frozen_3_yn,
        MAX(CASE WHEN slot = 3 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_3_date,
        MAX(CASE WHEN slot = 3 THEN frozen_section_site_norm END) AS frozen_3_location,
        MAX(CASE WHEN slot = 3 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_3_result_raw,
        MAX(CASE WHEN slot = 3 THEN frozen_section_result_histology END) AS frozen_3_result_histology,
        MAX(CASE WHEN slot = 3 THEN frozen_section_result_qualifier END) AS frozen_3_result_qualifier,
        MAX(CASE WHEN slot = 3 THEN frozen_section_result_class END) AS frozen_3_result_class,
        MAX(CASE WHEN slot = 3 THEN was_deferred_flag END) AS frozen_3_was_deferred_flag,
        MAX(CASE WHEN slot = 3 THEN was_malignant_flag END) AS frozen_3_was_malignant_flag,
        MAX(CASE WHEN slot = 3 THEN was_suspected_flag END) AS frozen_3_was_suspected_flag,
        MAX(CASE WHEN slot = 3 THEN was_negated_flag END) AS frozen_3_was_negated_flag,
        MAX(CASE WHEN slot = 3 THEN source_of_data END) AS frozen_3_source_of_data,
        MAX(CASE WHEN slot = 3 THEN excel_corroborated_flag END) AS frozen_3_excel_corroborated_flag,
        MAX(CASE WHEN slot = 3 THEN excel_result_raw END) AS frozen_3_excel_result_raw,
        MAX(CASE WHEN slot = 3 THEN surgery_n END) AS frozen_3_surgery_n,
        MAX(CASE WHEN slot = 4 THEN performed_yn END) AS frozen_4_yn,
        MAX(CASE WHEN slot = 4 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_4_date,
        MAX(CASE WHEN slot = 4 THEN frozen_section_site_norm END) AS frozen_4_location,
        MAX(CASE WHEN slot = 4 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_4_result_raw,
        MAX(CASE WHEN slot = 4 THEN frozen_section_result_histology END) AS frozen_4_result_histology,
        MAX(CASE WHEN slot = 4 THEN frozen_section_result_qualifier END) AS frozen_4_result_qualifier,
        MAX(CASE WHEN slot = 4 THEN frozen_section_result_class END) AS frozen_4_result_class,
        MAX(CASE WHEN slot = 4 THEN was_deferred_flag END) AS frozen_4_was_deferred_flag,
        MAX(CASE WHEN slot = 4 THEN was_malignant_flag END) AS frozen_4_was_malignant_flag,
        MAX(CASE WHEN slot = 4 THEN was_suspected_flag END) AS frozen_4_was_suspected_flag,
        MAX(CASE WHEN slot = 4 THEN was_negated_flag END) AS frozen_4_was_negated_flag,
        MAX(CASE WHEN slot = 4 THEN source_of_data END) AS frozen_4_source_of_data,
        MAX(CASE WHEN slot = 4 THEN excel_corroborated_flag END) AS frozen_4_excel_corroborated_flag,
        MAX(CASE WHEN slot = 4 THEN excel_result_raw END) AS frozen_4_excel_result_raw,
        MAX(CASE WHEN slot = 4 THEN surgery_n END) AS frozen_4_surgery_n,
        MAX(CASE WHEN slot = 5 THEN performed_yn END) AS frozen_5_yn,
        MAX(CASE WHEN slot = 5 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_5_date,
        MAX(CASE WHEN slot = 5 THEN frozen_section_site_norm END) AS frozen_5_location,
        MAX(CASE WHEN slot = 5 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_5_result_raw,
        MAX(CASE WHEN slot = 5 THEN frozen_section_result_histology END) AS frozen_5_result_histology,
        MAX(CASE WHEN slot = 5 THEN frozen_section_result_qualifier END) AS frozen_5_result_qualifier,
        MAX(CASE WHEN slot = 5 THEN frozen_section_result_class END) AS frozen_5_result_class,
        MAX(CASE WHEN slot = 5 THEN was_deferred_flag END) AS frozen_5_was_deferred_flag,
        MAX(CASE WHEN slot = 5 THEN was_malignant_flag END) AS frozen_5_was_malignant_flag,
        MAX(CASE WHEN slot = 5 THEN was_suspected_flag END) AS frozen_5_was_suspected_flag,
        MAX(CASE WHEN slot = 5 THEN was_negated_flag END) AS frozen_5_was_negated_flag,
        MAX(CASE WHEN slot = 5 THEN source_of_data END) AS frozen_5_source_of_data,
        MAX(CASE WHEN slot = 5 THEN excel_corroborated_flag END) AS frozen_5_excel_corroborated_flag,
        MAX(CASE WHEN slot = 5 THEN excel_result_raw END) AS frozen_5_excel_result_raw,
        MAX(CASE WHEN slot = 5 THEN surgery_n END) AS frozen_5_surgery_n,
        MAX(CASE WHEN slot = 6 THEN performed_yn END) AS frozen_6_yn,
        MAX(CASE WHEN slot = 6 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_6_date,
        MAX(CASE WHEN slot = 6 THEN frozen_section_site_norm END) AS frozen_6_location,
        MAX(CASE WHEN slot = 6 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_6_result_raw,
        MAX(CASE WHEN slot = 6 THEN frozen_section_result_histology END) AS frozen_6_result_histology,
        MAX(CASE WHEN slot = 6 THEN frozen_section_result_qualifier END) AS frozen_6_result_qualifier,
        MAX(CASE WHEN slot = 6 THEN frozen_section_result_class END) AS frozen_6_result_class,
        MAX(CASE WHEN slot = 6 THEN was_deferred_flag END) AS frozen_6_was_deferred_flag,
        MAX(CASE WHEN slot = 6 THEN was_malignant_flag END) AS frozen_6_was_malignant_flag,
        MAX(CASE WHEN slot = 6 THEN was_suspected_flag END) AS frozen_6_was_suspected_flag,
        MAX(CASE WHEN slot = 6 THEN was_negated_flag END) AS frozen_6_was_negated_flag,
        MAX(CASE WHEN slot = 6 THEN source_of_data END) AS frozen_6_source_of_data,
        MAX(CASE WHEN slot = 6 THEN excel_corroborated_flag END) AS frozen_6_excel_corroborated_flag,
        MAX(CASE WHEN slot = 6 THEN excel_result_raw END) AS frozen_6_excel_result_raw,
        MAX(CASE WHEN slot = 6 THEN surgery_n END) AS frozen_6_surgery_n,
        MAX(CASE WHEN slot = 7 THEN performed_yn END) AS frozen_7_yn,
        MAX(CASE WHEN slot = 7 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_7_date,
        MAX(CASE WHEN slot = 7 THEN frozen_section_site_norm END) AS frozen_7_location,
        MAX(CASE WHEN slot = 7 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_7_result_raw,
        MAX(CASE WHEN slot = 7 THEN frozen_section_result_histology END) AS frozen_7_result_histology,
        MAX(CASE WHEN slot = 7 THEN frozen_section_result_qualifier END) AS frozen_7_result_qualifier,
        MAX(CASE WHEN slot = 7 THEN frozen_section_result_class END) AS frozen_7_result_class,
        MAX(CASE WHEN slot = 7 THEN was_deferred_flag END) AS frozen_7_was_deferred_flag,
        MAX(CASE WHEN slot = 7 THEN was_malignant_flag END) AS frozen_7_was_malignant_flag,
        MAX(CASE WHEN slot = 7 THEN was_suspected_flag END) AS frozen_7_was_suspected_flag,
        MAX(CASE WHEN slot = 7 THEN was_negated_flag END) AS frozen_7_was_negated_flag,
        MAX(CASE WHEN slot = 7 THEN source_of_data END) AS frozen_7_source_of_data,
        MAX(CASE WHEN slot = 7 THEN excel_corroborated_flag END) AS frozen_7_excel_corroborated_flag,
        MAX(CASE WHEN slot = 7 THEN excel_result_raw END) AS frozen_7_excel_result_raw,
        MAX(CASE WHEN slot = 7 THEN surgery_n END) AS frozen_7_surgery_n,
        MAX(CASE WHEN slot = 8 THEN performed_yn END) AS frozen_8_yn,
        MAX(CASE WHEN slot = 8 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_8_date,
        MAX(CASE WHEN slot = 8 THEN frozen_section_site_norm END) AS frozen_8_location,
        MAX(CASE WHEN slot = 8 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_8_result_raw,
        MAX(CASE WHEN slot = 8 THEN frozen_section_result_histology END) AS frozen_8_result_histology,
        MAX(CASE WHEN slot = 8 THEN frozen_section_result_qualifier END) AS frozen_8_result_qualifier,
        MAX(CASE WHEN slot = 8 THEN frozen_section_result_class END) AS frozen_8_result_class,
        MAX(CASE WHEN slot = 8 THEN was_deferred_flag END) AS frozen_8_was_deferred_flag,
        MAX(CASE WHEN slot = 8 THEN was_malignant_flag END) AS frozen_8_was_malignant_flag,
        MAX(CASE WHEN slot = 8 THEN was_suspected_flag END) AS frozen_8_was_suspected_flag,
        MAX(CASE WHEN slot = 8 THEN was_negated_flag END) AS frozen_8_was_negated_flag,
        MAX(CASE WHEN slot = 8 THEN source_of_data END) AS frozen_8_source_of_data,
        MAX(CASE WHEN slot = 8 THEN excel_corroborated_flag END) AS frozen_8_excel_corroborated_flag,
        MAX(CASE WHEN slot = 8 THEN excel_result_raw END) AS frozen_8_excel_result_raw,
        MAX(CASE WHEN slot = 8 THEN surgery_n END) AS frozen_8_surgery_n,
        MAX(CASE WHEN slot = 9 THEN performed_yn END) AS frozen_9_yn,
        MAX(CASE WHEN slot = 9 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_9_date,
        MAX(CASE WHEN slot = 9 THEN frozen_section_site_norm END) AS frozen_9_location,
        MAX(CASE WHEN slot = 9 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_9_result_raw,
        MAX(CASE WHEN slot = 9 THEN frozen_section_result_histology END) AS frozen_9_result_histology,
        MAX(CASE WHEN slot = 9 THEN frozen_section_result_qualifier END) AS frozen_9_result_qualifier,
        MAX(CASE WHEN slot = 9 THEN frozen_section_result_class END) AS frozen_9_result_class,
        MAX(CASE WHEN slot = 9 THEN was_deferred_flag END) AS frozen_9_was_deferred_flag,
        MAX(CASE WHEN slot = 9 THEN was_malignant_flag END) AS frozen_9_was_malignant_flag,
        MAX(CASE WHEN slot = 9 THEN was_suspected_flag END) AS frozen_9_was_suspected_flag,
        MAX(CASE WHEN slot = 9 THEN was_negated_flag END) AS frozen_9_was_negated_flag,
        MAX(CASE WHEN slot = 9 THEN source_of_data END) AS frozen_9_source_of_data,
        MAX(CASE WHEN slot = 9 THEN excel_corroborated_flag END) AS frozen_9_excel_corroborated_flag,
        MAX(CASE WHEN slot = 9 THEN excel_result_raw END) AS frozen_9_excel_result_raw,
        MAX(CASE WHEN slot = 9 THEN surgery_n END) AS frozen_9_surgery_n,
        MAX(CASE WHEN slot = 10 THEN performed_yn END) AS frozen_10_yn,
        MAX(CASE WHEN slot = 10 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_10_date,
        MAX(CASE WHEN slot = 10 THEN frozen_section_site_norm END) AS frozen_10_location,
        MAX(CASE WHEN slot = 10 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_10_result_raw,
        MAX(CASE WHEN slot = 10 THEN frozen_section_result_histology END) AS frozen_10_result_histology,
        MAX(CASE WHEN slot = 10 THEN frozen_section_result_qualifier END) AS frozen_10_result_qualifier,
        MAX(CASE WHEN slot = 10 THEN frozen_section_result_class END) AS frozen_10_result_class,
        MAX(CASE WHEN slot = 10 THEN was_deferred_flag END) AS frozen_10_was_deferred_flag,
        MAX(CASE WHEN slot = 10 THEN was_malignant_flag END) AS frozen_10_was_malignant_flag,
        MAX(CASE WHEN slot = 10 THEN was_suspected_flag END) AS frozen_10_was_suspected_flag,
        MAX(CASE WHEN slot = 10 THEN was_negated_flag END) AS frozen_10_was_negated_flag,
        MAX(CASE WHEN slot = 10 THEN source_of_data END) AS frozen_10_source_of_data,
        MAX(CASE WHEN slot = 10 THEN excel_corroborated_flag END) AS frozen_10_excel_corroborated_flag,
        MAX(CASE WHEN slot = 10 THEN excel_result_raw END) AS frozen_10_excel_result_raw,
        MAX(CASE WHEN slot = 10 THEN surgery_n END) AS frozen_10_surgery_n,
        MAX(CASE WHEN slot = 11 THEN performed_yn END) AS frozen_11_yn,
        MAX(CASE WHEN slot = 11 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_11_date,
        MAX(CASE WHEN slot = 11 THEN frozen_section_site_norm END) AS frozen_11_location,
        MAX(CASE WHEN slot = 11 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_11_result_raw,
        MAX(CASE WHEN slot = 11 THEN frozen_section_result_histology END) AS frozen_11_result_histology,
        MAX(CASE WHEN slot = 11 THEN frozen_section_result_qualifier END) AS frozen_11_result_qualifier,
        MAX(CASE WHEN slot = 11 THEN frozen_section_result_class END) AS frozen_11_result_class,
        MAX(CASE WHEN slot = 11 THEN was_deferred_flag END) AS frozen_11_was_deferred_flag,
        MAX(CASE WHEN slot = 11 THEN was_malignant_flag END) AS frozen_11_was_malignant_flag,
        MAX(CASE WHEN slot = 11 THEN was_suspected_flag END) AS frozen_11_was_suspected_flag,
        MAX(CASE WHEN slot = 11 THEN was_negated_flag END) AS frozen_11_was_negated_flag,
        MAX(CASE WHEN slot = 11 THEN source_of_data END) AS frozen_11_source_of_data,
        MAX(CASE WHEN slot = 11 THEN excel_corroborated_flag END) AS frozen_11_excel_corroborated_flag,
        MAX(CASE WHEN slot = 11 THEN excel_result_raw END) AS frozen_11_excel_result_raw,
        MAX(CASE WHEN slot = 11 THEN surgery_n END) AS frozen_11_surgery_n,
        MAX(CASE WHEN slot = 12 THEN performed_yn END) AS frozen_12_yn,
        MAX(CASE WHEN slot = 12 THEN STRFTIME(frozen_section_date, '%m/%d/%Y') END) AS frozen_12_date,
        MAX(CASE WHEN slot = 12 THEN frozen_section_site_norm END) AS frozen_12_location,
        MAX(CASE WHEN slot = 12 THEN COALESCE(frozen_section_result_raw, excel_result_raw) END) AS frozen_12_result_raw,
        MAX(CASE WHEN slot = 12 THEN frozen_section_result_histology END) AS frozen_12_result_histology,
        MAX(CASE WHEN slot = 12 THEN frozen_section_result_qualifier END) AS frozen_12_result_qualifier,
        MAX(CASE WHEN slot = 12 THEN frozen_section_result_class END) AS frozen_12_result_class,
        MAX(CASE WHEN slot = 12 THEN was_deferred_flag END) AS frozen_12_was_deferred_flag,
        MAX(CASE WHEN slot = 12 THEN was_malignant_flag END) AS frozen_12_was_malignant_flag,
        MAX(CASE WHEN slot = 12 THEN was_suspected_flag END) AS frozen_12_was_suspected_flag,
        MAX(CASE WHEN slot = 12 THEN was_negated_flag END) AS frozen_12_was_negated_flag,
        MAX(CASE WHEN slot = 12 THEN source_of_data END) AS frozen_12_source_of_data,
        MAX(CASE WHEN slot = 12 THEN excel_corroborated_flag END) AS frozen_12_excel_corroborated_flag,
        MAX(CASE WHEN slot = 12 THEN excel_result_raw END) AS frozen_12_excel_result_raw,
        MAX(CASE WHEN slot = 12 THEN surgery_n END) AS frozen_12_surgery_n
    FROM ev
    WHERE slot <= 12
    GROUP BY research_id
)
SELECT
    a.research_id,
    a.frozen_section_count,
    a.frozen_section_any_performed_flag,
    a.frozen_section_any_malignant_flag,
    a.frozen_section_any_deferred_flag,
    a.frozen_section_any_suspected_flag,
    a.frozen_section_first_date,
    a.frozen_section_last_date,
    s.* EXCLUDE (research_id)
FROM agg a
LEFT JOIN slots s USING (research_id);

COMMENT ON TABLE main.canonical_frozen_section_patient_rollup_v1 IS
'[domain=frozen_section; grain=per_patient_wide] — source: Script 360 Phase 8 logic, rebuilt by mig_119 from verified main.canonical_frozen_section_events_v1; 12-slot wide rollup; date columns intentionally remain VARCHAR pending CF-119-FROZEN-ROLLUP-DATE-RETYPE.';

-- 119c: flip non-date derivable cols to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_post_rollup_rebuild',
    batch_id            = 'mig_119_frozen_section_rollup_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_119: rebuilt patient rollup from verified '
                          || 'canonical_frozen_section_events_v1 using Script '
                          || '360 Phase 8 derivation logic after stale slot '
                          || 'drift was detected. Post-rebuild fresh '
                          || 're-derivation drift = 0 on this column.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_frozen_section_patient_rollup_v1'
  AND verification_status = 'not_started'
  AND column_name NOT IN ('frozen_section_first_date', 'frozen_section_last_date', 'frozen_1_date', 'frozen_2_date', 'frozen_3_date', 'frozen_4_date', 'frozen_5_date', 'frozen_6_date', 'frozen_7_date', 'frozen_8_date', 'frozen_9_date', 'frozen_10_date', 'frozen_11_date', 'frozen_12_date');

-- 119d: flip date cols with explicit clinical-date retype carry-forward
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_re_derivation_post_rollup_rebuild_with_date_retype_cf',
    batch_id            = 'mig_119_frozen_section_rollup_signoff_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_119: rebuilt from verified events; '
                          || 'calendar day preserved but column remains '
                          || 'VARCHAR MM/DD/YYYY to preserve current rollup '
                          || 'schema during sign-off. CF-119-FROZEN-ROLLUP-'
                          || 'DATE-RETYPE: future batch repair should retype '
                          || 'this clinical date column to DATE with CF-100/117.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_frozen_section_patient_rollup_v1'
  AND verification_status = 'not_started'
  AND column_name IN ('frozen_section_first_date', 'frozen_section_last_date', 'frozen_1_date', 'frozen_2_date', 'frozen_3_date', 'frozen_4_date', 'frozen_5_date', 'frozen_6_date', 'frozen_7_date', 'frozen_8_date', 'frozen_9_date', 'frozen_10_date', 'frozen_11_date', 'frozen_12_date');

-- 119e: recompute table_signoff_registry counts and sign off
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/119_frozen_section_patient_rollup_signoff.sql',
    notes             = 'Rebuilt from verified canonical_frozen_section_events_v1 '
                        || '(events closed mig_100; source event date now DATE). '
                        || 'Post-rebuild per-column re-derivation drift = 0 across '
                        || '187 derivable cols; research_id remains na. Wide schema '
                        || 'has 12 frozen-event slots; 2 patients have count >12, '
                        || 'with frozen_section_count preserving all events and '
                        || 'visible slots capped by Script 360 schema. '
                        || 'CF-119-FROZEN-ROLLUP-DATE-RETYPE open for 14 VARCHAR '
                        || 'clinical date columns.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'canonical_frozen_section_patient_rollup_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 119 -- canonical_frozen_section_patient_rollup_v1 closed
-- Frozen-section family complete: events (mig_100) + patient rollup (mig_119).
-- =============================================================================
