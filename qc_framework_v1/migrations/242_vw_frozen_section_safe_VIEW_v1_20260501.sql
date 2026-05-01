-- =============================================================================
-- mig_242: semantic_publication.vw_frozen_section_safe_VIEW_v1 (compact)
-- Batch id: mig_242_frozen_safe
-- verified_by: cursor_composer_mig_242
-- =============================================================================
-- Source: main.canonical_frozen_section_patient_rollup_v1 (4,116 rows;
--         188 cols; mig_160/mig_237 DATE types on first/last + slot dates).
-- Excludes all 12 per-slot date/result blocks — use the base rollup for
-- slot-level drill-down. This view is manuscript-facing patient summary only.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

-- ── Idempotent prep (re-run safe) ─────────────────────────────────────────

DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'semantic_publication'
  AND table_name  = 'vw_frozen_section_safe_VIEW_v1';

DELETE FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'semantic_publication'
  AND table_name  = 'vw_frozen_section_safe_VIEW_v1';

-- ── View ────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW semantic_publication.vw_frozen_section_safe_VIEW_v1 AS
SELECT
    r.release_id,
    CAST(f.research_id AS VARCHAR) AS research_id,

    f.frozen_section_any_performed_flag AS any_frozen_section_performed,
    f.frozen_section_any_malignant_flag AS any_frozen_malignant,
    f.frozen_section_any_deferred_flag   AS any_frozen_deferred,
    f.frozen_section_count               AS n_frozen_events,
    f.frozen_section_first_date,
    f.frozen_section_last_date,

    f.frozen_section_any_suspected_flag AS any_frozen_suspected,

    (
        COALESCE(f.frozen_1_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_2_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_3_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_4_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_5_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_6_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_7_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_8_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_9_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_10_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_11_excel_corroborated_flag, FALSE)
        OR COALESCE(f.frozen_12_excel_corroborated_flag, FALSE)
    ) AS any_frozen_excel_corroborated

FROM main.canonical_frozen_section_patient_rollup_v1 f
CROSS JOIN semantic_publication.release_manifest_v1 r
;

-- ── Column verification registry ───────────────────────────────────────────

INSERT INTO main.canonical_column_verification_registry_v1
    (schema_name, table_name, column_name, data_type, ordinal_position,
     category, upstream_source, verification_status, verified_by, verified_ts,
     verification_method, batch_id, notes)
VALUES
    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'release_id', 'VARCHAR', 1,
     'identifier', 'semantic_publication.release_manifest_v1 (CROSS JOIN)', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'mig_242: publication release_id from single-row manifest.'),

    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'research_id', 'VARCHAR', 2,
     'identifier', 'main.canonical_frozen_section_patient_rollup_v1.research_id CAST VARCHAR', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'mig_239 convention: VARCHAR research_id for cross-domain joins.'),

    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'any_frozen_section_performed', 'BOOLEAN', 3,
     'clinical_flag', 'canonical_frozen_section_patient_rollup_v1.frozen_section_any_performed_flag', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'Patient-level any frozen section performed.'),

    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'any_frozen_malignant', 'BOOLEAN', 4,
     'clinical_flag', 'canonical_frozen_section_patient_rollup_v1.frozen_section_any_malignant_flag', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'Any frozen diagnosis malignant/suspicious for malignancy (rollup aggregate).'),

    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'any_frozen_deferred', 'BOOLEAN', 5,
     'clinical_flag', 'canonical_frozen_section_patient_rollup_v1.frozen_section_any_deferred_flag', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'Any frozen deferred to permanent section.'),

    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'n_frozen_events', 'BIGINT', 6,
     'clinical_measure', 'canonical_frozen_section_patient_rollup_v1.frozen_section_count', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'Count of frozen slot rows populated for patient.'),

    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'frozen_section_first_date', 'DATE', 7,
     'date', 'canonical_frozen_section_patient_rollup_v1.frozen_section_first_date', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'Earliest frozen-section date (DATE; mig_160/CF-119).'),

    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'frozen_section_last_date', 'DATE', 8,
     'date', 'canonical_frozen_section_patient_rollup_v1.frozen_section_last_date', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'Latest frozen-section date (DATE; mig_160/CF-119).'),

    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'any_frozen_suspected', 'BOOLEAN', 9,
     'clinical_flag', 'canonical_frozen_section_patient_rollup_v1.frozen_section_any_suspected_flag', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'Any frozen atypia / suspected malignancy flag at patient level.'),

    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1', 'any_frozen_excel_corroborated', 'BOOLEAN', 10,
     'data_quality',
     'OR of frozen_1..12_excel_corroborated_flag from canonical_frozen_section_patient_rollup_v1', 'verified',
     'cursor_composer_mig_242', CURRENT_TIMESTAMP, 'view_create_safe_view_over_canonical',
     'mig_242_frozen_safe',
     'TRUE if any populated frozen slot has structured Excel corroboration.')
;

-- ── Table signoff registry ─────────────────────────────────────────────────

INSERT INTO main.canonical_table_signoff_registry_v1
    (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
     table_status, signed_off_ts, signoff_migration, priority_tier, notes)
VALUES
    ('semantic_publication', 'vw_frozen_section_safe_VIEW_v1',
     10, 10, 0, 0, 0,
     'verified',
     CURRENT_TIMESTAMP,
     'qc_framework_v1/migrations/242_vw_frozen_section_safe_VIEW_v1_20260501.sql',
     'tier2_canonical_view',
     'mig_242 (2026-05-01): compact frozen-section safe view; 10 cols. Source 4,116 rows. batch_id=mig_242_frozen_safe.')
;

-- Path-C (post-apply):
-- SELECT COUNT(*) FROM semantic_publication.vw_frozen_section_safe_VIEW_v1;  -- expect 4116
-- SELECT * FROM manuscript_workspace.qc_audit_dashboard_VIEW_v1;
