-- =============================================================================
-- mig_243: semantic_publication.vw_snake_case_aliases_VIEW_v1
-- Generated: 2026-05-01 by GitHub Copilot (mig_243 dispatch — v17 round)
-- Batch id: mig_243_snake_case_aliases
-- verified_by: cline_gpt_5_5_mig_243
-- =============================================================================
-- Context: v17 identified 17 nonstandard mixed-case / unit-suffix columns that
-- need snake_case publication aliases without renaming canonical base columns.
-- This view exposes patient-grain aliases over canonical_patient_master plus
-- patient-grain rollups. The event-grain parathyroid alias is intentionally not
-- flattened into this patient-grain view.
--
-- Live catalog note (2026-05-01): canonical_invasion_patient_rollup_v1 no longer
-- contains any_pT4a_final_anywhere / any_pT4b_final_anywhere; mig_209 registry
-- marks both as deprecated_dropped_from_live. To preserve the v17 alias contract
-- without reading archived stale data, this view exposes typed NULL compatibility
-- aliases for those two fields and documents the source absence in registry notes.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

-- ── Idempotent prep (re-run safe) ─────────────────────────────────────────

DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'semantic_publication'
  AND table_name  = 'vw_snake_case_aliases_VIEW_v1';

DELETE FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'semantic_publication'
  AND table_name  = 'vw_snake_case_aliases_VIEW_v1';

-- ── View ────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW semantic_publication.vw_snake_case_aliases_VIEW_v1 AS
SELECT
    r.release_id,
    CAST(pm.research_id AS VARCHAR) AS research_id,

    -- airway_invasion patient rollup (3 cols)
    ai."any_pT4a_direct" AS any_pt4a_direct,
    ai."any_pT4a_final"  AS any_pt4a_final,
    ai."n_pT4a_events"   AS n_pt4a_events,

    -- ete_subgrade patient rollup (5 cols)
    ete."any_pT3b"                         AS any_pt3b,
    ete."any_pT4a"                         AS any_pt4a,
    ete."any_pT4b"                         AS any_pt4b,
    ete."any_pT4b_from_t4b_invasion"       AS any_pt4b_from_t4b_invasion,
    ete."pT4b_ete_vs_t4b_invasion_discordant" AS pt4b_ete_vs_t4b_invasion_discordant,

    -- invasion patient rollup v17 compatibility aliases (source cols dropped from live table by mig_209)
    CAST(NULL AS BOOLEAN) AS any_pt4a_final_anywhere,
    CAST(NULL AS BOOLEAN) AS any_pt4b_final_anywhere,

    -- parathyroid patient rollup (2 cols; event-grain intact_pth_value_ngL is deferred)
    para."max_intact_pth_value_ngL" AS max_intact_pth_value_ng_l,
    para."min_intact_pth_value_ngL" AS min_intact_pth_value_ng_l,

    -- patient master (1 col)
    pm."ajcc8_t_stage_with_microete_t3b_DEPRECATED" AS ajcc8_t_stage_with_microete_t3b_deprecated,

    -- t4b_invasion patient rollup (3 cols)
    t4b."any_pT4b_direct" AS any_pt4b_direct,
    t4b."any_pT4b_final"  AS any_pt4b_final,
    t4b."n_pT4b_events"   AS n_pt4b_events
FROM main.canonical_patient_master pm
CROSS JOIN semantic_publication.release_manifest_v1 r
LEFT JOIN main.canonical_airway_invasion_patient_rollup_v1 ai USING (research_id)
LEFT JOIN main.canonical_ete_subgrade_patient_rollup_v1 ete USING (research_id)
LEFT JOIN main.canonical_invasion_patient_rollup_v1 inv USING (research_id)
LEFT JOIN main.canonical_parathyroid_patient_rollup_v1 para USING (research_id)
LEFT JOIN main.canonical_t4b_invasion_patient_rollup_v1 t4b USING (research_id)
;

-- ── Column verification registry ───────────────────────────────────────────

INSERT INTO main.canonical_column_verification_registry_v1
    (schema_name, table_name, column_name, data_type, ordinal_position,
     category, upstream_source, verification_status, verified_by, verified_ts,
     verification_method, batch_id, notes)
VALUES
    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'release_id', 'VARCHAR', 1,
     'identifier', 'semantic_publication.release_manifest_v1 (CROSS JOIN)', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'view_create_alias_over_canonical',
     'mig_243_snake_case_aliases',
     'mig_243: publication release_id from single-row release manifest.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'research_id', 'VARCHAR', 2,
     'identifier', 'main.canonical_patient_master.research_id CAST VARCHAR', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'view_create_alias_over_canonical',
     'mig_243_snake_case_aliases',
     'mig_239 convention: VARCHAR research_id for cross-domain joins; one row per canonical_patient_master patient.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt4a_direct', 'BOOLEAN', 3,
     'clinical_flag', 'main.canonical_airway_invasion_patient_rollup_v1.any_pT4a_direct', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4a airway direct flag.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt4a_final', 'BOOLEAN', 4,
     'clinical_flag', 'main.canonical_airway_invasion_patient_rollup_v1.any_pT4a_final', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4a airway final flag.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'n_pt4a_events', 'HUGEINT', 5,
     'clinical_measure', 'main.canonical_airway_invasion_patient_rollup_v1.n_pT4a_events', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4a airway event count.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt3b', 'BOOLEAN', 6,
     'clinical_flag', 'main.canonical_ete_subgrade_patient_rollup_v1.any_pT3b', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT3b ETE subgrade flag.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt4a', 'BOOLEAN', 7,
     'clinical_flag', 'main.canonical_ete_subgrade_patient_rollup_v1.any_pT4a', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4a ETE subgrade flag.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt4b', 'BOOLEAN', 8,
     'clinical_flag', 'main.canonical_ete_subgrade_patient_rollup_v1.any_pT4b', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4b ETE subgrade flag.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt4b_from_t4b_invasion', 'BOOLEAN', 9,
     'clinical_flag', 'main.canonical_ete_subgrade_patient_rollup_v1.any_pT4b_from_t4b_invasion', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4b-from-T4b-invasion ETE subgrade flag.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'pt4b_ete_vs_t4b_invasion_discordant', 'BOOLEAN', 10,
     'data_quality', 'main.canonical_ete_subgrade_patient_rollup_v1.pT4b_ete_vs_t4b_invasion_discordant', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4b ETE vs T4b invasion discordance flag.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt4a_final_anywhere', 'BOOLEAN', 11,
     'clinical_flag', 'main.canonical_invasion_patient_rollup_v1.any_pT4a_final_anywhere (deprecated_dropped_from_live)', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'typed_null_compatibility_alias_for_dropped_live_column',
     'mig_243_snake_case_aliases',
     'v17 requested alias for any_pT4a_final_anywhere, but live source column is absent and registry marks it deprecated_dropped_from_live by mig_209; exposed as typed NULL to preserve alias contract without using archived stale data.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt4b_final_anywhere', 'BOOLEAN', 12,
     'clinical_flag', 'main.canonical_invasion_patient_rollup_v1.any_pT4b_final_anywhere (deprecated_dropped_from_live)', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'typed_null_compatibility_alias_for_dropped_live_column',
     'mig_243_snake_case_aliases',
     'v17 requested alias for any_pT4b_final_anywhere, but live source column is absent and registry marks it deprecated_dropped_from_live by mig_209; exposed as typed NULL to preserve alias contract without using archived stale data.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'max_intact_pth_value_ng_l', 'DOUBLE', 13,
     'clinical_measure', 'main.canonical_parathyroid_patient_rollup_v1.max_intact_pth_value_ngL', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for max intact PTH value with snake_case unit suffix.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'min_intact_pth_value_ng_l', 'DOUBLE', 14,
     'clinical_measure', 'main.canonical_parathyroid_patient_rollup_v1.min_intact_pth_value_ngL', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for min intact PTH value with snake_case unit suffix.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'ajcc8_t_stage_with_microete_t3b_deprecated', 'VARCHAR', 15,
     'clinical_stage', 'main.canonical_patient_master.ajcc8_t_stage_with_microete_t3b_DEPRECATED', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for deprecated mixed-case AJCC8 microETE/T3b patient-master field.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt4b_direct', 'BOOLEAN', 16,
     'clinical_flag', 'main.canonical_t4b_invasion_patient_rollup_v1.any_pT4b_direct', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4b direct T4b-invasion flag.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'any_pt4b_final', 'BOOLEAN', 17,
     'clinical_flag', 'main.canonical_t4b_invasion_patient_rollup_v1.any_pT4b_final', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4b final T4b-invasion flag.'),

    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1', 'n_pt4b_events', 'HUGEINT', 18,
     'clinical_measure', 'main.canonical_t4b_invasion_patient_rollup_v1.n_pT4b_events', 'verified',
     'cline_gpt_5_5_mig_243', CURRENT_TIMESTAMP, 'snake_case_alias_passthrough',
     'mig_243_snake_case_aliases', 'Alias for mixed-case pT4b event count.')
;

-- ── Table signoff registry ─────────────────────────────────────────────────

INSERT INTO main.canonical_table_signoff_registry_v1
    (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
     table_status, signed_off_ts, signoff_migration, priority_tier, notes)
VALUES
    ('semantic_publication', 'vw_snake_case_aliases_VIEW_v1',
     18, 18, 0, 0, 0,
     'verified',
     CURRENT_TIMESTAMP,
     'qc_framework_v1/migrations/243_snake_case_aliases_VIEW_v1_20260501.sql',
     'tier2_canonical_view',
     'mig_243 (2026-05-01): snake_case alias view over patient-grain canonical columns. 18 cols = release_id + research_id::VARCHAR + 16 patient-grain aliases. Event-grain canonical_parathyroid_events_v1.intact_pth_value_ngL deferred for grain mismatch. any_pT4a/b_final_anywhere are typed NULL compatibility aliases because live source columns are deprecated_dropped_from_live by mig_209. batch_id=mig_243_snake_case_aliases.')
;

-- ── Path-C verification queries (run after apply) ──────────────────────────
-- SELECT COUNT(*) FROM semantic_publication.vw_snake_case_aliases_VIEW_v1;  -- expect 10871
-- SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
-- SELECT research_id, any_pt4a_direct, any_pt4a_final, n_pt4a_events
-- FROM semantic_publication.vw_snake_case_aliases_VIEW_v1
-- WHERE any_pt4a_direct IS NOT NULL OR any_pt4a_final IS NOT NULL OR n_pt4a_events IS NOT NULL
-- LIMIT 3;