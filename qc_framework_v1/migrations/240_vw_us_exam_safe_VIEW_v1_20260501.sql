-- =============================================================================
-- mig_240: semantic_publication.vw_us_exam_safe_VIEW_v1
-- Generated: 2026-05-01 by Cline Sonnet 4.6 (mig_240 dispatch — v17 round)
-- Batch: mig_240_us_exam_safe
-- =============================================================================
-- Context: semantic_publication exposes nodule-level US (vw_us_nodule_safe_VIEW_v1)
-- but not exam-level US.  Analysts needing exam denominators (e.g. "distinct US exams
-- in cohort") had to drop into main.canonical_us_exam_master_VIEW_v2 directly,
-- breaking the safe-view convention.  This view closes that gap.
--
-- Source: main.canonical_us_exam_master_VIEW_v2 (11 880 rows as of 2026-05-01)
-- All 24 source columns are publication-relevant; none contain PHI.
-- CROSS JOIN to semantic_publication.release_manifest_v1 (single row) adds release_id.
-- research_id CAST to VARCHAR for cross-domain join uniformity (mig_239 convention).
-- =============================================================================

-- ── Step 1: Create the safe view ─────────────────────────────────────────────

CREATE OR REPLACE VIEW semantic_publication.vw_us_exam_safe_VIEW_v1 AS
SELECT
    r.release_id,
    CAST(e.research_id AS VARCHAR)          AS research_id,
    e.us_exam_id,
    e.exam_id_source,
    e.exam_date,
    -- nodule counts / sizes
    e.n_nodules_on_exam,
    e.largest_nodule_cm,
    e.second_largest_nodule_cm,
    -- exam-level spatial flags
    e.bilateral_flag,
    e.isthmus_nodule_flag,
    -- TI-RADS summary for the exam
    e.worst_tirads_category_this_exam,
    e.worst_tirads_points_this_exam,
    e.best_tirads_category_this_exam,
    e.count_tr5,
    e.count_tr4,
    e.count_tr3,
    e.count_tr2,
    e.count_tr1,
    -- gland / lymph node presence
    e.has_gland_findings,
    e.has_us_ln_findings,
    e.n_us_ln_total_on_exam,
    e.n_abnormal_us_ln_on_exam,
    -- exam ordering / context
    e.exam_rank_for_patient,
    e.is_preop_exam,
    -- data quality
    e.any_nlp_backfill_pending_on_exam
FROM main.canonical_us_exam_master_VIEW_v2 e
CROSS JOIN semantic_publication.release_manifest_v1 r
;

-- ── Step 2: Register columns in canonical_column_verification_registry_v1 ────

INSERT INTO main.canonical_column_verification_registry_v1
    (schema_name, table_name, column_name, data_type, ordinal_position,
     category, upstream_source, verification_status, verified_by, verified_ts,
     verification_method, batch_id, notes)
VALUES
    -- release_id
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'release_id', 'VARCHAR', 1,
     'identifier',
     'semantic_publication.release_manifest_v1 (CROSS JOIN)',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240 (2026-05-01): new exam-level safe view in semantic_publication over main.canonical_us_exam_master_VIEW_v2. release_id from CROSS JOIN to single-row release_manifest_v1.'),

    -- research_id
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'research_id', 'VARCHAR', 2,
     'identifier',
     'main.canonical_us_exam_master_VIEW_v2.research_id (CAST BIGINT→VARCHAR)',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_239 convention: research_id cast to VARCHAR for cross-domain join uniformity.'),

    -- us_exam_id
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'us_exam_id', 'VARCHAR', 3,
     'identifier',
     'main.canonical_us_exam_master_VIEW_v2.us_exam_id',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: exam-level unique identifier.'),

    -- exam_id_source
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'exam_id_source', 'VARCHAR', 4,
     'metadata',
     'main.canonical_us_exam_master_VIEW_v2.exam_id_source',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: source provenance for the exam_id.'),

    -- exam_date
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'exam_date', 'DATE', 5,
     'date',
     'main.canonical_us_exam_master_VIEW_v2.exam_date',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: date of the ultrasound exam.'),

    -- n_nodules_on_exam
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'n_nodules_on_exam', 'BIGINT', 6,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.n_nodules_on_exam',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: count of nodules described on this exam.'),

    -- largest_nodule_cm
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'largest_nodule_cm', 'DOUBLE', 7,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.largest_nodule_cm',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: size (cm) of the largest nodule on this exam.'),

    -- second_largest_nodule_cm
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'second_largest_nodule_cm', 'DOUBLE', 8,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.second_largest_nodule_cm',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: size (cm) of the second largest nodule on this exam.'),

    -- bilateral_flag
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'bilateral_flag', 'BOOLEAN', 9,
     'clinical_flag',
     'main.canonical_us_exam_master_VIEW_v2.bilateral_flag',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: TRUE if nodules present on both lobes in this exam.'),

    -- isthmus_nodule_flag
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'isthmus_nodule_flag', 'BOOLEAN', 10,
     'clinical_flag',
     'main.canonical_us_exam_master_VIEW_v2.isthmus_nodule_flag',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: TRUE if an isthmus nodule is present on this exam.'),

    -- worst_tirads_category_this_exam
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'worst_tirads_category_this_exam', 'VARCHAR', 11,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.worst_tirads_category_this_exam',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: highest (most suspicious) ACR TI-RADS category among all nodules on this exam.'),

    -- worst_tirads_points_this_exam
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'worst_tirads_points_this_exam', 'DOUBLE', 12,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.worst_tirads_points_this_exam',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: ACR TI-RADS points corresponding to the worst-category nodule on this exam.'),

    -- best_tirads_category_this_exam
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'best_tirads_category_this_exam', 'VARCHAR', 13,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.best_tirads_category_this_exam',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: lowest (least suspicious) ACR TI-RADS category among all nodules on this exam.'),

    -- count_tr5
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'count_tr5', 'HUGEINT', 14,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.count_tr5',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: number of TR5 (highly suspicious) nodules on this exam.'),

    -- count_tr4
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'count_tr4', 'HUGEINT', 15,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.count_tr4',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: number of TR4 (moderately suspicious) nodules on this exam.'),

    -- count_tr3
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'count_tr3', 'HUGEINT', 16,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.count_tr3',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: number of TR3 (mildly suspicious) nodules on this exam.'),

    -- count_tr2
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'count_tr2', 'HUGEINT', 17,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.count_tr2',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: number of TR2 (not suspicious) nodules on this exam.'),

    -- count_tr1
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'count_tr1', 'HUGEINT', 18,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.count_tr1',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: number of TR1 (benign) nodules on this exam.'),

    -- has_gland_findings
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'has_gland_findings', 'BOOLEAN', 19,
     'clinical_flag',
     'main.canonical_us_exam_master_VIEW_v2.has_gland_findings',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: TRUE if thyroid gland-level findings (e.g. echogenicity, volume) are recorded for this exam.'),

    -- has_us_ln_findings
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'has_us_ln_findings', 'BOOLEAN', 20,
     'clinical_flag',
     'main.canonical_us_exam_master_VIEW_v2.has_us_ln_findings',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: TRUE if cervical lymph node findings are described on this exam.'),

    -- n_us_ln_total_on_exam
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'n_us_ln_total_on_exam', 'BIGINT', 21,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.n_us_ln_total_on_exam',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: total number of lymph nodes described on this exam.'),

    -- n_abnormal_us_ln_on_exam
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'n_abnormal_us_ln_on_exam', 'HUGEINT', 22,
     'clinical_measure',
     'main.canonical_us_exam_master_VIEW_v2.n_abnormal_us_ln_on_exam',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: number of sonographically abnormal lymph nodes on this exam.'),

    -- exam_rank_for_patient
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'exam_rank_for_patient', 'BIGINT', 23,
     'metadata',
     'main.canonical_us_exam_master_VIEW_v2.exam_rank_for_patient',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: chronological rank of this exam within the patient''s US exam series (1 = earliest).'),

    -- is_preop_exam
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'is_preop_exam', 'BOOLEAN', 24,
     'clinical_flag',
     'main.canonical_us_exam_master_VIEW_v2.is_preop_exam',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: TRUE if this exam occurred before the patient''s first surgery date.'),

    -- any_nlp_backfill_pending_on_exam
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1', 'any_nlp_backfill_pending_on_exam', 'BOOLEAN', 25,
     'data_quality',
     'main.canonical_us_exam_master_VIEW_v2.any_nlp_backfill_pending_on_exam',
     'verified', 'cline_sonnet_4_6_mig_240', CURRENT_TIMESTAMP,
     'view_create_safe_view_over_canonical',
     'mig_240_us_exam_safe',
     'mig_240: TRUE if one or more nodules on this exam have NLP-structured fields awaiting backfill. Consumers should treat associated US fields as potentially incomplete.')
;

-- ── Step 3: Register in signoff registry ─────────────────────────────────────

INSERT INTO main.canonical_table_signoff_registry_v1
    (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
     table_status, signed_off_ts, signoff_migration, priority_tier, notes)
VALUES
    ('semantic_publication', 'vw_us_exam_safe_VIEW_v1',
     25, 25, 0, 0, 0,
     'verified',
     CURRENT_TIMESTAMP,
     'qc_framework_v1/migrations/240_vw_us_exam_safe_VIEW_v1_20260501.sql',
     'tier2_canonical_view',
     'mig_240 (2026-05-01): new exam-level safe view in semantic_publication. 25 columns (release_id + research_id::VARCHAR + 23 exam attributes). Source: main.canonical_us_exam_master_VIEW_v2 (11880 rows). Verified by cline_sonnet_4_6_mig_240. batch_id=mig_240_us_exam_safe.')
;

-- ── Step 4: Path-C verification query (run after apply) ──────────────────────
-- SELECT COUNT(*) FROM semantic_publication.vw_us_exam_safe_VIEW_v1;
--   expected: 11880
-- SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
--   expected: gate1 = 212, gates 2-5 = 0, cohort_parity_ok = TRUE
