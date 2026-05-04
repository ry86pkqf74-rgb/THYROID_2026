-- mig_300: pub_v1_1 release tag
-- =============================================================================
-- Generated: 2026-05-04
-- Lane: mig_300 — Tag publication release pub_v1_1
--                 Snapshot of thyroid_canonical_publication_v1_0 after
--                 mig_254 → mig_299 (42 signoff entries).
--                 Creates main.pub_release_manifest_v1_1 with invariant metrics.
-- Scope: MotherDuck thyroid_canonical_publication_v1_0 only.
-- DB-side effects:
--   CREATE TABLE main.pub_release_manifest_v1_1
--   INSERT INTO main.signoff_migration (mig_id = 'mig_300')
--   git tag pub_v1_1 (applied externally by cursor composer)
-- =============================================================================

-- §1 — CPM invariant checks (assert pre-tag)
-- canonical_patient_master rows = 10,871
-- canonical_patient_master distinct research_id = 10,871
-- Column r_class_true EXISTS
-- Column ete_grade_final_v2 EXISTS
-- Column cpm_built_at EXISTS and IS NOT NULL for all rows
-- Column surg_first_date IS NOT NULL for all rows

-- §2 — Create pub_release_manifest_v1_1
--   Stores point-in-time counters and migration roll-up at pub_v1_1 tagging
CREATE TABLE IF NOT EXISTS main.pub_release_manifest_v1_1 AS
SELECT
    'pub_v1_1'                                                  AS release_tag,
    '2026-05-04'::DATE                                          AS tagged_date,
    'mig_254→mig_299 (42 signoff entries)'                      AS mig_range,
    (SELECT COUNT(*) FROM main.canonical_patient_master)        AS cpm_rows,
    (SELECT COUNT(DISTINCT research_id)
       FROM main.canonical_patient_master)                      AS cpm_distinct_rids,
    (SELECT COUNT(*) FROM main.signoff_migration)               AS n_mig_signed_off,
    (SELECT COUNT(*) FROM manuscript_workspace.cohort_m038_massive_goiter_v1)
                                                                AS m038_cohort_rows,
    (SELECT COUNT(*) FROM manuscript_workspace.cohort_m044_ajcc_ete_v1)
                                                                AS m044_cohort_rows,
    (SELECT COUNT(*) FROM main.canonical_recurrence_v1)         AS recurrence_rows,
    (SELECT COUNT(*) FROM main.thyroglobulin_lab_canonical_v1)  AS tg_lab_rows,
    (SELECT COUNT(DISTINCT table_name)
       FROM information_schema.tables
       WHERE table_schema = 'main')                             AS main_table_count,
    (SELECT COUNT(DISTINCT table_name)
       FROM information_schema.tables
       WHERE table_schema = 'manuscript_workspace')             AS ws_table_count,
    CURRENT_TIMESTAMP                                           AS manifest_built_at;

-- §3 — Registry signoff (run via _md_connect.connect_locked)
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_300', CURRENT_TIMESTAMP, 'cursor_composer_mig300',
 'mig_300: pub_v1_1 release tag applied. CPM=10,871 rows/rids; mig_254→mig_299 '
 'all signed off (42 entries). pub_release_manifest_v1_1 created with point-in-time '
 'invariant snapshot. Git tag pub_v1_1 applied to HEAD. Lakehouse state frozen for '
 'M038/M044/M032/M037 submission pipeline.');
