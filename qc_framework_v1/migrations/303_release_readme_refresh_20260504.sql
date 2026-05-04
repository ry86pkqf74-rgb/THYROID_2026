-- mig_303: publication README / manuscript release-ID hygiene (pub_v1_1)
-- =============================================================================
-- Generated: 2026-05-04
-- Lane: mig_303 — Align submission-package READMEs, M038 builders, and
--                 manuscript_outputs drafts with git + signoff tag
--                 `pub_v1_1_20260504` (post–mig_281–mig_300 NLP augment).
-- Scope: MotherDuck thyroid_canonical_publication_v1_0 — signoff only.
-- Repo artifacts (no DDL):
--   M0{25,32,37,38,44}_submission_package_v1_0/00_README.md,
--   M038 validation + data_extraction README, M038 builders/SQL,
--   manuscript_outputs/v1_0_20260501/M032 + M038 drafts, M038_OUTLINE.md
-- Legacy Parquet path `parquet_export/pub_v1_0_20260430/` unchanged on disk.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT
    'mig_303',
    CURRENT_TIMESTAMP,
    'cursor_composer_mig303',
    'mig_303: Refreshed submission package READMEs + M038 package code + '
    'manuscript_outputs drafts to release pub_v1_1_20260504 '
    '(successor to pub_v1_0_20260430 after mig_281–mig_300). '
    'Closes pub_v1_1 versioning hygiene.'
WHERE NOT EXISTS (
    SELECT 1 FROM main.signoff_migration WHERE mig_id = 'mig_303'
);

SELECT mig_id, signed_off_at, by_actor, substring(summary, 1, 120) AS summary_head
FROM main.signoff_migration
WHERE mig_id = 'mig_303';
