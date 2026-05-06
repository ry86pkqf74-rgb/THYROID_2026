-- migration_id: mig_083_mig005_vc_paralysis_archive_20260506
-- description: MIG-005 — Archive legacy comp_vocal_cord_paralysis_* (3 cols) from canonical_patient_master; canonical layer remains comp_vc_paralysis_* (12 cols).
-- affected_dataset: pub_archive
-- affected_table: comp_vocal_cord_paralysis_columns_pre_archive_20260506
-- DFL: DFL-20260506-083 (logged before apply)
-- Linear: THY-17 → In Review + auto-close:pending; follow-up drop: THY-20
--
-- Pre-apply investigation (BQ 2026-05-06):
--   Predicate: definitive=TRUE AND COALESCE(confirmed,FALSE)=FALSE → 0 rows.
--   Cohort-wide: n_comp_vocal_cord_paralysis_definitive=5; n_comp_vc_paralysis_confirmed=23; all definitive ⊆ confirmed.
--   No CPM UPDATE required (gap-report single-goiter disagreement already absent in live BQ).
--
-- Dataset note: MotherDuck archive_pub_v1_0 ↔ BigQuery pub_archive for this project.
--
-- Dry-run (2026-05-06): upper bound ~98792 bytes processed.
-- =============================================================================

CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_archive.comp_vocal_cord_paralysis_columns_pre_archive_20260506` AS
SELECT
  research_id,
  comp_vocal_cord_paralysis_any_evidence,
  comp_vocal_cord_paralysis_definitive,
  comp_vocal_cord_paralysis_probable_or_better
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`;
