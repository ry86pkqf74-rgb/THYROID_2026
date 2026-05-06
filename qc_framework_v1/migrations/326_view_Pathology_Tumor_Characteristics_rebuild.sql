-- =============================================================================
-- mig_326_view_Pathology_Tumor_Characteristics_rebuild — THY-18
-- =============================================================================
-- Replaces tombstone from mig_040 once pub_canonical.canonical_tumor_characteristics_v1
-- exists. One-liner mirror of MotherDuck views_readable.Pathology_Tumor_Characteristics.
-- =============================================================================

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_views_readable.Pathology_Tumor_Characteristics` AS
SELECT * FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_tumor_characteristics_v1`;
