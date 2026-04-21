CREATE OR REPLACE VIEW "thyroid_canonical_publication_v1_0".views_readable."FNA_Episode_Master" AS
SELECT
  research_id,
  fna_index                        AS fna_episode_id,
  fna_date_resolved                AS resolved_fna_date,
  fna_date_status                  AS date_status,
  fna_date_confidence              AS date_confidence,
  specimen_site_raw,
  laterality,
  pathology_diagnosis,
  pathology_extended
FROM "thyroid_canonical_publication_v1_0".main."fna_event_v1"