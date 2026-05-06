-- MIG-080: H2 preoperative RLN injury and vocal-cord paralysis flags
-- DFL: DFL-20260506-102
-- Source: pub_workspace.nlp_preop_rln_vc_patient_v1
-- Notes:
--   * Source rollup contains research_id-keyed boolean/enum outputs only.
--   * No note text is present in BigQuery.
--   * Snowflake source field was CANONICAL_PATIENT_MASTER_FLAT.OPS_PREOP_LARYNGOSCOPY
--     because the available Snowflake note search table does not carry note_date.

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS comp_rln_injury_preop BOOL;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS comp_rln_injury_preop_source STRING;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS comp_vc_paralysis_preop BOOL;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS comp_vc_paralysis_preop_source STRING;

UPDATE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` AS c
SET
  comp_rln_injury_preop = w.comp_rln_injury_preop,
  comp_rln_injury_preop_source = w.comp_rln_injury_preop_source,
  comp_vc_paralysis_preop = w.comp_vc_paralysis_preop,
  comp_vc_paralysis_preop_source = w.comp_vc_paralysis_preop_source
FROM `thyroid-canonical-pub-2026.pub_workspace.nlp_preop_rln_vc_patient_v1` AS w
WHERE CAST(c.research_id AS STRING) = CAST(w.research_id AS STRING);
