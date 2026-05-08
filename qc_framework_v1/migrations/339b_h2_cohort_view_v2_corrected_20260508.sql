-- mig_339b: H2 v3.2 Phase 2b — cohort_h2_pathology_outcome_v2 (override-corrected NLP flags)
-- Lane: THY-40 / THY-32. Additive: CREATE VIEW only; v1 remains raw-NLP audit trail.
-- Pre-log: DFL-20260508-H2-COHORT-VIEW-V2-CORRECTED (recnh3SW0GcbPSEAT)
--
-- Applies canonical_path_benign_overrides_v1 for nlp_atypical_adenoma and nlp_thymic_tissue
-- via COALESCE(override_flag, raw_nlp_column).

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v2` AS
WITH overrides AS (
  SELECT research_id, category, override_flag
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_overrides_v1`
)
SELECT
  v1.* EXCEPT(nlp_atypical_adenoma, nlp_thymic_tissue),
  COALESCE(o_atyp.override_flag, v1.nlp_atypical_adenoma) AS nlp_atypical_adenoma,
  COALESCE(o_thym.override_flag, v1.nlp_thymic_tissue) AS nlp_thymic_tissue
FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v1` v1
LEFT JOIN overrides o_atyp
  ON SAFE_CAST(v1.research_id AS STRING) = o_atyp.research_id
  AND o_atyp.category = 'atypical_adenoma'
LEFT JOIN overrides o_thym
  ON SAFE_CAST(v1.research_id AS STRING) = o_thym.research_id
  AND o_thym.category = 'thymic_tissue';
