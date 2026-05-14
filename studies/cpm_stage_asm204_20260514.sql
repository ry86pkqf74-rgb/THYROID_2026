-- ASM204 — BigQuery-native analogue of scripts/204_canonical_master_assembly.py
-- Spine: pub_workspace.patient_analysis_resolved_v1 (replaces MotherDuck gold_master_patient_facts_v1)
-- Feeders: pub_canonical.canonical_* (diagnosis, recurrence, survival, molecular_tested)
-- Target (Phase 2 scratch): pub_workspace.cpm_stage_asm204_20260514
-- Cohort invariant: 10,871 rows × 10,871 distinct research_id

CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.cpm_stage_asm204_20260514` AS
WITH patient_spine AS (
  SELECT DISTINCT research_id
  FROM `thyroid-canonical-pub-2026.pub_workspace.patient_analysis_resolved_v1`
),

diag AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY
          CASE WHEN is_malignant THEN 0 ELSE 1 END,
          CASE source_table
            WHEN 'tumor_pathology' THEN 0
            WHEN 'gold_master_patient_facts_v1' THEN 1
            WHEN 'path_synoptics' THEN 2
            ELSE 3
          END
      ) AS rn
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_diagnosis_unified_v1`
  ) WHERE rn = 1
),

recur AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY
          CASE recurrence_confirmed
            WHEN TRUE THEN 0
            ELSE 1
          END,
          CASE recurrence_type
            WHEN 'structural_confirmed' THEN 1
            WHEN 'fna_confirmed' THEN 2
            WHEN 'structural_confirmed_legacy' THEN 3
            WHEN 'biochemical_tg_rise' THEN 4
            WHEN 'persistent_biochemical_disease' THEN 5
            WHEN 'imaging_suspicious_unconfirmed' THEN 6
            WHEN 'none' THEN 7
            ELSE 8
          END
      ) AS rn
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_recurrence_v1`
  ) WHERE rn = 1
),

surv AS (
  SELECT * EXCEPT (rn) FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY days_from_first_surgery_to_last_contact DESC NULLS LAST
      ) AS rn
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_survival_followup_v1`
  ) WHERE rn = 1
)

SELECT
  ps.research_id,

  p.age_at_surgery,
  p.sex,
  p.race,

  p.surg_first_date AS first_surgery_date,
  p.surg_procedure_type,
  p.surg_n_procedures,
  p.surg_total_thyroidectomy,
  p.surg_hemithyroidectomy,

  d.is_malignant,
  d.diagnosis_primary,
  d.diagnosis_variant,
  d.diagnosis_full,
  d.n_tumors,

  p.path_tumor_size_cm AS tumor_size_cm,
  p.path_multifocal_flag AS multifocal_flag,
  p.path_laterality AS laterality,
  p.ete_grade_final AS ete_grade,
  p.margin_status_final AS margin_status,
  p.closest_margin_mm,
  p.vascular_invasion_final AS vascular_invasion_grade,
  p.vascular_vessel_count AS vessel_count,
  p.path_lvi_raw AS lvi_grade,
  p.path_pni_raw AS perineural_invasion,

  p.ajcc8_t_stage,
  p.ajcc8_n_stage,
  p.ajcc8_m_stage,
  p.ajcc8_stage_group,
  p.ata_risk_category,
  p.ata_response_category,
  p.macis_score,
  p.ages_score,
  p.ames_risk_group,

  p.path_ln_examined_raw AS ln_total_examined,
  p.path_ln_positive_raw AS ln_total_positive,
  p.ln_ratio,
  p.ln_positive_final AS ln_positive_flag,
  p.lateral_neck_dissected AS ln_lateral_dissected,
  p.path_ene_raw AS ln_ene_status,
  p.ln_burden_band,

  p.fna_bethesda_final AS bethesda_final,

  p.imaging_tirads_best AS preop_tirads_best,
  p.imaging_tirads_worst AS preop_tirads_worst,
  p.imaging_tirads_category AS preop_tirads_category,
  p.imaging_nodule_size_cm AS preop_imaging_size_cm,

  COALESCE(m.molecular_tested_confirmed, FALSE) AS molecular_tested_confirmed,
  m.platform_canonical AS mol_platform,
  m.test_count AS mol_test_count,
  m.has_thyroseq AS mol_has_thyroseq,
  m.has_afirma AS mol_has_afirma,
  COALESCE(m.braf_positive_canonical, p.braf_positive_final, FALSE) AS braf_positive,
  p.braf_variant_raw AS braf_variant,
  COALESCE(m.ras_positive_canonical, p.ras_positive_final, FALSE) AS ras_positive,
  p.ras_subtype_raw AS ras_subtype,
  COALESCE(m.tert_positive_canonical, p.tert_positive_final, FALSE) AS tert_positive,
  COALESCE(p.molecular_risk_tier, m.molecular_risk_tier) AS molecular_risk_tier,
  m.first_test_date AS mol_first_test_date,

  COALESCE(p.rai_received_flag, FALSE) AS rai_received_flag,
  p.rai_first_date,
  p.rai_max_dose_mci,

  p.tg_nadir,
  p.tg_peak,
  p.tg_last_value,
  p.tg_rising_flag,
  p.tg_n_measurements,

  COALESCE(r.recurrence_confirmed, FALSE) AS recurrence_confirmed,
  COALESCE(r.recurrence_type, 'none') AS recurrence_type,
  r.recurrence_date,
  r.recurrence_site,
  CAST(NULL AS STRING) AS recurrence_histology,
  CAST(NULL AS STRING) AS recurrence_evidence_source,
  r.recurrence_definition,
  r.time_to_recurrence_days,
  r.biochemical_tg_nadir AS biochemical_tg_nadir_after_surgery,
  r.biochemical_tg_at_recurrence,
  CASE WHEN r.recurrence_type = 'imaging_suspicious_unconfirmed' THEN TRUE ELSE FALSE END AS imaging_suspicious_unconfirmed,

  p.rln_status,
  p.rln_permanent_flag,
  p.rln_transient_flag,
  p.hypocalcemia_status,
  p.hypoparathyroidism_status,
  p.chyle_leak_status,
  p.hematoma_status,
  p.seroma_status,
  p.wound_infection_status,

  p.op_rln_monitoring_any,
  p.op_drain_placed_any,

  sv.last_known_alive_date AS last_contact_date,
  sv.last_followup_source AS last_contact_source,
  sv.days_from_first_surgery_to_last_contact AS followup_days,
  SAFE_DIVIDE(
    SAFE_CAST(sv.days_from_first_surgery_to_last_contact AS FLOAT64),
    365.25
  ) AS followup_years,
  CAST(NULL AS STRING) AS followup_category,

  CASE
    WHEN d.source_table = 'tumor_pathology' THEN 'HIGH'
    WHEN d.source_table = 'gold_master_patient_facts_v1' THEN 'MEDIUM'
    WHEN d.source_table = 'path_synoptics' THEN 'HIGH'
    ELSE 'LOW'
  END AS diagnosis_confidence,
  CASE
    WHEN r.recurrence_confirmed = TRUE THEN 'HIGH'
    WHEN r.recurrence_type IN ('biochemical_tg_rise', 'persistent_biochemical_disease') THEN 'MEDIUM'
    WHEN r.recurrence_type = 'imaging_suspicious_unconfirmed' THEN 'LOW'
    WHEN r.recurrence_type = 'none' THEN 'HIGH'
    ELSE 'LOW'
  END AS recurrence_data_confidence,
  CASE
    WHEN m.molecular_tested_confirmed = TRUE THEN 'HIGH'
    ELSE 'NOT_TESTED'
  END AS molecular_data_confidence,
  p.lab_completeness_score AS followup_completeness_score

FROM patient_spine ps
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.patient_analysis_resolved_v1` p
  ON ps.research_id = p.research_id
LEFT JOIN diag d
  ON ps.research_id = d.research_id
LEFT JOIN recur r
  ON ps.research_id = r.research_id
LEFT JOIN surv sv
  ON ps.research_id = sv.research_id
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_tested_v1` m
  ON ps.research_id = m.research_id;
