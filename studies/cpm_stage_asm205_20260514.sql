-- ASM205 — BigQuery port of scripts/205_canonical_consolidation.py MASTER_SQL core
-- Gold surrogate: pub_workspace.patient_analysis_resolved_v1 (MotherDuck gold_master_patient_facts_v1)
-- PRM substitute: PAR columns only; fna_path_concordance_* not on PAR → NULL placeholders (gap note in report)
-- Target: pub_workspace.cpm_stage_asm205_20260514
-- Cohort: 10,871 × 10,871 distinct research_id

CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.cpm_stage_asm205_20260514` AS
WITH patient_spine AS (
  SELECT DISTINCT CAST(research_id AS STRING) AS research_id
  FROM `thyroid-canonical-pub-2026.pub_workspace.patient_analysis_resolved_v1`
),

diag AS (
  SELECT * EXCEPT(rn)
  FROM (
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
  )
  WHERE rn = 1
),

recur AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY
          CASE WHEN recurrence_confirmed THEN 0 ELSE 1 END,
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
  )
  WHERE rn = 1
),

surv AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY days_from_first_surgery_to_last_contact DESC NULLS LAST
      ) AS rn
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_survival_followup_v1`
  )
  WHERE rn = 1
),

tirads_combined AS (
  SELECT CAST(research_id AS STRING) AS research_id,
    tirads_acr_recalculated AS tirads_score,
    exam_date
  FROM `thyroid-canonical-pub-2026.pub_canonical.imaging_nodule_master_v1`
  WHERE tirads_acr_recalculated IS NOT NULL

  UNION ALL

  SELECT CAST(research_id AS STRING),
    CASE tirads_level_2017
      WHEN 'TR1' THEN 1 WHEN 'TR2' THEN 2 WHEN 'TR3' THEN 3
      WHEN 'TR4' THEN 4 WHEN 'TR5' THEN 5
    END,
    CAST(NULL AS DATE)
  FROM `thyroid-canonical-pub-2026.pub_canonical.tirads_llm_extracted_v2`
  WHERE tirads_level_2017 IS NOT NULL
),

tirads_patient AS (
  SELECT research_id,
    MAX(tirads_score) AS tirads_worst_combined,
    MIN(tirads_score) AS tirads_best_combined,
    COUNT(*) AS tirads_nodules_scored_combined
  FROM tirads_combined
  WHERE tirads_score IS NOT NULL
  GROUP BY research_id
),

bethesda_multi AS (
  SELECT CAST(research_id AS STRING) AS research_id,
    MAX(bethesda_2023_num) AS bethesda_2023,
    MAX(bethesda_2015_num) AS bethesda_2015,
    MAX(bethesda_2010_num) AS bethesda_2010,
    COUNT(*) AS n_fna_cytology_records
  FROM `thyroid-canonical-pub-2026.pub_canonical.fna_cytology`
  GROUP BY 1
),

tp_ln AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT CAST(research_id AS STRING) AS research_id,
      primary_ln_ln_total_examined AS tp_ln_examined,
      primary_ln_ln_total_positive AS tp_ln_positive,
      primary_ln_ln_extranodal_extension AS tp_ln_ene,
      primary_ln_ln_largest_deposit_cm AS tp_ln_largest_deposit_cm,
      ln_total_levels_involved AS tp_ln_levels_involved,
      primary_ln_ln_central_positive AS tp_ln_central_positive,
      primary_ln_ln_lateral_positive AS tp_ln_lateral_positive,
      ln_mets_ptc, ln_mets_ftc, ln_mets_mtc, ln_mets_atc,
      ln_mets_hurthle, ln_mets_pdtc,
      ln_mets_micrometastasis,
      ln_mets_extranodal_extension AS ln_mets_ene_count,
      ln_central_examined AS tp_central_examined,
      ln_central_positive AS tp_central_positive_total,
      ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY
          CASE WHEN primary_ln_ln_total_positive IS NOT NULL THEN 0 ELSE 1 END,
          primary_ln_ln_total_examined DESC NULLS LAST
      ) AS rn
    FROM `thyroid-canonical-pub-2026.pub_canonical.tumor_pathology`
  )
  WHERE rn = 1
),

prm_dedup AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    fna_path_outcome,
    CAST(NULL AS STRING) AS fna_path_concordance_category,
    CAST(NULL AS BOOL) AS fna_path_concordant
  FROM `thyroid-canonical-pub-2026.pub_workspace.patient_analysis_resolved_v1`
),

imaging_ln AS (
  SELECT CAST(research_id AS STRING) AS research_id,
    LOGICAL_OR(
      CASE
        WHEN lymph_node_assessment NOT IN (
          'No abnormal lymph nodes identified',
          'Normal cervical lymph nodes'
        ) THEN TRUE
        ELSE FALSE
      END
    ) AS imaging_ln_abnormal,
    COUNT(DISTINCT ultrasound_date) AS n_us_with_ln_assessment
  FROM `thyroid-canonical-pub-2026.pub_canonical.ultrasound_reports`
  WHERE lymph_node_assessment IS NOT NULL
  GROUP BY 1
)

SELECT
  ps.research_id,
  g.age_at_surgery,
  g.sex,
  g.race,
  g.surg_first_date AS first_surgery_date,
  g.surg_procedure_type,
  g.surg_n_procedures,
  g.surg_total_thyroidectomy,
  g.surg_hemithyroidectomy,
  d.is_malignant,
  d.diagnosis_primary,
  d.diagnosis_variant,
  d.diagnosis_full,
  d.n_tumors,
  g.path_tumor_size_cm AS tumor_size_cm,
  g.path_multifocal_flag AS multifocal_flag,
  g.path_laterality AS laterality,
  g.ete_grade_final AS ete_grade,
  g.margin_status_final AS margin_status,
  g.closest_margin_mm,
  g.vascular_invasion_final AS vascular_invasion_grade,
  g.vascular_vessel_count AS vessel_count,
  g.path_lvi_raw AS lvi_grade,
  g.path_pni_raw AS perineural_invasion,
  g.ajcc8_t_stage,
  g.ajcc8_n_stage,
  g.ajcc8_m_stage,
  g.ajcc8_stage_group,
  g.ata_risk_category,
  g.ata_response_category,
  g.macis_score,
  g.ages_score,
  g.ames_risk_group,
  g.path_ln_examined_raw AS ln_total_examined,
  g.path_ln_positive_raw AS ln_total_positive,
  g.ln_ratio,
  g.ln_positive_final AS ln_positive_flag,
  g.lateral_neck_dissected AS ln_lateral_dissected,
  g.path_ene_raw AS ln_ene_status,
  g.ln_burden_band,
  g.fna_bethesda_final AS bethesda_final,
  bm.bethesda_2023,
  bm.bethesda_2015,
  bm.bethesda_2010,
  bm.n_fna_cytology_records,
  prm.fna_path_outcome,
  prm.fna_path_concordance_category,
  prm.fna_path_concordant,
  g.imaging_tirads_best AS preop_tirads_best,
  g.imaging_tirads_worst AS preop_tirads_worst,
  g.imaging_tirads_category AS preop_tirads_category,
  g.imaging_nodule_size_cm AS preop_imaging_size_cm,
  tp_tirads.tirads_worst_combined,
  tp_tirads.tirads_best_combined,
  tp_tirads.tirads_nodules_scored_combined,
  COALESCE(iln.imaging_ln_abnormal, FALSE) AS imaging_ln_abnormal,
  iln.n_us_with_ln_assessment,
  COALESCE(m.molecular_tested_confirmed, FALSE) AS molecular_tested_confirmed,
  m.platform_canonical AS mol_platform,
  m.test_count AS mol_test_count,
  m.has_thyroseq AS mol_has_thyroseq,
  m.has_afirma AS mol_has_afirma,
  COALESCE(m.braf_positive_canonical, g.braf_positive_final, FALSE) AS braf_positive,
  g.braf_variant_raw AS braf_variant,
  COALESCE(m.ras_positive_canonical, g.ras_positive_final, FALSE) AS ras_positive,
  g.ras_subtype_raw AS ras_subtype,
  COALESCE(m.tert_positive_canonical, g.tert_positive_final, FALSE) AS tert_positive,
  COALESCE(g.molecular_risk_tier, m.molecular_risk_tier) AS molecular_risk_tier,
  m.first_test_date AS mol_first_test_date,
  COALESCE(g.rai_received_flag, FALSE) AS rai_received_flag,
  g.rai_first_date,
  g.rai_max_dose_mci,
  g.tg_nadir,
  g.tg_peak,
  g.tg_last_value,
  g.tg_rising_flag,
  g.tg_n_measurements,
  COALESCE(r.recurrence_confirmed, FALSE) AS recurrence_confirmed,
  COALESCE(r.recurrence_type, 'none') AS recurrence_type,
  r.recurrence_date,
  r.recurrence_site,
  r.recurrence_histology,
  r.recurrence_evidence_source,
  r.recurrence_definition,
  r.time_to_recurrence_days,
  r.biochemical_tg_nadir AS biochemical_tg_nadir_after_surgery,
  r.biochemical_tg_at_recurrence,
  CASE WHEN r.recurrence_type = 'imaging_suspicious_unconfirmed' THEN TRUE ELSE FALSE END AS imaging_suspicious_unconfirmed,
  g.rln_status,
  g.rln_permanent_flag,
  g.rln_transient_flag,
  g.hypocalcemia_status,
  g.hypoparathyroidism_status,
  g.chyle_leak_status,
  g.hematoma_status,
  g.seroma_status,
  g.wound_infection_status,
  g.op_rln_monitoring_any,
  g.op_drain_placed_any,
  tln.tp_ln_examined,
  tln.tp_ln_positive,
  tln.tp_ln_ene,
  tln.tp_ln_largest_deposit_cm,
  tln.tp_ln_levels_involved,
  tln.tp_ln_central_positive,
  tln.tp_ln_lateral_positive,
  tln.ln_mets_ptc,
  tln.ln_mets_ftc,
  tln.ln_mets_mtc,
  tln.ln_mets_atc,
  tln.ln_mets_hurthle,
  tln.ln_mets_pdtc,
  tln.ln_mets_micrometastasis,
  tln.ln_mets_ene_count,
  tln.tp_central_examined,
  tln.tp_central_positive_total,
  sv.last_known_alive_date AS last_contact_date,
  sv.last_followup_source AS last_contact_source,
  sv.days_from_first_surgery_to_last_contact AS followup_days,
  SAFE_DIVIDE(SAFE_CAST(sv.days_from_first_surgery_to_last_contact AS FLOAT64), 365.25) AS followup_years,
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
  g.lab_completeness_score AS followup_completeness_score

FROM patient_spine ps
LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.patient_analysis_resolved_v1` g
  ON ps.research_id = CAST(g.research_id AS STRING)
LEFT JOIN diag d
  ON ps.research_id = d.research_id
LEFT JOIN recur r
  ON ps.research_id = r.research_id
LEFT JOIN surv sv
  ON ps.research_id = sv.research_id
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_tested_v1` m
  ON ps.research_id = CAST(m.research_id AS STRING)
LEFT JOIN prm_dedup prm
  ON ps.research_id = prm.research_id
LEFT JOIN tirads_patient tp_tirads
  ON ps.research_id = tp_tirads.research_id
LEFT JOIN bethesda_multi bm
  ON ps.research_id = bm.research_id
LEFT JOIN tp_ln tln
  ON ps.research_id = tln.research_id
LEFT JOIN imaging_ln iln
  ON ps.research_id = iln.research_id;
