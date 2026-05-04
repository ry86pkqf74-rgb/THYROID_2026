"""01_pull_parquet.py — Extract M038 cohort to local parquet.

Auths via MD_SA_TOKEN from /Users/ros/THyroid 2026/motherduck.local.toml (.eras account).
Writes per-patient parquet (n=10,871 × ~80 cols) to ./m038_per_patient_v2.parquet.
"""
import os, sys, re
from pathlib import Path

HERE = Path(__file__).parent
TOML = Path("/Users/ros/THyroid 2026/motherduck.local.toml")
PARQUET = HERE / "m038_per_patient_v2.parquet"


def load_token():
    txt = TOML.read_text()
    m = re.search(r'MD_SA_TOKEN\s*=\s*"([^"]+)"', txt)
    if not m:
        sys.exit(f"MD_SA_TOKEN not found in {TOML}")
    return m.group(1)


SQL_EXTRACT = """
WITH base AS (
  SELECT * FROM manuscript_workspace.cohort_m038_massive_goiter_v1
)
SELECT
  research_id,
  -- Derived
  (COALESCE(b.gland_weight_final_g >= 100, FALSE)
   OR COALESCE(b.ct_substernal_extension_any, FALSE) OR COALESCE(b.mri_substernal_any, FALSE)
   OR COALESCE(b.ct_tracheal_deviation_any, FALSE) OR COALESCE(b.ct_tracheal_narrowing_any, FALSE)
   OR COALESCE(b.ct_airway_compromise_any, FALSE)) AS is_massive,
  COALESCE(b.gland_weight_final_g >= 100, FALSE) AS comp_weight_ge100,
  (COALESCE(b.ct_substernal_extension_any, FALSE) OR COALESCE(b.mri_substernal_any, FALSE)) AS comp_substernal_any,
  (COALESCE(b.ct_tracheal_deviation_any, FALSE) OR COALESCE(b.ct_tracheal_narrowing_any, FALSE)
   OR COALESCE(b.ct_airway_compromise_any, FALSE)) AS comp_airway_any,
  CASE WHEN b.surg_first_date IS NULL THEN 'unknown'
       WHEN b.surg_first_date <= '2004-12-31' THEN '1999-2004'
       WHEN b.surg_first_date <= '2009-12-31' THEN '2005-2009'
       WHEN b.surg_first_date <= '2014-12-31' THEN '2010-2014'
       WHEN b.surg_first_date <= '2019-12-31' THEN '2015-2019'
       ELSE '2020-2025' END AS era_bucket_6,
  CASE WHEN b.surg_first_date IS NULL THEN 'unknown'
       WHEN b.surg_first_date <= '2014-12-31' THEN 'pre-2015'
       WHEN b.surg_first_date <= '2019-12-31' THEN '2015-2019'
       ELSE '2020-2025' END AS era_bucket_3,
  CASE WHEN b.comp_hypoparathyroidism_confirmed AND b.comp_hypoparathyroidism_transient THEN 'transient_lt_6mo'
       WHEN b.comp_hypoparathyroidism_confirmed AND b.comp_hypoparathyroidism_permanent THEN 'permanent_gt_6mo'
       WHEN b.comp_hypoparathyroidism_confirmed THEN 'unclassified'
       ELSE 'none' END AS hypopara_postop_class,
  (COALESCE(b.comp_hypocalcemia_timing_window = 'pre_surgery', FALSE)
   OR COALESCE(b.comp_hypocalcemia_clinical_preexisting, FALSE)) AS hca_preop_flag,
  -- Demographics
  age_at_surgery, sex, race, bmi_combined, bmi_source, bmi_missingness_reason, nsqip_bmi,
  nsqip_asa_class, nsqip_smoker, nsqip_tobacco_use, pmhx_nlp_smoking_status,
  -- Comorbidities NLP
  pmhx_nlp_diabetes, pmhx_nlp_hypertension, pmhx_nlp_cad, pmhx_nlp_ckd, pmhx_nlp_copd,
  pmhx_nlp_n_comorbidities, pmhx_nlp_autoimmune_thyroid_hx,
  -- Comorbidities NSQIP
  nsqip_diabetes, nsqip_hypertension, nsqip_copd, nsqip_heart_failure,
  nsqip_bleeding_disorder, nsqip_disseminated_cancer, nsqip_functional_status,
  -- Thyroid history
  syn_graves, syn_hashimoto, ops_anticoagulation_meds,
  pshx_nlp_prior_thyroidectomy, pshx_nlp_prior_neck_surgery,
  -- Surgical
  surg_first_date, surg_procedure_type, surg_total_thyroidectomy, surg_hemithyroidectomy,
  surg_n_procedures, nsqip_central_neck_dissection, nsqip_lateral_neck_dissection,
  nsqip_operative_approach, nsqip_operative_duration_min, nsqip_drain_usage,
  nsqip_vessel_sealant, nsqip_rln_monitoring, ops_difficult_airway, ops_surgeon, ops_surg_date,
  nsqip_inpatient_outpatient, nsqip_same_day_discharge_flag, nsqip_primary_indication,
  -- Anatomy / exposure
  gland_weight_final_g, gland_weight_total_reported_g,
  ct_substernal_extension_any, mri_substernal_any,
  ct_tracheal_deviation_any, ct_tracheal_narrowing_any, ct_airway_compromise_any,
  ct_goiter_present_any, nlp_airway_has_data, nlp_airway_key_finding,
  syn_isthmus_height_cm, syn_left_lobe_height_cm, syn_right_lobe_height_cm,
  bilateral_disease_flag, bilateral_path_flag, closest_margin_mm,
  -- Pathology
  histology_final, is_malignant,
  -- LOS
  nsqip_length_of_stay_days, nsqip_hospital_los_days, nsqip_surgical_los_days,
  -- Complications strict
  any_confirmed_complication_flag, comp_hematoma_confirmed, comp_seroma_confirmed,
  comp_chyle_leak_confirmed, comp_rln_injury_confirmed, comp_vc_paresis_confirmed,
  comp_vc_paralysis_confirmed, comp_hypocalcemia_confirmed, comp_hypoparathyroidism_confirmed,
  comp_mortality_definitive, comp_airway_complication_definitive, comp_pneumothorax_definitive,
  -- Complications temporality
  comp_hypoparathyroidism_transient, comp_hypoparathyroidism_permanent,
  comp_hypoparathyroidism_timing_window, comp_hypoparathyroidism_preexisting,
  comp_hypoparathyroidism_new_postop, comp_hypopara_permanent_limitation_note,
  comp_hypocalcemia_transient, comp_hypocalcemia_permanent,
  comp_hypocalcemia_timing_window, comp_hypocalcemia_clinical_preexisting,
  comp_rln_injury_transient, comp_rln_injury_timing_window,
  comp_vc_paresis_timing_window, comp_vc_paralysis_timing_window,
  comp_vc_paresis_permanent, comp_vc_paralysis_permanent,
  -- NSQIP perioperative complications
  nsqip_transfusion, nsqip_neck_hematoma, nsqip_hematoma_flag, nsqip_rln_injury_flag,
  nsqip_hypocalcemia_flag, nsqip_unplanned_intubation, nsqip_unplanned_return_or,
  nsqip_readmission_30d_flag, nsqip_readmission_count, nsqip_death_30d,
  nsqip_pneumonia, nsqip_dvt, nsqip_pe, nsqip_sepsis,
  nsqip_superficial_ssi, nsqip_deep_ssi, nsqip_organ_space_ssi,
  -- Tracheostomy & follow-up
  proc_nlp_tracheostomy, proc_nlp_tracheostomy_date, proc_nlp_tracheostomy_days_from_surg,
  proc_nlp_tracheostomy_n_mentions,
  followup_years, death_occurred, any_recurrence_flag, biochemical_recurrence_flag
FROM base b
ORDER BY research_id
"""


def main():
    os.environ["motherduck_token"] = load_token()
    import duckdb
    print("→ Connecting to MotherDuck (.eras account)...")
    con = duckdb.connect("md:thyroid_canonical_publication_v1_0")
    print("→ Extracting cohort...")
    df = con.execute(SQL_EXTRACT).df()
    print(f"→ {len(df):,} rows × {len(df.columns)} cols")
    df.to_parquet(PARQUET, index=False)
    print(f"→ Wrote {PARQUET}  ({PARQUET.stat().st_size/1024:.1f} KB)")
    # Sanity
    print(f"→ is_massive: {int(df['is_massive'].sum()):,} ({100*df['is_massive'].mean():.1f}%)")
    con.close()


if __name__ == "__main__":
    main()
