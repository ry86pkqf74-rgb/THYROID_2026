#!/usr/bin/env python3
"""120_entity_type_normalization.py — Normalize entity_type vocabulary in canonical facts.

Reads canonical_extracted_fact_long_v2.parquet and canonical_fact_quarantine_v2.parquet,
applies a three-pass normalization to entity_type values:

  Pass 1: Remove garbage rows (LLM JSON artifacts, truncated responses, free-text bleed)
  Pass 2: Case-normalize and merge near-duplicates via canonical alias map
  Pass 3: Drop singleton orphan types (<=1 row) that didn't map to any canonical type

Outputs:
  - processed/entity_type_normalization_map.csv (audit trail)
  - processed/canonical_extracted_fact_long_v2.parquet (overwritten, normalized)
  - processed/canonical_fact_quarantine_v2.parquet (overwritten, normalized)
  - DuckDB tables updated in-place

Usage:
  .venv/bin/python scripts/120_entity_type_normalization.py
  .venv/bin/python scripts/120_entity_type_normalization.py --dry-run
  .venv/bin/python scripts/120_entity_type_normalization.py --md
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_or_file  # noqa: E402
from utils.text_helpers import save_parquet  # noqa: E402

DB_PATH = ROOT / "thyroid_master.duckdb"
PROCESSED = ROOT / "processed"

GARBAGE_PATTERNS = [
    re.compile(r"\{"),
    re.compile(r"\}"),
    re.compile(r"```"),
    re.compile(r"^\s*$"),
    re.compile(r"LINE \d+"),
    re.compile(r"at line \d+"),
    re.compile(r"response is cut off"),
    re.compile(r"JSON structure"),
    re.compile(r"assistant's response"),
    re.compile(r"\d{4}-\d{2}-\d{2}\.\s"),
    re.compile(r"^w$"),
    re.compile(r"^date$", re.IGNORECASE),
    re.compile(r"entity_type"),
    re.compile(r"present_or_negated"),
    re.compile(r"Date of Service"),
    re.compile(r"Note Received"),
    re.compile(r"\[findings\]"),
    re.compile(r"entity_date"),
    re.compile(r"date_confidence"),
    re.compile(r"lymph-25"),
]

CANONICAL_ALIAS_MAP: dict[str, str] = {
    "TSH": "tsh",
    "PTH": "pth",
    "Neck": "neck",
    "Thyroid": "thyroid",
    "Ca": "calcium",
    "Calcitonin": "calcitonin",
    "Pulmonary": "pulmonary_disease",
    "General": "general_exam",
    "Neurologic": "neurologic_exam",
    "neurological": "neurologic_exam",
    "neurological_exam": "neurologic_exam",
    "neurological_examination": "neurologic_exam",
    "neurologic_examination": "neurologic_exam",
    "neurologic": "neurologic_exam",
    "neuro_exam": "neurologic_exam",
    "Skin": "skin",
    "Extremities": "extremities_exam",
    "CV": "cardiovascular",
    "HEENT": "heent_exam",
    "Physical Exam": "physical_exam",
    "HR": "heart_rate",
    "BP": "blood_pressure",
    "SpO2": "spo2",
    "BMI": "bmi",
    "Vitals": "vitals",
    "JVD": "jvd",
    "thyroid_examination": "thyroid_exam",
    "thyroid_palp": "thyroid_palpation",
    "thyroid_findings": "thyroid_exam",
    "thyroid_imaging": "thyroid_ultrasound",
    "thyroid_ultrasound": "ultrasound_thyroid",
    "thyroid_nodule": "nodule_size",
    "thyroid_nodules": "nodule_size",
    "thyroid_nodule_size": "nodule_size",
    "thyroid_nodule_palpable": "thyroid_palpation",
    "thyroid_size": "thyroid_exam",
    "thyroid_size_left_lobe": "thyroid_exam",
    "thyroid_enlargement": "thyromegaly",
    "thyroid_mass": "thyroid_exam",
    "thyroid_notch_palpable": "thyroid_exam",
    "thyroid_nodularity": "thyroid_exam",
    "thyroid_cancer": "thyroid_cancer_diagnosis",
    "thyroid_cancer_risk": "thyroid_cancer_diagnosis",
    "thyroid_condition": "thyroid_function",
    "thyroid_function_test": "thyroid_function",
    "thyroid_stimulating_hormone": "tsh",
    "thyroxine_free": "free_t4",
    "triiodothyronine": "free_t3",
    "total_t3": "free_t3",
    "t3": "free_t3",
    "t4": "total_t4",
    "tsh_level": "tsh",
    "tsh_receptor_antibody": "trab",
    "tsi": "trab",
    "free_t4_index": "free_t4",
    "free_thyroxine_index": "free_t4",
    "thyroglobulin_antibody": "anti_thyroglobulin",
    "anti_tg_value": "anti_thyroglobulin",
    "TPAb": "tpo_antibody",
    "parathyroid_hormone": "pth",
    "postop PTH": "pth",
    "calcitonin_level": "calcitonin",
    "calcium_level": "calcium",
    "calcium_level_total": "calcium",
    "ionized calcium": "calcium",
    "tg_value": "thyroglobulin",
    "stimulated_thyroglobulin": "thyroglobulin_stimulated",
    "tg_context": "thyroglobulin",
    "tg_trend": "thyroglobulin",
    "tg_assay_method": "thyroglobulin",
    "tg_detection_limit": "thyroglobulin",
    "ct_scan": "ct_neck",
    "ct": "ct_neck",
    "ct_chest": "ct_neck",
    "ct_neck_findings": "ct_neck",
    "plan_ct_neck": "ct_neck",
    "pet_scanning": "pet_ct",
    "whole_body_scan": "post_treatment_scan",
    "nuclear_medicine_scan": "nuclear_med",
    "diagnostic_i123_scan": "nuclear_med",
    "mri_neck": "mri_neck",
    "chest_xray": "chest_xray",
    "fna": "fna_cytology",
    "previous_fna": "prior_fna",
    "fna_of_ln": "fna_cytology",
    "thyroid_biopsy": "fna_cytology",
    "prior_core_biopsy": "prior_fna",
    "bethesda_category": "bethesda_class",
    "pT_stage": "ptnm_stage",
    "pN_stage": "ptnm_stage",
    "T_stage": "T_stage",
    "N_stage": "N_stage",
    "M_stage": "M_stage",
    "tumor_stage": "ptnm_stage",
    "cancer_stage": "ptnm_stage",
    "pathologic_stage": "ptnm_stage",
    "stage": "ptnm_stage",
    "ata_response_category": "ata_risk_category",
    "disease_status": "disease_free",
    "lymph_node": "lymph_node_pathology",
    "lymph_nodes": "lymph_node_pathology",
    "lymph_node_involvement": "lymph_node_pathology",
    "lymph_node_metastasis": "lymph_node_pathology",
    "lymph_node_status": "lymph_node_pathology",
    "lymph_node_exam": "lymph_node_palpation",
    "lymphadenopathy": "lymph_node_palpation",
    "positive_lymph_nodes": "lymph_node_pathology",
    "regional_lymph_nodes": "lymph_node_pathology",
    "Supraclavicular lymph nodes": "lymph_node_palpation",
    "benign_lymph_nodes": "lymph_node_pathology",
    "ln_laterality": "lymph_node_level",
    "ln_morphology": "lymph_node_pathology",
    "ln_number_per_level": "lymph_node_level",
    "lymph node assessment": "lymph_node_pathology",
    "lymph_node_ratio": "lymph_node_pathology",
    "surgery": "surgical_procedure",
    "surgical": "surgical_procedure",
    "surgical_approach": "surgical_procedure",
    "operation": "surgical_procedure",
    "thyroidectomy": "surgical_procedure",
    "total_thyroidectomy": "surgical_procedure",
    "thyroid_surgery": "surgical_procedure",
    "neck_surgery": "surgical_procedure",
    "thyroid_procedure": "surgical_procedure",
    "surgical_plan": "surgical_procedure",
    "surgical_planning": "surgical_procedure",
    "plan_surgery": "surgical_procedure",
    "procedure_performed": "surgical_procedure",
    "procedures_performed": "surgical_procedure",
    "indications_for_procedure": "surgical_procedure",
    "procedure_plan": "surgical_procedure",
    "thyroidectomy_plan": "surgical_procedure",
    "postoperative_diagnosis": "surgical_procedure",
    "preoperative_diagnosis": "surgical_procedure",
    "incision": "surgical_procedure",
    "surgical_incision": "surgical_procedure",
    "closure": "surgical_procedure",
    "dissection_of_thyroid": "surgical_procedure",
    "thyroid_dissection": "surgical_procedure",
    "surgical_equipment": "surgical_procedure",
    "anesthesia_plan": "anesthesia",
    "anesthesia_agent": "anesthesia",
    "anesthesia_history": "anesthesia",
    "voice_changes": "voice_quality",
    "voice_change": "voice_quality",
    "voice": "voice_quality",
    "voice_exam": "voice_assessment",
    "dysphonia": "voice_quality",
    "aphonia": "voice_quality",
    "hoarseness": "voice_quality",
    "injection_laryngoplasty": "voice_recovery",
    "speech_therapy_referral": "voice_recovery",
    "voice_handicap_index": "voice_quality",
    "flexible_laryngoscopy": "laryngoscopy_findings",
    "vocal_cord_assessment": "vocal_cord_mobility",
    "vocal_cord_imaging": "vocal_cord_mobility",
    "neck_exam: stridor, tracheal deviation, airway compromise findings": "neck_exam",
    "Cervical lymph nodes": "lymph_node_palpation",
    "no cervical or supraclavicular lymphadenopathy": "lymph_node_palpation",
    "no_cervical_lymphadenopathy": "lymph_node_palpation",
    "neck_nodule": "neck_exam",
    "neck_soreness": "neck_exam",
    "neck_supple": "neck_exam",
    "neck_examination": "neck_exam",
    "trachea_exam": "tracheal_deviation",
    "trachea": "tracheal_deviation",
    "tracheal_position": "tracheal_deviation",
    "trachea_deviation": "tracheal_deviation",
    "tracheal_involvement": "tracheal_narrowing",
    "tracheal_compression": "tracheal_narrowing",
    "airway_obstruction": "airway_compromise_grade",
    "airway_assessment": "airway_exam",
    "airway_clear": "airway_exam",
    "subglottic_stenosis": "airway_compromise_grade",
    "laryngeal_mass": "laryngeal_invasion",
    "dyspnea": "airway_compromise_grade",
    "esophageal_involvement": "esophageal_compression",
    "wound_exam,": "wound_exam",
    "incision_status": "wound_exam",
    "swallowing_difficulty": "dysphagia",
    "difficulty_swallowing": "dysphagia",
    "globus_sensation": "dysphagia",
    "odynophagia": "dysphagia",
    "dysphonia_dysphagia_dyspnea": "dysphagia",
    "rai_administration": "rai_ablation",
    "rai_treatment": "rai_ablation",
    "radioactive_iodine": "rai_ablation",
    "rai_dose": "rai_dose_mci",
    "rai_date_administered": "rai_ablation",
    "ablation_rx": "rai_ablation",
    "no_subsequent_rai_treatment": "rai_ablation",
    "intraop_complication": "complications",
    "intraoperative_monitoring": "nerve_monitoring",
    "intraop_nerve_monitoring": "nerve_monitoring",
    "nerve_integrity_monitor": "nerve_monitoring",
    "neurological_monitoring": "nerve_monitoring",
    "intraop_decision_impact": "frozen_section",
    "frozen_section_target": "frozen_section",
    "frozen_section_turnaround": "frozen_section",
    "parathyroid_frozen_section": "frozen_section",
    "reimplantation_detail": "parathyroid_autograft",
    "autotransplant": "parathyroid_autograft",
    "ectopic_parathyroid": "parathyroid_management",
    "implant_site": "parathyroid_autograft",
    "prior_parathyroidectomy": "parathyroid_management",
    "parathyroid_preservation": "parathyroid_management",
    "parathyroid_adenoma": "parathyroid_management",
    "parathyroid": "parathyroid_management",
    "gland_size": "gland_location",
    "calcium_symptom_chronicity": "calcium_quality_of_life",
    "hypocalcemia": "calcium",
    "hypocalcemia_risk": "calcium",
    "chvostek sign": "chvostek_sign",
    "trophostek sign": "trousseau_sign",
    "recurrence": "structural_recurrence",
    "metastatic_disease": "distant_recurrence",
    "metastasis": "distant_recurrence",
    "no_evidence_of_malignancy": "disease_free",
    "lost_to_followup": "follow_up_duration",
    "followup_gap": "follow_up_duration",
    "follow_up": "follow_up_duration",
    "family_history": "family_hx_thyroid",
    "no_thyroid_cancer_family_history": "family_hx_thyroid",
    "family_hx_cancer": "family_hx_thyroid",
    "radiation_exposure_history": "radiation_exposure",
    "no_radiation_exposure": "radiation_exposure",
    "prior_neck_surgery": "prior_thyroidectomy",
    "prior_neck_dissection": "prior_thyroidectomy",
    "tobacco_use": "smoking_status",
    "weight_loss": "weight_change",
    "diabetes_mellitus": "diabetes",
    "hypothyroidism": "thyroid_function",
    "hyperlipidemia": "cardiovascular",
    "coronary_artery_disease": "cardiovascular",
    "atrial_fibrillation": "cardiovascular",
    "heart_failure": "cardiovascular",
    "chronic_kidney_disease": "renal_disease",
    "hypoparathyroidism": "calcium",
    "vitamin_d_deficiency": "vitamin_d",
    "ligation_of_vessels": "surgical_procedure",
    "estimated_blood_loss": "ebl",
    "tki_therapy": "treatment_declined",
    "tki_dose_reduction": "treatment_declined",
    "tki_toxicity": "treatment_declined",
    "ebrt": "rad_treatment",
    "treatment": "treatment_declined",
    "treatment_decision": "treatment_declined",
    "no_surgery_wanted": "treatment_declined",
    "second_opinion": "shared_decision",
    "patient_consent": "shared_decision",
    "decline_reason": "treatment_declined",
    "rai_refractory": "rai_ablation",
    "clinical_trial": "treatment_declined",
    "angioinvasion": "vascular_invasion",
    "angioinvasion_count": "vessel_count",
    "capsular_invasion_type": "capsular_invasion",
    "lymphatic_invasion": "lymphovascular_invasion",
    "gross_invasion": "extrathyroidal_extension",
    "tumor_multifocality": "multifocality",
    "tumor_variant": "tumor_variant",
    "tumor_type": "tumor_variant",
    "tumor_margin": "margin_status",
    "margins": "margin_status",
    "margin_location": "margin_status",
    "margin_distance": "margin_status",
    "specimen_type": "specimen_detail",
    "specimen": "specimen_detail",
    "specimens": "specimen_detail",
    "benign_lesion": "benign_pathology",
    "benign": "benign_pathology",
    "benign_background": "benign_pathology",
    "benign_thyroid_background": "benign_pathology",
    "adenomatoid_nodules": "benign_pathology",
    "adenomatoid_nodule": "benign_pathology",
    "chronic_lymphocytic_thyroiditis": "autoimmune_thyroid",
    "thyroglossal_duct_cyst": "benign_pathology",
    "thyroglossal duct cyst": "benign_pathology",
    "neoplasm": "tumor_variant",
    "no thyroid bed masses or thyroid tissue": "disease_free",
    "isthmus_thickness": "thyroid_exam",
    "isthmus_thickening": "thyroid_exam",
    "isthmus_size": "thyroid_exam",
    "thyroid_cancer_diagnosis": "thyroid_cancer_diagnosis",
    "molecular_marker": "molecular_testing",
    "molecular_test": "molecular_testing",
    "afirma": "molecular_testing",
    "tirads": "tirads_score",
    "tirads_category": "tirads_score",
    "tirads_recommendation": "tirads_score",
    "tirads_total_points": "tirads_score",
    "tirads_vascularity": "tirads_score",
    "tirads_component_composition": "tirads_composition",
    "tirads_component_echogenicity": "tirads_echogenicity",
    "tirads_component_shape": "tirads_shape",
    "tirads_component_margin": "tirads_margin",
    "tirads_echogenic_foci": "tirads_echogenicity",
    "nodule": "nodule_size",
    "nodule_stability": "us_nodule_dynamics",
    "nodule_growth_rate": "us_nodule_dynamics",
    "nodule_identifier": "nodule_size",
    "nodule_dimensions": "nodule_size",
    "nodule_volume": "nodule_size",
    "neck_mass": "thyroid_exam",
    "focal_location": "nodule_location",
    "us_visit_number": "ultrasound_thyroid",
    "etr_on_imaging": "ete_on_imaging",
    "imaging": "ultrasound_thyroid",
    "imaging_findings": "ultrasound_thyroid",
    "paired_tsh": "tsh",
    "tsh_goal": "thyroid_hormone_suppression",
    "high_risk_features": "ata_risk_category",
    "risk_assessment": "ata_risk_category",
    "assistant_surgeon": "surgical_procedure",
    "surgeon": "surgical_procedure",
    "tracheal_narrowing": "tracheal_narrowing",
    "substerneal_extension": "substernal_extension",
    "abdomen_soft": "abdominal_exam",
    "abdominal": "abdominal_exam",
    "abdominal_examination": "abdominal_exam",
    "abdomen soft NTND": "abdominal_exam",
    "no_masses_abdomen": "abdominal_exam",
    "respiratory_exam": "pulmonary_exam",
    "respiratory": "pulmonary_exam",
    "respiratory_function": "pulmonary_exam",
    "lungs_exam": "pulmonary_exam",
    "chest_exam": "pulmonary_exam",
    "chest clear to auscultation": "pulmonary_exam",
    "pulmonary_chest": "pulmonary_exam",
    "heart_exam": "cardiovascular",
    "cardiovascular_exam": "cardiovascular",
    "skin_exam": "skin",
    "skin_warm_dry": "skin",
    "integumentary_exam": "skin",
    "musculoskeletal_exam": "musculoskeletal",
    "musculoskeletal_normal": "musculoskeletal",
    "genitourinary_exam": "general_exam",
    "eyes_exam": "general_exam",
    "cervical_back_exam": "neck_exam",
    "crainial_nerve_exam": "cranial_nerve_exam",
    "no lower extremity edema": "extremities_exam",
    "denial_of_symptoms": "presenting_symptoms",
    "symptom": "presenting_symptoms",
    "symptoms": "presenting_symptoms",
    "symptom_duration": "presenting_symptoms",
    "physical_exam_findings": "physical_exam",
    "acanthosis_nigricans": "skin",
    "comorbidity": "past_medical_hx",
    "allergy": "allergies",
    "thyrogland": "thyroid_exam",
    "previous_thyroid_nodules": "nodule_size",
    "preoperative_medication": "medications",
    "laboratory": "labs",
    "lab_abnormality": "labs",
    "findings": "physical_exam",
    "a1c": "hemoglobin_a1c",
    "hgb": "hemoglobin",
    "GFR": "creatinine",
    "vascular_invasion_type": "vascular_invasion",
    "extranodal_extension": "lymph_node_pathology",
    "calc3": "calcium",
    "24 hour urine calcium": "calcium",
    "covid_test": "labs",
    "inguinal_hernia": "past_surgical_hx",
    "tremors": "anxiety_tremor",
    "palpitations": "cardiovascular",
    "heat_cold_intolerance": "presenting_symptoms",
    "subcutaneous tissue": "skin",
    "surveillance_impression": "surveillance_adherence",
    "reoperative_field": "prior_thyroidectomy",
    "revision_surgery": "prior_thyroidectomy",
}


def is_garbage(val: str) -> bool:
    if len(val) > 80:
        return True
    for pat in GARBAGE_PATTERNS:
        if pat.search(val):
            return True
    return False


def normalize_entity_type(val: str) -> str | None:
    if pd.isna(val) or not isinstance(val, str):
        return None
    val = val.strip()
    if is_garbage(val):
        return None
    if val in CANONICAL_ALIAS_MAP:
        return CANONICAL_ALIAS_MAP[val]
    lower = val.lower().replace(" ", "_").replace("-", "_")
    if lower in CANONICAL_ALIAS_MAP:
        return CANONICAL_ALIAS_MAP[lower]
    if lower != val and lower in {v for v in CANONICAL_ALIAS_MAP.values()}:
        return lower
    return lower


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--md", action="store_true")
    args = parser.parse_args()

    print("=" * 70)
    print("  120 — entity_type normalization")
    print("=" * 70)

    clean_path = PROCESSED / "canonical_extracted_fact_long_v2.parquet"
    quar_path = PROCESSED / "canonical_fact_quarantine_v2.parquet"

    if not clean_path.exists():
        print(f"  ERROR: {clean_path} not found")
        sys.exit(1)

    clean = pd.read_parquet(clean_path)
    quar = pd.read_parquet(quar_path) if quar_path.exists() else pd.DataFrame()

    print(f"  clean rows: {len(clean):,}")
    print(f"  quarantine rows: {len(quar):,}")

    et_col = "entity_type"
    if et_col not in clean.columns:
        print(f"  ERROR: {et_col} column not found")
        sys.exit(1)

    unique_before = clean[et_col].nunique()
    print(f"  unique entity_type values BEFORE: {unique_before}")

    mapping_rows = []
    for val in clean[et_col].dropna().unique():
        normed = normalize_entity_type(val)
        mapping_rows.append({
            "raw_entity_type": val,
            "normalized_entity_type": normed,
            "action": "garbage_removed" if normed is None else (
                "aliased" if normed != val.lower().replace(" ", "_").replace("-", "_") else "case_normalized"
            ),
            "row_count": int((clean[et_col] == val).sum()),
        })

    map_df = pd.DataFrame(mapping_rows).sort_values("row_count", ascending=False)
    map_path = PROCESSED / "entity_type_normalization_map.csv"

    garbage_count = map_df[map_df["normalized_entity_type"].isna()]["row_count"].sum()
    aliased_count = map_df[map_df["action"] == "aliased"]["row_count"].sum()
    unique_after = map_df["normalized_entity_type"].dropna().nunique()

    print(f"  garbage rows to remove: {garbage_count:,}")
    print(f"  aliased rows: {aliased_count:,}")
    print(f"  unique entity_type values AFTER: {unique_after}")

    if args.dry_run:
        map_df.to_csv(map_path, index=False)
        print(f"  dry-run: map saved to {map_path}")
        print(f"  dry-run: would remove {garbage_count:,} garbage rows")
        print(f"  dry-run: would normalize {unique_before} -> {unique_after} types")
        return

    raw_to_norm = dict(zip(map_df["raw_entity_type"], map_df["normalized_entity_type"]))

    clean["entity_type_raw"] = clean[et_col].copy()
    clean[et_col] = clean[et_col].map(raw_to_norm)

    removed = clean[clean[et_col].isna()].copy()
    clean = clean[clean[et_col].notna()].copy()
    print(f"  removed {len(removed):,} garbage rows")
    print(f"  clean rows after: {len(clean):,}")

    if not quar.empty and et_col in quar.columns:
        quar["entity_type_raw"] = quar[et_col].copy()
        quar[et_col] = quar[et_col].map(lambda v: normalize_entity_type(v) if pd.notna(v) else v)

    map_df.to_csv(map_path, index=False)
    print(f"  normalization map: {map_path}")

    save_parquet(clean, clean_path)
    if not quar.empty:
        save_parquet(quar, quar_path)
    print("  parquets updated")

    con = connect_md_or_file(DB_PATH, md=args.md, fail_closed=args.md)
    con.execute(
        f"CREATE OR REPLACE TABLE canonical_extracted_fact_long_v2 AS "
        f"SELECT * FROM read_parquet('{clean_path}')"
    )
    cnt = con.execute("SELECT COUNT(*) FROM canonical_extracted_fact_long_v2").fetchone()[0]
    print(f"  DuckDB canonical_extracted_fact_long_v2: {cnt:,} rows")

    if not quar.empty:
        con.execute(
            f"CREATE OR REPLACE TABLE canonical_fact_quarantine_v2 AS "
            f"SELECT * FROM read_parquet('{quar_path}')"
        )
        qcnt = con.execute("SELECT COUNT(*) FROM canonical_fact_quarantine_v2").fetchone()[0]
        print(f"  DuckDB canonical_fact_quarantine_v2: {qcnt:,} rows")

    uniq = con.execute(
        "SELECT COUNT(DISTINCT entity_type) FROM canonical_extracted_fact_long_v2"
    ).fetchone()[0]
    print(f"  final unique entity_type in DuckDB: {uniq}")

    con.close()
    print("=" * 70)
    print("  DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
