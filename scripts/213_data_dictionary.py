#!/usr/bin/env python3
"""
THYROID_2026 — Script 213: Data Dictionary + Source Truth Map
Database: thyroid_ete_fix_20260413
Table:    canonical_patient_master_v1

Outputs:
  scripts/output/data_dictionary.csv
  scripts/output/source_truth_map.md

Run:
  .venv/bin/python scripts/213_data_dictionary.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

DB = "thyroid_ete_fix_20260413"
CANONICAL = "canonical_patient_master_v1"
TOTAL_ROWS = 10_871
OUTPUT_DIR = REPO / "scripts" / "output"

# ─────────────────────────────────────────────────────────────────────────────
# Domain / source assignment rules — evaluated in order, first match wins
# ─────────────────────────────────────────────────────────────────────────────
DOMAIN_RULES: list[tuple[str, str, str, str]] = [
    # (prefix_regex, clinical_domain, source_table, source_script)
    (r"^research_id$|^age_|^sex$|^race$|^demo_",
     "demographics", "gold_master_patient_facts_v1", "204"),
    # ── Script 215: Deep NLP entity integration (8 sources) ──────────────────
    (r"^op_nlp_",
     "nlp_operative", "note_entities_operative_detail", "215"),
    (r"^med_nlp_",
     "nlp_medications", "note_entities_medications", "215"),
    (r"^pmhx_nlp_(hypertension|diabetes|hyperthyroidism|hypothyroidism|obesity|breast_cancer|depression|cad|ckd|afib|copd|asthma|gerd|lung_cancer|n_comorbidities|comorbidity_list|n_source|note_types|extraction)",
     "nlp_problem_list", "note_entities_problem_list", "215"),
    (r"^pmhx_nlp_(radiation|family_hx|smoking|men_syndrome|autoimmune|prior_cancer|coagulopathy|osteoporosis)",
     "nlp_pmhx_llm", "note_entities_llm_past_medical_hx", "215"),
    (r"^pmhx_llm_",
     "nlp_pmhx_llm", "note_entities_llm_past_medical_hx", "215"),
    (r"^pshx_(nlp|llm)_",
     "nlp_surgical_hx", "note_entities_llm_past_surgical_hx", "215"),
    (r"^proc_nlp_",
     "nlp_procedures", "note_entities_procedures", "215"),
    (r"^sx_(nlp|llm)_",
     "nlp_symptoms", "note_entities_llm_presenting_symptoms", "215"),
    (r"^radtx_(nlp|llm)_",
     "nlp_rad_treatment", "note_entities_llm_rad_treatment", "215"),
    (r"^(first_surgery_date|surg_|op_)",
     "surgery", "gold_master_patient_facts_v1 / operative_episode_detail_v2", "204/207"),
    (r"^(diagnosis_|is_malignant|n_tumors|tumor_size_cm|multifocal_flag|laterality)",
     "pathology", "canonical_diagnosis_unified_v1 / path_synoptics", "200/204"),
    (r"^(ete_grade|margin_status|closest_margin_mm|vascular_invasion_grade|vessel_count|lvi_grade|perineural_invasion|capsular_invasion|path_|histology_|aggressive_variant_flag|gross_ete_flag)",
     "pathology", "gold_master_patient_facts_v1 / patient_refined_master_clinical_v12", "207/211"),
    (r"^(ajcc8_|ata_|macis_|ages_|ames_|scoring_|distant_mets_proxy)",
     "scoring", "thyroid_scoring_py_v1", "207"),
    (r"^(ln_total_|ln_positive_|ln_ratio|ln_burden_|ln_ene_|ln_lateral_|ln_source|ln_confidence|ene_|tp_ln)",
     "lymph_nodes", "gold_master_patient_facts_v1 / tumor_pathology", "205/207"),
    (r"^(ln_rollup_|ln_level_|tp_)",
     "lymph_nodes", "ln_master_rollup_v1 / tumor_pathology", "208"),
    (r"^(bethesda_|n_fna_|fna_|worst_bethesda_|preop_fna_)",
     "fna", "fna_cytology / patient_refined_master_clinical_v12", "205/207"),
    (r"^(preop_tirads_|tirads_best_combined|tirads_worst_combined|tirads_.*_v12)",
     "tirads", "extracted_tirads_validated_v1 / raw_us_tirads_excel_v1", "205/207"),
    (r"^(imaging_|n_us_|bilateral_|dominant_|has_suspicious_|longitudinal_|max_tirads_|worst_tirads_category)",
     "imaging_us", "imaging_patient_summary_v1 / imaging_exam_master_v1", "207"),
    (r"^ct_",
     "imaging_ct", "ct_imaging", "207"),
    (r"^nucmed_",
     "imaging_nuclear", "nuclear_med", "207"),
    (r"^(mol_|molecular_|braf_|ras_|tert_|alk_|ret_|ntrk_|tp53_|eif1ax_|pax8_|any_fusion_|high_risk_mol|preop_sweep_)",
     "molecular", "gold_master_patient_facts_v1 / patient_refined_master_clinical_v12", "207/211"),
    (r"^rai_",
     "rai", "gold_master_patient_facts_v1 / rai_treatment_episode_v2", "207/211"),
    (r"^(tg_|tgab_|anti_tg_|n_tg_|n_tgab_)",
     "labs", "tg_timeline_patient_summary_v1 / gold_master_patient_facts_v1", "207"),
    (r"^(calcium_|pth_|postop_)",
     "labs", "extracted_postop_labs_expanded_v1", "211"),
    (r"^(recurrence_|any_recurrence|biochemical_recurrence|time_to_recurrence|rec_)",
     "recurrence", "canonical_recurrence_v1 / recurrence_event_clean_v1", "203/211"),
    (r"^(rln_|voice_)",
     "voice", "extracted_rln_injury_refined_v2 / patient_refined_master_clinical_v12", "211"),
    (r"^comp_",
     "complications", "complication_phenotype_v1", "211"),
    (r"^(lateral_|lat_neck_)",
     "lateral_neck", "patient_refined_master_clinical_v12", "207"),
    (r"^completion_",
     "completion", "patient_refined_master_clinical_v12", "207"),
    (r"^(surv_|survival_)",
     "survival", "survival_cohort_enriched / canonical_survival_followup_v1", "201/211"),
    (r"^(followup_|last_contact_)",
     "survival", "canonical_survival_followup_v1", "201"),
    # ── Script 212: LLM entity rollup ─────────────────────────────────────────
    (r"^nlp_llm_(pathology|synoptic_path)",
     "nlp_pathology", "note_entities_llm_pathology / note_entities_llm_synoptic_pathology_enrichment", "212"),
    (r"^nlp_llm_tirads",
     "nlp_tirads", "note_entities_llm_tirads_granular", "212"),
    (r"^nlp_llm_(cervical_ln|ln)",
     "nlp_ln", "note_entities_llm_cervical_ln_detail", "212"),
    (r"^nlp_llm_(tg_kinetics|labs)",
     "nlp_tg", "note_entities_llm_tg_kinetics / note_entities_llm_labs", "212"),
    (r"^nlp_llm_recurrence",
     "nlp_recurrence", "note_entities_llm_recurrence", "212"),
    (r"^nlp_llm_vascular",
     "nlp_vascular", "note_entities_llm_vascular_invasion", "212"),
    (r"^nlp_llm_",
     "nlp_other", "note_entities_llm_*", "212"),
    (r"^nlp_ne_",
     "nlp_other", "note_entities_* (non-LLM)", "212"),
    (r"^.*_(eligible_flag|eligible)$",
     "eligibility", "gold_master_patient_facts_v1", "207"),
    (r"^.*_(confidence|source|traceability|provenance)",
     "provenance", "gold_master_patient_facts_v1", "207"),
]

# ─────────────────────────────────────────────────────────────────────────────
# Clinical description templates (prefix → description)
# ─────────────────────────────────────────────────────────────────────────────
DESCRIPTION_TEMPLATES: dict[str, str] = {
    "research_id": "Unique de-identified research identifier for each patient.",
    "age_at_surgery": "Patient age in years at time of first surgery.",
    "sex": "Patient biological sex (Male / Female).",
    "race": "Patient self-reported race/ethnicity category.",
    "first_surgery_date": "Date of patient's first thyroid surgery.",
    "surg_": "Surgery-related flag or count derived from operative episode data.",
    "op_": "Operative detail flag derived from operative_episode_detail_v2 NLP.",
    "is_malignant": "TRUE if patient has a confirmed malignant thyroid diagnosis.",
    "diagnosis_primary": "Primary histologic diagnosis (e.g., PTC, FTC, MTC, ATC).",
    "diagnosis_variant": "Histologic subtype/variant of the primary diagnosis.",
    "ajcc8_": "AJCC 8th Edition staging element computed by thyroid_scoring_py_v1.",
    "ata_risk_category": "ATA 2015 initial risk stratification (low/intermediate/high).",
    "ata_response_category": "ATA response-to-therapy classification.",
    "macis_": "MACIS score element (Metastasis, Age, Completeness, Invasion, Size).",
    "ages_": "AGES score component for risk stratification.",
    "ames_": "AMES (Age, Metastasis, Extent, Size) risk classification element.",
    "ete_grade": "Extrathyroidal extension grade: none/microscopic/gross/present_ungraded.",
    "margin_status": "Surgical margin status: positive/negative/close/unknown.",
    "vascular_invasion_grade": "Vascular invasion WHO 2022 grade: focal/extensive/present_ungraded.",
    "lvi_grade": "Lymphovascular invasion grade derived from pathology synoptics.",
    "perineural_invasion": "Perineural invasion status from pathology report.",
    "ln_total_examined": "Total number of lymph nodes examined across all dissections.",
    "ln_positive_count": "Number of lymph nodes positive for malignancy.",
    "ln_ratio": "Ratio of positive to total examined lymph nodes.",
    "ln_burden_band": "Lymph node burden band: none/low/intermediate/high.",
    "ene_": "Extranodal extension attribute (multi-source: path, imaging, NLP).",
    "bethesda_": "FNA Bethesda category attribute.",
    "fna_": "Fine needle aspiration attribute.",
    "preop_tirads_": "Pre-operative TI-RADS score attribute from structured ultrasound data.",
    "tirads_": "TI-RADS score/category from ultrasound nodule assessment.",
    "imaging_": "Imaging summary attribute from ultrasound patient summary.",
    "ct_": "CT imaging attribute derived from ct_imaging table.",
    "nucmed_": "Nuclear medicine study attribute (RAI scan, PET).",
    "mol_": "Molecular testing summary attribute.",
    "braf_": "BRAF mutation attribute (detection across structured + NLP sources).",
    "ras_": "RAS mutation attribute (NRAS/HRAS/KRAS).",
    "tert_": "TERT promoter mutation attribute.",
    "rai_": "Radioactive iodine treatment or scan attribute.",
    "tg_": "Serum thyroglobulin lab attribute.",
    "tgab_": "Thyroglobulin antibody (TgAb) lab attribute.",
    "recurrence_": "Recurrence classification or date attribute.",
    "rln_": "Recurrent laryngeal nerve injury attribute.",
    "voice_": "Voice outcome or laryngoscopy attribute.",
    "comp_": "Post-operative complication attribute (refined extraction).",
    "surv_": "Survival analysis attribute from survival cohort.",
    "followup_": "Follow-up duration or date attribute.",
    "last_contact_": "Last known contact date or status.",
    "lateral_": "Lateral neck dissection attribute.",
    "completion_": "Completion thyroidectomy indicator or reason.",
    "nlp_llm_": "NLP entity rollup from note_entities_llm_* tables (additive, not override).",
    "nlp_ne_": "NLP entity metadata from non-LLM note_entities_* tables.",
    # Script 215 NLP prefixes
    "op_nlp_ebl_ml": "Estimated blood loss (mL) from operative note NLP (SPLIT_PART parse of '10 mL' format).",
    "op_nlp_ebl_date": "Date of operative note that documented EBL.",
    "op_nlp_ebl_n_mentions": "Number of operative note entities reporting EBL.",
    "op_nlp_nerve_monitoring_used": "TRUE if intraoperative nerve monitoring mentioned in operative notes.",
    "op_nlp_nerve_monitoring_type": "Type of nerve monitoring device (e.g., NIM, nerve integrity monitor).",
    "op_nlp_nerve_monitoring_date": "Date of earliest nerve monitoring mention.",
    "op_nlp_nerve_monitoring_n_mentions": "Count of nerve monitoring entity mentions.",
    "op_nlp_berry_ligament_dissected": "TRUE if Berry ligament dissection explicitly mentioned.",
    "op_nlp_berry_ligament_mentioned": "TRUE if Berry ligament mentioned in any context.",
    "op_nlp_berry_ligament_date": "Date of Berry ligament mention.",
    "op_nlp_berry_ligament_n_mentions": "Count of Berry ligament entity mentions.",
    "op_nlp_parathyroid_managed": "TRUE if parathyroid management mentioned in operative note.",
    "op_nlp_parathyroid_managed_n_mentions": "Count of parathyroid management mentions.",
    "op_nlp_parathyroid_autograft": "TRUE if parathyroid autograft mentioned in operative note.",
    "op_nlp_parathyroid_autograft_n_mentions": "Count of parathyroid autograft mentions.",
    "op_nlp_parathyroid_date": "Date of earliest parathyroid management/autograft mention.",
    "op_nlp_rln_finding": "TRUE if recurrent laryngeal nerve finding documented in operative note.",
    "op_nlp_rln_finding_n_mentions": "Count of RLN finding entity mentions.",
    "op_nlp_rln_finding_date": "Date of earliest RLN finding mention.",
    "op_nlp_drain_placed": "TRUE if surgical drain placement documented in operative notes.",
    "op_nlp_drain_placed_n_mentions": "Count of drain placement entity mentions.",
    "op_nlp_drain_date": "Date of drain placement documentation.",
    "op_nlp_strap_muscle_involved": "TRUE if strap muscle involvement mentioned in operative note.",
    "op_nlp_strap_muscle_n_mentions": "Count of strap muscle entity mentions.",
    "op_nlp_reoperative_field": "TRUE if reoperative/scarred operative field documented.",
    "op_nlp_reoperative_n_mentions": "Count of reoperative field entity mentions.",
    "op_nlp_intraop_complication": "TRUE if intraoperative complication documented in operative note.",
    "op_nlp_intraop_complication_n_mentions": "Count of intraoperative complication entity mentions.",
    "op_nlp_intraop_complication_date": "Date of intraoperative complication documentation.",
    "op_nlp_gross_invasion": "TRUE if gross local invasion mentioned in operative note.",
    "op_nlp_tracheal_involvement": "TRUE if tracheal involvement mentioned in operative note.",
    "op_nlp_tracheal_n_mentions": "Count of tracheal involvement entity mentions.",
    "op_nlp_esophageal_involvement": "TRUE if esophageal involvement mentioned in operative note.",
    "op_nlp_esophageal_n_mentions": "Count of esophageal involvement entity mentions.",
    "op_nlp_n_source_notes": "Number of distinct operative notes contributing to NLP rollup.",
    "op_nlp_note_types": "Comma-separated note types contributing to operative NLP rollup.",
    "op_nlp_extraction_method": "NLP extraction method for operative entities (regex_operative_v2).",
    "med_nlp_levothyroxine": "TRUE if levothyroxine documented in medication entities.",
    "med_nlp_levothyroxine_date": "Date of first levothyroxine medication mention.",
    "med_nlp_levothyroxine_n_mentions": "Count of levothyroxine medication entity mentions.",
    "med_nlp_calcium_supplement": "TRUE if calcium supplement documented in medication entities.",
    "med_nlp_calcium_supplement_date": "Date of first calcium supplement mention.",
    "med_nlp_calcium_supplement_n_mentions": "Count of calcium supplement entity mentions.",
    "med_nlp_calcitriol": "TRUE if calcitriol (active vitamin D) documented — implies hypoparathyroidism.",
    "med_nlp_calcitriol_date": "Date of first calcitriol mention.",
    "med_nlp_calcitriol_n_mentions": "Count of calcitriol entity mentions.",
    "med_nlp_n_source_notes": "Number of distinct notes contributing to medication NLP rollup.",
    "med_nlp_note_types": "Comma-separated note types for medication NLP rollup.",
    "med_nlp_extraction_method": "NLP extraction method for medication entities (regex_medication_v2).",
    "pmhx_nlp_hypertension": "TRUE if hypertension documented in problem list NLP.",
    "pmhx_nlp_hypertension_n_mentions": "Count of hypertension problem list mentions.",
    "pmhx_nlp_hypertension_first_date": "Date of first hypertension mention in problem list.",
    "pmhx_nlp_diabetes": "TRUE if diabetes (type 1 or 2) documented in problem list NLP.",
    "pmhx_nlp_diabetes_n_mentions": "Count of diabetes problem list mentions.",
    "pmhx_nlp_diabetes_first_date": "Date of first diabetes mention in problem list.",
    "pmhx_nlp_hyperthyroidism": "TRUE if hyperthyroidism documented in problem list NLP.",
    "pmhx_nlp_hyperthyroidism_n_mentions": "Count of hyperthyroidism mentions.",
    "pmhx_nlp_hyperthyroidism_first_date": "Date of first hyperthyroidism mention.",
    "pmhx_nlp_hypothyroidism": "TRUE if hypothyroidism documented in problem list NLP.",
    "pmhx_nlp_hypothyroidism_n_mentions": "Count of hypothyroidism mentions.",
    "pmhx_nlp_hypothyroidism_first_date": "Date of first hypothyroidism mention.",
    "pmhx_nlp_obesity": "TRUE if obesity documented in problem list NLP.",
    "pmhx_nlp_obesity_n_mentions": "Count of obesity mentions.",
    "pmhx_nlp_obesity_first_date": "Date of first obesity mention.",
    "pmhx_nlp_breast_cancer": "TRUE if breast cancer documented in problem list NLP.",
    "pmhx_nlp_breast_cancer_n_mentions": "Count of breast cancer mentions.",
    "pmhx_nlp_depression": "TRUE if depression documented in problem list NLP.",
    "pmhx_nlp_depression_n_mentions": "Count of depression mentions.",
    "pmhx_nlp_cad": "TRUE if coronary artery disease documented in problem list NLP.",
    "pmhx_nlp_cad_n_mentions": "Count of CAD mentions.",
    "pmhx_nlp_ckd": "TRUE if chronic kidney disease documented in problem list NLP.",
    "pmhx_nlp_ckd_n_mentions": "Count of CKD mentions.",
    "pmhx_nlp_afib": "TRUE if atrial fibrillation documented in problem list NLP.",
    "pmhx_nlp_afib_n_mentions": "Count of atrial fibrillation mentions.",
    "pmhx_nlp_copd": "TRUE if COPD documented in problem list NLP.",
    "pmhx_nlp_copd_n_mentions": "Count of COPD mentions.",
    "pmhx_nlp_asthma": "TRUE if asthma documented in problem list NLP.",
    "pmhx_nlp_asthma_n_mentions": "Count of asthma mentions.",
    "pmhx_nlp_gerd": "TRUE if GERD documented in problem list NLP.",
    "pmhx_nlp_gerd_n_mentions": "Count of GERD mentions.",
    "pmhx_nlp_lung_cancer": "TRUE if lung cancer documented in problem list NLP.",
    "pmhx_nlp_lung_cancer_n_mentions": "Count of lung cancer mentions.",
    "pmhx_nlp_n_comorbidities": "Count of distinct comorbid conditions from problem list NLP.",
    "pmhx_nlp_comorbidity_list": "Semicolon-separated list of all documented comorbidities.",
    "pmhx_nlp_n_source_notes": "Number of distinct notes contributing to problem list NLP rollup.",
    "pmhx_nlp_note_types": "Comma-separated note types for problem list NLP rollup.",
    "pmhx_nlp_extraction_method": "NLP extraction method for problem list entities (regex_problem_list_v2).",
    "pmhx_nlp_radiation_exposure": "TRUE if prior radiation exposure documented by LLM (NOVEL — no structured equivalent).",
    "pmhx_nlp_radiation_exposure_date": "Date of radiation exposure event (entity_date when available).",
    "pmhx_nlp_radiation_exposure_n_mentions": "Count of radiation exposure entity mentions.",
    "pmhx_nlp_radiation_exposure_confidence": "Mean confidence of radiation exposure LLM entities.",
    "pmhx_nlp_family_hx_thyroid": "TRUE if family history of thyroid disease documented by LLM.",
    "pmhx_nlp_family_hx_thyroid_n_mentions": "Count of family thyroid history entity mentions.",
    "pmhx_nlp_family_hx_cancer": "TRUE if family history of any cancer documented by LLM.",
    "pmhx_nlp_smoking_status": "Smoking status extracted by LLM (never/former/current).",
    "pmhx_nlp_men_syndrome": "TRUE if MEN syndrome (MEN2) documented — critical for MTC patients.",
    "pmhx_nlp_autoimmune_thyroid_hx": "TRUE if autoimmune thyroid history documented by LLM from patient history.",
    "pmhx_nlp_autoimmune_thyroid_hx_n_mentions": "Count of autoimmune thyroid history mentions.",
    "pmhx_nlp_prior_cancer_hx": "TRUE if prior non-thyroid cancer history documented by LLM.",
    "pmhx_nlp_prior_cancer_hx_n_mentions": "Count of prior cancer history entity mentions.",
    "pmhx_nlp_coagulopathy": "TRUE if coagulopathy/bleeding disorder documented by LLM.",
    "pmhx_nlp_osteoporosis": "TRUE if osteoporosis/osteopenia documented by LLM.",
    "pmhx_llm_n_source_notes": "Number of distinct notes contributing to past medical hx LLM rollup.",
    "pmhx_llm_note_types": "Comma-separated note types for past medical hx LLM rollup.",
    "pmhx_llm_extraction_method": "LLM extraction method for past medical history (qwen3_32b).",
    "pmhx_llm_min_confidence": "Minimum confidence score among past medical hx LLM entities used.",
    "pmhx_llm_mean_confidence": "Mean confidence score among past medical hx LLM entities used.",
    "pshx_nlp_prior_thyroidectomy": "TRUE if prior thyroidectomy documented in surgical history LLM.",
    "pshx_nlp_prior_thyroidectomy_n_mentions": "Count of prior thyroidectomy entity mentions.",
    "pshx_nlp_prior_thyroidectomy_date": "Date of prior thyroidectomy (entity_date when available).",
    "pshx_nlp_prior_fna": "TRUE if prior FNA biopsy documented in surgical history LLM.",
    "pshx_nlp_prior_fna_n_mentions": "Count of prior FNA entity mentions.",
    "pshx_nlp_prior_rai": "TRUE if prior RAI treatment documented in surgical history LLM.",
    "pshx_nlp_prior_rai_n_mentions": "Count of prior RAI entity mentions.",
    "pshx_nlp_prior_rai_date": "Date of prior RAI (entity_date when available).",
    "pshx_nlp_prior_neck_surgery": "TRUE if prior neck surgery (non-thyroid) documented by LLM.",
    "pshx_nlp_prior_neck_surgery_n_mentions": "Count of prior neck surgery mentions.",
    "pshx_nlp_prior_neck_dissection": "TRUE if prior neck dissection documented by LLM.",
    "pshx_nlp_prior_parathyroidectomy": "TRUE if prior parathyroidectomy documented by LLM.",
    "pshx_nlp_n_prior_procedures": "Count of distinct prior surgical procedure types documented.",
    "pshx_llm_n_source_notes": "Number of distinct notes contributing to past surgical hx LLM rollup.",
    "pshx_llm_note_types": "Comma-separated note types for past surgical hx LLM rollup.",
    "pshx_llm_extraction_method": "LLM extraction method for past surgical history (qwen3_32b).",
    "pshx_llm_min_confidence": "Minimum confidence score among past surgical hx LLM entities.",
    "pshx_llm_mean_confidence": "Mean confidence score among past surgical hx LLM entities.",
    "proc_nlp_tracheostomy": "TRUE if tracheostomy procedure documented in procedure NLP.",
    "proc_nlp_tracheostomy_date": "Date of tracheostomy procedure mention.",
    "proc_nlp_tracheostomy_n_mentions": "Count of tracheostomy mentions.",
    "proc_nlp_laryngoscopy": "TRUE if laryngoscopy procedure documented in procedure NLP.",
    "proc_nlp_laryngoscopy_date": "Date of laryngoscopy procedure mention.",
    "proc_nlp_laryngoscopy_n_mentions": "Count of laryngoscopy mentions.",
    "proc_nlp_mrnd": "TRUE if modified radical neck dissection documented in procedure NLP.",
    "proc_nlp_mrnd_n_mentions": "Count of MRND mentions.",
    "proc_nlp_lateral_neck_dissection": "TRUE if lateral neck dissection (any type) documented.",
    "proc_nlp_parathyroid_autotransplant": "TRUE if parathyroid autotransplant documented in procedure NLP.",
    "proc_nlp_n_source_notes": "Number of distinct notes contributing to procedure NLP rollup.",
    "proc_nlp_note_types": "Comma-separated note types for procedure NLP rollup.",
    "proc_nlp_extraction_method": "NLP extraction method for procedure entities (regex_procedure_v2).",
    "sx_nlp_dysphagia": "TRUE if dysphagia documented in presenting symptoms LLM. ⚠ LOW COVERAGE (<2%).",
    "sx_nlp_hoarseness": "TRUE if hoarseness/dysphonia documented in presenting symptoms LLM. ⚠ LOW COVERAGE.",
    "sx_nlp_neck_mass": "TRUE if neck mass documented in presenting symptoms LLM. ⚠ LOW COVERAGE.",
    "sx_nlp_dyspnea": "TRUE if dyspnea documented in presenting symptoms LLM. ⚠ LOW COVERAGE.",
    "sx_nlp_any_symptom_data": "TRUE if any presenting symptom entity extracted by LLM. ⚠ LOW COVERAGE.",
    "sx_llm_n_source_notes": "Number of distinct notes contributing to presenting symptoms LLM rollup.",
    "sx_llm_extraction_method": "LLM extraction method for presenting symptoms (qwen3_32b).",
    "sx_llm_mean_confidence": "Mean confidence score among presenting symptom LLM entities.",
    "radtx_nlp_rai_ablation": "TRUE if RAI ablation documented in radiation treatment LLM.",
    "radtx_nlp_rai_ablation_n_mentions": "Count of RAI ablation entity mentions.",
    "radtx_nlp_thyrogen_prep": "TRUE if thyrogen (recombinant TSH) preparation documented.",
    "radtx_nlp_hormone_withdrawal": "TRUE if levothyroxine withdrawal for RAI prep documented.",
    "radtx_nlp_post_tx_scan_negative": "TRUE if post-treatment whole body scan documented as negative.",
    "radtx_nlp_external_beam_radiation": "TRUE if external beam radiation therapy documented (uncommon in thyroid).",
    "radtx_nlp_has_data": "TRUE if any radiation treatment LLM entity was extracted.",
    "radtx_llm_n_source_notes": "Number of distinct notes contributing to radiation treatment LLM rollup.",
    "radtx_llm_extraction_method": "LLM extraction method for radiation treatment (qwen3_32b).",
    "radtx_llm_mean_confidence": "Mean confidence score among radiation treatment LLM entities.",
    "ln_rollup_": "Lymph node attribute from ln_master_rollup_v1 (x-marker corrected).",
    "ln_level_": "Per-level lymph node detail from ln_master_rollup_v1.",
    "tp_": "Attribute sourced directly from tumor_pathology table.",
    "n_fna_": "Count of FNA episodes for patient.",
    "gross_ete_flag": "TRUE if gross extrathyroidal extension confirmed on pathology.",
    "aggressive_variant_flag": "TRUE if histologic variant is clinically aggressive.",
    "distant_mets_proxy": "Proxy indicator for distant metastatic disease.",
    "calcium_": "Post-operative serum calcium lab attribute.",
    "pth_": "Post-operative parathyroid hormone (PTH) lab attribute.",
    "postop_": "Post-operative lab or clinical event attribute.",
    "molecular_": "Molecular testing result or platform attribute.",
    "high_risk_": "High-risk molecular marker flag.",
    "preop_sweep_": "Pre-operative Excel molecular sweep attribute.",
    "any_fusion_": "Gene fusion detection flag from molecular testing.",
    "bilateral_": "Bilateral thyroid disease or nodule flag.",
    "dominant_": "Dominant nodule attribute from ultrasound.",
    "has_suspicious_": "Flag for suspicious nodule on imaging.",
    "n_us_": "Count of ultrasound examinations.",
    "n_tg_": "Count of thyroglobulin lab measurements.",
    "n_tgab_": "Count of thyroglobulin antibody measurements.",
    "anti_tg_": "Anti-thyroglobulin antibody attribute.",
    "worst_tirads_": "Worst (highest risk) TI-RADS category across all nodules.",
    "max_tirads_": "Maximum TI-RADS score across all nodules.",
    "longitudinal_": "Longitudinal trend attribute (e.g., Tg kinetics).",
    "demo_": "Derived demographic attribute.",
    "date_traceability_": "Date provenance and traceability classification.",
    "survival_": "Survival endpoint or follow-up attribute.",
    "time_to_recurrence_": "Time from surgery to first recurrence event.",
    "any_recurrence": "TRUE if any recurrence documented (structural or biochemical).",
    "biochemical_recurrence": "TRUE if biochemical-only recurrence (rising Tg, no structural).",
    "rec_": "Recurrence sub-attribute.",
}


def assign_domain(col: str) -> tuple[str, str, str]:
    """Return (clinical_domain, source_table, source_script) for a column name."""
    for pattern, domain, src_table, src_script in DOMAIN_RULES:
        if re.match(pattern, col, re.IGNORECASE):
            return domain, src_table, src_script
    return "provenance", "gold_master_patient_facts_v1", "204"


def get_description(col: str) -> str:
    """Return a clinical description for a column."""
    # Exact match first
    if col in DESCRIPTION_TEMPLATES:
        return DESCRIPTION_TEMPLATES[col]
    # Prefix match
    for prefix, desc in DESCRIPTION_TEMPLATES.items():
        if prefix.endswith("_") and col.startswith(prefix):
            return desc
    # Generic fallback based on suffix
    if col.endswith("_flag") or col.endswith("_bool"):
        return f"Boolean indicator for {col.replace('_flag','').replace('_bool','').replace('_', ' ')}."
    if col.endswith("_count") or col.startswith("n_"):
        return f"Count of {col.replace('_count','').replace('n_','',1).replace('_', ' ')} records for patient."
    if col.endswith("_date"):
        return f"Date of {col.replace('_date','').replace('_', ' ')} event."
    if col.endswith("_days") or col.endswith("_years"):
        unit = "days" if col.endswith("_days") else "years"
        return f"Duration in {unit} for {col.replace('_days','').replace('_years','').replace('_',' ')}."
    if col.endswith("_pct") or col.endswith("_percent"):
        return f"Percentage value for {col.replace('_pct','').replace('_percent','').replace('_',' ')}."
    return f"Derived attribute: {col.replace('_', ' ')}."


def build_coverage_query(columns: list[tuple[str, str]]) -> str:
    """
    Build a single SQL that returns coverage stats for all columns at once.
    Uses a UNION ALL of simple aggregates per column.
    Batched into one call instead of 654 individual queries.
    """
    parts = []
    for col, dtype in columns:
        safe_col = f'"{col}"'
        upper_type = dtype.upper()

        if "BOOL" in upper_type:
            parts.append(f"""
SELECT
    '{col}' AS column_name,
    COUNT({safe_col}) AS non_null_count,
    COUNT(DISTINCT {safe_col}) AS n_distinct,
    CAST(SUM(CASE WHEN {safe_col} IS TRUE THEN 1 ELSE 0 END) AS VARCHAR)
        || ' TRUE / '
        || CAST(SUM(CASE WHEN {safe_col} IS FALSE THEN 1 ELSE 0 END) AS VARCHAR)
        || ' FALSE'
    AS sample_values
FROM {CANONICAL}
""")
        elif any(t in upper_type for t in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT", "REAL")):
            parts.append(f"""
SELECT
    '{col}' AS column_name,
    COUNT({safe_col}) AS non_null_count,
    COUNT(DISTINCT {safe_col}) AS n_distinct,
    'min=' || CAST(ROUND(CAST(MIN({safe_col}) AS DOUBLE), 2) AS VARCHAR)
        || ' | med=' || CAST(ROUND(CAST(MEDIAN({safe_col}) AS DOUBLE), 2) AS VARCHAR)
        || ' | max=' || CAST(ROUND(CAST(MAX({safe_col}) AS DOUBLE), 2) AS VARCHAR)
    AS sample_values
FROM {CANONICAL}
""")
        elif "DATE" in upper_type or "TIMESTAMP" in upper_type or "TIME" in upper_type:
            parts.append(f"""
SELECT
    '{col}' AS column_name,
    COUNT({safe_col}) AS non_null_count,
    COUNT(DISTINCT {safe_col}) AS n_distinct,
    'min=' || CAST(MIN({safe_col}) AS VARCHAR) || ' | max=' || CAST(MAX({safe_col}) AS VARCHAR)
    AS sample_values
FROM {CANONICAL}
""")
        else:  # VARCHAR / other text
            parts.append(f"""
SELECT
    '{col}' AS column_name,
    COUNT({safe_col}) AS non_null_count,
    COUNT(DISTINCT {safe_col}) AS n_distinct,
    (SELECT STRING_AGG(v, '; ')
     FROM (
         SELECT {safe_col}::VARCHAR AS v, COUNT(*) AS cnt
         FROM {CANONICAL}
         WHERE {safe_col} IS NOT NULL
         GROUP BY {safe_col}
         ORDER BY cnt DESC
         LIMIT 5
     ) t
    ) AS sample_values
FROM {CANONICAL}
""")
    return "\nUNION ALL\n".join(parts)


def run(dry_run: bool = False) -> None:
    token = get_token()
    if not token:
        print("[ERROR] No MotherDuck token found.", file=sys.stderr)
        sys.exit(1)

    print(f"[init] Connecting to md:{DB}")
    con = duckdb.connect(f"md:{DB}?motherduck_token={token}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── 1. Get column schema ─────────────────────────────────────────────────
    print("[1] Fetching schema …")
    schema_rows = con.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}'
        ORDER BY ordinal_position
    """).fetchall()
    print(f"    → {len(schema_rows)} columns")

    if dry_run:
        print("[dry-run] Schema fetched — skipping coverage queries and file writes.")
        con.close()
        return

    # ── 2. Run coverage queries in batches of 50 ──────────────────────────────
    print("[2] Running coverage queries in batches of 50 …")
    BATCH = 50
    coverage_rows: list[dict] = []
    total_batches = (len(schema_rows) + BATCH - 1) // BATCH

    for i in range(0, len(schema_rows), BATCH):
        batch = schema_rows[i : i + BATCH]
        batch_num = i // BATCH + 1
        print(f"    Batch {batch_num}/{total_batches} ({batch[0][0]} … {batch[-1][0]})")
        sql = build_coverage_query(batch)
        try:
            rows = con.execute(sql).fetchall()
            coverage_rows.extend(
                {"column_name": r[0], "non_null_count": r[1], "n_distinct": r[2], "sample_values": r[3]}
                for r in rows
            )
        except Exception as e:
            print(f"    [WARN] Batch {batch_num} failed: {e}")
            for col, _ in batch:
                coverage_rows.append({
                    "column_name": col,
                    "non_null_count": None,
                    "n_distinct": None,
                    "sample_values": "ERROR",
                })

    # ── 3. Build data dictionary DataFrame ───────────────────────────────────
    print("[3] Building data dictionary …")
    cov_map = {r["column_name"]: r for r in coverage_rows}

    records = []
    for col, dtype in schema_rows:
        cov = cov_map.get(col, {})
        domain, src_table, src_script = assign_domain(col)
        non_null = cov.get("non_null_count") or 0
        non_null_pct = round(non_null / TOTAL_ROWS * 100, 1) if TOTAL_ROWS else 0.0
        records.append({
            "column_name": col,
            "data_type": dtype,
            "non_null_count": non_null,
            "non_null_pct": non_null_pct,
            "n_distinct": cov.get("n_distinct"),
            "sample_values": cov.get("sample_values", ""),
            "source_table": src_table,
            "source_script": src_script,
            "clinical_domain": domain,
            "description": get_description(col),
        })

    dd_df = pd.DataFrame(records)
    out_csv = OUTPUT_DIR / "data_dictionary.csv"
    dd_df.to_csv(out_csv, index=False)
    print(f"    → Saved {len(dd_df)} rows to {out_csv}")

    # ── 4. Domain coverage summary ────────────────────────────────────────────
    print("[4] Building domain coverage summary …")
    domain_summary = (
        dd_df.groupby("clinical_domain")
        .agg(
            n_columns=("column_name", "count"),
            mean_coverage_pct=("non_null_pct", "mean"),
            min_coverage_pct=("non_null_pct", "min"),
            max_coverage_pct=("non_null_pct", "max"),
            n_100pct=("non_null_pct", lambda x: (x >= 100.0).sum()),
            n_below_10pct=("non_null_pct", lambda x: (x < 10.0).sum()),
        )
        .reset_index()
        .sort_values("mean_coverage_pct", ascending=False)
    )
    # Add key_columns (top 3 by coverage within domain)
    key_cols_map: dict[str, str] = {}
    for domain, grp in dd_df.groupby("clinical_domain"):
        top = grp.nlargest(3, "non_null_pct")["column_name"].tolist()
        key_cols_map[domain] = " | ".join(top)
    domain_summary["key_columns"] = domain_summary["clinical_domain"].map(key_cols_map)

    # Round numeric columns
    for c in ("mean_coverage_pct", "min_coverage_pct", "max_coverage_pct"):
        domain_summary[c] = domain_summary[c].round(1)

    con.close()

    # ── 5. Write source_truth_map.md ──────────────────────────────────────────
    print("[5] Writing source_truth_map.md …")
    today = date.today().isoformat()
    total_cols = len(dd_df)
    overall_mean_cov = round(dd_df["non_null_pct"].mean(), 1)
    n_100 = (dd_df["non_null_pct"] >= 100.0).sum()
    n_below10 = (dd_df["non_null_pct"] < 10.0).sum()

    # Domain table markdown
    dom_md_rows = []
    for _, row in domain_summary.iterrows():
        dom_md_rows.append(
            f"| {row['clinical_domain']} | {row['n_columns']} "
            f"| {row['mean_coverage_pct']} | {row['min_coverage_pct']} "
            f"| {row['max_coverage_pct']} | {row['n_100pct']} "
            f"| {row['n_below_10pct']} | {row['key_columns']} |"
        )
    dom_md = "\n".join(dom_md_rows)

    md_content = f"""# THYROID_2026 — Source Truth Map
Generated: {today}
Database: `{DB}`
Table: `{CANONICAL}`

---

## Summary
- **Total columns:** {total_cols}
- **Total rows:** {TOTAL_ROWS:,}
- **Overall mean coverage:** {overall_mean_cov}%
- **Fully populated columns (≥100%):** {n_100}
- **Sparse columns (<10%):** {n_below10}

---

## A. Database Hierarchy

| Priority | Database | Role | Notes |
|----------|----------|------|-------|
| 1 | `thyroid_ete_fix_20260413` | **CANONICAL — all reads and writes** | Created Apr 2026 as clean ETE-fix copy |
| 2 | `Thyroid 2026` (DuckLake) | **HISTORICAL READ ONLY** | Origin of `fna_path_outcome` via scripts 115/116 |
| 3 | `thyroid_research_ro_v2` (share) | **HISTORICAL READ ONLY** | Origin of `tirads_llm_extracted_v2`, `ln_master_rollup_v1` |
| 4 | `Thyroid 2026 Molecular *` | **DEPRECATED** | Do not use |
| 5 | `my_db`, `rosflow`, `sample_data` | **UNRELATED** | Ignore |

---

## B. Table Tiers (on `thyroid_ete_fix_20260413`)

| Tier | Tables | Description | In canonical? |
|------|--------|-------------|---------------|
| 0 | `canonical_patient_master_v1` | Single analytical table — {TOTAL_ROWS:,} patients × {total_cols} columns | **IS the canonical** |
| 1 | `gold_master_patient_facts_v1`, `patient_refined_master_clinical_v12`, `tumor_pathology`, `path_synoptics`, `ultrasound_reports`, `ct_imaging`, `nuclear_med`, `fna_cytology`, `fna_episode_master_v2`, `operative_episode_detail_v2`, `imaging_patient_summary_v1`, `longitudinal_lab_canonical_v1`, `molecular_results`, `molecular_test_episode_v2`, `specimen_master_v1`, `clinical_notes_long` | Source structured data from clinical databases | YES (scripts 200–211) |
| 2 | `extracted_tirads_validated_v1`, `extracted_braf_recovery_v1`, `extracted_ras_patient_summary_v1`, `thyroid_scoring_py_v1`, `tg_timeline_patient_summary_v1`, `complication_phenotype_v1`, `recurrence_event_clean_v1`, `survival_cohort_enriched`, `rai_treatment_episode_v2`, `ln_master_rollup_v1`, `extracted_rln_injury_refined_v2`, `extracted_postop_labs_expanded_v1` | LLM or deterministic processing of Tier 1 | YES (scripts 207–211) |
| 3 | `note_entities_llm_*` (23 tables), `note_entities_*` (7 tables) | NLP entity extraction from clinical notes | YES as `nlp_*`, `op_nlp_*`, `med_nlp_*`, `pmhx_*`, `pshx_*`, `proc_nlp_*`, `sx_*`, `radtx_*` columns (scripts 212, 215) |
| 4 | `linkage_*`, `val_*`, `review_queue_*`, `*_backup_*`, `imaging_fna_linkage_*`, `surgery_pathology_linkage_*`, `fna_molecular_linkage_*` | Internal linkage and QC tables | **NO** — internal plumbing |
| 5 | `analysis_*`, `manuscript_cohort_*` | Pre-built analysis subsets | **NO** — may be outdated; rebuild from canonical |
| 6 | `fhir_*`, `stg_thyroseq_*`, `rosflow_*` | Other/deprecated/unrelated | **NO** |

---

## C. Script Lineage

| Script | Commit | Purpose | Columns added / action |
|--------|--------|---------|------------------------|
| 200 | ac41da7 | Canonical diagnosis standardization | `diagnosis_primary`, `diagnosis_variant`, `is_malignant`, `diagnosis_full` |
| 201 | ac41da7 | Canonical survival / follow-up | `followup_days`, `followup_years`, `last_contact_date`, `vital_status` |
| 202 | ac41da7 | Canonical molecular tested | `molecular_tested_confirmed`, `mol_platform`, `mol_n_tests` |
| 203 | ac41da7 | Canonical recurrence | `recurrence_confirmed`, `recurrence_type`, `recurrence_date`, `time_to_recurrence_days` |
| 204 | ac41da7 | Canonical master assembly (original 96 columns) | 96 base columns from all Tier 1 sources |
| 205 | bdb0fdb | Consolidation — FNA, TIRADS, Bethesda, LN | `fna_path_outcome`, `preop_tirads_*`, `bethesda_*`, `tp_ln_*` |
| 206 | 192a352 | Fleet NLP validation + upload (171K JSONL rows) | No canonical columns — NLP to `note_entities_llm_*` |
| 207 | cf12d69 | Full canonical expansion (125 → 362 columns) | 237 columns from gold_master, PRM v12, CT, nuclear, imaging_summary, thyroid_scoring, extracted_tirads |
| 208 | 80ee3cf | LN master rollup integration (362 → 407 columns) | 45 `ln_rollup_*` and `ln_level_*` columns from `ln_master_rollup_v1` |
| 209 | ab751b9 | NLP cross-validation report | QC report only — no canonical columns |
| 210 | d90dcdf (partial) | Database audit + backup | QC artifacts — no canonical columns |
| 211 | d90dcdf | Gap-fill from 8 extracted/episode tables | ~129 columns: complications, RLN, ETE, postop labs, RAI episodes, recurrence events, survival, molecular variants |
| 212 | d90dcdf | NLP entity patient-level rollup | nlp_* columns from 26 note_entities tables (Tier 1/2/3 LLM + non-LLM) |
|| 214 | beb3aba | Final structured integration (gold_master, PRM v12, synoptics, labs, US) | gm_*, prm_*, syn_*, lab_tsh_*, us_* columns |
|| 215 | c1d9992 | Deep NLP entity integration with full provenance (8 sources, 156 cols) | op_nlp_*, med_nlp_*, pmhx_nlp_*, pmhx_llm_*, pshx_*, proc_nlp_*, sx_*, radtx_* |
| 213 | (current) | Data dictionary + source truth map | Documentation only — no canonical columns added |

---

## D. Domain Coverage Summary

| domain | n_columns | mean_coverage_pct | min_coverage_pct | max_coverage_pct | n_100pct | n_below_10pct | key_columns |
|--------|-----------|-------------------|-----------------|-----------------|---------|--------------|-------------|
{dom_md}

---

## E. Key Clinical Denominators

| Denominator | Value | Column | Notes |
|-------------|-------|--------|-------|
| Total surgical cohort | {TOTAL_ROWS:,} | — | All patients in canonical |
| Analysis-eligible cancer | ~4,136 | `histology_analysis_eligible_flag` | Confirmed malignancy with eligible staging |
| Molecular tested | ~10,025 | `molecular_tested_confirmed` | Any structured molecular test |
| TIRADS documented | ~3,474 | `tirads_best_combined` | At least one structured TIRADS score |
| RAI received | ~862 | `rai_received_flag` | Any RAI treatment documented |
| Recurrence documented | ~1,986 | `any_recurrence` | Any recurrence flag (structural or biochemical) |

---

## F. Column Naming Conventions

| Prefix | Domain | Example |
|--------|--------|---------|
| `demo_` | Demographics | `demo_age_group` |
| `surg_` | Surgery | `surg_total_thyroidectomy` |
| `op_` | Operative detail (NLP) | `op_rln_monitoring_any` |
| `ajcc8_` | AJCC 8th Ed staging | `ajcc8_t_stage`, `ajcc8_stage_group` |
| `ata_` | ATA risk / response | `ata_risk_category` |
| `macis_` | MACIS score | `macis_score` |
| `ln_` | Lymph nodes | `ln_total_examined`, `ln_ratio` |
| `ln_rollup_` | LN rollup (script 208) | `ln_rollup_central_n_examined` |
| `tp_` | tumor_pathology source | `tp_ln_positive` |
| `ene_` | Extranodal extension | `ene_best_grade` |
| `mol_` | Molecular results | `mol_braf_positive` |
| `braf_`, `ras_`, `tert_` | Specific mutations | `braf_positive_final` |
| `rai_` | RAI treatment | `rai_received_flag`, `rai_dose_mci` |
| `tg_` | Thyroglobulin | `tg_nadir`, `tg_rising_flag` |
| `comp_` | Complications | `comp_rln_status` |
| `surv_` | Survival | `surv_time_days`, `surv_event` |
| `rec_` | Recurrence sub | `rec_detection_category` |
| `nlp_llm_` | LLM NLP rollup (script 212) | `nlp_llm_pathology_ete_grade` |
| `nlp_ne_` | Non-LLM NLP metadata (script 212) | `nlp_ne_complications_n_rows` |
| `op_nlp_` | Operative NLP — regex_operative_v2 (script 215) | `op_nlp_ebl_ml`, `op_nlp_nerve_monitoring_used` |
| `med_nlp_` | Medication NLP — regex_medication_v2 (script 215) | `med_nlp_levothyroxine`, `med_nlp_calcitriol` |
| `pmhx_nlp_` | Problem list / PMH NLP — regex + qwen3:32b (script 215) | `pmhx_nlp_hypertension`, `pmhx_nlp_radiation_exposure` |
| `pmhx_llm_` | Past medical hx LLM provenance (script 215) | `pmhx_llm_mean_confidence`, `pmhx_llm_n_source_notes` |
| `pshx_nlp_`, `pshx_llm_` | Past surgical hx LLM (script 215) | `pshx_nlp_prior_thyroidectomy`, `pshx_nlp_prior_rai` |
| `proc_nlp_` | Procedure NLP — regex_procedure_v2 (script 215) | `proc_nlp_tracheostomy`, `proc_nlp_laryngoscopy` |
| `sx_nlp_`, `sx_llm_` | Presenting symptoms LLM ⚠ LOW COVERAGE (script 215) | `sx_nlp_dysphagia`, `sx_nlp_hoarseness` |
| `radtx_nlp_`, `radtx_llm_` | Radiation treatment LLM (script 215) | `radtx_nlp_rai_ablation`, `radtx_nlp_thyrogen_prep` |
| `_flag` suffix | Boolean indicator | `aggressive_variant_flag` |
| `_source` suffix | Provenance field | `ete_source_of_truth` |
| `_confidence` suffix | Confidence score | `tirads_reliability` |
| `_eligible_flag` suffix | Eligibility gate | `histology_analysis_eligible_flag` |

---

*Generated by script 213. Re-run to refresh after any canonical update.*
"""

    out_md = OUTPUT_DIR / "source_truth_map.md"
    out_md.write_text(md_content, encoding="utf-8")
    print(f"    → Saved {out_md}")

    # ── 6. Final summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"  Data dictionary: {total_cols} columns documented")
    print(f"  Overall coverage: {overall_mean_cov}%  |  100%: {n_100}  |  <10%: {n_below10}")
    print(f"  Domains: {len(domain_summary)}")
    print("=" * 60)
    print("\nOutputs:")
    print(f"  {out_csv}")
    print(f"  {out_md}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script 213: Data Dictionary + Source Truth Map")
    parser.add_argument("--dry-run", action="store_true", help="Validate connectivity only, no file writes")
    args = parser.parse_args()
    t0 = time.time()
    run(dry_run=args.dry_run)
    print(f"\n[done] {time.time() - t0:.1f}s")
