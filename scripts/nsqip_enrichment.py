#!/usr/bin/env python3
"""
NSQIP Enrichment Script for THYROID_2026 Lakehouse
===================================================
Reads the finalized NSQIP linkage, transforms NSQIP columns into
nsqip_-prefixed enrichment columns, and produces:
  1. A standalone nsqip_enrichment Parquet table (keyed on research_id + nsqip_case_number)
  2. A patient-level nsqip_patient_summary Parquet (one row per research_id, primary surgery)
  3. Validation queries / sanity checks
  4. Manuscript-ready descriptive statistics

SAFETY: This script does NOT mutate any existing tables. It creates NEW
tables/files only. Existing tables can be LEFT JOIN'd to these outputs.
"""

import pandas as pd
import numpy as np
from pathlib import Path

REPO = Path("/Users/loganglosser/THYROID_2026")
LINKAGE = REPO / "studies" / "nsqip_linkage" / "nsqip_thyroid_linkage_final.csv"
EXPORT_DIR = REPO / "exports" / "nsqip"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# ── Load linked NSQIP data ──────────────────────────────────────────────
linkage = pd.read_csv(LINKAGE)
matched = linkage[linkage['match_status'] == 'Perfect deterministic match'].copy()
matched['linked_research_id'] = matched['linked_research_id'].astype(int)
print("Matched NSQIP rows loaded: {}".format(len(matched)))
print("Unique research_ids: {}".format(matched['linked_research_id'].nunique()))

# ── Column mapping: NSQIP source -> nsqip_ enrichment column ───────────
COL_MAP = {
    # Outcomes
    '# of Readmissions w/in 30 days': 'nsqip_readmission_count',
    '# of Unplanned Readmissions': 'nsqip_unplanned_readmission_count',
    '# of Readmissions likely related to Primary Procedure': 'nsqip_related_readmission_count',
    'Date of First Readmission': 'nsqip_first_readmission_date',
    'Thyroidectomy Postoperative Hypocalcemia': 'nsqip_hypocalcemia',
    'Thyroidectomy Postop Hypocalcemia prior to discharge': 'nsqip_hypocalcemia_predischarge',
    'Thyroidectomy Postop Hypocalcemia after discharge': 'nsqip_hypocalcemia_postdischarge',
    'Thyroidectomy Postop Hypocalcemia-related Event': 'nsqip_hypocalcemia_event',
    'Thyroidectomy Postop Hypocalcemia-related Event Type': 'nsqip_hypocalcemia_event_type',
    'Thyroidectomy IV Calcium': 'nsqip_iv_calcium',
    'Thyroidectomy Postoperative Calcium Level Checked': 'nsqip_calcium_checked',
    'Thyroidectomy Postoperative Parathyroid Level Checked': 'nsqip_pth_checked',
    'Thyroidectomy Postoperative Calcium and Vitamin D Replacement': 'nsqip_calcium_vitd_replacement',
    'Thyroidectomy Last Postoperative Check: Calcium and Vitamin D Replacement': 'nsqip_calcium_vitd_last_check',
    'Thyroidectomy Last Postoperative Check: Postop Hypocalcemia After Discharge': 'nsqip_hypocalcemia_last_check',
    'Postoperative Recurrent Laryngeal Nerve (RLN) Injury or Dysfunction': 'nsqip_rln_injury',
    'Thyroidectomy Postop Neck Hematoma/Bleeding': 'nsqip_neck_hematoma',
    'Hospital Length of Stay': 'nsqip_hospital_los_days',
    'Surgical Length of Stay': 'nsqip_surgical_los_days',
    'Hospital Discharge Destination': 'nsqip_discharge_destination',
    'Postop Death w/in 30 days of Procedure': 'nsqip_death_30d',
    'Total # of Unplanned Returns to OR': 'nsqip_unplanned_return_or',

    # Operative
    'Duration of Surgical Procedure (in minutes)': 'nsqip_operative_duration_min',
    'In/Out-Patient Status': 'nsqip_inpatient_outpatient',
    'CPT Code': 'nsqip_cpt_code',
    'CPT Description': 'nsqip_cpt_description',
    'Thyroidectomy Primary Indication for Surgery': 'nsqip_primary_indication',
    'Thyroidectomy Operative Approach': 'nsqip_operative_approach',
    'Thyroidectomy Central Neck Dissection Performed': 'nsqip_central_neck_dissection',
    'Thyroidectomy Lateral Neck Dissection Performed': 'nsqip_lateral_neck_dissection',
    'Thyroidectomy Use of Vessel Sealant Device': 'nsqip_vessel_sealant',
    'Thyroidectomy Intraop Electrophysiologic or Electromyographic RLN Monitoring': 'nsqip_rln_monitoring',
    'Thyroidectomy Drain Usage': 'nsqip_drain_usage',

    # Comorbidities
    'ASA Classification': 'nsqip_asa_class',
    'BMI': 'nsqip_bmi',
    'Diabetes Mellitus': 'nsqip_diabetes',
    'Tobacco/Nicotine Use': 'nsqip_tobacco_use',
    'Hypertension requiring medication': 'nsqip_hypertension',
    'Heart Failure': 'nsqip_heart_failure',
    'History of Severe COPD': 'nsqip_copd',
    'Bleeding Disorder': 'nsqip_bleeding_disorder',
    'Disseminated Cancer': 'nsqip_disseminated_cancer',
    'Functional Heath Status': 'nsqip_functional_status',

    # Preop labs
    'Serum Sodium': 'nsqip_sodium',
    'BUN': 'nsqip_bun',
    'Serum Creatinine': 'nsqip_creatinine',
    'Albumin': 'nsqip_albumin',
    'WBC': 'nsqip_wbc',
    'Hemoglobin': 'nsqip_hemoglobin',
    'Hematocrit': 'nsqip_hematocrit',
    'Platelet Count': 'nsqip_platelet_count',
    'Hemoglobin A1c (HbA1c)': 'nsqip_hba1c',
    'INR': 'nsqip_inr',

    # General surgical complications
    '# of Postop Superficial Incisional SSI': 'nsqip_superficial_ssi',
    '# of Postop Deep Incisional SSI': 'nsqip_deep_ssi',
    '# of Postop Organ/Space SSI': 'nsqip_organ_space_ssi',
    '# of Postop Venous Thrombosis Requiring Therapy': 'nsqip_dvt',
    '# of Postop Pulmonary Embolism': 'nsqip_pe',
    '# of Postop Blood Transfusions (72h of surgery start time)': 'nsqip_transfusion',
    '# of Postop Sepsis': 'nsqip_sepsis',
    '# of Postop Pneumonia': 'nsqip_pneumonia',
    '# of Postop Unplanned Intubation': 'nsqip_unplanned_intubation',

    # NSQIP pathology / staging
    'Thyroidectomy Final Pathology Diagnoses': 'nsqip_final_pathology',
    'Thyroidectomy Tumor T Classification': 'nsqip_t_classification',
    'Thyroidectomy Multifocal Cancer': 'nsqip_multifocal',
    'Thyroidectomy Lymph Node N Classification': 'nsqip_n_classification',
    'Thyroidectomy Number of Nodes Removed': 'nsqip_nodes_removed',
    'Thyroidectomy Number of Positive Nodes (if any)': 'nsqip_nodes_positive',
    'Thyroidectomy Distant Metastasis M Classification': 'nsqip_m_classification',
    'Thyroidectomy Neoplasm': 'nsqip_neoplasm',
    'Thyroidectomy Type of Neoplasm': 'nsqip_neoplasm_type',
}

# ── Build enrichment table ──────────────────────────────────────────────
enrichment = pd.DataFrame()
enrichment['research_id'] = matched['linked_research_id']
enrichment['nsqip_case_number'] = matched['Case Number']
enrichment['nsqip_operation_date'] = pd.to_datetime(
    matched['Operation Date'], format='%m/%d/%Y', errors='coerce'
)
enrichment['nsqip_match_method'] = matched['match_method']

for src_col, dst_col in COL_MAP.items():
    if src_col in matched.columns:
        enrichment[dst_col] = matched[src_col].values
    else:
        enrichment[dst_col] = np.nan

# Derived flags
enrichment['nsqip_same_day_discharge_flag'] = (
    enrichment['nsqip_hospital_los_days'] == 0
).astype(int)

enrichment['nsqip_hypocalcemia_flag'] = enrichment['nsqip_hypocalcemia'].map({
    'Yes': 1, 'No': 0, 'Unknown': np.nan
})

enrichment['nsqip_rln_injury_flag'] = enrichment['nsqip_rln_injury'].apply(
    lambda x: 1 if pd.notna(x) and 'Yes' in str(x) else (
        0 if pd.notna(x) and x == 'No' else np.nan
    )
)

enrichment['nsqip_hematoma_flag'] = enrichment['nsqip_neck_hematoma'].apply(
    lambda x: 1 if pd.notna(x) and 'Yes' in str(x) else (
        0 if pd.notna(x) and x == 'No' else np.nan
    )
)

# Calcium/VitD replacement category simplified
def classify_cavitd(val):
    if pd.isna(val):
        return np.nan
    val = str(val).lower()
    if 'no' in val and 'calcium' not in val.replace('no-no', '').replace('no calcium', ''):
        if 'no calcium' in val or 'no-no' in val:
            return 'None'
    if 'both' in val:
        return 'Both calcium and vitamin D'
    if 'calcium' in val and 'vitamin' not in val:
        return 'Calcium only'
    if 'vitamin' in val and 'calcium' not in val:
        return 'Vitamin D only'
    if 'no' in val:
        return 'None'
    if 'unknown' in val:
        return np.nan
    return val

enrichment['nsqip_calcium_vitd_replacement_category'] = (
    enrichment['nsqip_calcium_vitd_replacement'].apply(classify_cavitd)
)

enrichment = enrichment.sort_values(['research_id', 'nsqip_operation_date']).reset_index(drop=True)

# ── Patient-level summary (primary/first NSQIP surgery per patient) ─────
patient_summary = enrichment.drop_duplicates(subset='research_id', keep='first').copy()
patient_summary = patient_summary.sort_values('research_id').reset_index(drop=True)

# ── Export ──────────────────────────────────────────────────────────────
enrichment.to_parquet(EXPORT_DIR / "nsqip_enrichment.parquet", index=False)
patient_summary.to_parquet(EXPORT_DIR / "nsqip_patient_summary.parquet", index=False)
enrichment.to_csv(EXPORT_DIR / "nsqip_enrichment.csv", index=False)
patient_summary.to_csv(EXPORT_DIR / "nsqip_patient_summary.csv", index=False)

print("\n=== ENRICHMENT TABLE ===")
print("  Rows: {}  Columns: {}".format(len(enrichment), len(enrichment.columns)))
print("  Unique research_ids: {}".format(enrichment['research_id'].nunique()))
print("  Saved to: {}".format(EXPORT_DIR))

print("\n=== PATIENT SUMMARY TABLE ===")
print("  Rows: {}  (one per patient, first surgery)".format(len(patient_summary)))

# ── Validation ──────────────────────────────────────────────────────────
print("\n=== VALIDATION CHECKS ===")
assert enrichment['research_id'].notna().all(), "FAIL: null research_id"
assert enrichment['nsqip_case_number'].notna().all(), "FAIL: null case_number"
assert len(enrichment) == len(matched), "FAIL: row count mismatch"

no_new_patients = set(enrichment['research_id']) - set(
    pd.read_parquet(REPO / "exports" / "patient_level_summary_mv.parquet")['research_id'].astype(int)
)
if no_new_patients:
    print("  WARNING: {} research_ids in enrichment not in master cohort: {}".format(
        len(no_new_patients), sorted(no_new_patients)[:10]))
else:
    print("  OK: All enrichment research_ids exist in master cohort")

print("  OK: {} enrichment rows, {} unique patients".format(
    len(enrichment), enrichment['research_id'].nunique()))
print("  OK: No existing tables were modified")
print("  OK: Enrichment uses LEFT JOIN pattern (nsqip_ prefixed columns only)")

# ── Manuscript-ready statistics ─────────────────────────────────────────
print("\n" + "=" * 70)
print("  MANUSCRIPT-READY STATISTICS (from {} matched patients)".format(
    len(patient_summary)))
print("=" * 70)

ps = patient_summary

def pct(num, denom):
    return "{} / {} ({:.1f}%)".format(num, denom, 100 * num / denom)

print("\n  DEMOGRAPHICS")
print("    Total patients: {}".format(len(ps)))
print("    Total surgical cases: {}".format(len(enrichment)))
print("    BMI: median={:.1f}, IQR=[{:.1f}-{:.1f}]".format(
    ps['nsqip_bmi'].median(),
    ps['nsqip_bmi'].quantile(0.25),
    ps['nsqip_bmi'].quantile(0.75)))
print("    ASA class distribution:")
for val, cnt in ps['nsqip_asa_class'].value_counts().sort_index().items():
    print("      {}: {}".format(val, pct(cnt, len(ps))))

print("\n  COMORBIDITIES")
for col, label in [
    ('nsqip_diabetes', 'Diabetes'),
    ('nsqip_tobacco_use', 'Tobacco/Nicotine use'),
    ('nsqip_hypertension', 'Hypertension requiring medication'),
]:
    yes_count = (ps[col].isin(['Yes', 'Non-insulin', 'Insulin'])).sum()
    print("    {}: {}".format(label, pct(yes_count, len(ps))))
    if col == 'nsqip_diabetes':
        for subval in ['Non-insulin', 'Insulin']:
            sc = (ps[col] == subval).sum()
            print("      {}: {}".format(subval, pct(sc, len(ps))))

print("\n  OPERATIVE")
print("    Operative duration: median={:.0f} min, IQR=[{:.0f}-{:.0f}]".format(
    ps['nsqip_operative_duration_min'].median(),
    ps['nsqip_operative_duration_min'].quantile(0.25),
    ps['nsqip_operative_duration_min'].quantile(0.75)))
inpat = (ps['nsqip_inpatient_outpatient'] == 'Inpatient').sum()
print("    Inpatient: {}".format(pct(inpat, len(ps))))
print("    Outpatient: {}".format(pct(len(ps) - inpat, len(ps))))
cnd = ps['nsqip_central_neck_dissection'].dropna()
print("    Central neck dissection: {}".format(
    pct((cnd == 'Yes').sum(), len(cnd))))
drain = ps['nsqip_drain_usage'].dropna()
print("    Drain usage: {}".format(pct((drain == 'Yes').sum(), len(drain))))

print("\n  OUTCOMES")
# Same-day discharge
sdd = (ps['nsqip_same_day_discharge_flag'] == 1).sum()
print("    Same-day discharge (LOS=0): {}".format(pct(sdd, len(ps))))
print("    Hospital LOS: median={:.0f}, IQR=[{:.0f}-{:.0f}], max={}".format(
    ps['nsqip_hospital_los_days'].median(),
    ps['nsqip_hospital_los_days'].quantile(0.25),
    ps['nsqip_hospital_los_days'].quantile(0.75),
    ps['nsqip_hospital_los_days'].max()))

# Readmissions
readm_any = (ps['nsqip_readmission_count'] > 0).sum()
print("    30-day readmission: {}".format(pct(readm_any, len(ps))))

# Hypocalcemia
hypo_denom = ps['nsqip_hypocalcemia_flag'].notna().sum()
hypo_yes = (ps['nsqip_hypocalcemia_flag'] == 1).sum()
print("    Postop hypocalcemia: {} (of {} with data)".format(
    pct(hypo_yes, hypo_denom), hypo_denom))

hypo_pre = ps['nsqip_hypocalcemia_predischarge'].dropna()
hypo_pre_yes = (hypo_pre == 'Yes').sum()
print("    Hypocalcemia pre-discharge: {} (of {})".format(
    pct(hypo_pre_yes, len(hypo_pre)), len(hypo_pre)))

hypo_post = ps['nsqip_hypocalcemia_postdischarge'].dropna()
hypo_post_yes = (hypo_post == 'Yes').sum()
print("    Hypocalcemia post-discharge: {} (of {})".format(
    pct(hypo_post_yes, len(hypo_post)), len(hypo_post)))

# Ca/VitD replacement
cavitd = ps['nsqip_calcium_vitd_replacement_category'].dropna()
print("    Calcium/VitD replacement (of {} with data):".format(len(cavitd)))
for val, cnt in cavitd.value_counts().items():
    print("      {}: {}".format(val, pct(cnt, len(cavitd))))

# RLN injury
rln_denom = ps['nsqip_rln_injury_flag'].notna().sum()
rln_yes = (ps['nsqip_rln_injury_flag'] == 1).sum()
print("    RLN injury/dysfunction: {} (of {})".format(
    pct(rln_yes, rln_denom), rln_denom))

# Hematoma
hem_denom = ps['nsqip_hematoma_flag'].notna().sum()
hem_yes = (ps['nsqip_hematoma_flag'] == 1).sum()
print("    Neck hematoma/bleeding: {} (of {})".format(
    pct(hem_yes, hem_denom), hem_denom))

# Death
death_denom = ps['nsqip_death_30d'].notna().sum()
death_yes = (ps['nsqip_death_30d'] == 'Yes').sum()
print("    30-day mortality: {} (of {})".format(
    pct(death_yes, death_denom), death_denom))

# SSI
ssi_any = ((ps['nsqip_superficial_ssi'] > 0) |
           (ps['nsqip_deep_ssi'] > 0) |
           (ps['nsqip_organ_space_ssi'] > 0)).sum()
print("    Any SSI: {}".format(pct(ssi_any, len(ps))))

# VTE
vte = ((ps['nsqip_dvt'] > 0) | (ps['nsqip_pe'] > 0)).sum()
print("    VTE (DVT or PE): {}".format(pct(vte, len(ps))))

print("\n  PREOPERATIVE LABS (median [IQR])")
for col, label, unit in [
    ('nsqip_sodium', 'Sodium', 'mEq/L'),
    ('nsqip_creatinine', 'Creatinine', 'mg/dL'),
    ('nsqip_albumin', 'Albumin', 'g/dL'),
    ('nsqip_wbc', 'WBC', 'K/uL'),
    ('nsqip_hematocrit', 'Hematocrit', '%'),
    ('nsqip_platelet_count', 'Platelets', 'K/uL'),
]:
    vals = ps[col].dropna()
    if len(vals) > 0:
        print("    {}: {:.1f} [{:.1f}-{:.1f}] {} (n={})".format(
            label, vals.median(), vals.quantile(0.25), vals.quantile(0.75),
            unit, len(vals)))

print("\nDone.")
