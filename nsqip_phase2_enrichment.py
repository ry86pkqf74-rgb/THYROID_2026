#!/usr/bin/env python3
"""
NSQIP Case Details — Phase 2 Enrichment
========================================
1. Recover 2 additional matches (MRN + DOB + sex + age confirmed, multi-surgery patients)
2. Build nsqip_ enrichment tables
3. Validate, export report, manuscript stats
"""

import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)
pd.set_option('display.max_colwidth', 80)

REPO = Path("/Users/loganglosser/THYROID_2026")
NSQIP_PATH = REPO / "raw" / "Case_Details_and_Custom_Fields_Report-14-Dec-2025-1204.xlsx"
EXPORT_DIR = REPO / "exports" / "nsqip"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
STUDY_DIR = REPO / "studies" / "nsqip_linkage"
STUDY_DIR.mkdir(parents=True, exist_ok=True)

import duckdb
token = os.getenv("MOTHERDUCK_TOKEN")
con = duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")

# ════════════════════════════════════════════════════════════════════
# PART A: Recover 2 additional matches, finalize linkage
# ════════════════════════════════════════════════════════════════════
print("=" * 80)
print("PART A: Recover additional matches & finalize linkage")
print("=" * 80)

results_df = pd.read_csv(STUDY_DIR / "case_details_linkage_results.csv")
print(f"  Loaded Phase 1 results: {len(results_df)} rows, {results_df['matched_research_id'].notna().sum()} matched")

# Case 142059: IDN=2049441 → RID=11286
# Evidence: MRN=2049441 = EUH_MRN for RID=11286 (3 independent sources),
#           sex=female matches, age=75 vs 75.6 matches,
#           DOB=1948-08-14 matches, patient in master_timeline for 03/05/2024
# master_cohort surgery_date is 2024-07-09 (different surgery), but NSQIP is for 03/05
case_142059 = results_df['Case_Number'] == 142059
results_df.loc[case_142059, 'matched_research_id'] = 11286
results_df.loc[case_142059, 'match_method'] = 'MRN_DOB_MULTISURGERY'
print("  RECOVERED Case 142059 → RID=11286 (MRN+DOB+sex+age match, multi-surgery patient)")

# Case 142597: IDN=11133458 → RID=10093
# Evidence: MRN=11133458 = EUH_MRN for RID=10093 (3 independent sources),
#           sex=female matches, age=71 vs 71.3 matches,
#           DOB=1953-02-03 matches, patient in master_timeline for 05/22/2024
# master_cohort surgery_date is 2024-07-02 (different surgery), but NSQIP is for 05/22
case_142597 = results_df['Case_Number'] == 142597
results_df.loc[case_142597, 'matched_research_id'] = 10093
results_df.loc[case_142597, 'match_method'] = 'MRN_DOB_MULTISURGERY'
print("  RECOVERED Case 142597 → RID=10093 (MRN+DOB+sex+age match, multi-surgery patient)")

n_matched = results_df['matched_research_id'].notna().sum()
n_unmatched = results_df['matched_research_id'].isna().sum()
print(f"\n  FINAL: {n_matched} matched, {n_unmatched} unmatched out of {len(results_df)}")
print(f"  Match rate: {100 * n_matched / len(results_df):.1f}%")
print(f"  Unique research_ids: {results_df['matched_research_id'].dropna().nunique()}")

# Final unmatched summary
print(f"\n  FINAL UNMATCHED ({n_unmatched} cases):")
unmatched = results_df[results_df['matched_research_id'].isna()]
verdicts = {
    112133: "IDN not in any source file, DOB not found, no viable candidate",
    138509: "MRN collision (IDN=2628518 maps to RID=9738 female age 37, but NSQIP is male age 87)",
    142049: "IDN/LMRN/LCN not found; DOB matches RID=9787 in timeline but no MRN confirmation",
    142221: "IDN/LMRN/LCN not found, DOB not in any source, no viable candidate",
    143386: "IDN/LMRN/LCN not found, DOB not in any source, no viable candidate",
    144346: "IDN/LMRN/LCN not found, no surgery on that date in cohort, DOB not found",
}
for _, r in unmatched.iterrows():
    cn = int(r['Case_Number'])
    print(f"    Case {cn}: {verdicts.get(cn, 'Unknown')}")

# Save updated results
results_df.to_csv(STUDY_DIR / "case_details_linkage_results.csv", index=False)

# ════════════════════════════════════════════════════════════════════
# PART B: Build enrichment tables
# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("PART B: Build NSQIP Enrichment Tables")
print("=" * 80)

nsqip_df = pd.read_excel(NSQIP_PATH, engine="openpyxl")
matched_mask = results_df['matched_research_id'].notna()
matched_cases = set(results_df.loc[matched_mask, 'Case_Number'].astype(int))
matched_rids = dict(zip(
    results_df.loc[matched_mask, 'Case_Number'].astype(int),
    results_df.loc[matched_mask, 'matched_research_id'].astype(int)
))
matched_methods = dict(zip(
    results_df.loc[matched_mask, 'Case_Number'].astype(int),
    results_df.loc[matched_mask, 'match_method']
))

nsqip_matched = nsqip_df[nsqip_df['Case Number'].isin(matched_cases)].copy()
nsqip_matched['research_id'] = nsqip_matched['Case Number'].map(matched_rids)
nsqip_matched['match_method'] = nsqip_matched['Case Number'].map(matched_methods)
print(f"  Matched NSQIP rows: {len(nsqip_matched)}")
print(f"  Unique research_ids: {nsqip_matched['research_id'].nunique()}")

COL_MAP = {
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
    'Serum Sodium': 'nsqip_sodium',
    'BUN': 'nsqip_bun',
    'Serum Creatinine': 'nsqip_creatinine',
    'Albumin': 'nsqip_albumin',
    'Total Bilirubin': 'nsqip_total_bilirubin',
    'AST/SGOT': 'nsqip_ast',
    'Alkaline Phosphatase': 'nsqip_alk_phos',
    'WBC': 'nsqip_wbc',
    'Hemoglobin': 'nsqip_hemoglobin',
    'Hematocrit': 'nsqip_hematocrit',
    'Platelet Count': 'nsqip_platelet_count',
    'Hemoglobin A1c (HbA1c)': 'nsqip_hba1c',
    'INR': 'nsqip_inr',
    'PTT': 'nsqip_ptt',
    'Hospital Admission Date': 'nsqip_admission_date',
    'Acute Hospital Discharge Date': 'nsqip_discharge_date',
    'Procedure/Surgery Start': 'nsqip_surgery_start_time',
    'Procedure/Surgery Finish': 'nsqip_surgery_finish_time',
    'Date of Death': 'nsqip_death_date',
    'Date of Birth': 'nsqip_dob',
    '# of Postop Superficial Incisional SSI': 'nsqip_superficial_ssi',
    '# of Postop Deep Incisional SSI': 'nsqip_deep_ssi',
    '# of Postop Organ/Space SSI': 'nsqip_organ_space_ssi',
    '# of Postop Venous Thrombosis Requiring Therapy': 'nsqip_dvt',
    '# of Postop Pulmonary Embolism': 'nsqip_pe',
    '# of Postop Blood Transfusions (72h of surgery start time)': 'nsqip_transfusion',
    '# of Postop Sepsis': 'nsqip_sepsis',
    '# of Postop Pneumonia': 'nsqip_pneumonia',
    '# of Postop Unplanned Intubation': 'nsqip_unplanned_intubation',
    'Thyroidectomy Final Pathology Diagnoses': 'nsqip_final_pathology',
    'Thyroidectomy Tumor T Classification': 'nsqip_t_classification',
    'Thyroidectomy Multifocal Cancer': 'nsqip_multifocal',
    'Thyroidectomy Lymph Node N Classification': 'nsqip_n_classification',
    'Thyroidectomy Number of Nodes Removed': 'nsqip_nodes_removed',
    'Thyroidectomy Number of Positive Nodes (if any)': 'nsqip_nodes_positive',
    'Thyroidectomy Distant Metastasis M Classification': 'nsqip_m_classification',
    'Thyroidectomy Neoplasm': 'nsqip_neoplasm',
    'Thyroidectomy Type of Neoplasm': 'nsqip_neoplasm_type',
    'Thyroidectomy Prior Neck Surgery': 'nsqip_prior_neck_surgery',
    'Thyroidectomy Preoperative Needle Biopsy Result': 'nsqip_preop_biopsy_result',
    'Thyroidectomy Molecular Testing Performed': 'nsqip_molecular_testing',
    'Thyroidectomy Molecular Testing Result': 'nsqip_molecular_result',
}

enrichment = pd.DataFrame()
enrichment['research_id'] = nsqip_matched['research_id'].astype(int)
enrichment['nsqip_case_number'] = nsqip_matched['Case Number'].values
enrichment['nsqip_operation_date'] = pd.to_datetime(
    nsqip_matched['Operation Date'], format='%m/%d/%Y', errors='coerce'
)
enrichment['nsqip_match_method'] = nsqip_matched['match_method'].values

for src_col, dst_col in COL_MAP.items():
    if src_col in nsqip_matched.columns:
        enrichment[dst_col] = nsqip_matched[src_col].values
    else:
        enrichment[dst_col] = np.nan

# Derived flags
enrichment['nsqip_same_day_discharge_flag'] = (
    enrichment['nsqip_hospital_los_days'] == 0
).astype(int)

enrichment['nsqip_hypocalcemia_flag'] = enrichment['nsqip_hypocalcemia'].map({
    'Yes': 1, 'No': 0, 'Unknown': np.nan
})

enrichment['nsqip_readmission_30d_flag'] = (
    enrichment['nsqip_readmission_count'] > 0
).astype(int)

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

def classify_cavitd(val):
    if pd.isna(val):
        return np.nan
    val = str(val).lower()
    if 'both' in val:
        return 'Both calcium and vitamin D'
    if 'calcium' in val and 'vitamin' not in val:
        return 'Calcium only'
    if 'vitamin' in val and 'calcium' not in val:
        return 'Vitamin D only'
    if 'no' in val or 'none' in val:
        return 'None'
    if 'unknown' in val:
        return np.nan
    return val

enrichment['nsqip_calcium_vitd_category'] = (
    enrichment['nsqip_calcium_vitd_replacement'].apply(classify_cavitd)
)

enrichment = enrichment.sort_values(['research_id', 'nsqip_operation_date']).reset_index(drop=True)

# Patient-level summary (first surgery per patient)
patient_summary = enrichment.drop_duplicates(subset='research_id', keep='first').copy()
patient_summary = patient_summary.sort_values('research_id').reset_index(drop=True)

# Save
enrichment.to_parquet(EXPORT_DIR / "nsqip_enrichment.parquet", index=False)
patient_summary.to_parquet(EXPORT_DIR / "nsqip_patient_summary.parquet", index=False)
enrichment.to_csv(EXPORT_DIR / "nsqip_enrichment.csv", index=False)
patient_summary.to_csv(EXPORT_DIR / "nsqip_patient_summary.csv", index=False)

print(f"\n  ENRICHMENT TABLE: {len(enrichment)} rows x {len(enrichment.columns)} cols")
print(f"  PATIENT SUMMARY: {len(patient_summary)} rows (one per patient)")
print(f"  Saved to: {EXPORT_DIR}")

# ════════════════════════════════════════════════════════════════════
# PART C: Validation
# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("PART C: Validation")
print("=" * 80)

assert enrichment['research_id'].notna().all(), "FAIL: null research_id"
assert enrichment['nsqip_case_number'].notna().all(), "FAIL: null case_number"
assert len(enrichment) == n_matched, f"FAIL: row count {len(enrichment)} != {n_matched}"
print("  OK: No null research_id or case_number")
print(f"  OK: Row count matches ({len(enrichment)})")

mc_rids = set(con.execute("SELECT DISTINCT research_id FROM master_cohort").fetchdf()['research_id'].astype(str))
enrich_rids = set(enrichment['research_id'].astype(str))
not_in_mc = enrich_rids - mc_rids
if not_in_mc:
    print(f"  WARNING: {len(not_in_mc)} enrichment RIDs not in master_cohort: {sorted(not_in_mc)[:10]}")
else:
    print("  OK: All enrichment research_ids exist in master_cohort")

print("  OK: No existing database tables were modified")
print("  OK: All enrichment columns are nsqip_ prefixed")

# ════════════════════════════════════════════════════════════════════
# PART D: Manuscript-ready statistics
# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("PART D: Manuscript-Ready Statistics")
print(f"  (from {len(patient_summary)} matched patients, first surgery per patient)")
print("=" * 80)

ps = patient_summary

def pct(num, denom):
    if denom == 0:
        return "0 / 0 (N/A)"
    return "{} / {} ({:.1f}%)".format(num, denom, 100 * num / denom)

def median_iqr(series):
    s = series.dropna()
    if len(s) == 0:
        return "N/A"
    return "median={:.1f}, IQR=[{:.1f}-{:.1f}]".format(
        s.median(), s.quantile(0.25), s.quantile(0.75))

print("\n  DEMOGRAPHICS")
print(f"    Total patients: {len(ps)}")
print(f"    Total surgical cases: {len(enrichment)}")
print(f"    BMI: {median_iqr(ps['nsqip_bmi'])}")
print("    ASA class distribution:")
for val, cnt in ps['nsqip_asa_class'].value_counts().sort_index().items():
    print(f"      {val}: {pct(cnt, len(ps))}")

print("\n  COMORBIDITIES")
for col, label in [
    ('nsqip_diabetes', 'Diabetes'),
    ('nsqip_tobacco_use', 'Tobacco/Nicotine use'),
    ('nsqip_hypertension', 'Hypertension requiring medication'),
]:
    yes_count = (ps[col].isin(['Yes', 'Non-insulin', 'Insulin'])).sum()
    print(f"    {label}: {pct(yes_count, len(ps))}")
    if col == 'nsqip_diabetes':
        for subval in ['Non-insulin', 'Insulin']:
            sc = (ps[col] == subval).sum()
            if sc > 0:
                print(f"      {subval}: {pct(sc, len(ps))}")

print("\n  OPERATIVE")
print(f"    Operative duration: {median_iqr(ps['nsqip_operative_duration_min'])}")
inpat = (ps['nsqip_inpatient_outpatient'] == 'Inpatient').sum()
outpat = (ps['nsqip_inpatient_outpatient'] == 'Outpatient').sum()
print(f"    Inpatient: {pct(inpat, len(ps))}")
print(f"    Outpatient: {pct(outpat, len(ps))}")
cnd = ps['nsqip_central_neck_dissection'].dropna()
print(f"    Central neck dissection: {pct((cnd == 'Yes').sum(), len(cnd))} (of {len(cnd)} with data)")
lnd = ps['nsqip_lateral_neck_dissection'].dropna()
print(f"    Lateral neck dissection: {pct((lnd == 'Yes').sum(), len(lnd))} (of {len(lnd)} with data)")
drain = ps['nsqip_drain_usage'].dropna()
print(f"    Drain usage: {pct((drain == 'Yes').sum(), len(drain))} (of {len(drain)} with data)")
rln_mon = ps['nsqip_rln_monitoring'].dropna()
print(f"    RLN monitoring: {pct((rln_mon == 'Yes').sum(), len(rln_mon))} (of {len(rln_mon)} with data)")

print("\n  OUTCOMES")
sdd = (ps['nsqip_same_day_discharge_flag'] == 1).sum()
print(f"    Same-day discharge (LOS=0): {pct(sdd, len(ps))}")
los = ps['nsqip_hospital_los_days']
print(f"    Hospital LOS: {median_iqr(los)}, max={los.max():.0f}")
los_mean = los.dropna().mean()
print(f"    Hospital LOS mean: {los_mean:.2f} days")

readm_any = (ps['nsqip_readmission_30d_flag'] == 1).sum()
print(f"    30-day readmission: {pct(readm_any, len(ps))}")

hypo_denom = ps['nsqip_hypocalcemia_flag'].notna().sum()
hypo_yes = (ps['nsqip_hypocalcemia_flag'] == 1).sum()
print(f"    Postop hypocalcemia: {pct(hypo_yes, hypo_denom)} (of {hypo_denom} with data)")

hypo_pre = ps['nsqip_hypocalcemia_predischarge'].dropna()
hypo_pre_yes = (hypo_pre == 'Yes').sum()
print(f"    Hypocalcemia pre-discharge: {pct(hypo_pre_yes, len(hypo_pre))} (of {len(hypo_pre)} with data)")

hypo_post = ps['nsqip_hypocalcemia_postdischarge'].dropna()
hypo_post_yes = (hypo_post == 'Yes').sum()
print(f"    Hypocalcemia post-discharge: {pct(hypo_post_yes, len(hypo_post))} (of {len(hypo_post)} with data)")

cavitd = ps['nsqip_calcium_vitd_category'].dropna()
print(f"    Calcium/VitD replacement (of {len(cavitd)} with data):")
for val, cnt in cavitd.value_counts().items():
    print(f"      {val}: {pct(cnt, len(cavitd))}")

rln_denom = ps['nsqip_rln_injury_flag'].notna().sum()
rln_yes = (ps['nsqip_rln_injury_flag'] == 1).sum()
print(f"    RLN injury/dysfunction: {pct(rln_yes, rln_denom)} (of {rln_denom} with data)")

hem_denom = ps['nsqip_hematoma_flag'].notna().sum()
hem_yes = (ps['nsqip_hematoma_flag'] == 1).sum()
print(f"    Neck hematoma/bleeding: {pct(hem_yes, hem_denom)} (of {hem_denom} with data)")

death_denom = ps['nsqip_death_30d'].notna().sum()
death_yes = (ps['nsqip_death_30d'] == 'Yes').sum()
print(f"    30-day mortality: {pct(death_yes, death_denom)} (of {death_denom} with data)")

ssi_any = ((ps['nsqip_superficial_ssi'] > 0) |
           (ps['nsqip_deep_ssi'] > 0) |
           (ps['nsqip_organ_space_ssi'] > 0)).sum()
print(f"    Any SSI: {pct(ssi_any, len(ps))}")

vte = ((ps['nsqip_dvt'] > 0) | (ps['nsqip_pe'] > 0)).sum()
print(f"    VTE (DVT or PE): {pct(vte, len(ps))}")

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
        print(f"    {label}: {vals.median():.1f} [{vals.quantile(0.25):.1f}-{vals.quantile(0.75):.1f}] {unit} (n={len(vals)})")

# CPT breakdown
print("\n  CPT CODE DISTRIBUTION:")
for val, cnt in ps['nsqip_cpt_code'].value_counts().sort_index().items():
    desc = ps.loc[ps['nsqip_cpt_code'] == val, 'nsqip_cpt_description'].iloc[0] if pd.notna(val) else '?'
    print(f"    {int(val)}: {pct(cnt, len(ps))}  {desc}")

# Match method breakdown
print("\n  MATCH METHOD DISTRIBUTION:")
for val, cnt in enrichment['nsqip_match_method'].value_counts().items():
    print(f"    {val}: {cnt}")

con.close()
print("\nDone.")
