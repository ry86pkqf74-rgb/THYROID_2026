#!/usr/bin/env python3
"""Resolve the 22 remaining unmatched/rejected NSQIP rows using all available sources."""

import pandas as pd
import numpy as np
import pickle

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 350)
pd.set_option('display.max_colwidth', 60)


def fmt_age(val):
    if val is None:
        return "?"
    return "{:.1f}".format(float(val))


nsqip_df = pd.read_excel("/Users/loganglosser/Downloads/Thyroid NSQIP dataset 2010-2023.xlsx")
nsqip_df['IDN_str'] = nsqip_df['IDN'].astype(str).str.strip()
thyroid_cpts = [60220, 60225, 60240, 60252, 60254, 60260, 60270, 60271]
nsqip_thyroid = nsqip_df[nsqip_df['CPT Code'].isin(thyroid_cpts)].copy().reset_index(drop=True)

unmatched_cases = [
    102597, 106441, 140893, 102536, 112133, 112824, 138509,
    140110, 119697, 140768, 100754, 101047, 101704, 105154,
    108275, 108813, 108953, 112454, 113202, 115472, 115818, 139806
]
unmatch_df = nsqip_thyroid[nsqip_thyroid['Case Number'].isin(unmatched_cases)].copy()
print("=== {} UNMATCHED ROWS TO RESOLVE ===\n".format(len(unmatch_df)))

# Load all source files
notes = pd.read_excel("raw/Notes 12_1_25.xlsx", sheet_name=0)
path_syn = pd.read_excel("raw/All Diagnoses & synoptic 12_1_2025.xlsx")
op_sheet = pd.read_excel("raw/Thyroid OP Sheet data.xlsx")
complications = pd.read_excel("raw/Thyroid all_Complications 12_1_25.xlsx")

for df in [notes, path_syn, op_sheet, complications]:
    for col in ['EUH_MRN', 'TEC_MRN']:
        if col in df.columns:
            df[col + '_str'] = df[col].astype(str).str.strip()

# Build ALL MRN -> RID pairs from every source
all_mrn_rids = []
for label, df in [('notes', notes), ('path_syn', path_syn),
                  ('op_sheet', op_sheet), ('complications', complications)]:
    rid_col = 'Research ID number' if 'Research ID number' in df.columns else None
    if not rid_col:
        continue
    for mrn_col in ['EUH_MRN_str', 'TEC_MRN_str']:
        if mrn_col not in df.columns:
            continue
        sub = df[[rid_col, mrn_col]].dropna()
        sub = sub[sub[mrn_col] != 'nan']
        sub.columns = ['rid', 'mrn']
        sub['rid'] = sub['rid'].astype(int)
        sub['source'] = label + '_' + mrn_col.replace('_str', '')
        all_mrn_rids.append(sub.drop_duplicates())

all_mrns = pd.concat(all_mrn_rids, ignore_index=True)
print("Total MRN->RID pairs: {}".format(len(all_mrns)))

mt = pd.read_parquet("exports/master_timeline.parquet")
pls = pd.read_parquet("exports/patient_level_summary_mv.parquet")
pls_dedup = pls.drop_duplicates(subset='research_id').copy()

pls_lookup = {}
for _, r in pls_dedup.iterrows():
    rid = int(r['research_id'])
    pls_lookup[rid] = {
        'sex': str(r['sex']).strip().lower() if pd.notna(r['sex']) else None,
        'age': int(r['age_at_surgery']) if pd.notna(r['age_at_surgery']) else None,
        'surg_date': pd.to_datetime(r['surgery_date'], errors='coerce')
    }

xref = pd.read_excel(
    "/Users/loganglosser/Downloads/Case_Details_and_Custom_Fields_Report-14-Dec-2025-1204.xlsx"
)

# Also load DOB from path_syn and op_sheet for independent age verification
ps_dob_map = {}
for _, r in path_syn.iterrows():
    mrn = str(r.get('EUH_MRN', '')).strip()
    dob = pd.to_datetime(r.get('DOB'), errors='coerce')
    if mrn and mrn != 'nan' and pd.notna(dob):
        ps_dob_map[mrn] = dob

op_dob_map = {}
for _, r in op_sheet.iterrows():
    mrn = str(r.get('EUH_MRN', '')).strip()
    dob = pd.to_datetime(r.get('Date of birth'), errors='coerce')
    if mrn and mrn != 'nan' and pd.notna(dob):
        op_dob_map[mrn] = dob

resolutions = []

for _, row in unmatch_df.iterrows():
    idn = str(row['IDN']).strip()
    case_num = row['Case Number']
    nsqip_date = pd.to_datetime(row['Operation Date'], format='%m/%d/%Y', errors='coerce')
    nsqip_age = row['Age at Time of Surgery']
    nsqip_age_int = int(nsqip_age) if pd.notna(nsqip_age) else None
    nsqip_sex = str(row['Sex at Birth']).strip().lower()
    nsqip_bmi = row.get('BMI', np.nan)
    nsqip_lmrn = row.get('LMRN', np.nan)
    nsqip_lcn = row.get('LCN', np.nan)
    nsqip_surgeon = str(row.get('Attending/Staff Surgeon', ''))
    nsqip_dob = pd.to_datetime(row.get('Date of Birth'), format='%m/%d/%Y', errors='coerce')

    print("  == Case#{} IDN={} ==".format(case_num, idn))
    print("     date={} sex={} age={:.1f} BMI={} CPT={} DOB={}".format(
        row['Operation Date'], row['Sex at Birth'], nsqip_age,
        nsqip_bmi, row['CPT Code'],
        nsqip_dob.strftime('%Y-%m-%d') if pd.notna(nsqip_dob) else 'N/A'))
    print("     surgeon={} LMRN={} LCN={}".format(nsqip_surgeon, nsqip_lmrn, nsqip_lcn))

    candidate_rids = {}

    # 1. IDN in all MRN fields
    for _, h in all_mrns[all_mrns['mrn'] == idn].iterrows():
        candidate_rids.setdefault(h['rid'], []).append("IDN=" + h['source'])

    # 2. LMRN
    if pd.notna(nsqip_lmrn):
        lmrn_str = str(int(nsqip_lmrn)).strip()
        for _, h in all_mrns[all_mrns['mrn'] == lmrn_str].iterrows():
            candidate_rids.setdefault(h['rid'], []).append("LMRN=" + h['source'])

    # 3. LCN (may contain non-numeric values like timestamps)
    if pd.notna(nsqip_lcn):
        try:
            lcn_str = str(int(float(nsqip_lcn))).strip()
            for _, h in all_mrns[all_mrns['mrn'] == lcn_str].iterrows():
                candidate_rids.setdefault(h['rid'], []).append("LCN=" + h['source'])
        except (ValueError, OverflowError):
            pass

    # 4. XREF LMRN
    xref_row = xref[xref['Case Number'] == case_num]
    if len(xref_row) > 0 and pd.notna(xref_row['LMRN'].iloc[0]):
        xl = str(int(xref_row['LMRN'].iloc[0])).strip()
        for _, h in all_mrns[all_mrns['mrn'] == xl].iterrows():
            candidate_rids.setdefault(h['rid'], []).append("XREF_LMRN=" + h['source'])

    # 5. Composite: date + sex + age (tight <=2yr)
    wider_candidates = []
    if pd.notna(nsqip_date) and nsqip_age_int is not None:
        same_date_rids = mt[mt['surgery_date'] == nsqip_date]['research_id'].unique()
        for crid in same_date_rids:
            info = pls_lookup.get(int(crid), {})
            if info.get('sex') != nsqip_sex:
                continue
            age_at_nsqip = None
            sd = info.get('surg_date')
            if info.get('age') is not None and sd is not None and pd.notna(sd):
                gap = (nsqip_date - sd).days / 365.25
                age_at_nsqip = info['age'] + gap
            elif info.get('age') is not None:
                age_at_nsqip = float(info['age'])
            wider_candidates.append((int(crid), age_at_nsqip))
            if age_at_nsqip is not None and abs(nsqip_age_int - age_at_nsqip) <= 2:
                candidate_rids.setdefault(int(crid), []).append(
                    "COMPOSITE(age~{})".format(fmt_age(age_at_nsqip)))

    # 6. DOB-based matching: compute expected DOB from NSQIP age + surgery date,
    #    then search for patients in master whose DOB matches within 30 days
    if pd.notna(nsqip_dob) and pd.notna(nsqip_date):
        for label, dob_map in [('ps_dob', ps_dob_map), ('op_dob', op_dob_map)]:
            for mrn, dob in dob_map.items():
                if abs((dob - nsqip_dob).days) <= 1:
                    mrn_hits = all_mrns[all_mrns['mrn'] == mrn]
                    for _, h in mrn_hits.iterrows():
                        i2 = pls_lookup.get(h['rid'], {})
                        if i2.get('sex') == nsqip_sex:
                            candidate_rids.setdefault(h['rid'], []).append(
                                "DOB_MATCH({}_mrn={})".format(label, mrn))

    # Print candidates
    if candidate_rids:
        print("     CANDIDATES:")
        for rid, sources in sorted(candidate_rids.items()):
            info = pls_lookup.get(rid, {})
            sd = info.get('surg_date')
            sd_s = sd.strftime('%Y-%m-%d') if sd is not None and pd.notna(sd) else '?'
            print("       RID={:>6}  sex={}  age={}  master_date={}  evidence={}".format(
                rid, info.get('sex', '?'), info.get('age', '?'), sd_s, sources))
    else:
        print("     NO CANDIDATES from identifiers or DOB")

    if wider_candidates:
        print("     WIDER (date+sex, any age): {} candidates".format(len(wider_candidates)))
        for crid, aa in wider_candidates:
            diff_val = abs(nsqip_age_int - aa) if aa is not None else None
            already = "  (already candidate)" if crid in candidate_rids else ""
            print("       RID={:>6}  age_at_nsqip={}  nsqip_age={}  diff={}{}".format(
                crid, fmt_age(aa), nsqip_age_int, fmt_age(diff_val), already))
    else:
        print("     WIDER: no date+sex candidates on that date")

    # Verdict
    verdict = None
    matched_rid = None
    reason = ""

    if len(candidate_rids) == 1:
        matched_rid = list(candidate_rids.keys())[0]
        sources = candidate_rids[matched_rid]
        info = pls_lookup.get(matched_rid, {})
        if info.get('sex') == nsqip_sex:
            verdict = "ACCEPT"
            reason = "Unique candidate via {}".format(sources)
        else:
            verdict = "REJECT"
            reason = "Sex mismatch nsqip={} master={}".format(nsqip_sex, info.get('sex'))
    elif len(candidate_rids) > 1:
        best_rid = max(candidate_rids, key=lambda k: len(candidate_rids[k]))
        best_sources = candidate_rids[best_rid]
        info = pls_lookup.get(best_rid, {})
        if len(best_sources) >= 2 and info.get('sex') == nsqip_sex:
            matched_rid = best_rid
            verdict = "ACCEPT"
            reason = "Best of {} candidates, {} evidence: {}".format(
                len(candidate_rids), len(best_sources), best_sources)
        elif info.get('sex') == nsqip_sex:
            # Single evidence but only option with correct sex
            sex_correct = [k for k in candidate_rids if pls_lookup.get(k, {}).get('sex') == nsqip_sex]
            if len(sex_correct) == 1:
                matched_rid = sex_correct[0]
                verdict = "ACCEPT"
                reason = "Only sex-concordant candidate of {}: {}".format(
                    len(candidate_rids), candidate_rids[matched_rid])
            else:
                verdict = "AMBIGUOUS"
                reason = "{} candidates, none dominant".format(len(candidate_rids))
        else:
            verdict = "AMBIGUOUS"
            reason = "{} candidates, best has sex mismatch".format(len(candidate_rids))
    elif len(wider_candidates) == 1:
        crid, aa = wider_candidates[0]
        diff_val = abs(nsqip_age_int - aa) if aa is not None else None
        if diff_val is not None and diff_val <= 5:
            matched_rid = crid
            verdict = "ACCEPT_WIDER"
            reason = "Sole date+sex candidate, age diff {}yr".format(fmt_age(diff_val))
        elif diff_val is not None:
            verdict = "NO_MATCH"
            reason = "Sole date+sex candidate but age diff {}yr".format(fmt_age(diff_val))
        else:
            verdict = "NO_MATCH"
            reason = "Sole date+sex candidate but no age to verify"
    else:
        verdict = "NO_MATCH"
        reason = "No candidates from any source"

    print("     >>> VERDICT: {}  RID={}  {}".format(verdict, matched_rid, reason))
    resolutions.append({
        'Case_Number': case_num, 'IDN': row['IDN'],
        'verdict': verdict, 'matched_rid': matched_rid, 'reason': reason
    })
    print()

res_df = pd.DataFrame(resolutions)
print("=" * 80)
print("RESOLUTION SUMMARY")
print("=" * 80)
for v in ['ACCEPT', 'ACCEPT_WIDER', 'REJECT', 'AMBIGUOUS', 'NO_MATCH']:
    grp = res_df[res_df['verdict'] == v]
    if len(grp) == 0:
        continue
    print("  {}: {}".format(v, len(grp)))
    for _, r in grp.iterrows():
        print("    Case#{} IDN={} -> RID={}  {}".format(
            r['Case_Number'], r['IDN'], r['matched_rid'], r['reason']))

n_resolved = len(res_df[res_df['verdict'].str.startswith('ACCEPT')])
n_no = len(res_df[res_df['verdict'] == 'NO_MATCH'])
n_rej = len(res_df[res_df['verdict'] == 'REJECT'])
n_amb = len(res_df[res_df['verdict'] == 'AMBIGUOUS'])
print()
print("Resolved: {}  |  No match: {}  |  Reject: {}  |  Ambiguous: {}".format(
    n_resolved, n_no, n_rej, n_amb))

with open('/tmp/round2_resolutions.pkl', 'wb') as f:
    pickle.dump(res_df, f)
