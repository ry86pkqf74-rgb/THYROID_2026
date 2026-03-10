#!/usr/bin/env python3
"""
Deep investigation of 8 unmatched NSQIP Case Details rows.
Searches every available source for potential matches.
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)
pd.set_option('display.max_colwidth', 80)

REPO = Path("/Users/loganglosser/THYROID_2026")

# Load the Case Details file
nsqip_df = pd.read_excel(
    REPO / "raw" / "Case_Details_and_Custom_Fields_Report-14-Dec-2025-1204.xlsx",
    engine="openpyxl"
)
nsqip_df['_op_date'] = pd.to_datetime(nsqip_df['Operation Date'], format='%m/%d/%Y', errors='coerce')
nsqip_df['_dob'] = pd.to_datetime(nsqip_df['Date of Birth'], format='%m/%d/%Y', errors='coerce')

unmatched_cases = [112133, 138509, 142049, 142059, 142221, 142597, 143386, 144346]
unmatched = nsqip_df[nsqip_df['Case Number'].isin(unmatched_cases)].copy()

# Connect to MotherDuck
import duckdb
token = os.getenv("MOTHERDUCK_TOKEN")
con = duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")

mc_df = con.execute("SELECT * FROM master_cohort").fetchdf()
mc_df['surgery_date_dt'] = pd.to_datetime(mc_df['surgery_date'], errors='coerce')

# Load all source files for MRN cross-referencing
sources = {}
source_configs = {
    'synoptic': ('raw/All Diagnoses & synoptic 12_1_2025.xlsx', 'synoptics + Dx merged'),
    'op_sheet': ('raw/Thyroid OP Sheet data.xlsx', 'Physical OP sheet data'),
    'notes': ('raw/Notes 12_1_25.xlsx', 'Sheet1'),
    'complications': ('raw/Thyroid all_Complications 12_1_25.xlsx', 'Complications'),
}
for name, (path, sheet) in source_configs.items():
    full_path = REPO / path
    if full_path.exists():
        sources[name] = pd.read_excel(full_path, sheet_name=sheet, engine='openpyxl')

# Build comprehensive MRN -> research_id map
all_mrn_pairs = []
for name, df in sources.items():
    rid_col = None
    for c in ['Research ID number', 'Research_ID#', 'research_id']:
        if c in df.columns:
            rid_col = c
            break
    if not rid_col:
        continue
    for mrn_col in ['EUH_MRN', 'TEC_MRN']:
        if mrn_col not in df.columns:
            continue
        pairs = df[[rid_col, mrn_col]].dropna()
        for _, row in pairs.iterrows():
            try:
                rid = int(float(row[rid_col]))
                mrn = str(int(float(row[mrn_col]))).strip()
                if mrn and mrn != 'nan':
                    all_mrn_pairs.append({
                        'rid': rid, 'mrn': mrn,
                        'source': f"{name}_{mrn_col}"
                    })
            except:
                pass

mrn_df = pd.DataFrame(all_mrn_pairs).drop_duplicates()

# Build DOB map from sources
dob_map = {}
for name, df in sources.items():
    rid_col = None
    for c in ['Research ID number', 'Research_ID#', 'research_id']:
        if c in df.columns:
            rid_col = c
            break
    if not rid_col:
        continue
    for dob_col in ['DOB', 'Date of birth', 'Date of Birth']:
        if dob_col not in df.columns:
            continue
        for _, row in df.iterrows():
            try:
                rid = int(float(row[rid_col]))
                dob = pd.to_datetime(row[dob_col], errors='coerce')
                if pd.notna(dob):
                    dob_map[rid] = dob
            except:
                pass

# Build master_cohort lookup
mc_lookup = {}
for _, row in mc_df.iterrows():
    try:
        rid = int(row['research_id'])
    except:
        continue
    mc_lookup[rid] = {
        'sex': str(row.get('sex', '')).strip().lower() if pd.notna(row.get('sex')) else None,
        'age': float(row['age_at_surgery']) if pd.notna(row.get('age_at_surgery')) else None,
        'surgery_date': pd.to_datetime(row.get('surgery_date'), errors='coerce'),
    }

# Also load master_timeline for additional surgery dates
mt_df = con.execute("SELECT research_id, surgery_date FROM master_timeline").fetchdf()
mt_df['surgery_date_dt'] = pd.to_datetime(mt_df['surgery_date'], errors='coerce')

print("=" * 100)
print("DEEP INVESTIGATION OF 8 UNMATCHED NSQIP CASE DETAILS ROWS")
print("=" * 100)

for _, row in unmatched.iterrows():
    case_num = int(row['Case Number'])
    idn = str(int(row['IDN'])).strip()
    op_date = row['_op_date']
    dob = row['_dob']
    age = row['Age at Time of Surgery']
    sex_raw = row.get('Sex at Birth')
    sex = str(sex_raw).strip().lower() if pd.notna(sex_raw) else None
    cpt = int(row['CPT Code'])
    lmrn = row.get('LMRN')
    lcn = row.get('LCN')
    surgeon = row.get('Attending/Staff Surgeon', '')

    print(f"\n{'─' * 100}")
    print(f"  CASE #{case_num}  IDN={idn}")
    print(f"  Op Date: {row['Operation Date']}  DOB: {row['Date of Birth']}  Age: {age:.1f}  Sex: {sex}  CPT: {cpt}")
    print(f"  Surgeon: {surgeon}  LMRN: {lmrn}  LCN: {lcn}")

    # 1. Search IDN across all MRN fields
    print(f"\n  [1] IDN '{idn}' in MRN cross-reference:")
    idn_hits = mrn_df[mrn_df['mrn'] == idn]
    if len(idn_hits) > 0:
        for _, h in idn_hits.iterrows():
            info = mc_lookup.get(h['rid'], {})
            sd = info.get('surgery_date')
            sd_str = sd.strftime('%Y-%m-%d') if sd is not None and pd.notna(sd) else '?'
            print(f"      RID={h['rid']:>6}  sex={info.get('sex','?')}  age={info.get('age','?')}  "
                  f"mc_date={sd_str}  via={h['source']}")
    else:
        print(f"      NOT FOUND in any MRN field")

    # 2. Search LMRN
    if pd.notna(lmrn):
        lmrn_str = str(int(float(lmrn))).strip()
        print(f"\n  [2] LMRN '{lmrn_str}' in MRN cross-reference:")
        lmrn_hits = mrn_df[mrn_df['mrn'] == lmrn_str]
        if len(lmrn_hits) > 0:
            for _, h in lmrn_hits.iterrows():
                info = mc_lookup.get(h['rid'], {})
                sd = info.get('surgery_date')
                sd_str = sd.strftime('%Y-%m-%d') if sd is not None and pd.notna(sd) else '?'
                print(f"      RID={h['rid']:>6}  sex={info.get('sex','?')}  age={info.get('age','?')}  "
                      f"mc_date={sd_str}  via={h['source']}")
        else:
            print(f"      NOT FOUND")
    else:
        print(f"\n  [2] LMRN: not available for this case")

    # 3. Search LCN
    if pd.notna(lcn):
        try:
            lcn_str = str(int(float(lcn))).strip()
            print(f"\n  [3] LCN '{lcn_str}' in MRN cross-reference:")
            lcn_hits = mrn_df[mrn_df['mrn'] == lcn_str]
            if len(lcn_hits) > 0:
                for _, h in lcn_hits.iterrows():
                    info = mc_lookup.get(h['rid'], {})
                    print(f"      RID={h['rid']:>6}  via={h['source']}")
            else:
                print(f"      NOT FOUND")
        except:
            print(f"\n  [3] LCN '{lcn}': cannot parse as integer")
    else:
        print(f"\n  [3] LCN: not available")

    # 4. Search by exact surgery date in master_cohort
    print(f"\n  [4] Exact surgery date {op_date.date()} in master_cohort:")
    date_matches = mc_df[mc_df['surgery_date_dt'].dt.date == op_date.date()]
    if len(date_matches) > 0:
        print(f"      {len(date_matches)} patients had surgery on this date:")
        for _, dm in date_matches.iterrows():
            rid = dm['research_id']
            try:
                rid_int = int(rid)
            except:
                rid_int = rid
            info = mc_lookup.get(rid_int, {})
            sex_match = "YES" if sex is not None and info.get('sex') == sex else ("?" if sex is None else "NO")
            age_diff = abs(age - info.get('age', 0)) if info.get('age') is not None else None
            age_str = f"diff={age_diff:.1f}yr" if age_diff is not None else "no-age"
            # Check DOB
            dob_for_rid = dob_map.get(rid_int)
            dob_match = "?"
            if dob_for_rid is not None and pd.notna(dob):
                dob_match = "YES" if dob_for_rid.date() == dob.date() else f"NO({dob_for_rid.date()})"
            print(f"      RID={rid}  sex={info.get('sex','?')} (match={sex_match})  "
                  f"age={info.get('age','?')} ({age_str})  dob_match={dob_match}")
    else:
        print(f"      NO patients had surgery on this date in master_cohort")

    # 5. Search by exact surgery date in master_timeline
    print(f"\n  [5] Exact surgery date in master_timeline:")
    mt_matches = mt_df[mt_df['surgery_date_dt'].dt.date == op_date.date()]
    if len(mt_matches) > 0:
        rids_in_mt = mt_matches['research_id'].unique()
        rids_not_in_mc = [r for r in rids_in_mt if r not in [dm['research_id'] for _, dm in date_matches.iterrows()]] if len(date_matches) > 0 else list(rids_in_mt)
        if rids_not_in_mc:
            print(f"      Additional RIDs in timeline (not in master_cohort date match): {rids_not_in_mc[:10]}")
        else:
            print(f"      Same RIDs as master_cohort")
    else:
        print(f"      NO entries on this date")

    # 6. DOB-based search: find patients whose DOB matches
    if pd.notna(dob):
        print(f"\n  [6] DOB {dob.date()} search across source files:")
        dob_candidates = [rid for rid, d in dob_map.items() if d.date() == dob.date()]
        if dob_candidates:
            print(f"      {len(dob_candidates)} patients share this DOB:")
            for rid in dob_candidates[:10]:
                info = mc_lookup.get(rid, {})
                sd = info.get('surgery_date')
                sd_str = sd.strftime('%Y-%m-%d') if sd is not None and pd.notna(sd) else '?'
                sex_match = "YES" if sex is not None and info.get('sex') == sex else ("?" if sex is None else "NO")
                print(f"      RID={rid:>6}  sex={info.get('sex','?')} (match={sex_match})  "
                      f"age={info.get('age','?')}  mc_date={sd_str}")
        else:
            print(f"      NO patients with this DOB in any source file")

    # 7. Final verdict
    print(f"\n  >>> VERDICT:", end=" ")
    # Check if ANY evidence exists
    has_mrn = len(idn_hits) > 0
    has_date = len(date_matches) > 0 if 'date_matches' in dir() else False
    has_dob = len(dob_candidates) > 0 if 'dob_candidates' in dir() else False

    if not has_mrn and not has_date:
        if op_date.year >= 2024:
            print("CONFIRMED UNMATCHED — 2024 case, patient not in master_cohort (cohort ends ~2023)")
        else:
            print("CONFIRMED UNMATCHED — IDN not found in any source, no date match")
    elif has_mrn and not has_date:
        print("MRN EXISTS but surgery date does not match — likely different procedure or data error")
    elif has_date and not has_mrn:
        print("DATE MATCH EXISTS but no MRN link — requires manual chart review")
    else:
        print("AMBIGUOUS — needs manual review")

con.close()
print(f"\n{'=' * 100}")
print("INVESTIGATION COMPLETE")
print("=" * 100)
