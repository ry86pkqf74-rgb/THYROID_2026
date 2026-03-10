#!/usr/bin/env python3
"""
NSQIP Case Details Linkage — Step-by-step, zero hallucination.
Loads the Case_Details_and_Custom_Fields_Report-14-Dec-2025-1204.xlsx
and performs deterministic matching to the THYROID_2026 lakehouse.
"""

import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)
pd.set_option('display.max_colwidth', 80)

REPO = Path("/Users/loganglosser/THYROID_2026")
NSQIP_PATH = REPO / "raw" / "Case_Details_and_Custom_Fields_Report-14-Dec-2025-1204.xlsx"

# ════════════════════════════════════════════════════════════════════
# STEP 1: Load NSQIP Excel
# ════════════════════════════════════════════════════════════════════
print("=" * 80)
print("STEP 1: Load NSQIP Excel")
print("=" * 80)

nsqip_df = pd.read_excel(NSQIP_PATH, engine="openpyxl")
print(f"  EXACT ROW COUNT: {len(nsqip_df)}")
assert len(nsqip_df) == 1281, f"FAIL: Expected 1281 rows, got {len(nsqip_df)}"
print("  ASSERTION PASSED: Row count == 1281")

# ════════════════════════════════════════════════════════════════════
# STEP 2: Connect to lakehouse, list tables, confirm research_id PK
# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("STEP 2: Connect to Lakehouse")
print("=" * 80)

import duckdb

token = os.getenv("MOTHERDUCK_TOKEN")
if not token:
    print("  WARNING: MOTHERDUCK_TOKEN not set — falling back to local DuckDB")
    db_path = REPO / "thyroid_master.duckdb"
    if not db_path.exists():
        print("  ERROR: Local DuckDB not found. Cannot proceed.")
        sys.exit(1)
    con = duckdb.connect(str(db_path), read_only=True)
    print(f"  Connected to local DuckDB: {db_path}")
else:
    con = duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    print("  Connected to MotherDuck: thyroid_research_2026")

print("\n  ALL TABLES:")
tables = con.execute("SHOW TABLES").fetchall()
for t in sorted(tables):
    print(f"    {t[0]}")

print(f"\n  Total tables: {len(tables)}")

print("\n  Checking research_id as primary key in master_cohort:")
mc_cols = con.execute("DESCRIBE master_cohort").fetchall()
for col in mc_cols:
    if 'research_id' in str(col[0]).lower():
        print(f"    Column: {col[0]}, Type: {col[1]}")

mc_count = con.execute("SELECT COUNT(*) FROM master_cohort").fetchone()[0]
mc_distinct = con.execute("SELECT COUNT(DISTINCT research_id) FROM master_cohort").fetchone()[0]
print(f"    master_cohort total rows: {mc_count}")
print(f"    master_cohort distinct research_id: {mc_distinct}")
print(f"    Is research_id unique (PK): {mc_count == mc_distinct}")

# ════════════════════════════════════════════════════════════════════
# STEP 3: Check for existing enrichment
# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("STEP 3: Check for Existing NSQIP Enrichment")
print("=" * 80)

search_terms = ["nsqip_", "readmission", "hypocalcemia", "los", "same_day", "pacu_pth", "calcium_vitd"]
target_tables = ["master_cohort", "parathyroid", "thyroid_weights"]

all_table_names = [t[0] for t in tables]
for tbl in target_tables + [t for t in all_table_names if t not in target_tables]:
    try:
        cols = con.execute(f"DESCRIBE {tbl}").fetchall()
        col_names = [c[0] for c in cols]
        matches = []
        for col_name in col_names:
            cl = col_name.lower()
            for term in search_terms:
                if term in cl:
                    matches.append(col_name)
                    break
        if matches:
            print(f"\n  TABLE '{tbl}' — ENRICHMENT COLUMNS FOUND:")
            for m in matches:
                col_type = [c[1] for c in cols if c[0] == m][0]
                print(f"    {m} ({col_type})")
        elif tbl in target_tables:
            print(f"\n  TABLE '{tbl}' — No enrichment columns found ({len(col_names)} columns checked)")
    except Exception as e:
        if tbl in target_tables:
            print(f"\n  TABLE '{tbl}' — ERROR: {e}")

print("\n  Checking for standalone nsqip tables:")
nsqip_tables = [t for t in all_table_names if 'nsqip' in t.lower()]
if nsqip_tables:
    for nt in nsqip_tables:
        cnt = con.execute(f"SELECT COUNT(*) FROM {nt}").fetchone()[0]
        cols = con.execute(f"DESCRIBE {nt}").fetchall()
        print(f"    {nt}: {cnt} rows, {len(cols)} columns")
else:
    print("    None found in MotherDuck/DuckDB")

print("\n  Checking for local nsqip export files:")
nsqip_exports = list((REPO / "exports" / "nsqip").glob("*")) if (REPO / "exports" / "nsqip").exists() else []
for f in nsqip_exports:
    print(f"    {f.name} ({f.stat().st_size / 1024:.1f} KB)")

print("\n  Checking existing linkage file:")
linkage_path = REPO / "studies" / "nsqip_linkage" / "nsqip_thyroid_linkage_final.csv"
if linkage_path.exists():
    existing_linkage = pd.read_csv(linkage_path)
    print(f"    Found: {linkage_path.name}")
    print(f"    Rows: {len(existing_linkage)}")
    print(f"    Columns: {list(existing_linkage.columns)}")
    if 'Case Number' in existing_linkage.columns:
        existing_cases = set(existing_linkage['Case Number'].dropna().astype(int))
        nsqip_cases = set(nsqip_df['Case Number'].dropna().astype(int))
        overlap = existing_cases & nsqip_cases
        only_existing = existing_cases - nsqip_cases
        only_new = nsqip_cases - existing_cases
        print(f"\n    Case Number overlap analysis:")
        print(f"      Existing linkage cases: {len(existing_cases)}")
        print(f"      New NSQIP file cases: {len(nsqip_cases)}")
        print(f"      Cases in BOTH: {len(overlap)}")
        print(f"      Cases ONLY in existing linkage: {len(only_existing)}")
        print(f"      Cases ONLY in new NSQIP file: {len(only_new)}")
else:
    print("    No existing linkage file found")

# Verdict
print("\n" + "-" * 80)
print("ENRICHMENT VERDICT:")
enrichment_exists_in_tables = False
for tbl in all_table_names:
    try:
        cols = con.execute(f"DESCRIBE {tbl}").fetchall()
        for c in cols:
            if 'nsqip_' in c[0].lower():
                enrichment_exists_in_tables = True
                break
    except:
        pass
    if enrichment_exists_in_tables:
        break

if enrichment_exists_in_tables:
    print("  NSQIP columns exist IN the database tables. Checking if enrichment is complete...")
else:
    print("  No nsqip_ prefixed columns found in ANY database table.")
    print("  Existing NSQIP data exists only as SEPARATE export files (LEFT JOIN pattern).")
    print("  Proceeding to Phase 1 matching for this Case Details file.")

# ════════════════════════════════════════════════════════════════════
# STEP 4: Phase 1 — Perfect Deterministic Matching
# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("STEP 4: Phase 1 — Perfect Deterministic Matching")
print("=" * 80)

# Load lakehouse reference data
print("\n  Loading lakehouse reference data...")

mc_df = con.execute("SELECT * FROM master_cohort").fetchdf()
print(f"    master_cohort: {len(mc_df)} rows, {len(mc_df.columns)} cols")

# Check for existing NSQIP linkage we can leverage
if linkage_path.exists():
    existing_linkage = pd.read_csv(linkage_path)
    existing_matched = existing_linkage[existing_linkage['match_status'] == 'Perfect deterministic match']
    existing_case_to_rid = dict(zip(
        existing_matched['Case Number'].astype(int),
        existing_matched['linked_research_id'].astype(int)
    ))
    print(f"    Existing linkage: {len(existing_case_to_rid)} matched Case Number -> research_id mappings")
else:
    existing_case_to_rid = {}
    print("    No existing linkage to leverage")

# Load additional reference tables
try:
    tp_df = con.execute("SELECT research_id, surgery_date FROM tumor_pathology").fetchdf()
    print(f"    tumor_pathology: {len(tp_df)} rows")
except:
    tp_df = pd.DataFrame()
    print("    tumor_pathology: not available")

try:
    op_df = con.execute("SELECT * FROM operative_details LIMIT 0").fetchdf()
    op_cols = list(op_df.columns)
    date_cols_op = [c for c in op_cols if 'date' in c.lower() or 'surg' in c.lower()]
    print(f"    operative_details available, date columns: {date_cols_op}")
    op_df = con.execute("SELECT * FROM operative_details").fetchdf()
except:
    op_df = pd.DataFrame()
    print("    operative_details: not available")

# Source data for MRN lookups
print("\n  Loading source files for MRN cross-reference...")
source_files = {
    'synoptic': REPO / 'raw' / 'All Diagnoses & synoptic 12_1_2025.xlsx',
    'op_sheet': REPO / 'raw' / 'Thyroid OP Sheet data.xlsx',
    'notes': REPO / 'raw' / 'Notes 12_1_25.xlsx',
    'complications': REPO / 'raw' / 'Thyroid all_Complications 12_1_25.xlsx',
}

mrn_to_rid = {}
rid_to_info = {}

for src_name, src_path in source_files.items():
    if not src_path.exists():
        print(f"    SKIP {src_name}: file not found")
        continue
    try:
        if src_name == 'synoptic':
            sdf = pd.read_excel(src_path, sheet_name='synoptics + Dx merged', engine='openpyxl')
        elif src_name == 'op_sheet':
            sdf = pd.read_excel(src_path, sheet_name='Physical OP sheet data', engine='openpyxl')
        elif src_name == 'notes':
            sdf = pd.read_excel(src_path, sheet_name='Sheet1', engine='openpyxl')
        elif src_name == 'complications':
            sdf = pd.read_excel(src_path, sheet_name='Complications', engine='openpyxl')
        else:
            sdf = pd.read_excel(src_path, engine='openpyxl')

        rid_col = None
        for candidate in ['Research ID number', 'Research_ID#', 'research_id', 'Research ID Number']:
            if candidate in sdf.columns:
                rid_col = candidate
                break
        if rid_col is None:
            print(f"    SKIP {src_name}: no research_id column found")
            continue

        for mrn_col in ['EUH_MRN', 'TEC_MRN']:
            if mrn_col in sdf.columns:
                pairs = sdf[[rid_col, mrn_col]].dropna()
                for _, row in pairs.iterrows():
                    try:
                        rid = int(float(row[rid_col]))
                        mrn = str(int(float(row[mrn_col]))).strip()
                        if mrn and mrn != 'nan':
                            mrn_to_rid.setdefault(mrn, set()).add(rid)
                    except (ValueError, OverflowError):
                        pass

        # Collect DOB, sex, surgery_date per research_id
        dob_col = None
        for c in ['DOB', 'Date of birth', 'Date of Birth', 'dob']:
            if c in sdf.columns:
                dob_col = c
                break
        sex_col = None
        for c in ['Gender', 'Sex', 'sex', 'gender']:
            if c in sdf.columns:
                sex_col = c
                break

        for _, row in sdf.iterrows():
            try:
                rid = int(float(row[rid_col]))
            except:
                continue
            if rid not in rid_to_info:
                rid_to_info[rid] = {}
            if dob_col and pd.notna(row.get(dob_col)):
                dob_val = pd.to_datetime(row[dob_col], errors='coerce')
                if pd.notna(dob_val):
                    rid_to_info[rid]['dob'] = dob_val
            if sex_col and pd.notna(row.get(sex_col)):
                rid_to_info[rid]['sex'] = str(row[sex_col]).strip().lower()

        print(f"    {src_name}: loaded ({len(sdf)} rows, MRN pairs extracted)")
    except Exception as e:
        print(f"    {src_name}: ERROR - {e}")

print(f"\n  Total unique MRN -> research_id mappings: {len(mrn_to_rid)}")
print(f"  Total research_ids with metadata: {len(rid_to_info)}")

# Also build lookup from master_cohort
mc_lookup = {}
for _, row in mc_df.iterrows():
    rid = row['research_id']
    try:
        rid_int = int(rid)
    except:
        continue
    mc_lookup[rid_int] = {
        'sex': str(row.get('sex', '')).strip().lower() if pd.notna(row.get('sex')) else None,
        'age': float(row['age_at_surgery']) if pd.notna(row.get('age_at_surgery')) else None,
        'surgery_date': pd.to_datetime(row.get('surgery_date'), errors='coerce'),
    }

print(f"  master_cohort lookup: {len(mc_lookup)} patients")

# ── MATCHING ─────────────────────────────────────────────────────────
print("\n  Starting matching...")

nsqip_df['_op_date'] = pd.to_datetime(nsqip_df['Operation Date'], format='%m/%d/%Y', errors='coerce')
nsqip_df['_dob'] = pd.to_datetime(nsqip_df['Date of Birth'], format='%m/%d/%Y', errors='coerce')
nsqip_df['_idn_str'] = nsqip_df['IDN'].astype(str).str.strip()
nsqip_df['_sex'] = nsqip_df['Sex at Birth'].apply(
    lambda x: str(x).strip().lower() if pd.notna(x) else None
)
nsqip_df['_age'] = nsqip_df['Age at Time of Surgery']
nsqip_df['_cpt'] = nsqip_df['CPT Code'].astype(int)

results = []
match_methods_count = {}

for idx, row in nsqip_df.iterrows():
    case_num = int(row['Case Number'])
    idn = row['_idn_str']
    op_date = row['_op_date']
    dob = row['_dob']
    age = row['_age']
    sex = row['_sex']
    cpt = row['_cpt']

    matched_rid = None
    match_method = None
    verification = {}

    # METHOD 1: Reuse existing verified linkage (same Case Number)
    if case_num in existing_case_to_rid:
        candidate_rid = existing_case_to_rid[case_num]
        matched_rid = candidate_rid
        match_method = "EXISTING_LINKAGE"
        verification['source'] = 'existing_linkage_final.csv'

    # METHOD 2: IDN = EUH_MRN exact match
    if matched_rid is None and idn in mrn_to_rid:
        candidate_rids = mrn_to_rid[idn]
        if len(candidate_rids) == 1:
            crid = list(candidate_rids)[0]
            info = mc_lookup.get(crid, {})
            # Verify: surgery_date must match exactly
            if pd.notna(op_date) and info.get('surgery_date') is not None and pd.notna(info['surgery_date']):
                if op_date.date() == info['surgery_date'].date():
                    # Also verify sex if available
                    sex_ok = (sex is None or info.get('sex') is None or
                              sex == info.get('sex') or
                              (sex == 'female' and info.get('sex') == 'f') or
                              (sex == 'male' and info.get('sex') == 'm'))
                    if sex_ok:
                        matched_rid = crid
                        match_method = "MRN_DATE_EXACT"
                        verification['mrn'] = idn
                        verification['date_match'] = True
        elif len(candidate_rids) > 1:
            date_matches = []
            for crid in candidate_rids:
                info = mc_lookup.get(crid, {})
                if pd.notna(op_date) and info.get('surgery_date') is not None and pd.notna(info['surgery_date']):
                    if op_date.date() == info['surgery_date'].date():
                        date_matches.append(crid)
            if len(date_matches) == 1:
                crid = date_matches[0]
                info = mc_lookup.get(crid, {})
                sex_ok = (sex is None or info.get('sex') is None or
                          sex == info.get('sex') or
                          (sex == 'female' and info.get('sex') == 'f') or
                          (sex == 'male' and info.get('sex') == 'm'))
                if sex_ok:
                    matched_rid = crid
                    match_method = "MRN_DATE_DISAMBIG"
                    verification['mrn'] = idn
                    verification['date_match'] = True
                    verification['multi_mrn_resolved'] = True

    # METHOD 3: IDN as TEC_MRN + date
    if matched_rid is None:
        for mrn_val, rids in mrn_to_rid.items():
            if mrn_val == idn:
                for crid in rids:
                    info = mc_lookup.get(crid, {})
                    if pd.notna(op_date) and info.get('surgery_date') is not None and pd.notna(info['surgery_date']):
                        if op_date.date() == info['surgery_date'].date():
                            sex_ok = (sex is None or info.get('sex') is None or
                                      sex == info.get('sex') or
                                      (sex == 'female' and info.get('sex') == 'f') or
                                      (sex == 'male' and info.get('sex') == 'm'))
                            if sex_ok:
                                matched_rid = crid
                                match_method = "MRN_DATE_EXACT"
                                verification['mrn'] = idn
                                break

    # METHOD 4: DOB + surgery_date + sex (triple deterministic)
    if matched_rid is None and pd.notna(dob) and pd.notna(op_date):
        dob_candidates = []
        for rid, info in rid_to_info.items():
            if 'dob' not in info:
                continue
            if info['dob'].date() != dob.date():
                continue
            mc_info = mc_lookup.get(rid, {})
            if mc_info.get('surgery_date') is not None and pd.notna(mc_info['surgery_date']):
                if op_date.date() == mc_info['surgery_date'].date():
                    sex_ok = (sex is None or mc_info.get('sex') is None or
                              sex == mc_info.get('sex') or
                              (sex == 'female' and mc_info.get('sex') == 'f') or
                              (sex == 'male' and mc_info.get('sex') == 'm'))
                    if sex_ok:
                        dob_candidates.append(rid)

        if len(dob_candidates) == 1:
            matched_rid = dob_candidates[0]
            match_method = "DOB_DATE_SEX"
            verification['dob_match'] = True
            verification['date_match'] = True

    if match_method:
        match_methods_count[match_method] = match_methods_count.get(match_method, 0) + 1

    results.append({
        'nsqip_row_idx': idx,
        'Case_Number': case_num,
        'IDN': row['IDN'],
        'Operation_Date': str(row['Operation Date']),
        'CPT_Code': cpt,
        'Age': age,
        'Sex': sex,
        'DOB': str(row['Date of Birth']),
        'matched_research_id': matched_rid,
        'match_method': match_method,
    })

results_df = pd.DataFrame(results)
n_matched = results_df['matched_research_id'].notna().sum()
n_unmatched = results_df['matched_research_id'].isna().sum()

print("\n" + "=" * 80)
print("  MATCH RESULTS")
print("=" * 80)
print(f"\n  Exact match rate: {n_matched} out of {len(nsqip_df)} rows have perfect deterministic matches to a research_id")
print(f"  Unmatched rows: {n_unmatched}")
print(f"  Match rate: {100 * n_matched / len(nsqip_df):.1f}%")

print(f"\n  Unique research_ids matched: {results_df['matched_research_id'].dropna().nunique()}")

print(f"\n  Match methods breakdown:")
for method, count in sorted(match_methods_count.items(), key=lambda x: -x[1]):
    print(f"    {method}: {count}")

# Side-by-side verification table
print("\n" + "=" * 80)
print("  VERIFICATION TABLE: First 10 perfectly matched rows")
print("=" * 80)

matched_results = results_df[results_df['matched_research_id'].notna()].head(10)
print(f"\n{'Case#':>8} | {'IDN':>10} | {'NSQIP Op Date':>14} | {'CPT':>6} | {'Age':>5} | {'Sex':>8} | {'DOB':>12} || {'RID':>6} | {'MC Sex':>8} | {'MC Age':>6} | {'MC Surg Date':>14} | {'Method':>20}")
print("-" * 160)

for _, r in matched_results.iterrows():
    rid = int(r['matched_research_id'])
    mc_info = mc_lookup.get(rid, {})
    mc_sex = mc_info.get('sex', '?')
    mc_age = mc_info.get('age')
    mc_age_str = f"{mc_age:.0f}" if mc_age is not None else "?"
    mc_date = mc_info.get('surgery_date')
    mc_date_str = mc_date.strftime('%Y-%m-%d') if mc_date is not None and pd.notna(mc_date) else "?"

    print(f"{r['Case_Number']:>8} | {r['IDN']:>10} | {r['Operation_Date']:>14} | {r['CPT_Code']:>6} | {r['Age']:>5.1f} | {str(r['Sex']):>8} | {r['DOB']:>12} || {rid:>6} | {mc_sex:>8} | {mc_age_str:>6} | {mc_date_str:>14} | {r['match_method']:>20}")

# Show some unmatched rows
print(f"\n  UNMATCHED ROWS ({n_unmatched} total):")
unmatched = results_df[results_df['matched_research_id'].isna()]
if len(unmatched) > 0:
    print(f"\n{'Case#':>8} | {'IDN':>10} | {'NSQIP Op Date':>14} | {'CPT':>6} | {'Age':>5} | {'Sex':>8} | {'DOB':>12}")
    print("-" * 90)
    for _, r in unmatched.head(30).iterrows():
        age_str = f"{r['Age']:.1f}" if pd.notna(r['Age']) else "?"
        print(f"{r['Case_Number']:>8} | {r['IDN']:>10} | {r['Operation_Date']:>14} | {r['CPT_Code']:>6} | {age_str:>5} | {str(r['Sex']):>8} | {r['DOB']:>12}")
else:
    print("    None — all rows matched!")

# Save intermediate results
output_path = REPO / "studies" / "nsqip_linkage" / "case_details_linkage_results.csv"
results_df.to_csv(output_path, index=False)
print(f"\n  Saved results to: {output_path}")

con.close()
print("\n  DONE. Awaiting 'APPROVE ENRICHMENT' before Phase 2.")
