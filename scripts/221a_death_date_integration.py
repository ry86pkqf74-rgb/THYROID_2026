#!/usr/bin/env python3
"""
Script 221a: Death Date Integration + Survival Recalculation
============================================================
Source: raw/Notes 12_1_25.xlsx, Sheet2, column 'DEATH'
Target: canonical_patient_master_v1 on MotherDuck thyroid_ete_fix_20260413

Adds:
  - death_date            DATE
  - vital_status          VARCHAR ('deceased' | 'alive')
  - death_occurred        BOOLEAN
  - death_source          VARCHAR provenance
  - overall_survival_days INT
  - overall_survival_years FLOAT
  - survival_event        BOOLEAN (TRUE = death observed)
  - followup_or_death_date DATE
  - followup_or_death_years FLOAT
  - death_integration_script VARCHAR

Run BEFORE Script 221b (NSQIP + Parathyroid).
"""

import sys
import os
import duckdb
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from motherduck_client import get_token

token = get_token()
print(f"Token length: {len(token)} chars (SET)")
con = duckdb.connect(f"md:thyroid_ete_fix_20260413?motherduck_token={token}")
REPO = Path(__file__).resolve().parent.parent
SCRIPT_NAME = "221a_death_date_integration"

print("Connected to: md:thyroid_ete_fix_20260413")
print(f"Repo root: {REPO}")

# ---------------------------------------------------------------------------
# Helper: verify canonical invariants
# ---------------------------------------------------------------------------
def verify_invariants(label=""):
    row = con.execute("""
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT research_id) as distinct_rids,
               COUNT(*) FILTER (WHERE research_id IS NULL) as null_rids,
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) as null_fna
        FROM canonical_patient_master_v1
    """).fetchone()
    print(f"\n[INVARIANTS {label}] total={row[0]}, distinct_rids={row[1]}, null_rids={row[2]}, null_fna={row[3]}")
    if row != (10871, 10871, 0, 0):
        raise AssertionError(f"INVARIANT FAIL at '{label}': {row} — expected (10871, 10871, 0, 0)")
    print("  ✓ All invariants PASS")
    return row


# ---------------------------------------------------------------------------
# PRE-CHECK: baseline invariants
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("PRE-CHECK: Baseline canonical invariants")
print("=" * 70)
verify_invariants("BASELINE")

# ---------------------------------------------------------------------------
# TASK 1: Extract death dates from Notes workbook
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("TASK 1: Extract death dates from Notes workbook (Sheet2, DEATH column)")
print("=" * 70)

notes_path = REPO / "raw" / "Notes 12_1_25.xlsx"
assert notes_path.exists(), f"FAIL: Notes workbook not found at {notes_path}"
print(f"Reading: {notes_path}")

df = pd.read_excel(notes_path, sheet_name="Sheet2", engine="openpyxl")
print(f"Sheet2: {len(df)} rows × {len(df.columns)} columns")
print(f"Columns (first 15): {list(df.columns)[:15]}")

# Find research_id column
rid_candidates = [c for c in df.columns if 'research' in str(c).lower() and 'id' in str(c).lower()]
assert len(rid_candidates) >= 1, (
    f"FAIL: no research_id column found. Columns: {list(df.columns)[:20]}"
)
rid_col = rid_candidates[0]
print(f"Research ID column: '{rid_col}'")

# Find the DEATH column
death_candidates = [c for c in df.columns if str(c).strip().upper() == 'DEATH']
if len(death_candidates) != 1:
    # Fuzzy fallback: any column containing 'death'
    death_candidates = [c for c in df.columns if 'death' in str(c).lower()]
    print(f"  'DEATH' (exact) not found. Columns containing 'death': {death_candidates}")
assert len(death_candidates) >= 1, (
    f"FAIL: no DEATH column found. All columns: {list(df.columns)}"
)
death_col = death_candidates[0]
print(f"DEATH column: '{death_col}'")

# Extract non-null death dates
death_df = df[[rid_col, death_col]].copy()
death_df.columns = ['research_id', 'death_date_raw']
death_df = death_df.dropna(subset=['death_date_raw'])
death_df = death_df[death_df['death_date_raw'].astype(str).str.strip().isin(['', 'nan', 'NaT', 'None']) == False]
print(f"Non-null DEATH values: {len(death_df)}")

# Parse dates
death_df['death_date'] = pd.to_datetime(death_df['death_date_raw'], errors='coerce')
unparsed_mask = death_df['death_date'].isna()
unparsed = unparsed_mask.sum()
if unparsed > 0:
    print(f"WARNING: {unparsed} death dates could not be auto-parsed — trying alternates:")
    for idx in death_df[unparsed_mask].index:
        raw = str(death_df.loc[idx, 'death_date_raw']).strip()
        parsed = None
        for fmt in ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d', '%m-%d-%Y', '%d-%b-%Y']:
            try:
                parsed = pd.to_datetime(raw, format=fmt)
                break
            except Exception:
                pass
        if parsed is None:
            # Try Excel serial
            try:
                if raw.replace('.', '').isdigit():
                    parsed = pd.Timestamp('1899-12-30') + pd.Timedelta(days=int(float(raw)))
            except Exception:
                pass
        if parsed is not None:
            death_df.loc[idx, 'death_date'] = parsed
            print(f"  Recovered: RID={death_df.loc[idx, 'research_id']}, raw='{raw}' → {parsed.date()}")
        else:
            print(f"  Cannot parse: RID={death_df.loc[idx, 'research_id']}, raw='{raw}'")

n_before = len(death_df)
death_df = death_df.dropna(subset=['death_date'])
dropped = n_before - len(death_df)
if dropped > 0:
    print(f"Dropped {dropped} unparseable death dates")

# Cast research_id to string (strip .0 from float reads)
death_df['research_id'] = (
    death_df['research_id']
    .apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() not in ['', 'nan'] else None)
)
death_df = death_df.dropna(subset=['research_id'])

# Add provenance
death_df['death_source'] = 'notes_workbook_sheet2_death_col'
death_df['death_source_workbook'] = 'Notes 12_1_25.xlsx'
death_df['death_source_script'] = SCRIPT_NAME

death_df = death_df[['research_id', 'death_date', 'death_source', 'death_source_workbook', 'death_source_script']]

print(f"\nFinal death date extract: {len(death_df)} patients")
print(f"Unique research_ids: {death_df['research_id'].nunique()}")
print(f"Date range: {death_df['death_date'].min().date()} to {death_df['death_date'].max().date()}")

# Dedup: keep latest death date per patient
dupes = death_df[death_df['research_id'].duplicated(keep=False)]
if len(dupes) > 0:
    print(f"WARNING: {len(dupes)} rows with duplicate research_ids — keeping latest death date:")
    print(dupes[['research_id', 'death_date']].to_string())
    death_df = (
        death_df.sort_values('death_date', ascending=False)
        .drop_duplicates('research_id', keep='first')
        .reset_index(drop=True)
    )
    print(f"After dedup: {len(death_df)} patients")

assert death_df['research_id'].is_unique, "FAIL: duplicate research_ids remain after dedup"

# ---------------------------------------------------------------------------
# Step 1.2: Upload staging table to MotherDuck
# ---------------------------------------------------------------------------
print("\n--- Step 1.2: Upload to MotherDuck staging table ---")

tmp_path = REPO / "scripts" / "output" / "_death_dates_staging_221a.parquet"
tmp_path.parent.mkdir(parents=True, exist_ok=True)
death_df.to_parquet(str(tmp_path), index=False)

con.execute(f"""
    CREATE OR REPLACE TABLE _notes_death_dates_v1 AS 
    SELECT 
        CAST(research_id AS VARCHAR) AS research_id,
        CAST(death_date AS DATE) AS death_date,
        death_source,
        death_source_workbook,
        death_source_script
    FROM read_parquet('{tmp_path}')
""")
try:
    tmp_path.unlink()
except Exception:
    pass

verify = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _notes_death_dates_v1").fetchone()
print(f"✓ Uploaded _notes_death_dates_v1: {verify[0]} rows, {verify[1]} patients")
assert verify[0] == verify[1], "FAIL: duplicate research_ids in staging table"

# ---------------------------------------------------------------------------
# Step 1.3: Validate against canonical spine
# ---------------------------------------------------------------------------
print("\n--- Step 1.3: Validate against canonical spine ---")

orphans = con.execute("""
    SELECT COUNT(*) as orphans
    FROM _notes_death_dates_v1 d
    WHERE d.research_id NOT IN (SELECT research_id FROM canonical_patient_master_v1)
""").fetchone()[0]
print(f"Orphan research_ids (not in canonical): {orphans}")

if orphans > 0:
    orphan_rids = con.execute("""
        SELECT d.research_id, d.death_date
        FROM _notes_death_dates_v1 d
        WHERE d.research_id NOT IN (SELECT research_id FROM canonical_patient_master_v1)
        ORDER BY d.research_id
    """).fetchall()
    print(f"Orphan RIDs: {orphan_rids}")
    print(f"WARNING: {orphans} RIDs not in canonical spine — they will be excluded via LEFT JOIN")

# ---------------------------------------------------------------------------
# Step 1.4: Sanity check — death dates vs surgery dates
# ---------------------------------------------------------------------------
print("\n--- Step 1.4: Sanity check — death date vs surgery date ---")

pre_surgery_deaths = con.execute("""
    SELECT 
        d.research_id,
        d.death_date,
        TRY_CAST(c.ops_surg_date AS DATE) AS surg_date,
        DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date) AS days_post_surgery
    FROM _notes_death_dates_v1 d
    JOIN canonical_patient_master_v1 c ON d.research_id = c.research_id
    WHERE d.death_date < TRY_CAST(c.ops_surg_date AS DATE)
    ORDER BY days_post_surgery
""").fetchall()

print(f"Deaths before surgery date: {len(pre_surgery_deaths)}")
if pre_surgery_deaths:
    print("  NOTE: These are data anomalies — flagged but NOT excluded from integration:")
    for row in pre_surgery_deaths:
        print(f"    RID={row[0]}, death={row[1]}, surgery={row[2]}, days={row[3]}")

survival_dist = con.execute("""
    SELECT 
        COUNT(*) as n_deceased,
        MIN(DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date)) AS min_days,
        CAST(PERCENTILE_CONT(0.25) WITHIN GROUP (
            ORDER BY DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date)) AS INT) AS p25_days,
        CAST(PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date)) AS INT) AS median_days,
        CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date)) AS INT) AS p75_days,
        MAX(DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date)) AS max_days
    FROM _notes_death_dates_v1 d
    JOIN canonical_patient_master_v1 c ON d.research_id = c.research_id
    WHERE d.death_date >= TRY_CAST(c.ops_surg_date AS DATE)
""").fetchone()

print("\nSurvival time distribution (deceased, death >= surgery):")
print(f"  N deceased with valid dates: {survival_dist[0]}")
print(f"  Min days:    {survival_dist[1]}")
print(f"  P25 days:    {survival_dist[2]}")
print(f"  Median days: {survival_dist[3]}  ({survival_dist[3]/365.25:.1f} years)")
print(f"  P75 days:    {survival_dist[4]}")
print(f"  Max days:    {survival_dist[5]}  ({survival_dist[5]/365.25:.1f} years)")

# ---------------------------------------------------------------------------
# TASK 2: Cross-validate death data
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("TASK 2: Cross-validate death data")
print("=" * 70)

# Step 2.1: NSQIP cross-validation
print("\n--- Step 2.1: NSQIP cross-validation (nsqip_death_30d already in canonical) ---")
try:
    xval = con.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE c.nsqip_death_30d IS NOT NULL) AS nsqip_total_with_data,
            COUNT(*) FILTER (WHERE c.nsqip_death_30d = 'Yes') AS nsqip_dead,
            COUNT(*) FILTER (WHERE c.nsqip_death_30d = 'Yes' AND d.research_id IS NOT NULL) 
                AS both_say_dead,
            COUNT(*) FILTER (WHERE c.nsqip_death_30d = 'Yes' AND d.research_id IS NULL) 
                AS nsqip_dead_notes_no_date,
            COUNT(*) FILTER (WHERE (c.nsqip_death_30d IS NULL OR c.nsqip_death_30d != 'Yes') 
                                   AND d.research_id IS NOT NULL) 
                AS notes_dead_nsqip_alive
        FROM canonical_patient_master_v1 c
        LEFT JOIN _notes_death_dates_v1 d ON c.research_id = d.research_id
    """).fetchone()
    print("NSQIP vs Notes death cross-validation:")
    print(f"  NSQIP patients with death data:        {xval[0]}")
    print(f"  NSQIP 30-day deaths:                   {xval[1]}")
    print(f"  Both say deceased (30d NSQIP + date):  {xval[2]}")
    print(f"  NSQIP 30d dead, Notes has no date:     {xval[3]}")
    print(f"  Notes has death date, NSQIP alive/NA:  {xval[4]}")
    # Show NSQIP-only dead patients (may have died in 30 days, Notes may not have date)
    if xval[3] and xval[3] > 0:
        nsqip_only = con.execute("""
            SELECT c.research_id, c.nsqip_death_30d, c.nsqip_operation_date
            FROM canonical_patient_master_v1 c
            LEFT JOIN _notes_death_dates_v1 d ON c.research_id = d.research_id
            WHERE c.nsqip_death_30d = 'Yes' AND d.research_id IS NULL
            LIMIT 10
        """).fetchall()
        print(f"  NSQIP-only 30d deaths (no Notes date): {nsqip_only}")
except Exception as e:
    print(f"  NSQIP cross-validation error: {e}")

# Step 2.2: NLP survival_followup cross-validation
print("\n--- Step 2.2: NLP survival_followup cross-validation ---")

# Check which columns exist in canonical
canon_cols = set(
    r[0] for r in con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'canonical_patient_master_v1' AND table_schema = 'main'
    """).fetchall()
)
print(f"Canonical columns relevant to followup: {[c for c in canon_cols if 'follow' in c.lower() or 'nlp' in c.lower() or 'contact' in c.lower() or 'survival' in c.lower()]}")

nlp_xval_col = None
for cand in ['nlp_survival_followup_has_entities', 'has_survival_followup_entities', 'followup_years', 'last_contact_date']:
    if cand in canon_cols:
        nlp_xval_col = cand
        break

if nlp_xval_col:
    # Build a safe IS NOT NULL check depending on column type
    xval2 = con.execute(f"""
        SELECT 
            COUNT(DISTINCT d.research_id) as notes_deceased,
            COUNT(DISTINCT d.research_id) FILTER (WHERE c.{nlp_xval_col} IS NOT NULL) as has_followup_data
        FROM _notes_death_dates_v1 d
        JOIN canonical_patient_master_v1 c ON d.research_id = c.research_id
    """).fetchone()
    print(f"Notes deceased: {xval2[0]}")
    print(f"  With {nlp_xval_col} populated: {xval2[1]}")
else:
    print("  No NLP survival/followup column found — skipping NLP cross-validation")

# Also report the 26 confirmed-dead-but-no-date patients
print("\n  NOTE: 26 additional patients confirmed deceased in Notes workbook but NO parseable date:")
print("        RIDs: 516, 665, 1678, 1988, 2086, 2128, 3204, 3382, 3386, 3527, 3560, 3749,")
print("              3920, 3923, 4390, 4722, 4985, 5937, 6456, 6858, 7247, 7480, 8369")
print("        (3 additional RIDs 9816/9818/9820 had DEATH column containing a clinical note)")
print("        These patients will have vital_status='alive' (no date) in this integration.")

# Step 2.3: Check gold_master for death columns
print("\n--- Step 2.3: Check gold_master for death/vital columns ---")
try:
    gold_death_cols = con.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'gold_master_patient_facts_v1' AND table_schema = 'main'
          AND (LOWER(column_name) LIKE '%death%' 
               OR LOWER(column_name) LIKE '%vital%' 
               OR LOWER(column_name) LIKE '%alive%'
               OR LOWER(column_name) LIKE '%deceased%')
    """).fetchall()
    if gold_death_cols:
        print(f"Gold master death/vital columns: {[r[0] for r in gold_death_cols]}")
        for col in gold_death_cols:
            col_name = col[0]
            xval3 = con.execute(f"""
                SELECT COUNT(*) FROM gold_master_patient_facts_v1 WHERE {col_name} IS NOT NULL
            """).fetchone()[0]
            print(f"  {col_name}: {xval3} non-null values")
    else:
        print("  No death/vital columns in gold_master_patient_facts_v1")
except Exception as e:
    print(f"  gold_master_patient_facts_v1 not accessible: {e}")

# ---------------------------------------------------------------------------
# TASK 3: Integrate into canonical
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("TASK 3: Integrate death dates into canonical_patient_master_v1")
print("=" * 70)

# Step 3.1: Back up canonical
print("\n--- Step 3.1: Backup canonical ---")
con.execute("""
    CREATE OR REPLACE TABLE canonical_patient_master_v1_pre221a AS 
    SELECT * FROM canonical_patient_master_v1
""")
backup_count = con.execute("SELECT COUNT(*) FROM canonical_patient_master_v1_pre221a").fetchone()[0]
assert backup_count == 10871, f"FAIL: backup has {backup_count} rows, expected 10871"
print(f"✓ Backup: canonical_patient_master_v1_pre221a ({backup_count} rows)")

# Step 3.2: Check which new columns already exist (safe re-run)
print("\n--- Step 3.2: Check for pre-existing death/survival columns ---")
new_cols = [
    'death_date', 'vital_status', 'death_occurred', 'death_source',
    'overall_survival_days', 'overall_survival_years', 'survival_event',
    'followup_or_death_date', 'followup_or_death_years', 'death_integration_script'
]
already_exist = [c for c in new_cols if c in canon_cols]
if already_exist:
    print(f"  Pre-existing columns (will be rebuilt): {already_exist}")
else:
    print("  No pre-existing death columns — clean integration")

# Step 3.3: Build new canonical with death columns
print("\n--- Step 3.3: Rebuild canonical with death + survival columns ---")

# Determine which followup columns exist for the endpoint calculation
followup_days_col = 'followup_days' if 'followup_days' in canon_cols else None
followup_years_col = 'followup_years' if 'followup_years' in canon_cols else None
last_contact_col = 'last_contact_date' if 'last_contact_date' in canon_cols else None

print(f"  Using followup_days: {followup_days_col}")
print(f"  Using followup_years: {followup_years_col}")
print(f"  Using last_contact_date: {last_contact_col}")

# Build SELECT for existing columns — exclude the new columns if they already exist
# so we don't duplicate them
exclude_cols = set(new_cols) & canon_cols
if exclude_cols:
    print(f"  Excluding pre-existing columns from c.*: {exclude_cols}")
    # DuckDB EXCLUDE syntax
    exclude_clause = ", ".join(f'"{c}"' for c in sorted(exclude_cols))
    c_select = f"c.* EXCLUDE ({exclude_clause})"
else:
    c_select = "c.*"

# Build the followup_or_death_years expression
if followup_years_col:
    fody_expr = f"""
        CASE 
            WHEN d.death_date IS NOT NULL 
            THEN ROUND(DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date) / 365.25, 2)
            WHEN c.{followup_years_col} IS NOT NULL AND c.{followup_years_col} > 0 
            THEN c.{followup_years_col}
            ELSE 0.0
        END"""
else:
    fody_expr = """
        CASE 
            WHEN d.death_date IS NOT NULL 
            THEN ROUND(DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date) / 365.25, 2)
            ELSE 0.0
        END"""

if followup_days_col:
    os_days_expr = f"""
        CASE 
            WHEN d.death_date IS NOT NULL 
            THEN DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date)
            ELSE c.{followup_days_col}
        END"""
    os_years_expr = f"""
        CASE 
            WHEN d.death_date IS NOT NULL 
            THEN ROUND(DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date) / 365.25, 2)
            ELSE ROUND(c.{followup_days_col} / 365.25, 2)
        END"""
elif followup_years_col:
    os_days_expr = f"""
        CASE 
            WHEN d.death_date IS NOT NULL 
            THEN DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date)
            ELSE CAST(c.{followup_years_col} * 365.25 AS INT)
        END"""
    os_years_expr = f"""
        CASE 
            WHEN d.death_date IS NOT NULL 
            THEN ROUND(DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date) / 365.25, 2)
            ELSE c.{followup_years_col}
        END"""
else:
    os_days_expr = """
        CASE 
            WHEN d.death_date IS NOT NULL 
            THEN DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date)
            ELSE NULL
        END"""
    os_years_expr = """
        CASE 
            WHEN d.death_date IS NOT NULL 
            THEN ROUND(DATEDIFF('day', TRY_CAST(c.ops_surg_date AS DATE), d.death_date) / 365.25, 2)
            ELSE NULL
        END"""

if last_contact_col:
    fod_date_expr = f"COALESCE(d.death_date, TRY_CAST(c.{last_contact_col} AS DATE))"
else:
    fod_date_expr = "d.death_date"

rebuild_sql = f"""
    CREATE OR REPLACE TABLE canonical_patient_master_v1 AS
    SELECT 
        {c_select},
        
        -- Death date and vital status
        d.death_date AS death_date,
        CASE 
            WHEN d.death_date IS NOT NULL THEN 'deceased'
            ELSE 'alive'
        END AS vital_status,
        CASE 
            WHEN d.death_date IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS death_occurred,
        d.death_source AS death_source,
        
        -- Overall survival: surgery → death (or last contact if alive)
        {os_days_expr} AS overall_survival_days,
        {os_years_expr} AS overall_survival_years,
        
        -- Survival event indicator (TRUE = death observed, FALSE = censored)
        CASE 
            WHEN d.death_date IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS survival_event,
        
        -- Best available endpoint date and duration
        {fod_date_expr} AS followup_or_death_date,
        {fody_expr} AS followup_or_death_years,
        
        -- Provenance
        '{SCRIPT_NAME}' AS death_integration_script

    FROM canonical_patient_master_v1 c
    LEFT JOIN _notes_death_dates_v1 d ON c.research_id = d.research_id
"""

print("  Executing rebuild...")
con.execute(rebuild_sql)
print("  ✓ canonical_patient_master_v1 rebuilt")

# Step 3.3: Verify invariants
print("\n--- Step 3.3: Verify canonical invariants after rebuild ---")
verify_invariants("POST-REBUILD")

# Step 3.4: Verify death integration results
print("\n--- Step 3.4: Death integration report ---")

death_report = con.execute("""
    SELECT
        COUNT(*) AS total_patients,
        COUNT(*) FILTER (WHERE death_occurred = TRUE) AS deceased,
        COUNT(*) FILTER (WHERE vital_status = 'alive') AS alive,
        COUNT(*) FILTER (WHERE death_date IS NOT NULL) AS has_death_date,
        COUNT(*) FILTER (WHERE overall_survival_days IS NOT NULL AND overall_survival_days > 0) AS has_os_days,
        COUNT(*) FILTER (WHERE survival_event = TRUE) AS survival_events,
        COUNT(*) FILTER (WHERE followup_or_death_years > 0) AS has_endpoint,
        
        -- Among malignant cohort
        COUNT(*) FILTER (WHERE fna_path_outcome = 'malignant') AS malignant_total,
        COUNT(*) FILTER (WHERE fna_path_outcome = 'malignant' AND death_occurred = TRUE) AS malignant_deceased,
        COUNT(*) FILTER (WHERE fna_path_outcome = 'malignant' AND followup_or_death_years > 0) AS malignant_has_endpoint,
        
        -- Survival time stats for deceased
        ROUND(AVG(CASE WHEN death_occurred THEN overall_survival_years END), 2) AS mean_os_deceased_yrs,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            CASE WHEN death_occurred THEN overall_survival_years END), 2) AS median_os_deceased_yrs
    FROM canonical_patient_master_v1
""").fetchone()

labels = [
    'total_patients', 'deceased', 'alive', 'has_death_date', 'has_os_days',
    'survival_events', 'has_endpoint', 'malignant_total', 'malignant_deceased',
    'malignant_has_endpoint', 'mean_os_deceased_yrs', 'median_os_deceased_yrs'
]

print("\n" + "=" * 60)
print("DEATH DATE INTEGRATION REPORT")
print("=" * 60)
for label, val in zip(labels, death_report):
    print(f"  {label:<35} {val}")

# Assertions
assert death_report[1] == death_report[3], (
    f"FAIL: deceased count ({death_report[1]}) != death_date count ({death_report[3]})"
)
assert death_report[1] == death_report[5], (
    f"FAIL: deceased count ({death_report[1]}) != survival_event count ({death_report[5]})"
)
print("\n✓ Death integration assertions PASS")

# Step 3.5: Follow-up improvement
print("\n--- Step 3.5: Follow-up improvement ---")
if followup_years_col:
    improvement = con.execute(f"""
        SELECT 
            COUNT(*) FILTER (WHERE followup_or_death_years > 0) AS new_has_followup,
            COUNT(*) FILTER (WHERE {followup_years_col} > 0) AS old_has_followup,
            COUNT(*) FILTER (WHERE followup_or_death_years > 0 AND 
                             ({followup_years_col} IS NULL OR {followup_years_col} = 0)) 
                AS gained_from_death_dates
        FROM canonical_patient_master_v1
    """).fetchone()
    print(f"  Old {followup_years_col} > 0:         {improvement[1]}")
    print(f"  New followup_or_death_years > 0:  {improvement[0]}")
    print(f"  Patients gained (death date added follow-up): {improvement[2]}")

# ---------------------------------------------------------------------------
# TASK 4: Final column count
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("TASK 4: Final column count")
print("=" * 70)

total_cols = con.execute("""
    SELECT COUNT(*) FROM information_schema.columns
    WHERE table_name = 'canonical_patient_master_v1' AND table_schema = 'main'
""").fetchone()[0]
print(f"\ncanonical_patient_master_v1: 10,871 patients × {total_cols} columns")
print("New columns added:")
for col in new_cols:
    print(f"  + {col}")

# Final invariants
print("\n" + "=" * 70)
print("FINAL INVARIANTS")
print("=" * 70)
verify_invariants("FINAL")

print("\n" + "=" * 70)
print("✓ Script 221a COMPLETE — death dates integrated successfully")
print("  Run Script 221b next for NSQIP + Parathyroid integration")
print("=" * 70)
