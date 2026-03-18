#!/usr/bin/env python3
"""Phase 3: MotherDuck live reconciliation for manuscript submission audit."""
import os, sys, json

# Load token
try:
    import toml
    t = toml.load('.streamlit/secrets.toml')
    os.environ.setdefault('MOTHERDUCK_TOKEN', t['MOTHERDUCK_TOKEN'])
except Exception:
    pass

import duckdb

con = duckdb.connect('md:thyroid_research_2026')

print("=" * 70)
print("PHASE 3: MOTHERDUCK LIVE RECONCILIATION")
print("=" * 70)

# 1. Core table row counts
tables = [
    'risk_enriched_mv',
    'advanced_features_v3',
    'survival_cohort_ready_mv',
    'survival_cohort_enriched',
    'manuscript_cohort_v1',
    'patient_analysis_resolved_v1',
    'episode_analysis_resolved_v1_dedup',
    'analysis_cancer_cohort_v1',
    'thyroid_scoring_py_v1',
    'path_synoptics',
    'tumor_pathology',
    'recurrence_risk_features_mv',
    'patient_level_summary_mv',
]

print("\n--- TABLE ROW COUNTS ---")
for t in tables:
    try:
        r = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        print(f"  {t}: {r:,}")
    except Exception as e:
        print(f"  {t}: ERROR - {e}")

# 2. Manuscript-critical counts
print("\n--- MANUSCRIPT-CRITICAL DERIVED COUNTS ---")

# Distinct patients in path_synoptics (manuscript: 11,673)
r = con.execute('SELECT COUNT(DISTINCT research_id) FROM path_synoptics').fetchone()[0]
print(f"  Distinct patients (path_synoptics): {r:,}  [manuscript claims 11,673]")

# risk_enriched_mv breakdown 
try:
    cols = [c[0] for c in con.execute("SELECT * FROM risk_enriched_mv LIMIT 0").description]
    print(f"  risk_enriched_mv columns ({len(cols)}): {cols[:15]}...")
    
    # ETE presence
    if 'extrathyroidal_extension' in cols:
        r = con.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN extrathyroidal_extension IS NOT NULL THEN 1 END) as has_ete
            FROM risk_enriched_mv
        """).fetchone()
        print(f"  risk_enriched_mv: total={r[0]:,}, has_ete={r[1]:,}")
    
    # ETE type distribution
    ete_col = 'extrathyroidal_extension' if 'extrathyroidal_extension' in cols else None
    if ete_col:
        rows = con.execute(f"""
            SELECT {ete_col}, COUNT(*) as n 
            FROM risk_enriched_mv 
            GROUP BY {ete_col} 
            ORDER BY n DESC 
            LIMIT 10
        """).fetchall()
        print(f"  ETE distribution:")
        for row in rows:
            print(f"    {row[0]}: {row[1]:,}")
except Exception as e:
    print(f"  risk_enriched_mv detail: ERROR - {e}")

# 3. Survival cohort verification
print("\n--- SURVIVAL COHORT VERIFICATION ---")
try:
    cols = [c[0] for c in con.execute("SELECT * FROM survival_cohort_ready_mv LIMIT 0").description]
    print(f"  survival_cohort_ready_mv columns: {cols[:10]}...")
    
    r = con.execute('SELECT COUNT(*) FROM survival_cohort_ready_mv').fetchone()[0]
    print(f"  Total rows: {r:,}")
    
    # Event counts
    for col in ['event', 'event_occurred', 'recurrence_flag']:
        if col in cols:
            r2 = con.execute(f"SELECT SUM(CASE WHEN {col} THEN 1 ELSE 0 END) FROM survival_cohort_ready_mv").fetchone()[0]
            print(f"  Events ({col}): {r2}")
except Exception as e:
    print(f"  survival_cohort_ready_mv: ERROR - {e}")

# 4. PSM source data (script 31 reads risk_enriched_mv for PSM)
print("\n--- PSM SOURCE VERIFICATION ---")
try:
    # Script 31 filters for analysis: needs ETE, recurrence_risk, etc.
    # Check the ETE groupings that script 31 uses
    r = con.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN extrathyroidal_extension IS NOT NULL 
                       AND extrathyroidal_extension != '' THEN 1 END) as has_ete_nonblank
        FROM risk_enriched_mv
    """).fetchone()
    print(f"  risk_enriched_mv: total={r[0]:,}, has_ete_nonblank={r[1]:,}")
    
    # Check what script 31 would classify as ETE groups
    r = con.execute("""
        SELECT 
            CASE 
                WHEN LOWER(extrathyroidal_extension) LIKE '%gross%' OR LOWER(extrathyroidal_extension) LIKE '%extensive%' THEN 'gross'
                WHEN LOWER(extrathyroidal_extension) LIKE '%micro%' OR LOWER(extrathyroidal_extension) LIKE '%minimal%' THEN 'microscopic'
                WHEN extrathyroidal_extension IS NULL OR TRIM(extrathyroidal_extension) = '' THEN 'none/missing'
                ELSE 'other: ' || LEFT(extrathyroidal_extension, 30)
            END as ete_group,
            COUNT(*) as n
        FROM risk_enriched_mv
        GROUP BY 1
        ORDER BY n DESC
    """).fetchall()
    print(f"  ETE classification for PSM:")
    for row in r:
        print(f"    {row[0]}: {row[1]:,}")
except Exception as e:
    print(f"  PSM source: ERROR - {e}")

# 5. Frozen cohort validation
print("\n--- FROZEN COHORT VERIFICATION ---")
try:
    r = con.execute('SELECT COUNT(*) FROM manuscript_cohort_v1').fetchone()[0]
    print(f"  manuscript_cohort_v1: {r:,}  [expected 10,871]")
    
    r = con.execute('SELECT COUNT(*) FROM analysis_cancer_cohort_v1').fetchone()[0]
    print(f"  analysis_cancer_cohort_v1: {r:,}  [expected 4,136]")
    
    r = con.execute('SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup').fetchone()[0]
    print(f"  episode_analysis_resolved_v1_dedup: {r:,}  [expected 9,368]")
    
    r = con.execute('SELECT COUNT(*) FROM thyroid_scoring_py_v1').fetchone()[0]
    print(f"  thyroid_scoring_py_v1: {r:,}  [expected 10,871]")
except Exception as e:
    print(f"  Frozen cohort: ERROR - {e}")

# 6. Key molecular/complication counts
print("\n--- MOLECULAR & COMPLICATION COUNTS ---")
try:
    r = con.execute('SELECT COUNT(*) FROM extracted_braf_recovery_v1').fetchone()[0]
    print(f"  extracted_braf_recovery_v1: {r:,}  [expected 546 after FP fix]")
except:
    print("  extracted_braf_recovery_v1: NOT FOUND")

try:
    r = con.execute('SELECT COUNT(*) FROM extracted_ras_patient_summary_v1').fetchone()[0]
    print(f"  extracted_ras_patient_summary_v1: {r:,}  [expected 292]")
except:
    print("  extracted_ras_patient_summary_v1: NOT FOUND")

try:
    r = con.execute("SELECT COUNT(*) FROM patient_refined_master_clinical_v12").fetchone()[0]
    print(f"  patient_refined_master_clinical_v12: {r:,}  [expected 12,886]")
except:
    print("  patient_refined_master_clinical_v12: NOT FOUND")

# 7. Zenodo DOI verification
print("\n--- ZENODO & VERSION TAGS ---")
try:
    import subprocess
    tag = subprocess.run(['git', 'tag', '-l', 'v2026.03.10*'], capture_output=True, text=True).stdout.strip()
    print(f"  Git tags matching v2026.03.10*: {tag or 'NONE'}")
except:
    print("  Git tag check: skipped")

print("\n" + "=" * 70)
print("RECONCILIATION COMPLETE")
print("=" * 70)

con.close()
