#!/usr/bin/env python3
"""Phase 3b: Follow-up reconciliation queries."""
import os, sys
try:
    import toml
    t = toml.load('.streamlit/secrets.toml')
    os.environ.setdefault('MOTHERDUCK_TOKEN', t['MOTHERDUCK_TOKEN'])
except Exception:
    pass

import duckdb
con = duckdb.connect('md:thyroid_research_2026')

print("=" * 70)
print("PHASE 3b: FOLLOW-UP QUERIES")
print("=" * 70)

# 1. ETE column in risk_enriched_mv
print("\n--- ETE DISTRIBUTION (risk_enriched_mv.ete) ---")
rows = con.execute("""
    SELECT ete, COUNT(*) as n 
    FROM risk_enriched_mv 
    GROUP BY ete 
    ORDER BY n DESC
""").fetchall()
for r in rows:
    print(f"  '{r[0]}': {r[1]:,}")

# 2. Check gross_ete column
print("\n--- GROSS_ETE DISTRIBUTION ---")
rows = con.execute("""
    SELECT gross_ete, COUNT(*) as n 
    FROM risk_enriched_mv 
    GROUP BY gross_ete 
    ORDER BY n DESC
""").fetchall()
for r in rows:
    print(f"  '{r[0]}': {r[1]:,}")

# 3. Where does 11,673 come from?
print("\n--- SOURCING THE 11,673 CLAIM ---")
for tbl in ['demographics_harmonized_v2', 'patient_level_summary_mv', 'benign_pathology']:
    try:
        r = con.execute(f'SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {tbl}').fetchone()
        print(f"  {tbl}: rows={r[0]:,}, distinct_patients={r[1]:,}")
    except Exception as e:
        print(f"  {tbl}: ERROR - {e}")

# Also check operative_details
try:
    r = con.execute('SELECT COUNT(DISTINCT research_id) FROM operative_details').fetchone()[0]
    print(f"  operative_details distinct patients: {r:,}")
except Exception as e:
    print(f"  operative_details: ERROR - {e}")

# 4. BRAF recovery breakdown
print("\n--- BRAF RECOVERY BREAKDOWN ---")
try:
    cols = [c[0] for c in con.execute("SELECT * FROM extracted_braf_recovery_v1 LIMIT 0").description]
    print(f"  Columns: {cols}")
    
    # Check if there's a status/positive column
    for col in ['braf_status', 'detection_method', 'braf_recovered_status', 'concordance_status']:
        if col in cols:
            rows = con.execute(f"""
                SELECT {col}, COUNT(*) as n 
                FROM extracted_braf_recovery_v1 
                GROUP BY {col} 
                ORDER BY n DESC
            """).fetchall()
            print(f"  {col} distribution:")
            for r in rows:
                print(f"    '{r[0]}': {r[1]:,}")
except Exception as e:
    print(f"  BRAF detail: ERROR - {e}")

# 5. RAS patient summary breakdown
print("\n--- RAS PATIENT SUMMARY ---")
try:
    cols = [c[0] for c in con.execute("SELECT * FROM extracted_ras_patient_summary_v1 LIMIT 0").description]
    print(f"  Columns: {cols}")
    r = con.execute("SELECT COUNT(*) FROM extracted_ras_patient_summary_v1").fetchone()[0]
    print(f"  Total rows: {r}")
    
    for col in ['ras_positive', 'ras_positive_final', 'primary_subtype']:
        if col in cols:
            rows = con.execute(f"SELECT {col}, COUNT(*) FROM extracted_ras_patient_summary_v1 GROUP BY {col} ORDER BY 2 DESC LIMIT 10").fetchall()
            print(f"  {col}:")
            for row in rows:
                print(f"    '{row[0]}': {row[1]}")
except Exception as e:
    print(f"  RAS detail: ERROR - {e}")

# 6. Survival cohort — what does manuscript claim?
print("\n--- SURVIVAL COHORT DEEP DIVE ---")
try:
    r = con.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN event_occurred THEN 1 ELSE 0 END) as events,
            AVG(time_to_event_days) as avg_tte,
            MEDIAN(time_to_event_days) as med_tte,
            MAX(time_to_event_days) as max_tte
        FROM survival_cohort_ready_mv
    """).fetchone()
    print(f"  Total: {r[0]:,}, Events: {r[1]}, Avg TTE: {r[2]:.0f}d, Median: {r[3]:.0f}d, Max: {r[4]:.0f}d")
except Exception as e:
    print(f"  Survival detail: ERROR - {e}")

# 7. survival_cohort_enriched column overview and event count
print("\n--- SURVIVAL_COHORT_ENRICHED ---")
try:
    cols = [c[0] for c in con.execute("SELECT * FROM survival_cohort_enriched LIMIT 0").description]
    print(f"  Columns ({len(cols)}): {cols[:15]}...")
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN event THEN 1 ELSE 0 END) as events
        FROM survival_cohort_enriched
    """).fetchone()
    print(f"  Rows: {r[0]:,}, Distinct patients: {r[1]:,}, Events: {r[2]}")
except Exception as e:
    print(f"  ERROR: {e}")

# 8. What does script 31 actually read? Check the analytic N=6,630 derivation
print("\n--- RISK_ENRICHED_MV FULL COLUMN LIST ---")
cols = [c[0] for c in con.execute("SELECT * FROM risk_enriched_mv LIMIT 0").description]
print(f"  All columns ({len(cols)}): {cols}")

# 9. Check advanced_features_v3 distinct patients
print("\n--- ADVANCED_FEATURES_V3 ---")
r = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM advanced_features_v3").fetchone()
print(f"  Rows: {r[0]:,}, Distinct patients: {r[1]:,}")

# 10. How does the manuscript derive N=6,630? 
# It filters risk_enriched_mv for patients with non-null ETE info
print("\n--- N=6,630 DERIVATION CHECK ---")
r = con.execute("""
    SELECT COUNT(*) FROM risk_enriched_mv 
    WHERE ete IS NOT NULL
""").fetchone()[0]
print(f"  risk_enriched_mv WHERE ete IS NOT NULL: {r:,}")

r = con.execute("""
    SELECT COUNT(*) FROM risk_enriched_mv 
    WHERE ete IS NOT NULL AND TRIM(ete) != ''
""").fetchone()[0]
print(f"  risk_enriched_mv WHERE ete IS NOT NULL and non-blank: {r:,}")

print("\n" + "=" * 70)
print("PHASE 3b COMPLETE")
print("=" * 70)
con.close()
