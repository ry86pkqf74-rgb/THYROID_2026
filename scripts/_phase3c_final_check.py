#!/usr/bin/env python3
"""Phase 3c: Final verification queries against risk_enriched_mv."""
import os, sys
try:
    import toml
    t = toml.load('.streamlit/secrets.toml')
    os.environ.setdefault('MOTHERDUCK_TOKEN', t['MOTHERDUCK_TOKEN'])
except Exception:
    pass

import duckdb
con = duckdb.connect('md:thyroid_research_2026')

print("--- BRAF/RAS/TERT/RET in risk_enriched_mv (analytic source) ---")
r = con.execute("""
    SELECT 
        SUM(CASE WHEN braf_positive THEN 1 ELSE 0 END) as braf_pos,
        SUM(CASE WHEN ras_positive THEN 1 ELSE 0 END) as ras_pos,
        SUM(CASE WHEN tert_positive THEN 1 ELSE 0 END) as tert_pos,
        SUM(CASE WHEN ret_positive THEN 1 ELSE 0 END) as ret_pos,
        SUM(CASE WHEN event_occurred THEN 1 ELSE 0 END) as events,
        COUNT(*) as total
    FROM risk_enriched_mv
""").fetchone()
print(f"  BRAF+: {r[0]}, RAS+: {r[1]}, TERT+: {r[2]}, RET+: {r[3]}")
print(f"  Events: {r[4]}, Total: {r[5]}")

print("\n--- ETE arm sizes ---")
rows = con.execute("""
    SELECT 
        CASE WHEN ete THEN 'ETE_present' ELSE 'No_ETE' END as arm,
        COUNT(*) as n,
        SUM(CASE WHEN event_occurred THEN 1 ELSE 0 END) as events,
        AVG(time_to_event_days) as mean_tte
    FROM risk_enriched_mv
    GROUP BY ete ORDER BY ete
""").fetchall()
for row in rows:
    print(f"  {row[0]}: n={row[1]:,}, events={row[2]}, mean_tte={row[3]:.0f}d")

print("\n--- Cox PH complete-case check ---")
r2 = con.execute("""
    SELECT COUNT(*) FROM risk_enriched_mv
    WHERE ete IS NOT NULL 
      AND survival_age_at_surgery IS NOT NULL
      AND tumor_size_cm IS NOT NULL
      AND ln_positive IS NOT NULL
      AND overall_stage IS NOT NULL
      AND braf_positive IS NOT NULL
""").fetchone()[0]
print(f"  Complete cases for multivariable Cox: {r2:,}")

print("\n--- BRAF-positive count restricted to analytic cohort ---")
r3 = con.execute("""
    SELECT COUNT(*) FROM risk_enriched_mv
    WHERE braf_positive IS TRUE AND ete IS NOT NULL
""").fetchone()[0]
print(f"  BRAF+ in N=6,630 analytic cohort: {r3}")

print("\n--- Manuscript table1 BRAF check ---")
r4 = con.execute("""
    SELECT 
        SUM(CASE WHEN braf_positive THEN 1 ELSE 0 END) as braf_pos,
        COUNT(*) as total,
        ROUND(100.0 * SUM(CASE WHEN braf_positive THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
    FROM risk_enriched_mv
    WHERE ete IS NOT NULL
""").fetchone()
print(f"  BRAF+: {r4[0]} / {r4[1]} = {r4[2]}%")

con.close()
print("\nDone.")
