"""Prompt 12: Synoptic pathology coverage + label drift (Snowflake mirror).

Requires PATH_SYNOPTICS_FLAT loaded into Snowflake (not in current 9-table set;
add to TABLES list in 01_export_md_to_parquet.py — 11,688 rows × 582 cols).
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/12_synoptic_validation.md")

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 12: Synoptic Pathology (Snowflake re-run)\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n---\n"]

# 1. Structure
cur.execute("""
SELECT
  COUNT(*) AS n_total,
  COUNT(DISTINCT research_id) AS n_pts,
  MIN(surg_date) AS min_dt, MAX(surg_date) AS max_dt,
  COUNT_IF(surg_date IS NULL) AS n_null_dt
FROM PATH_SYNOPTICS_FLAT
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## Structure\n\n" + md_table(rows, cols) + "\n")

# 2. Histology distribution (raw — drift visible)
cur.execute("""
SELECT tumor_1_histologic_type AS val, COUNT(*) AS n
FROM PATH_SYNOPTICS_FLAT
WHERE tumor_1_histologic_type IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 20
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## Histology distribution (top 20, raw — drift visible)\n\n" + md_table(rows, cols) + "\n")

# 3. ETE label distribution
cur.execute("""
SELECT tumor_1_extrathyroidal_extension AS val, COUNT(*) AS n
FROM PATH_SYNOPTICS_FLAT
WHERE tumor_1_extrathyroidal_extension IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 20
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## ETE label distribution\n\n" + md_table(rows, cols) + "\n")

# 4. LVI label distribution
cur.execute("""
SELECT tumor_1_lymphatic_invasion AS val, COUNT(*) AS n
FROM PATH_SYNOPTICS_FLAT
WHERE tumor_1_lymphatic_invasion IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 20
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## LVI label distribution\n\n" + md_table(rows, cols) + "\n")

# 5. Multi-tumor coverage
cur.execute("""
SELECT
  COUNT_IF(tumor_1_size_greatest_dimension_cm IS NOT NULL) AS tumor1_n,
  COUNT_IF(tumor_2_size_greatest_dimension_cm IS NOT NULL) AS tumor2_n,
  COUNT_IF(tumor_3_size_greatest_dimension_cm IS NOT NULL) AS tumor3_n,
  COUNT_IF(tumor_4_size_greatest_dimension_cm IS NOT NULL) AS tumor4_n,
  COUNT_IF(tumor_5_size_greatest_dimension_cm IS NOT NULL) AS tumor5_n,
  COUNT_IF(tumor_focality IS NOT NULL) AS focality_n
FROM PATH_SYNOPTICS_FLAT
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## Multi-tumor coverage\n\n" + md_table(rows, cols) + "\n")

# 6. LVI cross-validation vs canonical
cur.execute("""
WITH syn_lvi AS (
  SELECT DISTINCT research_id FROM PATH_SYNOPTICS_FLAT
  WHERE LOWER(TRIM(tumor_1_lymphatic_invasion)) IN ('present','extensive','focal','suspicious')
),
canon_lym AS (
  SELECT DISTINCT research_id FROM CANONICAL_INVASION_EVENTS_V1_FLAT
  WHERE invasion_type='lymphatic_microscopic' AND finding_status='present'
)
SELECT
  (SELECT COUNT(*) FROM syn_lvi) AS syn_n,
  (SELECT COUNT(*) FROM canon_lym) AS canon_n,
  (SELECT COUNT(*) FROM syn_lvi s JOIN canon_lym c USING(research_id)) AS overlap,
  (SELECT COUNT(*) FROM syn_lvi s LEFT JOIN canon_lym c USING(research_id) WHERE c.research_id IS NULL) AS syn_only,
  (SELECT COUNT(*) FROM canon_lym c LEFT JOIN syn_lvi s USING(research_id) WHERE s.research_id IS NULL) AS canon_only
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## Synoptic-LVI vs canonical_invasion lymphatic_microscopic\n\n" + md_table(rows, cols) + "\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
