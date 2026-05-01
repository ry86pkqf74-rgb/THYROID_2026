"""Prompt 2: Molecular testing audit + AI_FILTER for internal contradictions."""
import sys, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/02_molecular_validation.md")
OUT.parent.mkdir(parents=True, exist_ok=True)

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 2: Molecular Testing Audit\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**Source:** CANONICAL_PATIENT_MASTER_FLAT + CANONICAL_MOLECULAR_GENETICS_V2_FLAT\n\n---\n"]

# 1. Molecular testing rates by surgery era
print("=== Molecular testing rate by era ===")
cur.execute("""
SELECT
  CASE
    WHEN EXTRACT(YEAR FROM FIRST_SURGERY_DATE) < 2015 THEN '<2015'
    WHEN EXTRACT(YEAR FROM FIRST_SURGERY_DATE) < 2020 THEN '2015-2019'
    ELSE '2020+'
  END AS era,
  COUNT(*) AS n,
  COUNT_IF(MOLECULAR_TESTED_CONFIRMED) AS n_tested,
  ROUND(100.0 * COUNT_IF(MOLECULAR_TESTED_CONFIRMED) / COUNT(*), 1) AS pct_tested
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE FIRST_SURGERY_DATE IS NOT NULL
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
print(md_table(rows, cols))
report.append("## Molecular testing rate by era\n\n")
report.append(md_table(rows, cols) + "\n")

# 2. Mutation positivity among tested
print("=== BRAF/RAS/TERT positivity ===")
cur.execute("""
SELECT
  COUNT(*) AS n_tested,
  COUNT_IF(BRAF_POSITIVE_FINAL) AS n_braf_pos,
  COUNT_IF(RAS_POSITIVE_FINAL) AS n_ras_pos,
  COUNT_IF(BRAF_POSITIVE_FINAL AND RAS_POSITIVE_FINAL) AS n_both_braf_ras
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE MOLECULAR_TESTED_CONFIRMED
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
print(md_table(rows, cols))
report.append("## Mutation positivity (among tested)\n\n")
report.append(md_table(rows, cols) + "\n")

# 3. Top platforms
print("=== Top molecular platforms ===")
cur.execute("""
SELECT MOL_PLATFORM, COUNT(*) AS n
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE MOLECULAR_TESTED_CONFIRMED
GROUP BY 1 ORDER BY 2 DESC
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
print(md_table(rows, cols))
report.append("## Molecular platforms (among tested)\n\n")
report.append(md_table(rows, cols) + "\n")

# 4. AI_FILTER contradictions: tested=false but mutations found
print("=== AI_FILTER: tested=false but BRAF/RAS positive ===")
cur.execute("""
SELECT
  RESEARCH_ID,
  MOLECULAR_TESTED_CONFIRMED AS tested,
  BRAF_POSITIVE_FINAL AS braf_pos,
  RAS_POSITIVE_FINAL AS ras_pos,
  MOL_PLATFORM
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE MOLECULAR_TESTED_CONFIRMED = FALSE
  AND (BRAF_POSITIVE_FINAL = TRUE OR RAS_POSITIVE_FINAL = TRUE)
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
print(f"Found {len(rows)} contradictions")
report.append(f"## Internal contradictions: tested=FALSE but BRAF or RAS positive\n\n")
report.append(f"**N flagged:** {len(rows)}\n\n")
if rows:
    report.append(md_table(rows, cols, max_rows=20) + "\n")

# 5. AI_CLASSIFY: bucket BRAF+ patients into surgery-era cohorts
print("=== AI_CLASSIFY: era buckets for BRAF+ patients ===")
cur.execute("""
SELECT RESEARCH_ID, BRAF_POSITIVE_FINAL,
       FIRST_SURGERY_DATE,
       AI_CLASSIFY(
         FIRST_SURGERY_DATE::VARCHAR,
         ARRAY_CONSTRUCT('Pre-2015', '2015-2019', '2020-2024', '2025+', 'Unknown')
       ) AS era_bucket
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE BRAF_POSITIVE_FINAL = TRUE
  AND FIRST_SURGERY_DATE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (ORDER BY HASH(RESEARCH_ID)) <= 30
""")
rows = cur.fetchall()
print(f"  {len(rows)} BRAF+ classifications")
report.append("## AI_CLASSIFY: era bucket for sample of BRAF+ patients (n=30)\n\n")
parsed = []
for rid, braf, dt, eb in rows:
    try:
        d = json.loads(eb)
        label = d.get("labels", ["?"])[0]
    except Exception:
        label = "?"
    parsed.append((rid, dt, label))
report.append(md_table(parsed, ["research_id", "surgery_date", "AI_era_bucket"], max_rows=15) + "\n")

# 6. Detail-table cross-validation
print("=== Detail vs CPM concordance ===")
cur.execute("""
WITH detail AS (
  SELECT RESEARCH_ID::VARCHAR AS rid, COUNT(*) AS n_in_detail
  FROM CANONICAL_MOLECULAR_GENETICS_V2_FLAT
  GROUP BY 1
),
master AS (
  SELECT RESEARCH_ID::VARCHAR AS rid, MOLECULAR_TESTED_CONFIRMED AS master_tested
  FROM CANONICAL_PATIENT_MASTER_FLAT
)
SELECT
  COUNT(*) AS n_with_detail,
  COUNT_IF(m.master_tested = TRUE) AS n_master_says_tested,
  COUNT_IF(m.master_tested = FALSE) AS n_master_says_NOT_tested
FROM detail d
LEFT JOIN master m ON d.rid = m.rid
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## Detail-table concordance (molecular_genetics_v2 vs CPM rollup)\n\n")
report.append(md_table(rows, cols) + "\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
