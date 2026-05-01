"""Prompt 6: ETE / Vascular invasion validation."""
import sys, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/06_invasion_validation.md")

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 6: ETE / Vascular Invasion\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**Sources:** CANONICAL_PATIENT_MASTER_FLAT + CANONICAL_INVASION_EVENTS_V1_FLAT\n\n---\n"]

# 1. ETE grade distribution at patient level
print("=== ETE grade distribution ===")
cur.execute("""
SELECT ETE_GRADE, COUNT(*) AS n,
       COUNT_IF(IS_MALIGNANT) AS n_malignant
FROM CANONICAL_PATIENT_MASTER_FLAT
GROUP BY 1 ORDER BY n DESC
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## ETE grade × malignancy\n\n")
report.append(md_table(rows, cols) + "\n")
print(md_table(rows, cols))

# 2. Invasion-events table characterization
print("=== Invasion event types ===")
cur.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='CANONICAL_INVASION_EVENTS_V1_FLAT'
ORDER BY ORDINAL_POSITION
""")
inv_cols = [r[0] for r in cur.fetchall()]
print(f"  Invasion event cols: {inv_cols}")
report.append(f"## Invasion event schema\n\n{len(inv_cols)} columns: " + ", ".join(f"`{c}`" for c in inv_cols[:20]) + "\n\n")

# 3. Find the type/category column and tally
type_col = next((c for c in inv_cols if 'TYPE' in c.upper() or 'CATEGORY' in c.upper() or 'INVASION_TYPE' in c.upper()), None)
if type_col:
    cur.execute(f"""
    SELECT {type_col} AS invasion_type, COUNT(*) AS n,
           COUNT(DISTINCT RESEARCH_ID) AS n_pts
    FROM CANONICAL_INVASION_EVENTS_V1_FLAT
    GROUP BY 1 ORDER BY n DESC LIMIT 25
    """)
    rows = cur.fetchall(); cols = [c[0] for c in cur.description]
    report.append(f"## Invasion event types (`{type_col}`)\n\n")
    report.append(md_table(rows, cols) + "\n")

# 4. Cross-validation: ETE_GRADE vs invasion events
print("=== ETE concordance: CPM ete_grade vs canonical_invasion_events_v1 ===")
cur.execute("""
WITH inv_pts AS (
  SELECT DISTINCT RESEARCH_ID::VARCHAR AS rid FROM CANONICAL_INVASION_EVENTS_V1_FLAT
)
SELECT
  cpm.ETE_GRADE,
  COUNT(*) AS n_pts,
  COUNT_IF(i.rid IS NOT NULL) AS n_with_invasion_event,
  ROUND(100.0 * COUNT_IF(i.rid IS NOT NULL)/COUNT(*), 1) AS pct_with_event
FROM CANONICAL_PATIENT_MASTER_FLAT cpm
LEFT JOIN inv_pts i ON cpm.RESEARCH_ID::VARCHAR = i.rid
WHERE cpm.IS_MALIGNANT
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## ETE-grade vs invasion-event concordance\n\n")
report.append(md_table(rows, cols) + "\n")

# 5. AI_CLASSIFY: anomalous T-stage × ETE combinations
print("=== AI_CLASSIFY: implausible T × ETE ===")
cur.execute("""
SELECT RESEARCH_ID, AJCC8_T_STAGE, ETE_GRADE, TUMOR_SIZE_CM_MAX,
  AI_CLASSIFY(
    CONCAT('T-stage: ', COALESCE(AJCC8_T_STAGE,'NULL'),
           '; ETE grade: ', COALESCE(ETE_GRADE,'NULL'),
           '; Tumor size: ', COALESCE(TUMOR_SIZE_CM_MAX::VARCHAR,'NULL'), ' cm'),
    ARRAY_CONSTRUCT('Consistent','T-low + gross-ETE inconsistent','Size-stage mismatch','Insufficient data')
  ) AS verdict
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT AND AJCC8_T_STAGE IS NOT NULL AND ETE_GRADE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (ORDER BY HASH(RESEARCH_ID)) <= 50
""")
ai_rows = cur.fetchall()
counts = {}
flagged = []
for r in ai_rows:
    try:
        d = json.loads(r[4])
        label = d.get("labels", ["Other"])[0]
    except Exception:
        label = "Other"
    counts[label] = counts.get(label, 0) + 1
    if label != "Consistent":
        flagged.append((r[0], r[1], r[2], r[3], label))
report.append("## AI_CLASSIFY: T-stage × ETE consistency (50-pt sample)\n\n")
for k, v in counts.items():
    report.append(f"- **{k}:** {v}\n")
report.append("\n")
if flagged:
    report.append("### Flagged cases\n\n")
    report.append(md_table(flagged, ['rid','t_stage','ete','size_cm','verdict'], max_rows=20) + "\n")
print(f"  AI grades: {counts}")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
