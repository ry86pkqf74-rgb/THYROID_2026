"""Prompt 8: Complications patterns + AI_AGG."""
import sys, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/08_complications_validation.md")

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 8: Complications Patterns\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**Source:** CANONICAL_COMPLICATIONS_EVENTS_V1_FLAT (5,050 events)\n\n---\n"]

# 0. Schema discovery
cur.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='CANONICAL_COMPLICATIONS_EVENTS_V1_FLAT'
ORDER BY ORDINAL_POSITION
""")
comp_cols = [r[0] for r in cur.fetchall()]
print(f"  Complication event cols: {comp_cols}")

type_col = next((c for c in comp_cols if c.upper() == 'COMPLICATION_TYPE' or 'COMP_TYPE' in c.upper() or 'TYPE' in c.upper() and 'OUTPUT' not in c.upper()), 'COMPLICATION_TYPE')
status_col = next((c for c in comp_cols if 'FINDING_STATUS' in c.upper() or 'STATUS' in c.upper()), 'FINDING_STATUS')
strength_col = next((c for c in comp_cols if 'EVIDENCE_STRENGTH' in c.upper() or 'STRENGTH' in c.upper()), 'EVIDENCE_STRENGTH')
print(f"  Using: type={type_col}  status={status_col}  strength={strength_col}")
report.append(f"## Schema\n\nUsing columns: `{type_col}`, `{status_col}`, `{strength_col}`\n\n")

# 1. Distribution
print("=== Complication type × finding status × evidence ===")
cur.execute(f"""
SELECT
  LOWER({type_col}) AS comp_type,
  {status_col} AS status,
  {strength_col} AS strength,
  COUNT(*) AS n,
  COUNT(DISTINCT RESEARCH_ID) AS n_pts
FROM CANONICAL_COMPLICATIONS_EVENTS_V1_FLAT
GROUP BY 1, 2, 3
ORDER BY 1, n DESC
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## Complication type × finding_status × evidence_strength\n\n")
report.append(md_table(rows, cols, max_rows=40) + "\n")

# 2. Patient-level summary
print("=== Patient-level: any complication confirmed ===")
cur.execute(f"""
SELECT
  LOWER({type_col}) AS comp_type,
  COUNT(DISTINCT RESEARCH_ID) AS n_pts_with_any_event,
  COUNT(DISTINCT CASE WHEN {status_col} = 'present' THEN RESEARCH_ID END) AS n_pts_with_present,
  COUNT(DISTINCT CASE WHEN {status_col} = 'present' AND {strength_col} IN ('definitive','probable') THEN RESEARCH_ID END) AS n_strict_confirmed
FROM CANONICAL_COMPLICATIONS_EVENTS_V1_FLAT
GROUP BY 1 ORDER BY n_pts_with_any_event DESC
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## Patient-level: any-event vs strict-confirmed by complication type\n\n")
report.append(md_table(rows, cols) + "\n")
print(md_table(rows, cols))

# 3. AI_AGG: theme summary per complication type (sample)
print("=== AI_AGG: theme summary per type (top 5 types) ===")
text_col = next((c for c in comp_cols if 'EVIDENCE' in c.upper() and 'TEXT' in c.upper()), None) \
    or next((c for c in comp_cols if 'NOTE' in c.upper() or 'DESCRIPTION' in c.upper()), None) \
    or next((c for c in comp_cols if 'TEXT' in c.upper()), None)
print(f"  Text col for AI_AGG: {text_col}")
if text_col:
    cur.execute(f"""
    SELECT comp_type, n_events, theme
    FROM (
      SELECT
        LOWER({type_col}) AS comp_type,
        COUNT(*) AS n_events,
        AI_AGG(
          {text_col},
          'Summarize the most common patterns in these complication descriptions in <=3 sentences. Focus on (1) typical presentation and (2) timing relative to surgery.'
        ) AS theme
      FROM CANONICAL_COMPLICATIONS_EVENTS_V1_FLAT
      WHERE {text_col} IS NOT NULL
      GROUP BY 1
      ORDER BY n_events DESC
    )
    LIMIT 5
    """)
    rows = cur.fetchall(); cols = [c[0] for c in cur.description]
    report.append("## AI_AGG: theme summary by complication type (top 5)\n\n")
    for r in rows:
        report.append(f"### {r[0]} (n={r[1]:,} events)\n\n")
        theme = (r[2] or '').strip()
        report.append(theme + "\n\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
