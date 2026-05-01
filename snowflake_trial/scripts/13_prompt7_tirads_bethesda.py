"""Prompt 7: TIRADS / Bethesda diagnostic accuracy validation."""
import sys, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/07_tirads_bethesda_validation.md")

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 7: TIRADS / Bethesda Diagnostic Accuracy\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**Sources:** CANONICAL_PATIENT_MASTER_FLAT + CANONICAL_FNA_EVENTS_V1_FLAT\n\n---\n"]

# 0. Discover TIRADS / Bethesda columns
cur.execute("""
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='CANONICAL_PATIENT_MASTER_FLAT'
  AND (LOWER(COLUMN_NAME) LIKE '%tirads%' OR LOWER(COLUMN_NAME) LIKE '%bethesda%')
ORDER BY COLUMN_NAME
""")
tb_cols = [r[0] for r in cur.fetchall()]
print(f"  TIRADS/Bethesda cols on CPM: {tb_cols[:15]}")

tirads_col = next((c for c in tb_cols if 'TIRADS_BEST_CATEGORY_V12' in c.upper()), None) \
    or next((c for c in tb_cols if 'TIRADS' in c.upper()), None)
beth_col = next((c for c in tb_cols if 'BETHESDA_FINAL' in c.upper()), None) \
    or next((c for c in tb_cols if 'BETHESDA' in c.upper()), None)
print(f"  Using: TIRADS={tirads_col}  BETHESDA={beth_col}")

# 1. Bethesda × malignancy (rate of malignancy = ROM)
print("=== Bethesda × ROM ===")
cur.execute(f"""
SELECT {beth_col} AS bethesda, COUNT(*) AS n,
       COUNT_IF(IS_MALIGNANT) AS n_malignant,
       ROUND(100.0 * COUNT_IF(IS_MALIGNANT) / COUNT(*), 1) AS rom_pct
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE {beth_col} IS NOT NULL
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## Bethesda category × Risk of Malignancy (ROM)\n\n")
report.append("**Expected ranges (Bethesda 2023):** I 5-10%, II 0-3%, III 6-18%, IV 10-40%, V 45-60%, VI 94-96%\n\n")
report.append(md_table(rows, cols) + "\n")
print(md_table(rows, cols))

# 2. TIRADS × malignancy rate
print("=== TIRADS × malignancy ===")
cur.execute(f"""
SELECT {tirads_col} AS tirads, COUNT(*) AS n,
       COUNT_IF(IS_MALIGNANT) AS n_malignant,
       ROUND(100.0 * COUNT_IF(IS_MALIGNANT) / COUNT(*), 1) AS rate_pct
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE {tirads_col} IS NOT NULL
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## TIRADS × malignancy\n\n")
report.append("**ACR TI-RADS expected:** TR1 ~0%, TR2 <2%, TR3 <5%, TR4 5-20%, TR5 >20%\n\n")
report.append(md_table(rows, cols) + "\n")
print(md_table(rows, cols))

# 3. AI_FILTER: Bethesda-VI but benign
print("=== AI_FILTER: Bethesda VI but benign final ===")
cur.execute(f"""
SELECT RESEARCH_ID, {beth_col} AS bethesda, IS_MALIGNANT, HISTOLOGY_FINAL
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE {beth_col} = 6 AND IS_MALIGNANT = FALSE
""")
b6_benign = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append(f"## Bethesda VI patients with benign final histology\n\n")
report.append(f"**N flagged:** {len(b6_benign)}\n\n")
if b6_benign:
    report.append(md_table(b6_benign, cols, max_rows=15) + "\n")

# 4. AI_FILTER: Bethesda-II but malignant final
cur.execute(f"""
SELECT RESEARCH_ID, {beth_col} AS bethesda, IS_MALIGNANT, HISTOLOGY_FINAL
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE {beth_col} = 2 AND IS_MALIGNANT = TRUE
""")
b2_malig = cur.fetchall()
report.append(f"## Bethesda II patients with malignant final histology\n\n")
report.append(f"**N flagged:** {len(b2_malig)}\n\n")
if b2_malig:
    report.append(md_table(b2_malig, [c[0] for c in cur.description], max_rows=15) + "\n")

# 5. AI_CLASSIFY: TIRADS prediction accuracy on a sample
print("=== AI_CLASSIFY: TIRADS expected vs observed ===")
cur.execute(f"""
SELECT {tirads_col} AS tirads, COUNT(*) AS n,
       ROUND(100.0 * COUNT_IF(IS_MALIGNANT)/COUNT(*), 1) AS observed_rate,
       AI_CLASSIFY(
         CONCAT('TIRADS ', {tirads_col}, ' has ',
                ROUND(100.0 * COUNT_IF(IS_MALIGNANT)/COUNT(*),1),
                '% malignancy rate (n=', COUNT(*), ')'),
         ARRAY_CONSTRUCT('Within ACR expected range',
                         'Above ACR expected range (over-malignant)',
                         'Below ACR expected range (under-malignant)',
                         'Insufficient context')
       ) AS verdict
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE {tirads_col} IS NOT NULL
GROUP BY 1 ORDER BY 1
""")
ai_rows = cur.fetchall()
report.append("## AI_CLASSIFY: TIRADS observed vs ACR expected\n\n")
parsed = []
for r in ai_rows:
    try:
        d = json.loads(r[3])
        label = d.get("labels", ["?"])[0]
    except Exception:
        label = "?"
    parsed.append((r[0], r[1], r[2], label))
report.append(md_table(parsed, ['tirads','n','observed_rate_pct','ai_verdict']) + "\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
