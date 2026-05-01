"""Re-run Prompt 11 PMH probes to verify mig_265 _definitive rule fix."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/11_comorbidity_revalidation_post_mig265.md")
ctx, cur = get_cursor()

# 9 conditions that had _definitive=0 across all rows pre-mig_265
TARGET_CONDS = [
    'autoimmune_thyroid_hx', 'radiation_exposure', 'osteoporosis',
    'family_hx_thyroid', 'family_hx_cancer', 'coagulopathy',
    'men_syndrome', 'smoking_current', 'smoking_never', 'smoking_former',
]

# Discover which CPM cols correspond
print("=== Discovering pmhx_*_definitive cols ===")
cur.execute("""
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='CANONICAL_PATIENT_MASTER_FLAT'
  AND LOWER(COLUMN_NAME) LIKE 'pmhx%definitive%'
ORDER BY 1
""")
def_cols = [r[0] for r in cur.fetchall()]
print(f"  Found {len(def_cols)} definitive cols")
for c in def_cols[:20]:
    print(f"    {c}")

print("\n=== mig_265 _definitive rebuild verification ===")
report = ["# Prompt 11 Re-validation — Post mig_265\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**mig_265 change:** `_definitive` cols now require evidence_strength IN ('definitive','probable') instead of = 'definitive' only.\n\n"]

# Check the 9 affected
report.append("## Per-condition any_evidence vs definitive (post-mig_265)\n\n")
report.append("| Condition | any_n | definitive_n |\n| --- | --- | --- |\n")
for cond in TARGET_CONDS:
    any_col = f"PMHX_NLP_{cond.upper()}_ANY_EVIDENCE"
    def_col = f"PMHX_NLP_{cond.upper()}_DEFINITIVE"
    if any_col not in [c.upper() for c in def_cols] and def_col not in def_cols:
        # Try alternate naming
        cur.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='CANONICAL_PATIENT_MASTER_FLAT'
          AND LOWER(COLUMN_NAME) LIKE '%{cond.lower()}%'
        """)
        matches = [r[0] for r in cur.fetchall()]
        if not matches:
            report.append(f"| {cond} | (col not found) | — |\n")
            continue
    try:
        cur.execute(f"""
        SELECT
          COUNT_IF({any_col} > 0) AS any_n,
          COUNT_IF({def_col} > 0) AS def_n
        FROM CANONICAL_PATIENT_MASTER_FLAT
        """)
        any_n, def_n = cur.fetchone()
        report.append(f"| {cond} | {any_n} | {def_n} |\n")
        print(f"  {cond}: any={any_n} def={def_n}")
    except Exception as e:
        report.append(f"| {cond} | (probe error) | — |\n")
        print(f"  {cond}: probe failed — {str(e)[:80]}")

report.append("\n## Expected vs observed\n\n")
report.append("Per Logan's mig_265 report, post-fix counts should be:\n\n")
report.append("- autoimmune_thyroid_hx: 78 / 78\n")
report.append("- radiation_exposure: 33 / 33\n")
report.append("- osteoporosis: 23 / 23\n")
report.append("- family_hx_thyroid: 30 / 30\n")
report.append("- family_hx_cancer: 16 / 16\n")
report.append("- coagulopathy: 13 / 13\n")
report.append("- men_syndrome: 6 / 6\n\n")
report.append("If observed = expected, mig_265 round-tripped to Snowflake successfully.\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
