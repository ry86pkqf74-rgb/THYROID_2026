"""Prompt 7: TIRADS / Bethesda diagnostic accuracy validation.

mig_260 / CF-mig260f: TIRADS is taken from CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT
(max_tirads_category_ever), not from CANONICAL_PATIENT_MASTER_FLAT — live CPM retains
only NLP TIRADS columns after mig_265-style cleanup.

Requires parquet export + Snowflake load + 04_build_flat_views so *_FLAT exists.
"""
import sys, time, json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = REPO_ROOT / "snowflake_trial" / "reports" / "07_tirads_bethesda_validation.md"
OUT.parent.mkdir(parents=True, exist_ok=True)

CPM = "CANONICAL_PATIENT_MASTER_FLAT"
CUPM = "CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT"
TIRADS_COL = "max_tirads_category_ever"

ctx, cur = get_cursor()
report = [
    "# Snowflake Cortex Validation — Prompt 7: TIRADS / Bethesda Diagnostic Accuracy\n",
    f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
]

# 0. Discover Bethesda on CPM; confirm cupm flat + TIRADS column exist
cur.execute(
    f"""
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='{CPM}'
  AND (LOWER(COLUMN_NAME) LIKE '%bethesda%')
ORDER BY COLUMN_NAME
"""
)
beth_candidates = [r[0] for r in cur.fetchall()]
print(f"  Bethesda cols on CPM: {beth_candidates[:12]}")

cur.execute(
    f"""
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='{CUPM}'
  AND LOWER(COLUMN_NAME) LIKE '%tirads%'
ORDER BY COLUMN_NAME
"""
)
cupm_tirads = [r[0] for r in cur.fetchall()]
print(f"  TIRADS cols on {CUPM}: {cupm_tirads}")

try:
    tirads_sql = next(c for c in cupm_tirads if c.upper() == TIRADS_COL.upper())
except StopIteration:
    ctx.close()
    raise RuntimeError(
        f"mig_260: expected column {TIRADS_COL} on {CUPM}. "
        "Re-run 01_export_md_to_parquet.py, 02_load_to_snowflake.py, "
        "04_build_flat_views.py."
    ) from None

beth_col = next((c for c in beth_candidates if "BETHESDA_FINAL" in c.upper()), None) or (
    beth_candidates[0] if beth_candidates else None
)
if not beth_col:
    ctx.close()
    raise RuntimeError("No Bethesda column found on CANONICAL_PATIENT_MASTER_FLAT.")

print(f"  Using: TIRADS={CUPM}.{tirads_sql}  BETHESDA={beth_col}")

report.append(
    f"**Sources:** {CPM} (Bethesda / malignancy) + {CUPM} ({tirads_sql})\n\n---\n"
)

join_base = f"""
FROM {CPM} c
JOIN {CUPM} v ON c.research_id = v.research_id
"""

# 1. Bethesda × malignancy (rate of malignancy = ROM)
print("=== Bethesda × ROM ===")
cur.execute(f"""
SELECT c.{beth_col} AS bethesda, COUNT(*) AS n,
       COUNT_IF(c.is_malignant) AS n_malignant,
       ROUND(100.0 * COUNT_IF(c.is_malignant) / COUNT(*), 1) AS rom_pct
FROM {CPM} c
WHERE c.{beth_col} IS NOT NULL
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall()
cols = [c[0] for c in cur.description]
report.append("## Bethesda category × Risk of Malignancy (ROM)\n\n")
report.append("**Expected ranges (Bethesda 2023):** I 5-10%, II 0-3%, III 6-18%, IV 10-40%, V 45-60%, VI 94-96%\n\n")
report.append(md_table(rows, cols) + "\n")
print(md_table(rows, cols))

# 2. TIRADS × malignancy rate (cupm_v2)
print("=== TIRADS × malignancy ===")
cur.execute(f"""
SELECT v.{tirads_sql} AS tirads, COUNT(*) AS n,
       COUNT_IF(c.is_malignant) AS n_malignant,
       ROUND(100.0 * COUNT_IF(c.is_malignant) / COUNT(*), 1) AS rate_pct
{join_base}
WHERE v.{tirads_sql} IS NOT NULL
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall()
cols = [c[0] for c in cur.description]
report.append("## TIRADS × malignancy\n\n")
report.append("**ACR TI-RADS expected:** TR1 ~0%, TR2 <2%, TR3 <5%, TR4 5-20%, TR5 >20%\n\n")
report.append(md_table(rows, cols) + "\n")
print(md_table(rows, cols))

# 3. AI_FILTER: Bethesda-VI but benign
print("=== AI_FILTER: Bethesda VI but benign final ===")
cur.execute(f"""
SELECT c.research_id, c.{beth_col} AS bethesda, c.is_malignant, c.histology_final
FROM {CPM} c
WHERE c.{beth_col} = 6 AND c.is_malignant = FALSE
""")
b6_benign = cur.fetchall()
cols = [c[0] for c in cur.description]
report.append("## Bethesda VI patients with benign final histology\n\n")
report.append(f"**N flagged:** {len(b6_benign)}\n\n")
if b6_benign:
    report.append(md_table(b6_benign, cols, max_rows=15) + "\n")

# 4. AI_FILTER: Bethesda-II but malignant final
cur.execute(f"""
SELECT c.research_id, c.{beth_col} AS bethesda, c.is_malignant, c.histology_final
FROM {CPM} c
WHERE c.{beth_col} = 2 AND c.is_malignant = TRUE
""")
b2_malig = cur.fetchall()
report.append("## Bethesda II patients with malignant final histology\n\n")
report.append(f"**N flagged:** {len(b2_malig)}\n\n")
if b2_malig:
    report.append(md_table(b2_malig, [c[0] for c in cur.description], max_rows=15) + "\n")

# 5. AI_CLASSIFY: TIRADS prediction accuracy on a sample
print("=== AI_CLASSIFY: TIRADS expected vs observed ===")
cur.execute(f"""
SELECT tirads, COUNT(*) AS n,
       ROUND(100.0 * COUNT_IF(is_malignant)/COUNT(*), 1) AS observed_rate,
       AI_CLASSIFY(
         CONCAT('TIRADS category ', tirads, ' has ',
                ROUND(100.0 * COUNT_IF(is_malignant)/COUNT(*),1),
                '% malignancy rate (n=', COUNT(*), ')'),
         ARRAY_CONSTRUCT('Within ACR expected range',
                         'Above ACR expected range (over-malignant)',
                         'Below ACR expected range (under-malignant)',
                         'Insufficient context')
       ) AS verdict
FROM (
  SELECT v.{tirads_sql} AS tirads, c.is_malignant AS is_malignant
  {join_base}
  WHERE v.{tirads_sql} IS NOT NULL
) x
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
report.append(md_table(parsed, ["tirads", "n", "observed_rate_pct", "ai_verdict"]) + "\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
