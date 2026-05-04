"""Verify infrastructure: validation log + Cortex Search."""
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")

print("=== VALIDATION_RUN_LOG_v1 contents ===")
cur.execute("SELECT * FROM VALIDATION_RUN_LOG_V1 ORDER BY RUN_ID DESC LIMIT 15")
n_rows = 0
for r in cur.fetchall():
    n_rows += 1
    sym = '✓' if r[5]=='PASS' else '✗'
    print(f"  {sym} run={r[0]:>3}  {r[2]:30s}  exp={r[3]:>8s}  obs={r[4]:>8s}  [{r[5]}]")
print(f"  ({n_rows} rows total)")

print("\n=== COHORT_SUMMARY_DASHBOARD ===")
cur.execute("SELECT * FROM COHORT_SUMMARY_DASHBOARD ORDER BY MANUSCRIPT")
for r in cur.fetchall():
    print(f"  {r[0]:25s}  cohort={r[1]:>6}  events={(r[2] if r[2] is not None else 'NA'):>6}")

print("\n=== Pipeline registry ===")
cur.execute("SELECT COMPONENT, KIND, STATUS FROM COWORK_PIPELINE_REGISTRY_V1 ORDER BY KIND, COMPONENT")
for r in cur.fetchall():
    print(f"  [{r[1]:20s}] {r[0]:55s} {r[2]}")

# Test Cortex Search
print("\n=== Cortex Search test query ===")
try:
    cur.execute("""
SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'THYROID_NOTES_SEARCH',
  '{"query": "lateral neck dissection lymph node positive", "limit": 3}'
))
""")
    raw = cur.fetchone()[0]
    if isinstance(raw, str): raw = json.loads(raw)
    if 'results' in raw:
        for i, hit in enumerate(raw.get('results', []), 1):
            txt = hit.get('note_text','')[:120]
            print(f"  hit {i}: research_id={hit.get('research_id','?')[:12]}  type={hit.get('note_type','?')[:30]}")
            print(f"          {txt!r}")
    else:
        print(f"  raw response keys: {list(raw.keys())}")
except Exception as e:
    print(f"  ⚠ {e}")

ctx.close()
