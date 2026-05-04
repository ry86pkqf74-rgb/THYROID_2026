"""Run VALIDATE_ALL_COHORTS() + check log + cortex search test."""
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")

print("=== CALL VALIDATE_ALL_COHORTS() ===")
cur.execute("CALL VALIDATE_ALL_COHORTS()")
n = 0
for r in cur.fetchall():
    n += 1
    sym = '✓' if r[3]=='PASS' else '✗'
    print(f"  {sym} {r[0]:30s} expected={r[1]:>8s} observed={r[2]:>8s} [{r[3]}]")
print(f"  ({n} checks)")

print("\n=== Last 15 rows of VALIDATION_RUN_LOG_v1 ===")
cur.execute("SELECT RUN_ID, RUN_TS, CHECK_NAME, EXPECTED, OBSERVED, STATUS FROM VALIDATION_RUN_LOG_V1 ORDER BY RUN_ID DESC LIMIT 15")
for r in cur.fetchall():
    sym = '✓' if r[5]=='PASS' else '✗'
    print(f"  {sym} run={r[0]:>3} {r[2]:30s} exp={r[3]:>8s} obs={r[4]:>8s}")

print("\n=== COHORT_SUMMARY_DASHBOARD ===")
cur.execute("SELECT * FROM COHORT_SUMMARY_DASHBOARD ORDER BY MANUSCRIPT")
for r in cur.fetchall():
    print(f"  {r[0]:25s} cohort={r[1]:>6} events={(r[2] if r[2] is not None else 'NA'):>6}")

print("\n=== Cortex Search test ===")
try:
    cur.execute("""
SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'THYROID_NOTES_SEARCH',
  '{"query": "lateral neck dissection lymph node", "limit": 3}'
))::VARIANT
""")
    raw = cur.fetchone()[0]
    if isinstance(raw, str): raw = json.loads(raw)
    if isinstance(raw, dict) and 'results' in raw:
        for i, hit in enumerate(raw['results'], 1):
            print(f"  hit {i}: research_id={(hit.get('research_id','?') or '?')[:12]} note_type={(hit.get('note_type','?') or '?')[:30]}")
    else:
        print(f"  response: {str(raw)[:300]}")
except Exception as e:
    print(f"  ⚠ {str(e)[:200]}")

ctx.close()
print("=== DONE ===")
