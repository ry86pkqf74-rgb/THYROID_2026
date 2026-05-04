"""Quick probe: what's in COHORT_M037 VARIANT $1 col."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")
cur.execute('SELECT $1 FROM COHORT_M037_LN_METASTASIS_V1 LIMIT 1')
r = cur.fetchone()
import json
data = r[0] if isinstance(r[0], dict) else json.loads(r[0])
print("Keys:", list(data.keys())[:20])
print("Sample:", {k: v for k, v in list(data.items())[:5]})
ctx.close()
