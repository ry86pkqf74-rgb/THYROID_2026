"""Quick probe of SF cohort flat-view cols."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")

for view in [
    'COHORT_M037_LN_METASTASIS_V1_FLAT',
    'COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT',
    'COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT',
    'COHORT_M044_AJCC_ETE_V1_FLAT',
    'COHORT_M038_MASSIVE_GOITER_V1_FLAT',
]:
    print(f"\n=== {view} ===")
    cur.execute(f"DESC VIEW {view}")
    cols = cur.fetchall()
    print(f"  total cols: {len(cols)}")
    for r in cols[:5]:
        print(f"  {r[0]}: {r[1]}")
    # Look for key cols
    relevant = [r[0] for r in cols if any(k in r[0].upper() for k in ['MALIG','N_STAGE','AJCC','LN','TIRADS','SMOK','RECUR','ETE_GRADE','GLAND_WEIGHT','FAMILY_HX'])]
    print(f"  relevant: {relevant[:15]}")
ctx.close()
