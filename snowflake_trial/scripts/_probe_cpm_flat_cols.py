"""Probe CPM_FLAT for smoking + family hx cols."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")
cur.execute("DESC VIEW CANONICAL_PATIENT_MASTER_FLAT")
cols = [r[0] for r in cur.fetchall()]
print(f"Total cols: {len(cols)}")
matches = [c for c in cols if 'SMOK' in c.upper() or 'FAMILY_HX' in c.upper() or 'TIRADS' in c.upper()]
print(f"Smoke/family/tirads matches: {matches}")

# Get raw variant keys
cur.execute("SELECT $1 FROM CANONICAL_PATIENT_MASTER LIMIT 1")
import json
r = cur.fetchone()[0]
data = r if isinstance(r, dict) else json.loads(r)
print(f"\nVARIANT keys count: {len(data.keys())}")
key_matches = [k for k in data.keys() if 'smok' in k.lower() or 'family_hx' in k.lower() or 'tirads' in k.lower()]
print(f"VARIANT smoke/family/tirads keys: {key_matches}")
ctx.close()
