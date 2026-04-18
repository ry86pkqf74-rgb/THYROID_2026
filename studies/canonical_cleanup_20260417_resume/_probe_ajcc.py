import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from _md_connect import connect_locked  # type: ignore

con = connect_locked()
rows = con.execute(
    "SELECT column_name FROM information_schema.columns "
    "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
    "AND table_schema='main' AND table_name='canonical_patient_master' "
    "AND column_name ILIKE '%ajcc8_t_stage%' "
    "ORDER BY column_name"
).fetchall()
for r in rows:
    print(r[0])
