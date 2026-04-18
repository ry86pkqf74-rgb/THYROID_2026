"""Dump full path_tumor_size_invariant_v1 violations for Logan review."""
import csv
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore

con = connect_locked()
rows = con.execute(
    """
    SELECT
      research_id,
      path_tumor_size_cm,
      tumor_size_cm_max,
      ROUND(path_tumor_size_cm - tumor_size_cm_max, 2) AS delta_cm
    FROM manuscript_workspace.path_tumor_size_invariant_v1
    ORDER BY delta_cm DESC
    """
).fetchall()
keys = [d[0] for d in con.description]

out = HERE / "phase4_invariant_violations.csv"
with out.open("w", newline="") as fp:
    w = csv.writer(fp)
    w.writerow(keys)
    for r in rows:
        w.writerow(r)

# Aggregates
n = len(rows)
extreme = sum(1 for r in rows if (r[3] or 0) > 5)
moderate = sum(1 for r in rows if 1 < (r[3] or 0) <= 5)
small = sum(1 for r in rows if (r[3] or 0) <= 1)
print(f"n_rows = {n}")
print(f"  delta > 5.0 cm (extreme): {extreme}")
print(f"  1.0 < delta <= 5.0 cm (moderate): {moderate}")
print(f"  delta <= 1.0 cm (small): {small}")
print(f"\nTop 15 by delta:")
print(f"  {keys}")
for r in rows[:15]:
    print(f"  {r}")
print(f"\nCSV written to {out}")
