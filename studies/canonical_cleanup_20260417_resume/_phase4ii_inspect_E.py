"""Inspect the E (unresolvable) cases to understand the data shape."""
import json
from pathlib import Path

p = Path(__file__).resolve().parent / "phase4ii_classification.json"
data = json.loads(p.read_text())
es = [r for r in data if r["bucket"] == "E"]
ds = [r for r in data if r["bucket"] == "D"]

print(f"Total: A=0 B=0 C=0 D={len(ds)} E={len(es)}")
print()

# Cross-tab of E cases by delta band
from collections import Counter
bd = Counter(r["delta_band"] for r in es)
print(f"E cases by delta band: {dict(bd)}")
print()

print("=== Top 13 extreme E cases (descending Δ) ===\n")
for r in sorted(es, key=lambda x: x["delta_cm"], reverse=True)[:13]:
    print(f"rid {r['research_id']}: path={r['path_tumor_size_cm']}  max={r['tumor_size_cm_max']}  Δ={r['delta_cm']}")
    print(f"  observed_max_tumor_focus={r['observed_max_tumor_focus']}")
    print(f"  n_tumor_focus={r['n_tumor_focus_values']}  n_anatomic={r['n_anatomic_values']}")
    if r["tumor_focus_dump"]:
        focus = json.loads(r["tumor_focus_dump"])
        for f in focus[:8]:
            print(f"    focus: v={f['v']} feeder={f['feeder']} label={f['label']}")
    if r["anatomic_pool"]:
        anat = json.loads(r["anatomic_pool"])
        for a in anat[:8]:
            print(f"    anat:  v={a['v']} label={a['label']}")
    print()

print("=== Sample 5 moderate E cases ===\n")
mod = [r for r in es if r["delta_band"] == "moderate(1<Δ≤5)"]
for r in sorted(mod, key=lambda x: x["delta_cm"], reverse=True)[:5]:
    print(f"rid {r['research_id']}: path={r['path_tumor_size_cm']}  max={r['tumor_size_cm_max']}  Δ={r['delta_cm']}")
    print(f"  observed_max_tumor_focus={r['observed_max_tumor_focus']}")
    if r["tumor_focus_dump"]:
        focus = json.loads(r["tumor_focus_dump"])
        for f in focus[:8]:
            print(f"    focus: v={f['v']} feeder={f['feeder']} label={f['label']}")
    if r["anatomic_pool"]:
        anat = json.loads(r["anatomic_pool"])
        for a in anat[:8]:
            print(f"    anat:  v={a['v']} label={a['label']}")
    print()

print("=== Sample 5 small E cases ===\n")
sm = [r for r in es if r["delta_band"] == "small(≤1)"]
for r in sm[:5]:
    print(f"rid {r['research_id']}: path={r['path_tumor_size_cm']}  max={r['tumor_size_cm_max']}  Δ={r['delta_cm']}")
    print(f"  observed_max_tumor_focus={r['observed_max_tumor_focus']}")
    if r["tumor_focus_dump"]:
        focus = json.loads(r["tumor_focus_dump"])
        for f in focus[:8]:
            print(f"    focus: v={f['v']} feeder={f['feeder']} label={f['label']}")
    print()
