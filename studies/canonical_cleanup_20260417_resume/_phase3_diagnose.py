"""Diagnose why Phase 3 classified everything LIVE."""
import json
from pathlib import Path

p = Path(__file__).resolve().parent / "phase3_object_signals.json"
data = json.loads(p.read_text())

print("=== empty tables ===")
empties = [
    (n, r["row_count"], r["is_referenced_by_view"], r["is_referenced_by_script"])
    for n, r in data.items() if r["is_empty"]
]
print(f"n_empty = {len(empties)}")
for e in empties:
    print(" ", e)

print("\n=== version twins (would-be DELETE/DEPRECATE candidates) ===")
twins = [
    n for n, r in data.items()
    if r["has_version_twin"] and not r["twin_is_higher_version"] is False
]
twins_with_higher = [n for n, r in data.items() if r.get("twin_is_higher_version")]
print(f"n_with_version_twin = {sum(1 for n,r in data.items() if r['has_version_twin'])}")
print(f"n_with_higher_twin (lower-versioned, candidates to retire) = {len(twins_with_higher)}")
for n in twins_with_higher[:30]:
    r = data[n]
    print(
        f"  {n} v{r['my_version']}  twin={r['twin_name']} v{r['twin_version']}  "
        f"identical={r['is_identical_to_twin']}  view_refs={r['n_view_refs']}  "
        f"script_refs={r['n_script_refs']}"
    )

print("\n=== name-token archive candidates ===")
toks = ("backup", "snapshot", "pre_", "prev_")
arch_candidates = [
    n for n in data if any(t in n.lower() for t in toks)
]
print(f"n_archive_name_candidates = {len(arch_candidates)}")
for n in arch_candidates:
    r = data[n]
    print(
        f"  {n}  rows={r['row_count']}  view_refs={r['n_view_refs']}  "
        f"script_refs={r['n_script_refs']}"
    )

print("\n=== top 25 lowest script-ref counts (potential cleanup) ===")
ranked = sorted(data.items(), key=lambda kv: (kv[1]["n_script_refs"], kv[1]["n_view_refs"]))
for n, r in ranked[:25]:
    print(
        f"  {n}  rows={r['row_count']}  py_refs={r['n_script_refs']}  "
        f"view_refs={r['n_view_refs']}  twin={r['twin_name']}"
    )

print("\n=== version twin pairs ===")
from collections import defaultdict
import re
groups = defaultdict(list)
for n, r in data.items():
    stem = r.get("twin_stem")
    if stem:
        groups[stem].append((n, r["my_version"], r["row_count"], r["n_columns"], r["n_view_refs"], r["n_script_refs"]))
for stem, members in sorted(groups.items()):
    if len(members) >= 2:
        print(f"  STEM={stem}")
        for m in sorted(members, key=lambda x: x[1]):
            print(f"    v{m[1]}  rows={m[2]}  cols={m[3]}  view={m[4]}  py={m[5]}  | name={m[0]}")
