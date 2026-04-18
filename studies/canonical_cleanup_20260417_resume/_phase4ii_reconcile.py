"""Reconcile the 75-rid TEM-under-report set against the original
60 F-bucket rids from Phase 4 (ii) classifier."""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
scope = json.loads((HERE / "phase4ii_scope_check.json").read_text())
clas = json.loads((HERE / "phase4ii_classification.json").read_text())

scope_75 = {str(d["research_id"]) for d in scope["detail"]}
F_60 = {r["research_id"] for r in clas if r["bucket"] == "F"}
D_13 = {r["research_id"] for r in clas if r["bucket"] == "D"}
E_7 = {r["research_id"] for r in clas if r["bucket"] == "E"}
all_80 = {r["research_id"] for r in clas}

print(f"scope_75 (TEM under-report)            : n={len(scope_75)}")
print(f"F_60 (original Phase4ii F bucket)      : n={len(F_60)}")
print(f"D_13                                    : n={len(D_13)}")
print(f"E_7                                     : n={len(E_7)}")
print(f"all_80 (invariant violations)           : n={len(all_80)}")
print()
print(f"In scope_75 but NOT in F_60 (new F):   n={len(scope_75 - F_60)}")
print(f"  rids: {sorted(scope_75 - F_60)}")
print(f"    of which were originally D:        n={len(scope_75 & D_13)}")
print(f"    of which were originally E:        n={len(scope_75 & E_7)}")
print(f"    not in invariant-violation set:    n={len(scope_75 - all_80)}")
print()
print(f"In F_60 but NOT in scope_75 (lose F):  n={len(F_60 - scope_75)}")
print(f"  rids: {sorted(F_60 - scope_75)}")
print()
print(f"Invariant-violation rids NOT in scope_75:")
print(f"  n={len(all_80 - scope_75)}")
print(f"  rids: {sorted(all_80 - scope_75)}")
