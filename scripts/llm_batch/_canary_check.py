#!/usr/bin/env python3
"""Quick canary check for a _results.jsonl file.

Usage:  python3 _canary_check.py <path_to_results.jsonl>
"""
import json
import sys
import collections


def main() -> None:
    path = sys.argv[1]
    recs = [json.loads(l) for l in open(path)]
    n = len(recs)
    n_err = sum(r["error"] for r in recs)
    print(f"n={n} errors={n_err}")

    keys = collections.Counter()
    grades = collections.Counter()
    empty = 0
    for r in recs:
        pj = r.get("parsed_json") or {}
        if not pj:
            empty += 1
            continue
        for k in pj.keys():
            keys[k] += 1
        for k in ("ete_grade", "t_stage_component", "vi_present", "airway_invasion", "parathyroid_autotransplant"):
            if k in pj:
                grades[f"{k}={pj[k]}"] += 1

    print(f"n_empty_parsed_json={empty}")
    print("top parsed_json keys:", keys.most_common(10))
    print("top field values:", grades.most_common(15))
    print()
    print("--- first 5 parsed records ---")
    for r in recs[:5]:
        print(r["research_id"], r["note_type"], "|", r.get("parsed_json"))
    if empty:
        print()
        print("--- 3 empty parsed_json records (content fell through) ---")
        for r in [x for x in recs if not x.get("parsed_json")][:3]:
            raw = (r.get("raw_llm_response") or "")[:200]
            print(r["research_id"], r["note_type"], "| raw:", raw.replace("\n", " "))


if __name__ == "__main__":
    main()
