#!/usr/bin/env python3
"""
Part B / Phase 1: Re-confirm canonical coverage.

Authoritative MAPPING source: scripts/preB_cupm_v2_canonical_backfill.py ::
phase5_coverage_rerun (committed in c7ac6d5). This script imports that function
verbatim — DO NOT inline a competing MAPPING here.

Drift-detection asserts the post-pre-B status counts match exactly:
  mapped_cupm_v2:        30
  retired_redesign:       8
  drop_no_replacement:    6
  gap_other_v2_table:     6
  mapped_category:        2
  mapped_points:          1
  TOTAL:                 53
  n_gap_ABORT:            0   (was 13 before pre-B)

Any deviation aborts Part B.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.preB_cupm_v2_canonical_backfill import phase5_coverage_rerun  # noqa: E402

OUT = REPO / "scripts" / "output"

EXPECTED = {
    "mapped_cupm_v2":      30,
    "retired_redesign":     8,
    "drop_no_replacement":  6,
    "gap_other_v2_table":   6,
    "mapped_category":      2,
    "mapped_points":        1,
}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    print("=== Part B Phase 1: re-confirm canonical coverage ===")
    log = phase5_coverage_rerun()

    counts = log.get("status_counts", {})
    n_gap = log.get("n_gap_ABORT", -1)
    diffs: list[str] = []
    for k, v in EXPECTED.items():
        if counts.get(k) != v:
            diffs.append(f"{k}: expected {v}, got {counts.get(k)}")
    extra = set(counts) - set(EXPECTED)
    for k in extra:
        diffs.append(f"unexpected status code: {k} = {counts[k]}")
    if n_gap != 0:
        diffs.append(f"n_gap_ABORT expected 0, got {n_gap}")

    log["partB_phase1_drift_diffs"] = diffs
    log["partB_phase1_status"] = "OK" if not diffs else "STOP_GATE_DRIFT"

    out_path = OUT / "partB_phase1_coverage.json"
    out_path.write_text(json.dumps(log, indent=2, default=str))
    print(f"Phase 1 report: {out_path.relative_to(REPO)}")

    if diffs:
        for d in diffs:
            print(f"DRIFT: {d}")
        raise SystemExit(
            "Part B Phase 1 STOP gate: coverage status drifted from pre-B QA. "
            "Inspect partB_phase1_coverage.json before continuing."
        )

    total = sum(counts.values())
    print(f"Coverage status counts (total {total} rows):")
    for k in [
        "mapped_cupm_v2",
        "retired_redesign",
        "drop_no_replacement",
        "gap_other_v2_table",
        "mapped_category",
        "mapped_points",
    ]:
        print(f"  {k:25s} {counts.get(k, 0)}")
    print(f"  n_gap_ABORT: {n_gap}")
    print("Part B Phase 1 OK — proceed to Phase 2.")


if __name__ == "__main__":
    main()
