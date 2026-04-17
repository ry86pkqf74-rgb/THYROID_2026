"""Canonical cleanup 20260417 — Phase 4.6 RENAME ONLY.

Run only after script 274 reports 'PRE-GATE passed (0 bare refs)' AND Logan
gives explicit 'execute rename' approval. Re-runs the PRE-GATE before renaming
as a safety belt.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO))

# Re-use functions from script 274
import importlib.util
spec = importlib.util.spec_from_file_location(
    "_p4", REPO / "scripts" / "274_canonical_cleanup_phase4.py"
)
mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
spec.loader.exec_module(mod)  # type: ignore[union-attr]

from _md_connect import connect_locked  # type: ignore


def main() -> int:
    con = connect_locked()
    mod.assert_invariants(con)
    n_bare = mod.phase_4_6_pre_gate(con)
    if n_bare > 0:
        mod.stop(f"Re-check failed: {n_bare} bare refs; do not rename")
    mod.phase_4_6_rename(con)
    mod.assert_invariants(con)
    print("Phase 4.6 rename done; invariants OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
