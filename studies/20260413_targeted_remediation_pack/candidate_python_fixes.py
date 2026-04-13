#!/usr/bin/env python3
"""
Candidate remediation driver — uses motherduck_client.get_token() (motherduck.local.toml).

Does NOT auto-mutate production data. Safe actions: deploy views, re-run linkage builder, re-run audits.

Citable gap rows: studies/20260413_source_truth_completeness_audit/
  linkage_gap_worklist_unresolved_20260413_174900.csv lines 2-129
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Targeted remediation helper (MotherDuck token via TOML).")
    p.add_argument("--dry-run", action="store_true", help="Print steps only.")
    p.add_argument(
        "--phase",
        choices=("token-check", "views", "linkage", "audits"),
        default="token-check",
        help="Which remediation phase to run.",
    )
    args = p.parse_args()

    tok = get_token()
    print("token_mode:", token_mode())
    print("token:", "SET" if tok else "MISSING", f"len={len(tok) if tok else 0}")

    if args.phase == "token-check":
        return 0 if tok else 1

    py = ROOT / ".venv" / "bin" / "python"
    if not py.is_file():
        py = Path(sys.executable)

    scripts: list[tuple[str, list[str]]] = []
    if args.phase == "views":
        scripts.append((str(py), ["scripts/151_source_truth_confirmation_v1.py", "--md"]))
    elif args.phase == "linkage":
        scripts.append((str(py), ["scripts/129_imaging_fna_linkage_mm_v1.py", "--md"]))
    elif args.phase == "audits":
        scripts.extend(
            [
                (str(py), ["studies/20260413_source_truth_completeness_audit/run_source_truth_audit.py"]),
                (str(py), ["studies/20260413_us_nodule_tirads_linkage_audit/run_us_nodule_tirads_linkage_audit.py"]),
                (str(py), ["studies/20260413_us_lymph_node_audit/run_us_lymph_node_audit.py"]),
                (str(py), ["studies/20260413_fna_bethesda_audit/run_fna_bethesda_audit.py"]),
            ]
        )

    for exe, argv in scripts:
        cmd = [exe] + [str(ROOT / a) if not Path(a).is_absolute() else a for a in argv]
        if args.dry_run:
            print("would run:", " ".join(cmd))
            continue
        r = subprocess.run(cmd, cwd=str(ROOT))
        if r.returncode != 0:
            return r.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
