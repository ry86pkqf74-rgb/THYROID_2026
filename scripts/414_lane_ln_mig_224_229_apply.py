#!/usr/bin/env python3
"""Apply Lane LN migrations mig_224–mig_229 to MotherDuck publication DB (ordered SQL batch).

Usage:
  .venv/bin/python scripts/414_lane_ln_mig_224_229_apply.py [--dry-run]

Requires RW MotherDuck token (motherduck.local.toml or env per motherduck_client).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

MIG_FILES = [
    "qc_framework_v1/migrations/224_histology_vocab_extension_20260430.sql",
    "qc_framework_v1/migrations/225_vw_ln_surgery_publication_safe_20260430.sql",
    "qc_framework_v1/migrations/226_vw_ln_patient_publication_safe_20260430.sql",
    "qc_framework_v1/migrations/227_vw_ln_histology_attribution_20260430.sql",
    # 229 must precede 228: qc_ln_impossible_counts_v1 references vw_ln_surgery (post-quarantine).
    "qc_framework_v1/migrations/229_borderline_quarantine_flag_20260430.sql",
    "qc_framework_v1/migrations/228_qc_ln_histology_tables_20260430.sql",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print paths only; do not connect.")
    args = ap.parse_args()

    import os

    # Publication SSOT per AGENTS.md (override with MOTHERDUCK_DATABASE if needed).
    os.environ.setdefault("MOTHERDUCK_DATABASE", "thyroid_canonical_publication_v1_0")

    paths = [REPO / p for p in MIG_FILES]
    for p in paths:
        if not p.is_file():
            raise SystemExit(f"missing migration file: {p}")

    if args.dry_run:
        for p in paths:
            print(f"would apply: {p.relative_to(REPO)}")
        return

    from motherduck_client import MotherDuckClient

    con = MotherDuckClient.for_env("prod").connect_rw()
    try:
        for p in paths:
            sql = p.read_text(encoding="utf-8")
            print(f"Applying {p.name} …")
            con.execute(sql)
        print("Lane LN mig_224–229 apply finished OK.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
