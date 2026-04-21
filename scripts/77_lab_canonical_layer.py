#!/usr/bin/env python3
"""
77_lab_canonical_layer.py -- FROZEN.

Original purpose: build the unified long-format lab canonical table
(formerly ``main.longitudinal_lab_canonical_v1``, Wave 1 — structured
Tg / TgAb).

This script was the bootstrap canonical-layer builder; it has been
SUPERSEDED by Script 347 (per-analyte canonicalization on
``main.canonical_labs_*_v1``) and Script 348 (refactored ingestion
paths). The legacy target was DROPPED on 2026-04-21 and replaced by
``main.longitudinal_lab_VIEW_v1`` (UNION ALL across the five
per-analyte canonical tables) plus ``main.thyroglobulin_lab_VIEW_v1``
(Tg/TgAb compatibility shim).

Re-running this script as-is would attempt to write to a table that no
longer exists in the publication schema. The original DDL/DML has been
removed in place; the original code is preserved in git history and the
materialized data is preserved in
``"Thyroid 2026 UPdated".archive_pub_v1_0.longitudinal_lab_canonical_v1_pre347_*``.

For NEW ingestion, use:
  - ``scripts/113_tg_lab_ingestion.py``                 (Tg / TgAb path)
  - ``scripts/127_analyst_institutional_lab_append.py`` (institutional CSVs)
  - ``scripts/347_lab_master_canonical_v1_build.py``    (full canonical rebuild)
"""

import sys as _sys


def main() -> None:
    print(f"{__file__} is FROZEN (Script 348, 2026-04-21).")
    print("It targeted main.longitudinal_lab_canonical_v1 which was dropped by")
    print("Script 347. Use scripts/113, 127, or 347 instead.")
    _sys.exit(2)


if __name__ == "__main__":
    main()
