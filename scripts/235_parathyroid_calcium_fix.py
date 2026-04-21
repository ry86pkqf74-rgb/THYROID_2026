#!/usr/bin/env python3
"""
235_parathyroid_calcium_fix.py -- FROZEN.

Original purpose: parathyroid + calcium / PTH data-quality fix on the
publication database. Among other things, it ran a
``CREATE OR REPLACE TABLE`` against the legacy
``main.longitudinal_lab_canonical_v1`` to add a ``value_corrected`` /
``calcium_correction_applied`` pair derived from a calcium-unit
heuristic, then rebuilt downstream complication-phenotype and
patient-summary tables off the corrected lab values.

That script has been SUPERSEDED by:
  - Script 347 — uniform per-analyte normalization through
    ``scripts/_lab_value_normalizer.py``. Calcium values are now
    corrected at canonicalization time (no separate
    ``value_corrected`` column); the fixed values land in
    ``main.canonical_labs_calcium_v1.value_numeric``.
  - Script 348 — refactored 113 / 127 ingestion paths writing directly
    to the per-analyte canonicals.

Re-running this script as-is would attempt to write to
``main.longitudinal_lab_canonical_v1`` which was dropped by Script 347
on 2026-04-21. The original DDL/DML has been removed in place; the
original code is preserved in git history. Pre-347 lab data lives in
``"Thyroid 2026 UPdated".archive_pub_v1_0.longitudinal_lab_canonical_v1_pre347_*``;
the calcium / NSQIP / complication-phenotype fixes that this script
applied are already reflected in current canonical state.

For NEW calcium-related work, use the per-analyte canonical
``main.canonical_labs_calcium_v1`` directly.
"""

import sys as _sys


def main() -> None:
    print(f"{__file__} is FROZEN (Script 348, 2026-04-21).")
    print("It targeted main.longitudinal_lab_canonical_v1 which was dropped by")
    print("Script 347. Use main.canonical_labs_calcium_v1 / scripts/347 instead.")
    _sys.exit(2)


if __name__ == "__main__":
    main()
