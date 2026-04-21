#!/usr/bin/env python3
"""
331_calcium_denominator_recovery.py -- FROZEN.

Original purpose: parse calcium / PTH entities from
``note_entities_llm_labs.result_json``, normalize to mg/dL (calcium) /
ng/dL (PTH), apply a plausibility filter, and ADDITIVE-INSERT into the
legacy ``main.longitudinal_lab_canonical_v1`` to widen the calcium
denominator for postop hypocalcemia analytics.

This script has been SUPERSEDED by:
  - Script 347 — moved calcium / PTH into per-analyte canonicals
    (``main.canonical_labs_calcium_v1``,
    ``main.canonical_labs_pth_v1``) with uniform normalization +
    plausibility-correction via ``scripts/_lab_value_normalizer.py``.
  - Script 348 — refactored ingestion paths writing directly to the
    per-analyte canonicals.

Re-running this script as-is would attempt to ``INSERT INTO`` a table
that no longer exists. The original DML has been removed in place; the
original code is preserved in git history.

For NEW LLM-derived calcium / PTH ingestion, write rows directly into
``main.canonical_labs_calcium_v1`` / ``main.canonical_labs_pth_v1``
with ``source = 'clinical_note'`` and apply
``scripts/_lab_value_normalizer.normalize_lab_value`` to the raw value.
"""

import sys as _sys


def main() -> None:
    print(f"{__file__} is FROZEN (Script 348, 2026-04-21).")
    print("It targeted main.longitudinal_lab_canonical_v1 which was dropped by")
    print("Script 347. Use main.canonical_labs_{calcium,pth}_v1 instead.")
    _sys.exit(2)


if __name__ == "__main__":
    main()
