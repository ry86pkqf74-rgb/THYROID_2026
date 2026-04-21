#!/usr/bin/env python3
"""
291_tsh_llm_integration.py -- FROZEN.

Original purpose: integrate TSH entities parsed from
``note_entities_llm_labs.result_json`` into the legacy
``main.longitudinal_lab_canonical_v1`` so the LLM-derived TSH values
were available alongside the structured EHR rows.

This script has been SUPERSEDED by:
  - Script 347 — moved TSH into its own per-analyte canonical
    (``main.canonical_labs_tsh_v1``) with uniform normalization via
    ``scripts/_lab_value_normalizer.py``. The 42 TSH rows that this
    script contributed under ``source = 'clinical_note'`` are already
    present in ``main.canonical_labs_tsh_v1``.
  - Script 348 — refactored ingestion paths writing directly to the
    per-analyte canonicals (no more
    ``main.longitudinal_lab_canonical_v1`` write target).

Re-running this script as-is would attempt to ``INSERT INTO`` a table
that no longer exists. The original DML has been removed in place; the
original code is preserved in git history.

For NEW TSH ingestion from clinical-note LLM extracts, write rows
directly into ``main.canonical_labs_tsh_v1`` with
``source = 'clinical_note'`` and apply
``scripts/_lab_value_normalizer.normalize_lab_value`` to the raw value.
"""

import sys as _sys


def main() -> None:
    print(f"{__file__} is FROZEN (Script 348, 2026-04-21).")
    print("It targeted main.longitudinal_lab_canonical_v1 which was dropped by")
    print("Script 347. Use main.canonical_labs_tsh_v1 instead.")
    _sys.exit(2)


if __name__ == "__main__":
    main()
