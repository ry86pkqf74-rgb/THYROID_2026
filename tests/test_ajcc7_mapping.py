"""Regression tests for the canonical AJCC 7th mapping.

Covers two things:

1. The single shared implementation maps T3b -> T3 (not T4a).
2. No *executable* path in the repo still inlines the stale T3b -> T4a rule.
   The audit-comparator function in ``audit_reproduce.py::derive_ajcc7_original``
   is explicitly whitelisted because it exists only to reproduce the
   pre-correction output for diffing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
import pytest


STUDY_DIR = Path(__file__).resolve().parents[1] / "studies" / "proposal2_ete_staging"
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STUDY_DIR))


def _build_df(rows):
    return pd.DataFrame(rows)


def test_t3b_maps_to_t3_not_t4a():
    from ajcc7_mapping import add_ajcc7_columns, AJCC7_T3B_MAPS_TO

    assert AJCC7_T3B_MAPS_TO == "T3"

    df = _build_df([
        {"t_stage_ajcc8": "T3b", "ete_group": "Gross ETE", "largest_tumor_cm": 3.0,
         "age_at_surgery": 60, "n_stage_ajcc8": "N0", "m_stage_ajcc8": "M0"},
        {"t_stage_ajcc8": "T3b", "ete_group": "No ETE", "largest_tumor_cm": 5.0,
         "age_at_surgery": 50, "n_stage_ajcc8": "N1a", "m_stage_ajcc8": "M0"},
    ])
    out = add_ajcc7_columns(df)
    assert list(out["t_stage_ajcc7"]) == ["T3", "T3"]
    # T3 + N0 + age>=45 => Stage III; T3 + N1a + age>=45 => Stage IVA.
    assert list(out["overall_stage_ajcc7"]) == ["III", "IVA"]


def test_t4a_t4b_passthrough_and_microscopic_ete():
    from ajcc7_mapping import add_ajcc7_columns

    df = _build_df([
        {"t_stage_ajcc8": "T4a", "ete_group": "Gross ETE", "largest_tumor_cm": 2.0,
         "age_at_surgery": 70, "n_stage_ajcc8": "N0", "m_stage_ajcc8": "M0"},
        {"t_stage_ajcc8": "T4b", "ete_group": "Gross ETE", "largest_tumor_cm": 3.0,
         "age_at_surgery": 50, "n_stage_ajcc8": "N0", "m_stage_ajcc8": "M0"},
        {"t_stage_ajcc8": "T1a", "ete_group": "Microscopic ETE", "largest_tumor_cm": 0.8,
         "age_at_surgery": 60, "n_stage_ajcc8": "N0", "m_stage_ajcc8": "M0"},
        {"t_stage_ajcc8": "T2", "ete_group": "No ETE", "largest_tumor_cm": 3.0,
         "age_at_surgery": 60, "n_stage_ajcc8": "N0", "m_stage_ajcc8": "M0"},
    ])
    out = add_ajcc7_columns(df)
    assert list(out["t_stage_ajcc7"]) == ["T4a", "T4b", "T3", "T2"]
    # T4a+N0+age>=45 => IVA; T4b+age>=45 => IVB; T3 micro -> Stage III; T2 -> II
    assert list(out["overall_stage_ajcc7"]) == ["IVA", "IVB", "III", "II"]


def test_young_patient_overall_stage_is_i_or_ii_only():
    from ajcc7_mapping import add_ajcc7_columns

    df = _build_df([
        {"t_stage_ajcc8": "T3b", "ete_group": "Gross ETE", "largest_tumor_cm": 3.0,
         "age_at_surgery": 30, "n_stage_ajcc8": "N1b", "m_stage_ajcc8": "M0"},
        {"t_stage_ajcc8": "T4b", "ete_group": "Gross ETE", "largest_tumor_cm": 4.0,
         "age_at_surgery": 30, "n_stage_ajcc8": "N1b", "m_stage_ajcc8": "M1"},
    ])
    out = add_ajcc7_columns(df)
    # Under 45: M0 -> Stage I regardless of T/N; M1 -> Stage II.
    assert list(out["overall_stage_ajcc7"]) == ["I", "II"]


EXECUTABLE_CODE_GLOBS = [
    "studies/proposal2_ete_staging/proposal2_ete_analysis.py",
    "studies/proposal2_ete_staging/proposal2_expanded_cohort.py",
    "studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py",
    "studies/proposal2_ete_staging/proposal2_recommendations.py",
    "studies/proposal2_ete_staging/proposal2_cox_regression.py",
]


# The audit comparator intentionally reproduces the OLD (incorrect) mapping.
WHITELIST_STALE_MAPPING = {
    "studies/proposal2_ete_staging/audit_reproduce.py",
}


_STALE_PATTERN = re.compile(r'ajcc7[_A-Za-z]*\.append\(\s*["\']T4a["\']\s*\)')


def test_no_stale_t3b_to_t4a_in_executable_paths():
    """Fail if any executable ETE code path still appends 'T4a' after a T3b check.

    We look for the very specific appender pattern because it's the shape the
    stale logic took (``ajcc7.append("T4a")`` right after ``elif t == "T3b"``).
    The comparator in audit_reproduce.py is whitelisted.
    """
    offenders = []
    for rel in EXECUTABLE_CODE_GLOBS:
        p = REPO_ROOT / rel
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        # Quick check: look for the stale appender pattern in proximity to T3b.
        # Any match inside any of these executable files is a fail.
        for m in _STALE_PATTERN.finditer(text):
            # Context: look 200 chars back for the 'T3b' trigger
            start = max(0, m.start() - 200)
            ctx = text[start:m.end()]
            if "T3b" in ctx:
                offenders.append(f"{rel} @offset {m.start()}: {ctx.splitlines()[-1].strip()}")
    assert not offenders, (
        "Stale T3b -> T4a mapping found in executable ETE paths. "
        "All executable paths must use ajcc7_mapping.add_ajcc7_columns. "
        "Offenders:\n  - " + "\n  - ".join(offenders)
    )
