"""CI guard: fail if discontinued M044 headline statistics reappear in tracked surfaces.

Stale benchmarks (pre–mig_254/258 lineage + legacy recurrence audit wording):
  surgery-date missing n=914; 1999–2024 window n=3,212; structural_recurrence headline 1,819;
  structural + recurrence_status none 1,467; Cox manuscript sample n=2,018 / figure n=2,025;
  Table 2 microscopic person-year rate 0.73 (positive-FU denominator); total thyroidectomy Table 1B n=2,098.

Violations report file path, rule id, and line excerpt. Legacy documentation that *explains* these
numbers may live only in EXCLUDED_PATH_SUFFIXES.

Environment:
  M044_SKIP_STALE_MANUSCRIPT_GUARD=1 — skip this module (emergency only).
  M044_EXPECT_COX_FINAL_N — final Cox/KM row count from inclusion-flow QA CSV (default 2490).
"""

from __future__ import annotations

import csv
import os
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
from m044_validate_canonical_v1_runner import (  # noqa: E402
    EXPECTED_SURGERY_DATE_LINEAGE,
    EXPECTED_TABLE1B_TT_ETE,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("M044_SKIP_STALE_MANUSCRIPT_GUARD") == "1",
    reason="M044_SKIP_STALE_MANUSCRIPT_GUARD=1",
)


# Historical / handoff docs that intentionally quote pre-fix headline numbers.
EXCLUDED_PATH_SUFFIXES: frozenset[str] = frozenset(
    {
        "M044_ETE_validation_report.md",
        "M044_ETE_supplement.md",
        "M044_ETE_claude_handoff_notes.md",
        "M044_ETE_NEW_CHAT_CONTINUATION_PROMPT.md",
        "M044_submission_package_v1_0/09_validation_report.md",
    }
)

# Text surfaces that must not regress to stale headline values.
SCAN_PATHS: tuple[Path, ...] = (
    REPO / "M044_ETE_manuscript_draft.md",
    REPO / "M044_ETE_analysis_plan.md",
    REPO / "M044_ETE_analysis.sql",
    REPO / "studies/m044_validation",
    REPO / "data/m044",
    REPO / "scripts/m044_validate_canonical_v1.sql",
    REPO / "scripts/m044_validate_canonical_v1_runner.py",
    REPO / "scripts/m044_regenerate_outputs.py",
    REPO / "scripts/m044_ete_fit_models.py",
    REPO / "scripts/m044_master_analytic.sql",
    REPO / "scripts/m044_master_crosswalk.json",
)

LEGACY_OK_SUBSTR = (
    "obsolete",
    "scaffolding",
    "no longer keyed",
    "legacy",
    "do not use",
    "not used as a primary",
    "audit only",
    "inconsistent with canonical",
    "historical chatgpt",
    "1467/1819",
    "1819/1467",
    "for contrast",
    "replace legacy",
    "superseded",
)


def _is_excluded(path: Path) -> bool:
    try:
        rel = path.relative_to(REPO).as_posix()
    except ValueError:
        return False
    for suf in EXCLUDED_PATH_SUFFIXES:
        if rel == suf or rel.endswith("/" + suf):
            return True
    return False


def _line_flags_stale(line: str) -> list[tuple[str, str]]:
    """Return list of (rule_id, matching_span) for stale patterns; empty if clean."""
    if any(s in line.lower() for s in LEGACY_OK_SUBSTR):
        return []

    hits: list[tuple[str, str]] = []
    low = line.lower()

    if re.search(r"(?i)\b914\b", line) and (
        "missing" in low
        or re.search(r"914/4,128|914/4128", line)
        or "22.1%" in line
    ):
        hits.append(("stale_surgery_missing_914", line.strip()[:240]))

    if re.search(r"(?i)\b3,212\b|\b3212\b", line) and re.search(
        r"(?i)surgery|1999|2024|window|non-missing", line
    ):
        hits.append(("stale_surgery_window_3212", line.strip()[:240]))

    if re.search(r"(?i)\b1,819\b|\b1819\b", line) and "structural" in low:
        hits.append(("stale_structural_recurrence_1819", line.strip()[:240]))

    if re.search(r"(?i)\b1,467\b|\b1467\b", line):
        structural_hit = "structural" in low or "legacy_structural" in low
        if structural_hit and re.search(r"(?i)\bnone\b|'none'|status", line):
            hits.append(("stale_structural_plus_none_1467", line.strip()[:240]))

    if re.search(r"(?i)\bcox\b.{0,120}\bn\s*=\s*2,?018\b", line):
        hits.append(("stale_cox_n_2018", line.strip()[:240]))
    if re.search(r"(?i)\bn\s*=\s*2,?018\b.{0,40}\bcox\b", line):
        hits.append(("stale_cox_n_2018", line.strip()[:240]))

    if re.search(
        r"(?i)(Kaplan|EHR|Kaplan[^\n]{0,3}Meier|figure\s*[67]|KM|forest).{0,140}\bn\s*=\s*2,?025\b",
        line,
    ) or re.search(
        r"(?i)\bn\s*=\s*2,?025\b.{0,60}(Kaplan|\bKM\b|figure\s*[67])",
        line,
    ):
        hits.append(("stale_cox_or_km_figure_n_2025", line.strip()[:240]))

    if re.search(r"(?i)total[^\n]{0,40}thyroidectomy[^\n]{0,80}\b2,?098\b|\b2,?098\b[^\n]{0,80}(?:total[^\n]{0,20})?thyroidectomy", line):
        hits.append(("stale_tt_table1b_2098", line.strip()[:240]))

    if "microscopic" in low and re.search(r"\b0\.73\b", line):
        if re.search(
            r"(?i)(100\s*py|100py|person[\s-]*year|/100|pp_per|rate|table\s*2)",
            line,
        ):
            hits.append(("stale_table2_microscopic_py_rate_073", line.strip()[:240]))

    dedup: dict[str, str] = {}
    for rid, span in hits:
        dedup[rid] = span
    return list(dedup.items())


def _iter_scan_files() -> list[Path]:
    out: list[Path] = []
    for base in SCAN_PATHS:
        if not base.exists():
            continue
        if base.is_file():
            if base.suffix.lower() in {".md", ".sql", ".py", ".json", ".csv", ".toml"}:
                out.append(base)
            continue
        if base.is_dir():
            for p in sorted(base.rglob("*")):
                if not p.is_file():
                    continue
                if _is_excluded(p):
                    continue
                if p.suffix.lower() in {".md", ".sql", ".py", ".json", ".csv"}:
                    out.append(p)
    return sorted(set(out))


def test_m044_no_stale_headline_numbers_in_primary_surfaces() -> None:
    """Scan tracked M044 manuscript / validation / runner / analytic CSV text for regressions."""

    violations: list[str] = []
    for path in _iter_scan_files():
        if _is_excluded(path):
            continue
        raw = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for lineno, line in enumerate(raw, start=1):
            for rule_id, excerpt in _line_flags_stale(line):
                rel = path.relative_to(REPO)
                violations.append(f"{rule_id} :: {rel}:{lineno} :: {excerpt}")

    assert not violations, "Stale manuscript metrics detected:\n" + "\n".join(violations)


STALE_COX_INCLUSION_N = frozenset({2018, 2025})


def test_m044_inclusion_flow_not_stale_cox_ns() -> None:
    """Ban discontinued Cox/KM row counts from m044_inclusion_flow_qc.csv (source: m044_ete_fit_models QA export)."""

    csv_path = REPO / "data" / "m044" / "m044_inclusion_flow_qc.csv"
    if not csv_path.exists():
        pytest.skip("data/m044/m044_inclusion_flow_qc.csv not present")

    rows: list[dict[str, str]] = []
    with csv_path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            rows.append(row)

    for row in rows:
        crit = row.get("criterion") or ""
        n_cell = row.get("n", "").strip()
        if not n_cell:
            continue
        try:
            n_val = int(n_cell)
        except ValueError:
            continue
        if n_val not in STALE_COX_INCLUSION_N:
            continue
        if not any(k in crit for k in ("Cox", "Kaplan", "lifelines", "time-to-event", "KM")):
            continue
        rel = csv_path.relative_to(REPO)
        raise AssertionError(
            f"{rel}: discontinued Cox/sample n={n_val} in '{crit[:120]}'"
        )

    exp_final = int(os.environ.get("M044_EXPECT_COX_FINAL_N", "2490"))
    final_hit = [
        r
        for r in rows
        if r.get("criterion") and "lifelines sample" in r["criterion"]
    ]
    assert final_hit, f"{csv_path.relative_to(REPO)}: missing lifelines-sample final inclusion row"
    r_final = final_hit[-1]
    got_final = int(r_final["n"])
    assert got_final == exp_final, (
        f"{csv_path.relative_to(REPO)}: final Cox/KM n={got_final}, expected {exp_final} "
        f"(adjust extract or set M044_EXPECT_COX_FINAL_N after validation)."
    )


def test_m044_parquet_table2_microscopic_rate_not_point_73() -> None:
    """0.710–0.724 was superseded (~0.71); exactly 0.73 (two decimals) flags old positive-FU extract."""

    pq = REPO / "data" / "m044" / "analytic_file_v1.parquet"
    if not pq.exists():
        pytest.skip("data/m044/analytic_file_v1.parquet not built")

    sys.path.insert(0, str(REPO / "scripts"))
    import m044_ete_fit_models as m044  # noqa: E402

    import pandas as pd

    df = pd.read_parquet(pq)
    tbl = m044.build_table2_recurrence_summary(df)
    mic = tbl.loc[tbl["ete_group"] == "Microscopic ETE"].iloc[0]
    py_rate = round(float(mic["pp_per_100py"]), 2)
    assert py_rate != 0.73, (
        f"Table2 Microscopic pp_per_100py rounded to 0.73 (stale). Got {mic['pp_per_100py']!r}; "
        f"source query: m044.build_table2_recurrence_summary + data/m044/analytic_file_v1.parquet"
    )


def test_m044_expected_constants_match_freeze_not_stale() -> None:
    """Runner EXPECTED_* dicts are the SSOT for CI; catch accidental revert to 3212/914/2098."""

    assert EXPECTED_SURGERY_DATE_LINEAGE["surg_date_1999_2024_n"] != 3212
    assert EXPECTED_SURGERY_DATE_LINEAGE["surg_first_missing"] != 914
    assert EXPECTED_TABLE1B_TT_ETE["tt_n_total"] != 2098
