#!/usr/bin/env python3
"""Run M044 master validation SQL against MotherDuck/local DuckDB.

Executes scripts/m044_validate_canonical_v1.sql (query blocks) and compares
metrics to the manuscript-frozen snapshot. Writes Markdown + JSON.

Usage:
  .venv/bin/python scripts/m044_validate_canonical_v1_runner.py --md
  .venv/bin/python scripts/m044_validate_canonical_v1_runner.py --local

Connection: ``--md`` uses ``_md_connect.connect_locked()`` (``USE`` publication main).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))

SQL_PATH = ROOT / "scripts" / "m044_validate_canonical_v1.sql"

# Manuscript-pinned expectations (frozen counts).
EXPECTED_MAIN = {
    "n_rows": 4128,
    "distinct_research_id": 4128,
    "duplicate_extra_rows": 0,
    "ete_microscopic": 2576,
    "ete_gross": 1266,
    "ete_no_negative": 192,
    "ete_present_ungraded": 29,
    "ete_missing_other": 65,
    "recurrence_path_proven_n": 145,
    "recurrence_imaging_only_n": 195,
    "recurrence_composite_n": 340,
    "fu_zero_n": 1400,
    "fu_positive_n": 2728,
}

EXPECTED_MEMBERSHIP = {
    "cohort_rows_not_in_cpm_filter": 0,
    "cpm_filter_missing_from_cohort": 0,
}

EXPECTED_ETE_CONSISTENCY = {
    "ete_mismatch_n": 0,
}

EXPECTED_RECURRENCE_COHERENCE = {
    "v_path_status_missing_bool": 0,
    "v_imaging_only_incoherent": 0,
    "v_none_but_evidence_bool": 0,
}


def _split_queries(sql_text: str) -> dict[str, str]:
    # Line-start markers: -- QUERY: block_name
    pattern = r"(?m)^--\s*QUERY:\s*(\w+)\s*\n(.*?)(?=^--\s*QUERY:|\Z)"
    chunks = re.findall(pattern, sql_text, flags=re.DOTALL)
    out: dict[str, str] = {}
    for name, body in chunks:
        stmt = body.strip().rstrip(";").strip()
        if stmt:
            out[name.strip()] = stmt + ";"
    return out


def _row_to_plain_dict(row: tuple[Any, ...], columns: list[str]) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for k, v in zip(columns, row):
        if hasattr(v, "item"):
            try:
                d[k] = v.item()
            except Exception:
                d[k] = v
        else:
            d[k] = v
    return d


def _compare_block(
    name: str,
    actual: dict[str, Any],
    expected: dict[str, int],
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    failures: list[str] = []
    details: dict[str, dict[str, Any]] = {}
    for k, exp_v in expected.items():
        got = actual.get(k)
        if got is None:
            failures.append(f"{name}.{k}: missing column (expected {exp_v})")
            details[k] = {"expected": exp_v, "actual": None, "pass": False}
            continue
        try:
            ig = int(got)
        except (TypeError, ValueError):
            failures.append(f"{name}.{k}: non-integer actual {got!r} (expected {exp_v})")
            details[k] = {"expected": exp_v, "actual": got, "pass": False}
            continue
        ok = ig == exp_v
        details[k] = {"expected": exp_v, "actual": ig, "pass": ok}
        if not ok:
            failures.append(f"{name}.{k}: expected {exp_v}, got {ig}")
    return failures, details


def _markdown_report(
    ts: str,
    checks: dict[str, Any],
    raw_dist: list[dict[str, Any]],
) -> str:
    lines = [
        "# M044 canonical cohort validation",
        "",
        f"- **Generated (UTC):** {ts}",
        "- **Cohort:** `manuscript_workspace.cohort_m044_ajcc_ete_v1`",
        "- **SQL:** `scripts/m044_validate_canonical_v1.sql`",
        "",
        "## Summary",
        "",
        f"| Status | `{checks['overall_status']}` |",
        f"| Errors | {len(checks['failures'])} |",
        "",
        "## Main audit (`main_audit`)",
        "",
        "| Metric | Expected | Actual | OK |",
        "|--------|---------|--------|-----|",
    ]
    for k, v in checks["details"]["main_audit"].items():
        lines.append(f"| `{k}` | {v['expected']} | {v['actual']} | {'yes' if v['pass'] else 'no'} |")
    lines += [
        "",
        "## Cohort membership vs CPM filter",
        "",
        "| Metric | Expected | Actual | OK |",
        "|--------|---------|--------|-----|",
    ]
    for k, v in checks["details"]["cohort_membership"].items():
        exp = v.get("expected")
        if exp is None:
            lines.append(f"| `{k}` | — | {v['actual']} | — |")
        else:
            lines.append(f"| `{k}` | {exp} | {v['actual']} | {'yes' if v['pass'] else 'no'} |")
    lines += [
        "",
        "## CPM ETE consistency (`cpm_ete_consistency`)",
        "",
        "| Metric | Expected | Actual | OK |",
        "|--------|---------|--------|-----|",
    ]
    for k, v in checks["details"]["cpm_ete_consistency"].items():
        exp = v.get("expected")
        if exp is None:
            lines.append(f"| `{k}` | — | {v['actual']} | — |")
        else:
            lines.append(f"| `{k}` | {exp} | {v['actual']} | {'yes' if v['pass'] else 'no'} |")
    lines += [
        "",
        "## Recurrence coherence (`recurrence_coherence`)",
        "",
        "| Metric | Expected | Actual | OK |",
        "|--------|---------|--------|-----|",
    ]
    for k, v in checks["details"].get("recurrence_coherence", {}).items():
        exp = v.get("expected")
        if exp is None:
            lines.append(f"| `{k}` | — | {v['actual']} | — |")
        else:
            lines.append(f"| `{k}` | {exp} | {v['actual']} | {'yes' if v['pass'] else 'no'} |")
    if checks["failures"]:
        lines += ["", "## Failures", ""]
        for f in checks["failures"]:
            lines.append(f"- {f}")
    lines += [
        "",
        "## Raw `ete_grade_final` distribution (diagnostic)",
        "",
        "| ete_grade_final_raw | n |",
        "|---------------------|---|",
    ]
    for r in raw_dist:
        lines.append(f"| {r.get('ete_grade_final_raw', '')} | {r.get('n', '')} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", help="MotherDuck publication DB (default if no flag)")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Local thyroid_master.duckdb (may lack manuscript_workspace objects)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT / "studies" / "m044_validation",
        help="Output directory for JSON + Markdown",
    )
    args = parser.parse_args()

    if not args.md and not args.local:
        args.md = True

    if args.md:
        from _md_connect import connect_locked  # noqa: E402

        con = connect_locked()
    else:
        import os

        dbp = os.environ.get("LOCAL_DB_PATH", str(ROOT / "thyroid_master.duckdb"))
        con = duckdb.connect(dbp)
        con.execute('USE "thyroid_canonical_publication_v1_0"')
        con.execute('USE "thyroid_canonical_publication_v1_0".main')

    sql_text = SQL_PATH.read_text(encoding="utf-8")
    queries = _split_queries(sql_text)
    required = (
        "main_audit",
        "cohort_membership",
        "cpm_ete_consistency",
        "recurrence_coherence",
        "ete_grade_final_raw",
    )
    for r in required:
        if r not in queries:
            print(f"Missing QUERY block: {r}", file=sys.stderr)
            return 2

    failures: list[str] = []
    details: dict[str, Any] = {
        "main_audit": {},
        "cohort_membership": {},
        "cpm_ete_consistency": {},
        "recurrence_coherence": {},
    }

    def _exec_or_binder_help(sql: str) -> None:
        try:
            con.execute(sql)
        except duckdb.BinderException as ex:
            print(
                "DuckDB binder error (often stale `cohort_m044_ajcc_ete_v1` vs CPM after retype). "
                "Recreate the manuscript_workspace cohort view.\n"
                f"Detail: {ex}",
                file=sys.stderr,
            )
            raise SystemExit(3) from ex

    # main_audit
    _exec_or_binder_help(queries["main_audit"])
    cols = [x[0] for x in con.description]
    row = con.fetchone()
    main_d = _row_to_plain_dict(row, cols)
    f1, d1 = _compare_block("main_audit", main_d, EXPECTED_MAIN)
    failures.extend(f1)
    details["main_audit"] = d1

    # membership
    _exec_or_binder_help(queries["cohort_membership"])
    cols = [x[0] for x in con.description]
    row = con.fetchone()
    mem_d = _row_to_plain_dict(row, cols)
    f2, d2 = _compare_block("cohort_membership", mem_d, EXPECTED_MEMBERSHIP)
    failures.extend(f2)
    details["cohort_membership"] = d2
    # informational
    for info_k in ("cpm_malignant_staged_n", "cohort_n"):
        if info_k in mem_d:
            details["cohort_membership"][info_k] = {
                "expected": None,
                "actual": mem_d[info_k],
                "pass": True,
            }

    # ETE consistency
    _exec_or_binder_help(queries["cpm_ete_consistency"])
    cols = [x[0] for x in con.description]
    row = con.fetchone()
    ete_d = _row_to_plain_dict(row, cols)
    f3, d3 = _compare_block("cpm_ete_consistency", ete_d, EXPECTED_ETE_CONSISTENCY)
    failures.extend(f3)
    details["cpm_ete_consistency"] = d3
    for info_k in ("n_joined", "ete_match_n"):
        if info_k in ete_d:
            details["cpm_ete_consistency"][info_k] = {
                "expected": None,
                "actual": ete_d[info_k],
                "pass": True,
            }

    # recurrence status vs BOOL coherence (canonical_recurrence_resolved_v1)
    _exec_or_binder_help(queries["recurrence_coherence"])
    cols = [x[0] for x in con.description]
    row = con.fetchone()
    coh_d = _row_to_plain_dict(row, cols)
    fc, dc = _compare_block("recurrence_coherence", coh_d, EXPECTED_RECURRENCE_COHERENCE)
    failures.extend(fc)
    details["recurrence_coherence"] = dc

    # raw distribution
    _exec_or_binder_help(queries["ete_grade_final_raw"])
    cols = [x[0] for x in con.description]
    raw_rows = [_row_to_plain_dict(t, cols) for t in con.fetchall()]

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    overall = "PASS" if not failures else "FAIL"
    payload = {
        "generated_at_utc": ts,
        "cohort_object": "manuscript_workspace.cohort_m044_ajcc_ete_v1",
        "sql_path": str(SQL_PATH.relative_to(ROOT)),
        "overall_status": overall,
        "failures": failures,
        "details": details,
        "snapshots": {
            "main_audit_row": main_d,
            "cohort_membership_row": mem_d,
            "cpm_ete_consistency_row": ete_d,
            "recurrence_coherence_row": coh_d,
        },
        "ete_grade_final_distribution": raw_rows,
    }

    args.out_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.out_dir / "m044_canonical_audit.json"
    md_path = args.out_dir / "m044_canonical_audit.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(
        _markdown_report(
            ts,
            {
                "overall_status": overall,
                "failures": failures,
                "details": details,
            },
            raw_rows,
        ),
        encoding="utf-8",
    )

    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print(f"Status: {overall}")
    if failures:
        for line in failures:
            print(f"  - {line}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
