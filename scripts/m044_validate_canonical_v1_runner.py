#!/usr/bin/env python3
"""Run M044 master validation SQL against MotherDuck/local DuckDB.

Executes scripts/m044_validate_canonical_v1.sql (query blocks) and compares
metrics to the manuscript-frozen snapshot. Writes Markdown + JSON.

After this reports **PASS**, regenerate manuscript outputs (tables, figures,
per-patient workbook, package sync, validation summary) with::

  .venv/bin/python scripts/m044_regenerate_outputs.py --skip-validation

Or run validation + full regeneration in one step::

  .venv/bin/python scripts/m044_regenerate_outputs.py

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
    # mig_315 (2026-05-05): post-mig_313 M-stage fix + ete_grade_final normalization.
    # v5 locked counts (pre-mig_313): n_rows=4128, ete_gross=1266, ete_no_negative=192.
    # v6 counts below reflect mig_313 staging recompute (151 patients lost valid stage_group).
    "n_rows": 3868,
    "distinct_research_id": 3868,
    "duplicate_extra_rows": 0,
    "ete_microscopic": 2413,
    "ete_gross": 1243,
    "ete_no_negative": 173,
    "ete_present_ungraded": 28,
    "ete_missing_other": 11,
    # Final M044 endpoint: path-proven primary excludes implausible-date quarantines.
    # Note: events increased from 105 (v5 strict-DTC) to 136 due to cohort expansion.
    "recurrence_path_proven_raw_n": 228,
    "recurrence_path_proven_quarantined_n": 24,
    "recurrence_path_proven_n": 204,
    "recurrence_imaging_only_n": 24,
    "recurrence_composite_n": 228,
    "primary_quarantined_n": 0,
    "primary_negative_days_n": 0,
    "recurrence_path_proven_positive_fu_n": 199,
    "recurrence_path_proven_zero_fu_n": 5,
    "fu_zero_n": 1400,
    "fu_positive_n": 2468,
}

EXPECTED_MEMBERSHIP = {
    "cohort_rows_not_in_cpm_filter": 0,
    "cpm_filter_missing_from_cohort": 0,
}

EXPECTED_ETE_CONSISTENCY = {
    # cohort_m044 exposes canonical_patient_master.ete_grade_final (pinned column-of-record).
    # It need not equal ete_grade_final_v2 (adjudicated alternate). Count is informational-ish but frozen.
    "ete_mismatch_n": 178,
}

EXPECTED_RECURRENCE_COHERENCE = {
    "v_path_status_missing_bool": 0,
    "v_imaging_only_incoherent": 0,
    "v_none_but_evidence_bool": 0,
}

# Post–mig_254 + mig_258 lineage flags (frozen 2026-05-01).
EXPECTED_SURGERY_DATE_LINEAGE = {
    # mig_315: updated for post-mig_313 cohort N=3868
    "n_cohort": 3868,
    "surg_first_nonmissing": 3868,
    "surg_first_missing": 0,
    "surg_date_pre_1999_n": 3,
    "surg_date_1999_2024_n": 3830,
    "surg_date_post_2024_n": 35,
    "surg_date_after_2024_06_04_n": 245,
    "calendar_partition_violations": 0,
}

# Table 1B — total thyroidectomy subset × ETE (canonical union: BOOLEAN OR procedure_type).
# Frozen with publication v1.0 CPM + mig_253 procedure harmonization (2026-05-01).
EXPECTED_TABLE1B_TT_ETE = {
    "tt_n_total": 2798,
    "tt_n_noneg": 59,
    "tt_n_microscopic": 1732,
    "tt_n_gross": 956,
    "tt_n_present_ungraded": 23,
    "tt_n_missing_other": 28,
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
        "## Surgery-date lineage (`surgery_date_lineage`)",
        "",
        "| Metric | Expected | Actual | OK |",
        "|--------|---------|--------|-----|",
    ]
    for k, v in checks["details"].get("surgery_date_lineage", {}).items():
        exp = v.get("expected")
        if exp is None:
            lines.append(f"| `{k}` | — | {v['actual']} | — |")
        else:
            lines.append(f"| `{k}` | {exp} | {v['actual']} | {'yes' if v['pass'] else 'no'} |")
    lines += [
        "",
        "## Table 1B — total thyroidectomy × ETE (`table1b_tt_ete_audit`)",
        "",
        "Union rule: `surg_total_thyroidectomy IS TRUE` OR `surg_procedure_type` normalized to "
        "`total_thyroidectomy` on `main.canonical_patient_master` (M044 eligibility filter).",
        "",
        "| Metric | Expected | Actual | OK |",
        "|--------|---------|--------|-----|",
    ]
    for k, v in checks["details"].get("table1b_tt_ete", {}).items():
        exp = v.get("expected")
        if exp is None:
            lines.append(f"| `{k}` | — | {v['actual']} | — |")
        else:
            lines.append(f"| `{k}` | {exp} | {v['actual']} | {'yes' if v['pass'] else 'no'} |")
    lines += [
        "",
        "## Surgery date vs operative v2 (`surgery_date_vs_operative_v2_optional`, informational)",
        "",
        "| Metric | Actual |",
        "|--------|--------|",
    ]
    for k, v in checks["details"].get("surgery_date_operative_v2", {}).items():
        lines.append(f"| `{k}` | {v.get('actual')} |")
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
    lines += [
        "",
        "## Legacy recurrence audit (`legacy_recurrence_audit`)",
        "",
        "Live counts from `manuscript_workspace.m044_legacy_recurrence_flag_audit_v1` (mig_257/258). "
        "Legacy flags are **not** analytic endpoints.",
        "",
        "| Metric | Actual |",
        "|--------|--------|",
    ]
    for k, v in checks["details"].get("legacy_recurrence_audit", {}).items():
        lines.append(f"| `{k}` | {v.get('actual')} |")
    lines.append("")
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
        "surgery_date_lineage",
        "recurrence_coherence",
        "legacy_recurrence_audit",
        "table1b_tt_ete_audit",
        "ete_grade_final_raw",
        "surgery_date_vs_operative_v2_optional",
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
        "surgery_date_lineage": {},
        "recurrence_coherence": {},
        "legacy_recurrence_audit": {},
        "table1b_tt_ete": {},
        "surgery_date_operative_v2": {},
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

    # Surgery-date lineage flags (mig_258 cohort columns)
    _exec_or_binder_help(queries["surgery_date_lineage"])
    cols = [x[0] for x in con.description]
    row = con.fetchone()
    sdl_d = _row_to_plain_dict(row, cols)
    fs, ds = _compare_block("surgery_date_lineage", sdl_d, EXPECTED_SURGERY_DATE_LINEAGE)
    failures.extend(fs)
    details["surgery_date_lineage"] = ds

    _exec_or_binder_help(queries["table1b_tt_ete_audit"])
    cols = [x[0] for x in con.description]
    row = con.fetchone()
    t1b_d = _row_to_plain_dict(row, cols)
    ft1b, dt1b = _compare_block("table1b_tt_ete_audit", t1b_d, EXPECTED_TABLE1B_TT_ETE)
    failures.extend(ft1b)
    details["table1b_tt_ete"] = dt1b

    _exec_or_binder_help(queries["surgery_date_vs_operative_v2_optional"])
    cols = [x[0] for x in con.description]
    row = con.fetchone()
    opv2_d = _row_to_plain_dict(row, cols)
    for k, v in opv2_d.items():
        details["surgery_date_operative_v2"][k] = {
            "expected": None,
            "actual": v,
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

    # Legacy recurrence flags vs canonical (informational; no frozen expected counts)
    _exec_or_binder_help(queries["legacy_recurrence_audit"])
    cols = [x[0] for x in con.description]
    row = con.fetchone()
    leg_d = _row_to_plain_dict(row, cols)
    for k, v in leg_d.items():
        details["legacy_recurrence_audit"][k] = {
            "expected": None,
            "actual": v,
            "pass": True,
        }

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
            "surgery_date_lineage_row": sdl_d,
            "table1b_tt_ete_audit_row": t1b_d,
            "surgery_date_vs_operative_v2_row": opv2_d,
            "recurrence_coherence_row": coh_d,
            "legacy_recurrence_audit_row": leg_d,
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
