#!/usr/bin/env python3
"""
Execute the QC framework live against MotherDuck's
thyroid_canonical_publication_v1_0 database and print a report.

Reads token from motherduck.local.toml / env (same resolution order as
motherduck_client.py).
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

import duckdb

REPO = Path(__file__).resolve().parent.parent
QC_DIR = Path(__file__).resolve().parent


def load_token() -> str:
    for env_key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        v = os.environ.get(env_key)
        if v:
            return v
    toml_path = REPO / "motherduck.local.toml"
    if toml_path.exists():
        with open(toml_path, "rb") as f:
            cfg = tomllib.load(f)
        for k in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
            if cfg.get(k):
                return cfg[k]
    raise SystemExit("ERROR: no MotherDuck token found.")


COMMENT_LINE = re.compile(r"^\s*--.*$", re.MULTILINE)


def split_statements(sql: str) -> list[str]:
    """Split on ; while ignoring semicolons inside single-quoted strings."""
    cleaned = COMMENT_LINE.sub("", sql)
    stmts, buf, in_str = [], [], False
    for ch in cleaned:
        if ch == "'":
            in_str = not in_str
        if ch == ";" and not in_str:
            s = "".join(buf).strip()
            if s:
                stmts.append(s)
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def main() -> int:
    os.environ["motherduck_token"] = load_token()
    con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0")

    for fn in (
        "02_qc_violations_schema.sql",
        "03_qc_violations_populate.sql",
        "04_cohort_v2_views.sql",
    ):
        print(f"\n--- executing {fn} ---")
        sql = (QC_DIR / fn).read_text()
        stmts = split_statements(sql)
        print(f"    {len(stmts)} statements")
        for i, s in enumerate(stmts):
            try:
                con.execute(s)
            except Exception as e:
                print(f"    FAILED at statement {i}:")
                print(f"    {s[:400]}")
                print(f"    error: {e}")
                return 1
        print(f"    OK")

    print("\n=== VIOLATION SUMMARY ===")
    rows = con.execute(
        "SELECT rule_id, severity, category, source_object, n_patients_flagged, n_events_total "
        "FROM manuscript_workspace.qc_violations_summary_v1 "
        "ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, rule_id"
    ).fetchall()
    print(f"{'rule_id':<42} {'sev':<8} {'category':<11} {'source':<45} {'patients':>9} {'events':>9}")
    print("-" * 130)
    for r in rows:
        src = (r[3] or "")[:43]
        ev  = "" if r[5] is None else f"{r[5]:>9}"
        pat = "" if r[4] is None else f"{r[4]:>9}"
        print(f"{r[0]:<42} {r[1]:<8} {r[2]:<11} {src:<45} {pat} {ev}")

    print("\n=== COHORT FLOW ===")
    flow = con.execute(
        "SELECT * FROM manuscript_workspace.qc_cohort_flow_v1_to_v2"
    ).fetchone()
    cols = [c[0] for c in con.execute(
        "DESCRIBE manuscript_workspace.qc_cohort_flow_v1_to_v2"
    ).fetchall()]
    for c, v in zip(cols, flow):
        print(f"  {c:<24} {v:,}")

    print("\n=== EXCLUSION ATTRIBUTION (critical rules that drop patients) ===")
    for r in con.execute(
        "SELECT rule_id, n_patients, source_object, description "
        "FROM manuscript_workspace.qc_cohort_exclusion_attribution_v1"
    ).fetchall():
        print(f"  {r[0]:<42} n={r[1]:>5}  src={r[2]}")
        print(f"    {r[3]}")

    print("\n=== DRILL-DOWN EXAMPLES (3 rows per critical rule) ===")
    crits = [r[0] for r in rows if r[1] == "critical" and (r[4] or 0) > 0]
    for rule in crits:
        print(f"\n  [{rule}]")
        samples = con.execute(
            f"SELECT research_id, source_pk, details "
            f"FROM manuscript_workspace.qc_event_issues_v1 "
            f"WHERE rule_id = '{rule}' LIMIT 3"
        ).fetchall()
        if not samples:
            samples = con.execute(
                f"SELECT research_id, NULL, details "
                f"FROM manuscript_workspace.qc_violations_v1 "
                f"WHERE rule_id = '{rule}' LIMIT 3"
            ).fetchall()
        for s in samples:
            print(f"    rid={s[0]}  pk={s[1]}  {s[2]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
