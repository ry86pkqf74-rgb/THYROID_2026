#!/usr/bin/env python3
"""Lane M mig_234 — regenerate manuscript Table 1–5 + cohort flow CSVs from live MotherDuck.

Reads SQL fragments from qc_framework_v1/manuscript/mig234_lane_m/*.sql and writes UTF-8 CSVs under:
  manuscript_outputs/v1_0_20260501/

Usage:
  .venv/bin/python qc_framework_v1/scripts/build_mig234_lane_m_table1_refresh.py
  .venv/bin/python qc_framework_v1/scripts/build_mig234_lane_m_table1_refresh.py --apply-provenance

Token resolution mirrors qc_framework_v1/scripts/build_mig204_populate_manuscript_csvs.py
(do not print token values).
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIG234_SQL_DIR = REPO_ROOT / "qc_framework_v1" / "manuscript" / "mig234_lane_m"
OUT_DIR = REPO_ROOT / "manuscript_outputs" / "v1_0_20260501"
PROVENANCE_SQL_PATH = REPO_ROOT / "qc_framework_v1" / "migrations" / "234_table1_csv_refresh_20260501.sql"


OUTPUT_MAP = [
    ("table_1_cohort_demographics_v15.sql", "Table_1_cohort_demographics_v1_0_20260501.csv"),
    ("table_2_tumor_stage_distribution_v15.sql", "Table_2_tumor_stage_distribution_v1_0_20260501.csv"),
    ("table_3_ln_summary_safe_v15.sql", "Table_3_LN_summary_safe_v1_0_20260501.csv"),
    ("table_4_recurrence_survival_v15.sql", "Table_4_recurrence_survival_v1_0_20260501.csv"),
    ("table_5_molecular_distribution_v15.sql", "Table_5_molecular_distribution_v1_0_20260501.csv"),
    ("cohort_flow_v15.sql", "cohort_flow_v1_0_20260501.csv"),
]


def _get_token() -> str:
    for env_key in ("MOTHERDUCK_TOKEN", "MD_SA_TOKEN"):
        v = os.environ.get(env_key, "")
        if v:
            print(f"[token] loaded from env {env_key}, length={len(v)}")
            return v
    try:
        import toml

        d = toml.load(REPO_ROOT / "motherduck.local.toml")
        for k in ("MOTHERDUCK_TOKEN", "MD_SA_TOKEN"):
            v = d.get(k, "")
            if v:
                print(f"[token] loaded from motherduck.local.toml key={k}, length={len(v)}")
                return v
    except Exception as e:
        print(f"[token] toml load failed: {e}")
    raise RuntimeError("No MotherDuck token found in env or motherduck.local.toml")


def _strip_use(sql: str) -> str:
    lines = [ln for ln in sql.splitlines() if not ln.strip().upper().startswith("USE ")]
    return "\n".join(lines)


def _run_rows(con, sql: str, label: str) -> tuple[list[dict], list[str]] | tuple[None, None]:
    clean = _strip_use(sql).strip()
    while clean.endswith(";"):
        clean = clean[:-1].strip()
    print(f"  [query] {label} ({len(clean)} chars)…")
    try:
        rel = con.execute(clean)
        rows_raw = rel.fetchall()
        cols = [d[0] for d in rel.description]
        rows = [dict(zip(cols, row)) for row in rows_raw]
        print(f"  [query] {label} — {len(rows)} rows")
        return rows, cols
    except Exception as e:
        print(f"  [ERROR] {label}: {e}")
        traceback.print_exc()
        return None, None


def _write_csv(path: Path, rows: list, fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  [csv] wrote {len(rows)} rows → {path.relative_to(REPO_ROOT)}")


def _apply_provenance(con, dry_run: bool) -> None:
    print("[provenance] DELETE + INSERT mig_234_table1_refresh_v15 …")
    if dry_run:
        print("[provenance] dry-run; skipping execution")
        return
    con.execute(
        "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = 'mig_234_table1_refresh_v15'"
    )
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
        VALUES (
          'mig_234_table1_refresh_v15',
          CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
          CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE),
          'lane_m_mig234_csv_refresh_methods_bundle_v15_table1_to_table5_cohort_flow',
          '0',
          '0',
          '0',
          '0 | mig_234: regenerated manuscript_outputs/v1_0_20260501 CSV bundle via semantic_publication '
          || 'safe views + LN patient-safe rollup + recurrence-safe filter + supplemental canonical joins '
          || 'documented in docs/Methods_thyroid_canonical_pub_v1_0_20260501.md'
        )
        """
    )
    print("[provenance] done.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--apply-provenance",
        action="store_true",
        help="Execute DELETE/INSERT in qc_framework_v1/migrations/234_table1_csv_refresh_20260501.sql "
        "(omit USE lines; mutates manuscript_workspace.cpm_reconciliation_provenance_v1).",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print targets only; no MotherDuck calls.")
    args = ap.parse_args()

    if args.dry_run:
        print("[dry-run] would write:")
        for _, dest in OUTPUT_MAP:
            print(f"  {OUT_DIR / dest}")
        return 0

    token = _get_token()
    os.environ["motherduck_token"] = token

    import duckdb

    print(f"[connect] duckdb version={duckdb.__version__}")
    con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={token}")
    con.execute("USE thyroid_canonical_publication_v1_0")

    fail = False
    for src_name, dest_name in OUTPUT_MAP:
        sql_path = MIG234_SQL_DIR / src_name
        if not sql_path.is_file():
            print(f"[missing] {sql_path}")
            fail = True
            continue
        sql_text = sql_path.read_text(encoding="utf-8")
        rows, cols = _run_rows(con, sql_text, src_name)
        if rows is None or cols is None:
            fail = True
            continue
        _write_csv(OUT_DIR / dest_name, rows, cols)

    if args.apply_provenance:
        _apply_provenance(con, dry_run=False)

    con.close()

    sha = "unknown"
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip()
    except Exception:
        pass
    print(f"\n[done] mig_234 CSV bundle complete (git HEAD={sha}).")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
