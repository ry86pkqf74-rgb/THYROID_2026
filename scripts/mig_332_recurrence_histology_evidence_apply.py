#!/usr/bin/env python3
"""
mig_332 — Restore recurrence_histology + recurrence_evidence_source on main.canonical_recurrence_v1

Applies qc_framework_v1/migrations/332_recurrence_v1_histology_evidence_restore_20260514.sql
via scripts/_md_connect.connect_locked.

Usage:
  .venv/bin/python scripts/mig_332_recurrence_histology_evidence_apply.py --validate-only
  .venv/bin/python scripts/mig_332_recurrence_histology_evidence_apply.py --apply
  .venv/bin/python scripts/mig_332_recurrence_histology_evidence_apply.py --apply --force-apply

Post-apply (BigQuery feeder + reconciliation):
  bq_migrations/mig_101_canonical_recurrence_v1_bq_native_histology_evidence_20260514.sql
  (BQ-native rebuild of recurrence_histology / recurrence_evidence_source — replaces deprecated
  mig_100 parquet-from-archive path.)
  scripts/mig_332_recurrence_export_reconcile.py — optional MotherDuck reconcile only
  qc_framework_v1/migrations/333_recurrence_v1_bq_feeder_provenance_placeholder_20260514.sql (before BQ)
  qc_framework_v1/migrations/334_recurrence_v1_bq_feeder_provenance_post_mig100_20260514.sql (after BQ)
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402

SQL_PATH = (
    REPO_ROOT
    / "qc_framework_v1"
    / "migrations"
    / "332_recurrence_v1_histology_evidence_restore_20260514.sql"
)


def _split_sql_statements(sql_text: str) -> list[str]:
    statements = [s.strip() for s in re.split(r";\s*\n", sql_text) if s.strip()]
    out: list[str] = []

    def is_all_comment(stmt: str) -> bool:
        for line in stmt.splitlines():
            t = line.strip()
            if not t:
                continue
            if not t.startswith("--"):
                return False
        return True

    for s in statements:
        if is_all_comment(s):
            continue
        out.append(s)
    return out


def _already_signed(con) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_332'"
    ).fetchone()
    return row is not None and int(row[0]) > 0


def validate(con) -> None:
    cols = con.execute(
        "SELECT column_name FROM (DESCRIBE canonical_recurrence_v1)"
    ).fetchall()
    names = [r[0] for r in cols]
    n = con.execute("SELECT COUNT(*) FROM main.canonical_recurrence_v1").fetchone()[0]
    nh = con.execute(
        """SELECT COUNT(*) FROM main.canonical_recurrence_v1
           WHERE recurrence_histology IS NOT NULL
           AND TRIM(CAST(recurrence_histology AS VARCHAR)) <> ''"""
    ).fetchone()[0]
    ne = con.execute(
        """SELECT COUNT(*) FROM main.canonical_recurrence_v1
           WHERE recurrence_evidence_source IS NOT NULL
           AND TRIM(CAST(recurrence_evidence_source AS VARCHAR)) <> ''"""
    ).fetchone()[0]
    print(f"canonical_recurrence_v1 columns ({len(names)}): {names}")
    print(f"ROW COUNT: {n} (expect 10871)")
    print(f"recurrence_histology non-empty: {nh}")
    print(f"recurrence_evidence_source non-null/non-empty: {ne}")
    assert "recurrence_histology" in names and "recurrence_evidence_source" in names, (
        "missing clinical columns"
    )
    assert len(names) == 12, f"expected 12 columns, got {len(names)}"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true")
    p.add_argument("--validate-only", action="store_true")
    p.add_argument("--force-apply", action="store_true")
    args = p.parse_args()

    if not SQL_PATH.exists():
        sys.stderr.write(f"[ERROR] missing {SQL_PATH}\n")
        sys.exit(1)

    if not args.apply and not args.validate_only:
        sys.stderr.write("[ERROR] pass --apply and/or --validate-only\n")
        sys.exit(1)

    con = connect_locked()

    if args.apply:
        if _already_signed(con) and not args.force_apply:
            sys.stderr.write(
                "[STOP] mig_332 already in signoff_migration — use --force-apply\n"
            )
            con.close()
            sys.exit(1)
        if args.force_apply and _already_signed(con):
            con.execute("DELETE FROM main.signoff_migration WHERE mig_id = 'mig_332'")

        sql_text = SQL_PATH.read_text(encoding="utf-8")
        batches = _split_sql_statements(sql_text)
        print(f"[INFO] executing {len(batches)} batches")
        for i, stmt in enumerate(batches, 1):
            con.execute(stmt)
            print(f"  [ok] {i}/{len(batches)}")

    if args.validate_only or args.apply:
        validate(con)

    con.close()


if __name__ == "__main__":
    main()
