#!/usr/bin/env python3
"""mig_273 — Snowflake-equivalent thin M038 cohort view on MotherDuck (main schema).

Creates main.cohort_m038_massive_goiter_v1 (mirror of Snowflake COHORT_M038_MASSIVE_GOITER).
See qc_framework_v1/migrations/273_cohort_m038_view_20260502.sql .

Usage:
  .venv/bin/python scripts/mig_273_m038_cohort_mirror.py --dry-run
  .venv/bin/python scripts/mig_273_m038_cohort_mirror.py --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

MIG_SQL = REPO_ROOT / "qc_framework_v1/migrations/273_cohort_m038_view_20260502.sql"
OUT_LOG = REPO_ROOT / "scripts/output/mig_273_apply_log.txt"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.apply == args.dry_run:
        print("Specify exactly one of --apply | --dry-run", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    con = connect_locked()
    log_lines: list[str] = []

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log(f"mig_273 started utc {stamp}")

    if not MIG_SQL.is_file():
        log(f"ABORT: missing {MIG_SQL}")
        OUT_LOG.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        con.close()
        return 1

    probe_sql = """
SELECT
  CASE
    WHEN gland_weight_final_g IS NULL THEN 'unknown'
    WHEN gland_weight_final_g >= 200 THEN 'massive_200g_plus'
    WHEN gland_weight_final_g >= 50 THEN 'moderate_50_to_199g'
    ELSE 'small_under_50g'
  END AS weight_bucket,
  COUNT(*)::BIGINT AS n
FROM main.canonical_patient_master
GROUP BY 1 ORDER BY 2 DESC
"""
    log("--- Probe weight_bucket from CPM (same CASE as view) ---")
    probe = con.execute(probe_sql).fetchall()
    for row in probe:
        log(f"  {row[0]}: {row[1]:,}")

    if args.dry_run:
        log("--dry-run: no CREATE VIEW, no signoff")
        OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
        OUT_LOG.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        con.close()
        return 0

    body = MIG_SQL.read_text(encoding="utf-8")
    # Strip leading comments/use; executor already locked to publication main.
    lines = []
    for line in body.splitlines():
        s = line.strip()
        if s.startswith("--"):
            continue
        if s.upper().startswith("USE "):
            continue
        lines.append(line)
    ddl = "\n".join(lines).strip()
    if not ddl:
        log("ABORT: empty DDL after stripping USE/comments")
        OUT_LOG.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        con.close()
        return 1

    log("--- APPLY: CREATE OR REPLACE VIEW main.cohort_m038_massive_goiter_v1 ---")
    con.execute(ddl)

    nrow = int(
        con.execute(
            "SELECT COUNT(*) FROM main.cohort_m038_massive_goiter_v1"
        ).fetchone()[0]
    )
    log(f"VERIFY: view rowcount = {nrow} (expect 10871)")
    if nrow != 10871:
        log("WARN: rowcount invariant mismatch")

    log("--- VERIFY: weight_bucket distribution ---")
    for row in con.execute(
        """SELECT weight_bucket, COUNT(*)::BIGINT AS n
           FROM main.cohort_m038_massive_goiter_v1
           GROUP BY 1 ORDER BY 2 DESC"""
    ).fetchall():
        log(f"  {row[0]}: {row[1]:,}")

    dup = int(
        con.execute(
            "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_273'"
        ).fetchone()[0]
    )
    if dup == 0:
        con.execute(
            """
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_273',
  CAST(now() AS TIMESTAMP),
  'logan_via_cursor',
  'Built main.cohort_m038_massive_goiter_v1 view (≥200g threshold). Mirror of Snowflake COHORT_M038_MASSIVE_GOITER. Used by M038 Massive Goiter Definition Paper + downstream complications audit.'
)
"""
        )
        log("INSERT signoff_migration mig_273 OK")
    else:
        log(f"SKIP: signoff_migration already has mig_273 (rows={dup})")

    OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
    OUT_LOG.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    log(f"wrote {OUT_LOG.relative_to(REPO_ROOT)}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
