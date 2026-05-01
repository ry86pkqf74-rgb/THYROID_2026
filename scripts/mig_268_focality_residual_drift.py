#!/usr/bin/env python3
"""mig_268 — residual path_synoptics.tumor_focality cleanup (mig_261 long tail).

Normalizes a handful of literals missed by mig_261 LOWER/TRIM/CHR(10) only:
Multifocal/Unifocal casing, trailing asterisk, trailing space, multifocal+newline.

Archive: ``Thyroid 2026 UPdated``.archive_pub_v1_0.path_synoptics_pre_mig268_20260502

Artifacts:
  qc_framework_v1/migrations/268_focality_residual_20260502.sql
  scripts/output/mig_268_apply_log.txt

Usage:
  .venv/bin/python scripts/mig_268_focality_residual_drift.py --dry-run
  .venv/bin/python scripts/mig_268_focality_residual_drift.py --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

SCRIPT_DIR = __file__.rsplit("/", 1)[0]
REPO_ROOT = SCRIPT_DIR.rsplit("/", 1)[0]
sys.path.insert(0, SCRIPT_DIR)

ARCHIVE_SCHEMA = '"Thyroid 2026 UPdated".archive_pub_v1_0'

RESIDUAL_PREDICATE = """
tumor_focality IN (
  'Multifocal', 'Unifocal', 'unifocal*', 'unifocal ', 'multifocal ',
  'multifocal' || CHR(10)
)
"""


def _run(con, sql: str):
    return con.execute(sql).fetchdf()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.apply and args.dry_run:
        print("Use only one of --apply or --dry-run", file=sys.stderr)
        return 2
    if not args.apply and not args.dry_run:
        print("Specify --dry-run or --apply", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    con = connect_locked()
    log_lines: list[str] = []
    utc_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    log(f"mig_268 started utc {utc_stamp}")

    probe = _run(
        con,
        f"""
SELECT tumor_focality, COUNT(*) AS n
FROM main.path_synoptics
WHERE {RESIDUAL_PREDICATE}
GROUP BY 1 ORDER BY n DESC
""",
    )
    log(f"Residual focality probe:\n{probe.to_string(index=False)}")
    total_residual = int(probe["n"].sum()) if len(probe) else 0

    signed_n = int(
        con.execute(
            "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_268'"
        ).fetchone()[0]
    )

    apply_path = f"{REPO_ROOT}/scripts/output/mig_268_apply_log.txt"

    if args.dry_run:
        log(f"Dry-run: would UPDATE {total_residual} row(s); signoff present={signed_n}")
        with open(apply_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(log_lines) + "\n")
        log(f"Wrote {apply_path}")
        con.close()
        return 0

    if signed_n > 0:
        log("signoff_migration mig_268 already present — refusing duplicate --apply")
        con.close()
        return 3

    if total_residual == 0:
        log("No residual rows match predicate — nothing to apply (already clean).")
        with open(apply_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(log_lines) + "\n")
        log(f"Wrote {apply_path}")
        con.close()
        return 0

    archive_tbl = "path_synoptics_pre_mig268_20260502"
    log(f"CREATE OR REPLACE TABLE {ARCHIVE_SCHEMA}.{archive_tbl}")
    con.execute(
        f"""
CREATE OR REPLACE TABLE {ARCHIVE_SCHEMA}.{archive_tbl} AS
SELECT research_id, tumor_focality
FROM main.path_synoptics
WHERE {RESIDUAL_PREDICATE}
"""
    )

    log("UPDATE main.path_synoptics (strip *, CHR(10), LOWER, TRIM)")
    con.execute(
        f"""
UPDATE main.path_synoptics
SET tumor_focality = LOWER(TRIM(REPLACE(REPLACE(tumor_focality, '*', ''), CHR(10), '')))
WHERE {RESIDUAL_PREDICATE}
"""
    )

    left = _run(
        con,
        f"""
SELECT tumor_focality, COUNT(*) AS n
FROM main.path_synoptics
WHERE {RESIDUAL_PREDICATE}
GROUP BY 1
""",
    )
    if len(left):
        log(f"FAIL: residual literals remain:\n{left.to_string(index=False)}")
        con.close()
        return 1

    top5 = _run(
        con,
        """
SELECT tumor_focality, COUNT(*) AS n
FROM main.path_synoptics
WHERE tumor_focality IS NOT NULL
GROUP BY 1 ORDER BY n DESC LIMIT 5
""",
    )
    log(f"Top focality values post-fix:\n{top5.to_string(index=False)}")

    con.execute(
        """INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
           VALUES (
             'mig_268', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
             'Cleared residual focality drift values from mig_261 long tail (asterisk + case + whitespace).'
           )"""
    )
    log("INSERT signoff_migration mig_268 OK")

    with open(apply_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(log_lines) + "\n")
    log(f"Wrote {apply_path}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
