#!/usr/bin/env python3
"""mig_257 — clamp followup_years to overall_survival_years for deceased CPM rows.

Snowflake validation (round 2): ~100 patients had death_occurred TRUE but
followup_years > overall_survival_years (mostly <1y gap — rounding / last-contact
after death bookkeeping). Rule: stale rollup repair — set
followup_years = overall_survival_years for that cohort.

Archive: \"Thyroid 2026 UPdated\".archive_pub_v1_0.cpm_pre_mig257_20260501

Usage:
  .venv/bin/python scripts/mig_257_followup_post_death.py --dry-run
  .venv/bin/python scripts/mig_257_followup_post_death.py --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

SCRIPT_DIR = __file__.rsplit("/", 1)[0]
REPO_ROOT = SCRIPT_DIR.rsplit("/", 1)[0]
sys.path.insert(0, SCRIPT_DIR)

ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = f"{ARCHIVE_DB}.archive_pub_v1_0"

COUNT_BAD = """
SELECT COUNT(*) AS n
FROM main.canonical_patient_master
WHERE COALESCE(death_occurred, FALSE) = TRUE
  AND followup_years IS NOT NULL
  AND overall_survival_years IS NOT NULL
  AND followup_years > overall_survival_years
"""


def _run(con, sql: str):
    return con.execute(sql).fetchdf()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Execute writes")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only")
    args = parser.parse_args()
    if args.apply and args.dry_run:
        print("Use only one of --apply or --dry-run", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    con = connect_locked()
    log_lines: list[str] = []
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    log(f"mig_257 started utc {stamp}")

    n_bad = int(_run(con, COUNT_BAD)["n"].iloc[0])
    log(f"PRE  deceased followup_years > overall_survival_years: {n_bad}")

    if args.dry_run or not args.apply:
        log("Dry-run / no --apply: skipping DDL+DML.")
        pre_path = f"{REPO_ROOT}/scripts/output/mig_257_pre_snapshot_log.txt"
        with open(pre_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(log_lines) + "\n")
        log(f"Wrote {pre_path}")
        con.close()
        return 0

    arch = f"{ARCHIVE_SCHEMA}.cpm_pre_mig257_20260501"
    con.execute(f"""
CREATE OR REPLACE TABLE {arch} AS
SELECT
  research_id,
  followup_years,
  overall_survival_years,
  death_occurred,
  vital_status,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig257_snapshot_ts
FROM main.canonical_patient_master
WHERE COALESCE(death_occurred, FALSE) = TRUE
  AND followup_years IS NOT NULL
  AND overall_survival_years IS NOT NULL
  AND followup_years > overall_survival_years
""")
    snap_n = int(con.execute(f"SELECT COUNT(*) FROM {arch}").fetchone()[0])
    log(f"Archive snapshot {arch} n={snap_n}")
    if snap_n != n_bad:
        log(f"WARN: snapshot row count {snap_n} != pre-count {n_bad}")

    con.execute("""
UPDATE main.canonical_patient_master
SET
  followup_years = overall_survival_years,
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE COALESCE(death_occurred, FALSE) = TRUE
  AND followup_years IS NOT NULL
  AND overall_survival_years IS NOT NULL
  AND followup_years > overall_survival_years
""")
    log("UPDATE canonical_patient_master: followup_years clamped to overall_survival_years")

    n_after = int(_run(con, COUNT_BAD)["n"].iloc[0])
    log(f"POST deceased followup > survival (expect 0): {n_after}")

    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    log(f"CPM rows/distinct_rid: {row[0]} / {row[1]}")

    if n_after != 0:
        log("FAIL: post-death followup gap not cleared")
        con.close()
        return 1
    if row[0] != 10871 or row[1] != 10871:
        log("FAIL: CPM row invariant broken")
        con.close()
        return 1

    summary = (
        "mig_257: followup_years set to overall_survival_years for deceased patients where "
        f"followup exceeded survival (pre n={n_bad}; post mismatch n={n_after}). "
        "Closes CF-mig257-FU-POST-DEATH."
    )
    con.execute("""
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?)
""", ["mig_257", "cursor_composer_mig257", summary])

    log("INSERT signoff_migration mig_257 OK")
    log("mig_257 PASS — re-run Snowflake: "
        "COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT WHERE DEATH_OCCURRED AND "
        "FOLLOWUP_YEARS > OVERALL_SURVIVAL_YEARS (expect 0)")

    out_apply = f"{REPO_ROOT}/scripts/output/mig_257_apply_log.txt"
    with open(out_apply, "w", encoding="utf-8") as fh:
        fh.write("\n".join(log_lines) + "\n")
    log(f"Wrote {out_apply}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
