#!/usr/bin/env python3
"""mig_255 — reconcile any_recurrence_flag vs time_to_recurrence_days on CPM.

Finding (MotherDuck 2026-05-01): 740 rows had any_recurrence_flag=FALSE but
time_to_recurrence_days NOT NULL. Drift analysis shows canonical_recurrence_v1
(SSOT for TTR via mig_139) also has recurrence_confirmed=FALSE for those rows —
mostly biochemical TG definitions (persistent/rising Tg) — while HYBRID
any_recurrence_flag (mig_163b) correctly stays FALSE except path_proven union.

Disposition:
  (B') NULL canonical_recurrence_v1.time_to_recurrence_days when
       recurrence_confirmed=FALSE AND TTR IS NOT NULL AND patient is NOT
       recurrence_status_final='path_proven' on canonical_recurrence_resolved_v1.
  (A') SET canonical_patient_master.any_recurrence_flag=TRUE for ALL patients
       with path_proven on resolved_v1 who still have the flag FALSE (46 rows).

Then resync PM.time_to_recurrence_days FROM canonical_recurrence_v1 (mig_139 pattern).

Archives to \"Thyroid 2026 UPdated\".archive_pub_v1_0 per AGENTS.md.

Usage:
  .venv/bin/python scripts/mig_255_recurrence_flag_timing.py --dry-run
  .venv/bin/python scripts/mig_255_recurrence_flag_timing.py --apply
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
PUB = "thyroid_canonical_publication_v1_0"

QUERIES = {
    "n_mismatch_pm": """
SELECT COUNT(*) AS n
FROM main.canonical_patient_master
WHERE COALESCE(any_recurrence_flag, FALSE) = FALSE
  AND time_to_recurrence_days IS NOT NULL
""",
    "n_cr_null_ttr_candidates": """
SELECT COUNT(*) AS n
FROM main.canonical_recurrence_v1 cr
WHERE COALESCE(cr.recurrence_confirmed, FALSE) = FALSE
  AND cr.time_to_recurrence_days IS NOT NULL
  AND CAST(cr.research_id AS VARCHAR) NOT IN (
    SELECT CAST(research_id AS VARCHAR)
    FROM main.canonical_recurrence_resolved_v1
    WHERE recurrence_status_final = 'path_proven'
  )
""",
    "n_path_proven_any_false": """
SELECT COUNT(*) AS n
FROM main.canonical_patient_master pm
WHERE COALESCE(pm.any_recurrence_flag, FALSE) = FALSE
  AND CAST(pm.research_id AS VARCHAR) IN (
    SELECT CAST(research_id AS VARCHAR)
    FROM main.canonical_recurrence_resolved_v1
    WHERE recurrence_status_final = 'path_proven'
  )
""",
}


def _run(con, sql: str):
    return con.execute(sql).fetchdf()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Execute writes")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only")
    args = parser.parse_args()
    if args.apply and args.__dict__.get("dry_run"):
        print("Use only one of --apply or --dry-run", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    con = connect_locked()
    log_lines: list[str] = []
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    log(f"mig_255 started utc {stamp}")

    pre_path = f"{REPO_ROOT}/scripts/output/mig_255_pre_snapshot_log.txt"

    for name, sql in QUERIES.items():
        n = int(_run(con, sql)["n"].iloc[0])
        log(f"PRE  {name}: {n}")

    n_bad = int(_run(con, QUERIES["n_mismatch_pm"])["n"].iloc[0])
    n_cr_upd = int(_run(con, QUERIES["n_cr_null_ttr_candidates"])["n"].iloc[0])
    n_flag = int(_run(con, QUERIES["n_path_proven_any_false"])["n"].iloc[0])

    if args.dry_run or not args.apply:
        log("Dry-run / no --apply: skipping DDL+DML.")
        with open(pre_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(log_lines) + "\n")
        log(f"Wrote {pre_path}")
        con.close()
        return 0

    with open(pre_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(log_lines) + "\n")
    log(f"Wrote {pre_path}")

    # --- Snapshots ---
    arch_pm = f'{ARCHIVE_SCHEMA}.canonical_patient_master_pre_mig255_20260501'
    con.execute(f"""
CREATE OR REPLACE TABLE {arch_pm} AS
SELECT
  research_id,
  any_recurrence_flag,
  recurrence_confirmed,
  time_to_recurrence_days,
  recurrence_definition,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig255_snapshot_ts
FROM main.canonical_patient_master
WHERE COALESCE(any_recurrence_flag, FALSE) = FALSE
  AND time_to_recurrence_days IS NOT NULL
""")
    snap_n = int(con.execute(f"SELECT COUNT(*) FROM {arch_pm}").fetchone()[0])
    log(f"Archive snapshot {arch_pm} n={snap_n}")

    arch_cr = f'{ARCHIVE_SCHEMA}.canonical_recurrence_v1_pre_mig255_20260501'
    con.execute(f"""
CREATE OR REPLACE TABLE {arch_cr} AS
SELECT
  cr.*,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig255_snapshot_ts
FROM main.canonical_recurrence_v1 cr
WHERE COALESCE(cr.recurrence_confirmed, FALSE) = FALSE
  AND cr.time_to_recurrence_days IS NOT NULL
""")
    log(f"Archive snapshot {arch_cr} n="
        f"{con.execute(f'SELECT COUNT(*) FROM {arch_cr}').fetchone()[0]}")

    # --- Upstream: clear TTR where not confirmed and not path_proven ---
    con.execute("""
UPDATE main.canonical_recurrence_v1 AS cr
SET time_to_recurrence_days = NULL
WHERE COALESCE(cr.recurrence_confirmed, FALSE) = FALSE
  AND cr.time_to_recurrence_days IS NOT NULL
  AND CAST(cr.research_id AS VARCHAR) NOT IN (
    SELECT CAST(research_id AS VARCHAR)
    FROM main.canonical_recurrence_resolved_v1
    WHERE recurrence_status_final = 'path_proven'
  )
""")
    log("UPDATE canonical_recurrence_v1: cleared time_to_recurrence_days "
        f"(expect {n_cr_upd} rows)")

    # --- PM: resync TTR from CR (full join by rid; mig_139 pattern) ---
    con.execute("""
UPDATE main.canonical_patient_master AS pm
SET time_to_recurrence_days = cr.time_to_recurrence_days
FROM main.canonical_recurrence_v1 AS cr
WHERE CAST(pm.research_id AS VARCHAR) = CAST(cr.research_id AS VARCHAR)
""")
    log("UPDATE canonical_patient_master: time_to_recurrence_days FROM canonical_recurrence_v1")

    # --- HYBRID flag repair (path_proven union per mig_163b) ---
    con.execute("""
UPDATE main.canonical_patient_master AS pm
SET any_recurrence_flag = TRUE
WHERE COALESCE(pm.any_recurrence_flag, FALSE) = FALSE
  AND CAST(pm.research_id AS VARCHAR) IN (
    SELECT CAST(research_id AS VARCHAR)
    FROM main.canonical_recurrence_resolved_v1
    WHERE recurrence_status_final = 'path_proven'
  )
""")
    log(f"UPDATE canonical_patient_master: any_recurrence_flag=TRUE for path_proven gap "
        f"(expect ~{n_flag} rows)")

    # --- Registry notes ---
    note = (
        " | mig_255 (2026-05-01): Closed CF-mig255-RECUR-FLAG-TIMING. "
        "Snowflake Prompt3 mismatch any_recurrence_flag=FALSE vs time_to_recurrence_days NOT NULL. "
        "Root cause: canonical_recurrence_v1 kept biochemical TTR while recurrence_confirmed=FALSE; "
        "PM mirrored CR (mig_139) while any_recurrence_flag followed mig_163b HYBRID. "
        "Fix: NULL CR.time_to_recurrence_days when NOT confirmed AND NOT path_proven on resolved_v1; "
        "resync PM TTR from CR; SET any_recurrence_flag=TRUE for remaining path_proven FALSE gaps."
    )
    con.execute(f"""
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '') || '{note.replace("'", "''")}',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('any_recurrence_flag', 'time_to_recurrence_days')
""")

    # --- Verify ---
    n_after = int(_run(con, QUERIES["n_mismatch_pm"])["n"].iloc[0])
    log(f"POST n_mismatch_pm (expect 0): {n_after}")

    n_cr_residual = int(con.execute("""
SELECT COUNT(*) FROM main.canonical_recurrence_v1
WHERE COALESCE(recurrence_confirmed, FALSE) = FALSE
  AND time_to_recurrence_days IS NOT NULL
""").fetchone()[0])
    log(f"POST CR recurrence_confirmed=FALSE AND TTR NOT NULL (path_proven-only OK): {n_cr_residual}")

    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    log(f"CPM rows/distinct_rid: {row[0]} / {row[1]}")

    if n_after != 0:
        log("FAIL: mismatch cohort not cleared")
        con.close()
        return 1

    if row[0] != 10871 or row[1] != 10871:
        log("FAIL: CPM row invariant broken")
        con.close()
        return 1

    summary = (
        "mig_255: NULL biochemical TTR on canonical_recurrence_v1 when recurrence_confirmed=FALSE "
        f"(excl path_proven); resync PM TTR; flip any_recurrence_flag for path_proven gaps (~{n_flag}). "
        f"Pre mismatch PM={n_bad}; post mismatch PM={n_after}. "
        "Closes CF-mig255-RECUR-FLAG-TIMING."
    )
    con.execute("""
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?)
""", ["mig_255", "cursor_composer_mig255", summary])

    log("INSERT signoff_migration mig_255 OK")
    log("mig_255 PASS")

    out_apply = f"{REPO_ROOT}/scripts/output/mig_255_apply_log.txt"
    with open(out_apply, "w", encoding="utf-8") as fh:
        fh.write("\n".join(log_lines) + "\n")

    md_path = f"{REPO_ROOT}/scripts/output/mig_255_report_20260501.md"
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("# mig_255 recurrence flag vs timing\n\n")
        fh.write(summary + "\n\n")
        fh.write("## Pre probes\n\n")
        fh.write(f"- PM mismatch (flag FALSE, TTR non-null): **{n_bad}**\n")
        fh.write(f"- CR candidates for TTR NULL (not path_proven): **{n_cr_upd}**\n")
        fh.write(f"- path_proven + any_recurrence_flag FALSE: **{n_flag}**\n\n")
        fh.write("## Lineage\n\n")
        fh.write(
            "- `time_to_recurrence_days` on PM: **canonical_recurrence_v1** (mig_139 resync).\n"
            "- `any_recurrence_flag`: **mig_163b HYBRID** — "
            "`recurrence_confirmed` UNION `path_proven`.\n"
            "- Table `canonical_recurrence_events_v1` **does not exist** on publication DB; "
            "use `canonical_recurrence_v1` + `canonical_recurrence_resolved_v1`.\n\n"
        )
        fh.write("## Post verify\n\n")
        fh.write(f"- PM mismatch after fix: **{n_after}** (expect 0)\n")
        fh.write(f"- CR residual FALSE+TTR: **{n_cr_residual}** (expected ~path_proven biochemical)\n")

    log(f"Wrote {out_apply}")
    log(f"Wrote {md_path}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
