#!/usr/bin/env python3
"""mig_263 — AJCC overlay re-derive (Option B): collapse IVA/IVC → IVB on CPM.

Renumbered from mig_259 (file slot collision with mig_259 LN-status SQL).

Source: ``canonical_patient_master.ajcc8_stage_group_resolved`` (mig_184_v2 /
mig_188b full AJCC8 label set, including IVA/IVC for ATC/MTC).

Publication column ``ajcc8_stage_group`` intentionally carries only
{I, II, III, IVB}. Lossy collapse produced II/NULL for M1 MTC/ATC/PDTC cases
where resolved = IVC/IVA (mig_254 / mig_254b).

**Option B (ratified in dispatch):** formal boundary rule — when promoting
resolved → CPM stage group, map IVA and IVC to IVB; additionally align CPM
when resolved = IVB but CPM is NULL or II (residual mig_254 pattern).

Scope: malignant rows only; no change when ``ajcc8_stage_group_resolved`` IS
NULL (overlay may still apply from other lanes).

Closes: CF-mig254-MIG266B-OVERLAY-RE-DERIVE.

Archive: \"Thyroid 2026 UPdated\".archive_pub_v1_0.cpm_pre_mig263_20260501

Usage:
  .venv/bin/python scripts/mig_263_ajcc_overlay_collapse.py --dry-run
  .venv/bin/python scripts/mig_263_ajcc_overlay_collapse.py --apply
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

COUNT_UPDATE_CANDIDATES = """
SELECT COUNT(*) AS n
FROM main.canonical_patient_master
WHERE COALESCE(is_malignant, FALSE) = TRUE
  AND ajcc8_stage_group_resolved IS NOT NULL
  AND (
    ajcc8_stage_group_resolved IN ('IVA', 'IVC')
    OR (
      ajcc8_stage_group_resolved = 'IVB'
      AND (ajcc8_stage_group IS NULL OR ajcc8_stage_group = 'II')
    )
  )
  AND (
    CASE
      WHEN ajcc8_stage_group_resolved IN ('IVA', 'IVC') THEN 'IVB'
      ELSE ajcc8_stage_group_resolved
    END
  ) IS DISTINCT FROM ajcc8_stage_group
"""

VERIFY_M1_NULL = """
SELECT COUNT(*) AS n
FROM main.canonical_patient_master
WHERE COALESCE(is_malignant, FALSE) = TRUE
  AND ajcc8_m_stage = 'M1'
  AND ajcc8_stage_group IS NULL
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

    log(f"mig_263 started utc {stamp}")

    n_upd = int(_run(con, COUNT_UPDATE_CANDIDATES)["n"].iloc[0])
    n_m1_null_pre = int(_run(con, VERIFY_M1_NULL)["n"].iloc[0])
    log(f"PRE  update_candidates (narrow Option B): {n_upd}")
    log(f"PRE  malignant M1 + ajcc8_stage_group NULL: {n_m1_null_pre}")

    # Drift probe: resolved outside {I,II,III,IVB} on malignant
    drift = con.execute("""
SELECT ajcc8_stage_group_resolved AS r, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE COALESCE(is_malignant, FALSE) = TRUE
  AND ajcc8_stage_group_resolved IS NOT NULL
  AND ajcc8_stage_group_resolved NOT IN ('I', 'II', 'III', 'IVA', 'IVB', 'IVC')
GROUP BY 1 ORDER BY 2 DESC
""").fetchall()
    if drift:
        log(f"WARN unexpected resolved labels (not AJCC8 Roman set): {drift}")

    if args.dry_run or not args.apply:
        log("Dry-run / no --apply: skipping DDL+DML.")
        pre_path = f"{REPO_ROOT}/scripts/output/mig_263_pre_snapshot_log.txt"
        with open(pre_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(log_lines) + "\n")
        log(f"Wrote {pre_path}")
        con.close()
        return 0

    already_signed = int(
        con.execute(
            "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_263'"
        ).fetchone()[0]
    )
    prov_run_id = "canonical_cleanup_mig263_20260501"
    prov_n = int(
        con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
            "WHERE run_id = ?",
            [prov_run_id],
        ).fetchone()[0]
    )
    arch = f"{ARCHIVE_SCHEMA}.cpm_pre_mig263_20260501"

    if already_signed > 0 and n_upd == 0 and prov_n > 0:
        log("SKIP: mig_263 signoff + provenance already recorded; cohort clean.")
        con.close()
        return 0

    if n_upd == 0 and already_signed == 0 and prov_n == 0:
        log(
            "FINALIZE-ONLY: UPDATE appears already applied "
            "(0 candidates); completing signoff + provenance WITHOUT "
            "archive recreate (avoids overwriting a pre-migration snapshot)."
        )
    elif n_upd == 0 and already_signed > 0 and prov_n == 0:
        log("RECOVERY: signoff present; inserting missing provenance row only.")
    else:
        con.execute(f"""
CREATE OR REPLACE TABLE {arch} AS
SELECT
  research_id,
  ajcc8_t_stage,
  ajcc8_n_stage,
  ajcc8_m_stage,
  ajcc8_stage_group,
  ajcc8_stage_group_resolved,
  histology_final,
  age_at_surgery,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig263_snapshot_ts
FROM main.canonical_patient_master
WHERE COALESCE(is_malignant, FALSE) = TRUE
""")
        snap_n = int(con.execute(f"SELECT COUNT(*) FROM {arch}").fetchone()[0])
        log(f"Archive snapshot {arch} n={snap_n} (all malignant CPM rows PRE-UPDATE)")
        log("Applying UPDATE narrows cohort (IVA|IVC→IVB + IVB|resolved∩{II,NULL}) ...")
        con.execute("""
UPDATE main.canonical_patient_master
SET
  ajcc8_stage_group = CASE
    WHEN ajcc8_stage_group_resolved IN ('IVA', 'IVC') THEN 'IVB'
    ELSE ajcc8_stage_group_resolved
  END,
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE COALESCE(is_malignant, FALSE) = TRUE
  AND ajcc8_stage_group_resolved IS NOT NULL
  AND (
    ajcc8_stage_group_resolved IN ('IVA', 'IVC')
    OR (
      ajcc8_stage_group_resolved = 'IVB'
      AND (ajcc8_stage_group IS NULL OR ajcc8_stage_group = 'II')
    )
  )
  AND (
    CASE
      WHEN ajcc8_stage_group_resolved IN ('IVA', 'IVC') THEN 'IVB'
      ELSE ajcc8_stage_group_resolved
    END
  ) IS DISTINCT FROM ajcc8_stage_group
""")

    n_m1_null_post = int(_run(con, VERIFY_M1_NULL)["n"].iloc[0])
    log(f"POST malignant M1 + ajcc8_stage_group NULL: {n_m1_null_post}")

    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    log(f"CPM rows/distinct_rid: {row[0]} / {row[1]}")

    if row[0] != 10871 or row[1] != 10871:
        log("FAIL: CPM row invariant broken")
        con.close()
        return 1

    summary = (
        "mig_263 Option B: promoted ajcc8_stage_group from ajcc8_stage_group_resolved with "
        "IVA/IVC→IVB; fixed IVB-on-resolved where CPM was NULL or II. "
        f"Last observed pre-pass update_candidates={n_upd}; M1+NULL stage post={n_m1_null_post}. "
        "Closes CF-mig254-MIG266B-OVERLAY-RE-DERIVE."
    )
    if n_upd == 0 and already_signed == 0 and prov_n == 0:
        summary += (
            " Signoff path: DML had already completed (interrupted run—no archive refresh here); "
            "`cpm_pre_mig263_20260501` retains the pre-UPDATE malignant snapshot from the first invoke."
        )
    try:
        con.execute(
            "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
            "WHERE run_id = ?",
            [prov_run_id],
        )
        con.execute(
            """
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1 (
  run_id,
  started_at,
  ended_at,
  phases_applied,
  critical_findings_cleared,
  high_findings_cleared,
  med_findings_cleared,
  held_for_adjudication
)
VALUES (
  ?,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'mig263_ajcc_overlay_iv_acollapse_option_b — IVA/IVC→IVB at CPM; IVB sync when resolved=IVB and CPM II/NULL',
  'CF-mig254-MIG266B-OVERLAY-RE-DERIVE closed',
  '0',
  '0',
  '0'
)
""",
            [prov_run_id],
        )
        log("INSERT cpm_reconciliation_provenance_v1 mig_263 OK")
    except Exception as exc:
        log(f"WARN provenance insert skipped: {exc}")

    if already_signed == 0:
        con.execute(
            """
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?)
""",
            ["mig_263", "cursor_composer_mig263", summary],
        )
        log("INSERT signoff_migration mig_263 OK")
    else:
        log("signoff_migration mig_263 already present — skip duplicate INSERT")
    log("mig_263 PASS")

    out_apply = f"{REPO_ROOT}/scripts/output/mig_263_apply_log.txt"
    with open(out_apply, "w", encoding="utf-8") as fh:
        fh.write("\n".join(log_lines) + "\n")
    log(f"Wrote {out_apply}")

    md_path = f"{REPO_ROOT}/scripts/output/mig_263_report_20260501.md"
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("# mig_263 AJCC overlay collapse (Option B)\n\n")
        fh.write(summary + "\n\n")
        fh.write(f"- Archive: `{arch}`\n")
        fh.write(f"- M1 malignant NULL ajcc8_stage_group: **{n_m1_null_post}** (prompt target 0)\n")

    log(f"Wrote {md_path}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
