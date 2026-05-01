#!/usr/bin/env python3
"""mig_271 — NIFTP + AJCC stage sweep after mig_264b (MotherDuck publication DB).

Clears AJCC 8 patient-level stage (T/N/M/stage_group) for patients who are
IS_MALIGNANT=FALSE with NIFTP or follicular adenoma histology — NIFTP is not
staged under AJCC 8.

Archive: \"Thyroid 2026 UPdated\".archive_pub_v1_0.cpm_pre_mig271_20260502

Closes CF-mig264b-DOWNSTREAM-CASCADE.

Usage:
  .venv/bin/python scripts/mig_271_niftp_ajcc_sweep.py --dry-run
  .venv/bin/python scripts/mig_271_niftp_ajcc_sweep.py --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

SCRIPT_DIR = __file__.rsplit("/", 1)[0]
REPO_ROOT = SCRIPT_DIR.rsplit("/", 1)[0]
sys.path.insert(0, SCRIPT_DIR)

ARCHIVE_SCHEMA = '"Thyroid 2026 UPdated".archive_pub_v1_0'
ARCH_TBL = f"{ARCHIVE_SCHEMA}.cpm_pre_mig271_20260502"

_HIST_NIFTP_FA_SQL = """(
    histology_final = 'NIFTP'
    OR LOWER(TRIM(histology_final)) IN ('follicular adenoma', 'atypical follicular adenoma')
    OR histology_final ILIKE '%follicular adenoma%'
  )"""


def _run(con, sql: str):
    return con.execute(sql).fetchdf()


def _write_out(path: str, lines: list[str]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


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
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    log(f"mig_271 started utc {stamp}")

    dup = int(
        con.execute(
            "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_271'"
        ).fetchone()[0]
    )
    if dup > 0 and args.apply:
        log(f"SKIP: signoff_migration already has mig_271 (rows={dup})")
        _write_out(f"{REPO_ROOT}/scripts/output/mig_271_apply_log.txt", log_lines)
        con.close()
        return 0

    probe_targets = int(
        con.execute(
            f"""
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE is_malignant = FALSE
  AND ajcc8_stage_group IS NOT NULL
  AND {_HIST_NIFTP_FA_SQL}
"""
        ).fetchone()[0]
    )
    log(f"Probe: NIFTP/FA + is_malignant=FALSE + ajcc8_stage_group NOT NULL → {probe_targets}")

    malig = con.execute(
        """
SELECT
  COUNT(*) AS n_total,
  CAST(SUM(CASE WHEN COALESCE(is_malignant, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS n_malig,
  ROUND(100.0 * SUM(CASE WHEN COALESCE(is_malignant, FALSE) THEN 1 ELSE 0 END) / COUNT(*), 2) AS malig_pct_2dp,
  ROUND(100.0 * SUM(CASE WHEN COALESCE(is_malignant, FALSE) THEN 1 ELSE 0 END) / COUNT(*), 1) AS malig_pct_1dp
FROM main.canonical_patient_master
"""
    ).fetchone()
    log(
        "Cohort malignancy: "
        f"n_total={malig[0]}, n_malig={malig[1]}, "
        f"pct≈{malig[3]}% (1 dp) / {malig[2]}% (2 dp)"
    )

    for label, fq in (
        ("main.cohort_m037_ln_predictors", "main.cohort_m037_ln_predictors"),
        ("m037 metastasis cohort", "manuscript_workspace.cohort_m037_ln_metastasis_v1"),
    ):
        try:
            n_bad = int(
                con.execute(
                    f"""
SELECT COUNT(*) FROM {fq}
WHERE histology_final = 'NIFTP'
   OR histology_final ILIKE '%follicular adenoma%'
"""
                ).fetchone()[0]
            )
            log(f"Cohort check {label}: rows with NIFTP/FA string in histology_final = {n_bad} (expect 0)")
        except Exception as exc:
            log(f"Cohort check {label}: SKIP ({exc.__class__.__name__})")

    if args.dry_run:
        log("Dry-run OK — no writes.")
        _write_out(f"{REPO_ROOT}/scripts/output/mig_271_dry_run_{stamp.replace(':', '')}.txt", log_lines)
        log(f"Wrote scripts/output dry-run log.")
        con.close()
        return 0

    if probe_targets == 0:
        log("Nothing to UPDATE (probe_targets=0). Inserting signoff only.")
    else:
        log(f"CREATE OR REPLACE {ARCH_TBL}")
        con.execute(
            f"""
CREATE OR REPLACE TABLE {ARCH_TBL} AS
SELECT
  research_id,
  ajcc8_stage_group,
  ajcc8_t_stage,
  ajcc8_n_stage,
  ajcc8_m_stage,
  histology_final,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig271_snapshot_ts
FROM main.canonical_patient_master
WHERE is_malignant = FALSE
  AND ajcc8_stage_group IS NOT NULL
  AND {_HIST_NIFTP_FA_SQL}
"""
        )
        snap_n = int(con.execute(f"SELECT COUNT(*) FROM {ARCH_TBL}").fetchone()[0])
        log(f"Archive rows: {snap_n}")

        con.execute(
            f"""
UPDATE main.canonical_patient_master
SET
  ajcc8_stage_group = NULL,
  ajcc8_t_stage = NULL,
  ajcc8_n_stage = NULL,
  ajcc8_m_stage = NULL,
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE is_malignant = FALSE
  AND ajcc8_stage_group IS NOT NULL
  AND {_HIST_NIFTP_FA_SQL}
"""
        )
        log("UPDATE canonical_patient_master (NULL AJCC8 T/N/M/stage for NIFTP/FA non-malignant) complete")

    post = int(
        con.execute(
            f"""
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE is_malignant = FALSE
  AND ajcc8_stage_group IS NOT NULL
  AND {_HIST_NIFTP_FA_SQL}
"""
        ).fetchone()[0]
    )
    log(f"Post-apply probe (expect 0): {post}")
    if post != 0:
        log("FAIL: residual NIFTP/FA rows still carry ajcc8_stage_group")
        con.close()
        return 1

    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    if row[0] != 10871 or row[1] != 10871:
        log("FAIL: CPM row invariant broken")
        con.close()
        return 1

    malig2 = con.execute(
        """
SELECT
  CAST(SUM(CASE WHEN COALESCE(is_malignant, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS n_malig,
  ROUND(100.0 * SUM(CASE WHEN COALESCE(is_malignant, FALSE) THEN 1 ELSE 0 END) / COUNT(*), 1) AS malig_pct_1dp
FROM main.canonical_patient_master
"""
    ).fetchone()
    log(f"Post-apply malignancy: n_malig={malig2[0]}, pct≈{malig2[1]}% (1 dp)")

    summary = (
        "mig_271: NULLed ajcc8_stage_group/t/n/m for NIFTP/follicular adenoma rows with "
        f"is_malignant=FALSE (rows cleared from stage: {probe_targets}). "
        f"Post n_malig={malig2[0]} (~{malig2[1]}%). Closes CF-mig264b-DOWNSTREAM-CASCADE."
    )

    prov_run_id = "canonical_cleanup_mig271_20260502"
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
  'mig271 niftp fa ajcc stage null sweep',
  'CF-mig264b-DOWNSTREAM-CASCADE',
  '0',
  '0',
  '0'
)
""",
            [prov_run_id],
        )
        log("INSERT cpm_reconciliation_provenance_v1 mig_271 OK")
    except Exception as exc:
        log(f"WARN provenance insert: {exc}")

    con.execute(
        """
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?)
""",
        ["mig_271", "cursor_composer_mig271", summary],
    )
    log("INSERT signoff_migration mig_271 OK")

    out_apply = f"{REPO_ROOT}/scripts/output/mig_271_apply_log.txt"
    _write_out(out_apply, log_lines)
    log(f"Wrote {out_apply}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
