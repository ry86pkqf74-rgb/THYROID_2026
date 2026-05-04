#!/usr/bin/env python3
"""mig_275 — Surgical complexity scaffold on canonical_patient_master (MotherDuck).

Populates cpm_op_time_min / cpm_ebl_ml / cpm_los_days (+ *_source) for M038 Table 1.

Archive: \"Thyroid 2026 UPdated\".archive_pub_v1_0.cpm_pre_mig275_20260503

Roll-up (CF-mig275-MULTI-OP-ROLLUP-RULE):
  EBL: SUM(COALESCE(ebl_ml, ebl_ml_nlp)) over canonical_operative_events_v1 rows whose
       surgery_date_native matches CPM.first_surgery_date; if sum NULL or <= 0, fall back to
       ops_ebl_ml then op_nlp_ebl_ml.
  Op time: nsqip_operative_duration_min only (operative_events table has no duration column in publication).
  LOS: COALESCE(nsqip_hospital_los_days, nsqip_length_of_stay_days, nsqip_surgical_los_days).

Usage:
  .venv/bin/python scripts/mig_275_m038_surgical_complexity.py --dry-run
  .venv/bin/python scripts/mig_275_m038_surgical_complexity.py --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

MIG_SQL = REPO_ROOT / "qc_framework_v1/migrations/275_surgical_complexity_cpm_scaffold_20260503.sql"
COHORT_VIEW_SQL = REPO_ROOT / "qc_framework_v1/migrations/273_cohort_m038_view_20260502.sql"
OUT_LOG = REPO_ROOT / "scripts/output/mig_275_apply_log.txt"
ARCHIVE_SCHEMA = '"Thyroid 2026 UPdated".archive_pub_v1_0'
ARCH_TBL = f"{ARCHIVE_SCHEMA}.cpm_pre_mig275_20260503"

NEW_COLS: tuple[tuple[str, str], ...] = (
    ("cpm_op_time_min", "DOUBLE"),
    ("cpm_ebl_ml", "DOUBLE"),
    ("cpm_los_days", "DOUBLE"),
    ("cpm_op_time_min_source", "VARCHAR"),
    ("cpm_ebl_ml_source", "VARCHAR"),
    ("cpm_los_days_source", "VARCHAR"),
)


def _col_exists(con, col: str) -> bool:
    row = con.execute(
        """
SELECT COUNT(*) FROM information_schema.columns
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name = ?
""",
        [col],
    ).fetchone()
    return bool(row and row[0] > 0)


def _ensure_columns(con, log) -> None:
    for name, typ in NEW_COLS:
        if _col_exists(con, name):
            continue
        log(f"ALTER TABLE ADD COLUMN {name} {typ}")
        con.execute(f"ALTER TABLE main.canonical_patient_master ADD COLUMN {name} {typ}")


def _strip_use_comments(sql_body: str) -> str:
    lines_out: list[str] = []
    for line in sql_body.splitlines():
        s = line.strip()
        if s.startswith("--"):
            continue
        if s.upper().startswith("USE "):
            continue
        lines_out.append(line)
    return "\n".join(lines_out).strip()


UPDATE_SQL = """
WITH ebl_evt AS (
  SELECT
    CAST(e.research_id AS VARCHAR) AS rid_v,
    SUM(COALESCE(
      TRY_CAST(e.ebl_ml AS DOUBLE),
      TRY_CAST(e.ebl_ml_nlp AS DOUBLE)
    )) AS sum_ebl
  FROM main.canonical_operative_events_v1 AS e
  INNER JOIN main.canonical_patient_master AS c
    ON CAST(e.research_id AS VARCHAR) = c.research_id
   AND TRY_CAST(e.surgery_date_native AS DATE) = TRY_CAST(c.first_surgery_date AS DATE)
  GROUP BY 1
),
src AS (
  SELECT
    cpm.research_id AS rid_v,
    TRY_CAST(cpm.nsqip_operative_duration_min AS DOUBLE) AS op_min,
    CASE
      WHEN ee.sum_ebl IS NOT NULL AND ee.sum_ebl > 0 THEN ee.sum_ebl
      ELSE COALESCE(
        TRY_CAST(cpm.ops_ebl_ml AS DOUBLE),
        TRY_CAST(cpm.op_nlp_ebl_ml AS DOUBLE)
      )
    END AS ebl,
    COALESCE(
      TRY_CAST(cpm.nsqip_hospital_los_days AS DOUBLE),
      TRY_CAST(cpm.nsqip_length_of_stay_days AS DOUBLE),
      TRY_CAST(cpm.nsqip_surgical_los_days AS DOUBLE)
    ) AS los,
    CASE
      WHEN cpm.nsqip_operative_duration_min IS NOT NULL THEN 'nsqip_operative_duration_min'
      ELSE NULL
    END AS src_op,
    CASE
      WHEN ee.sum_ebl IS NOT NULL AND ee.sum_ebl > 0 THEN 'operative_events_index_surgery_sum'
      WHEN cpm.ops_ebl_ml IS NOT NULL THEN 'ops_ebl_ml'
      WHEN cpm.op_nlp_ebl_ml IS NOT NULL THEN 'op_nlp_ebl_ml'
      ELSE NULL
    END AS src_ebl,
    CASE
      WHEN cpm.nsqip_hospital_los_days IS NOT NULL THEN 'nsqip_hospital_los_days'
      WHEN cpm.nsqip_length_of_stay_days IS NOT NULL THEN 'nsqip_length_of_stay_days'
      WHEN cpm.nsqip_surgical_los_days IS NOT NULL THEN 'nsqip_surgical_los_days'
      ELSE NULL
    END AS src_los
  FROM main.canonical_patient_master AS cpm
  LEFT JOIN ebl_evt AS ee ON ee.rid_v = cpm.research_id
)
UPDATE main.canonical_patient_master AS cpm
SET
  cpm_op_time_min = src.op_min,
  cpm_ebl_ml = src.ebl,
  cpm_los_days = src.los,
  cpm_op_time_min_source = src.src_op,
  cpm_ebl_ml_source = src.src_ebl,
  cpm_los_days_source = src.src_los
FROM src
WHERE cpm.research_id = src.rid_v
"""


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
    log(f"mig_275 started utc {stamp}")

    if not MIG_SQL.is_file():
        log(f"WARN: doc migration missing {MIG_SQL} (continuing)")

    dup = int(
        con.execute(
            "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_275'"
        ).fetchone()[0]
    )
    if dup > 0 and args.apply:
        log(f"SKIP: signoff_migration already has mig_275 (rows={dup})")
        OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
        OUT_LOG.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        con.close()
        return 0

    log("--- Discovery: operative_events complexity-ish columns ---")
    for r in con.execute(
        """
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main' AND table_name = 'canonical_operative_events_v1'
  AND (LOWER(column_name) LIKE '%ebl%' OR LOWER(column_name) LIKE '%time%')
ORDER BY 1
"""
    ).fetchall():
        log(f"  {r[0]}: {r[1]}")

    n_ns_op = int(
        con.execute(
            """
SELECT COUNT_IF(nsqip_operative_duration_min IS NOT NULL)::BIGINT
FROM main.canonical_patient_master
"""
        ).fetchone()[0]
    )
    n_ns_los = int(
        con.execute(
            """
SELECT COUNT_IF(
  nsqip_hospital_los_days IS NOT NULL
  OR nsqip_length_of_stay_days IS NOT NULL
  OR nsqip_surgical_los_days IS NOT NULL
)::BIGINT
FROM main.canonical_patient_master
"""
        ).fetchone()[0]
    )
    log(f"Probe NSQIP: n with operative_duration_min={n_ns_op:,}; n with any LOS field={n_ns_los:,}")

    if args.dry_run:
        log("--dry-run: no ALTER/UPDATE/signoff")
        OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
        dry_path = OUT_LOG.with_name(f"mig_275_dry_run_{stamp.replace(':', '')}.txt")
        dry_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        log(f"wrote {dry_path.relative_to(REPO_ROOT)}")
        con.close()
        return 0

    _ensure_columns(con, log)

    log(f"CREATE OR REPLACE TABLE {ARCH_TBL} (archive snapshot)")
    con.execute(
        f"""
CREATE OR REPLACE TABLE {ARCH_TBL} AS
SELECT
  research_id,
  cpm_op_time_min,
  cpm_ebl_ml,
  cpm_los_days,
  cpm_op_time_min_source,
  cpm_ebl_ml_source,
  cpm_los_days_source,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig275_snapshot_ts
FROM main.canonical_patient_master
"""
    )
    arch_n = int(con.execute(f"SELECT COUNT(*) FROM {ARCH_TBL}").fetchone()[0])
    log(f"Archive rows: {arch_n}")

    log("UPDATE canonical_patient_master (complexity columns)")
    con.execute(UPDATE_SQL)

    n_op = int(
        con.execute(
            "SELECT COUNT_IF(cpm_op_time_min IS NOT NULL)::BIGINT FROM main.canonical_patient_master"
        ).fetchone()[0]
    )
    n_ebl = int(
        con.execute(
            "SELECT COUNT_IF(cpm_ebl_ml IS NOT NULL)::BIGINT FROM main.canonical_patient_master"
        ).fetchone()[0]
    )
    n_los = int(
        con.execute(
            "SELECT COUNT_IF(cpm_los_days IS NOT NULL)::BIGINT FROM main.canonical_patient_master"
        ).fetchone()[0]
    )
    log(f"Post-fill: cpm_op_time_min={n_op:,}; cpm_ebl_ml={n_ebl:,}; cpm_los_days={n_los:,}")

    log("Stamp cpm_built_at where any complexity metric non-null")
    con.execute(
        """
UPDATE main.canonical_patient_master
SET cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE cpm_op_time_min IS NOT NULL
   OR cpm_ebl_ml IS NOT NULL
   OR cpm_los_days IS NOT NULL
"""
    )

    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    if row[0] != 10871 or row[1] != 10871:
        log(f"FAIL: CPM invariant broken rows={row[0]} distinct={row[1]}")
        con.close()
        return 1

    summary = (
        "mig_275: Added/filled cpm_op_time_min (NSQIP duration), cpm_ebl_ml (operative_events index-date SUM "
        "then ops_ebl_ml then op_nlp_ebl_ml), cpm_los_days (NSQIP LOS hierarchy) + *_source. "
        f"Non-null counts: op_time={n_op}, ebl={n_ebl}, los={n_los}. "
        "CF-mig275-MULTI-OP-ROLLUP-RULE + CF-mig275-NSQIP-LIMITATION documented in migration header."
    )

    try:
        con.execute(
            "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 WHERE run_id = ?",
            ["canonical_cleanup_mig275_20260503"],
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
  'mig275 surgical complexity cpm scaffold',
  '0',
  '0',
  '0',
  'CF-mig275-NSQIP-LIMITATION; CF-mig275-MULTI-OP-ROLLUP-RULE'
)
""",
            ["canonical_cleanup_mig275_20260503"],
        )
        log("INSERT cpm_reconciliation_provenance_v1 mig_275 OK")
    except Exception as exc:
        log(f"WARN provenance insert: {exc}")

    con.execute(
        """
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_275', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'cursor_composer_mig275', ?)
""",
        [summary],
    )
    log("INSERT signoff_migration mig_275 OK")

    for stmt in (
        """COMMENT ON COLUMN main.canonical_patient_master.cpm_op_time_min IS
'mig_275: operative time (minutes) from nsqip_operative_duration_min; NULL outside NSQIP linkage.'""",
        """COMMENT ON COLUMN main.canonical_patient_master.cpm_ebl_ml IS
'mig_275: EBL mL — SUM(ebl_ml,ebl_ml_nlp) on index surgery date else ops_ebl_ml else op_nlp_ebl_ml.'""",
        """COMMENT ON COLUMN main.canonical_patient_master.cpm_los_days IS
'mig_275: hospital LOS — COALESCE(nsqip_hospital_los_days, nsqip_length_of_stay_days, nsqip_surgical_los_days).';""",
    ):
        try:
            con.execute(stmt)
        except Exception as exc:
            log(f"WARN COMMENT: {exc}")

    if COHORT_VIEW_SQL.is_file():
        ddl_view = _strip_use_comments(COHORT_VIEW_SQL.read_text(encoding="utf-8"))
        if ddl_view:
            log("--- REFRESH main.cohort_m038_massive_goiter_v1 (273 DDL; adds mig_275 cols) ---")
            con.execute(ddl_view)
            nrow = int(
                con.execute(
                    "SELECT COUNT(*) FROM main.cohort_m038_massive_goiter_v1"
                ).fetchone()[0]
            )
            log(f"VERIFY cohort_m038 view rows={nrow} (expect 10871)")
            if nrow != 10871:
                log("WARN: cohort view rowcount mismatch")

    OUT_LOG.parent.mkdir(parents=True, exist_ok=True)
    OUT_LOG.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    log(f"wrote {OUT_LOG.relative_to(REPO_ROOT)}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
