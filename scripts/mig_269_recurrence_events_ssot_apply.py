#!/usr/bin/env python3
"""
mig_269 — canonical recurrence event SSOT + CPM recurrence repoint (MotherDuck)

Applies qc_framework_v1/migrations/269_recurrence_events_ssot_20260503.sql
to thyroid_canonical_publication_v1_0.main using scripts/_md_connect.connect_locked.

Usage:
  .venv/bin/python scripts/mig_269_recurrence_events_ssot_apply.py --dry-run
  .venv/bin/python scripts/mig_269_recurrence_events_ssot_apply.py --apply

Options:
  --force-apply   Proceed even when signoff_migration already has mig_269.
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


SQL_PATH = REPO_ROOT / "qc_framework_v1" / "migrations" / "269_recurrence_events_ssot_20260503.sql"
OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "mig_269_apply_log.txt"


def _split_sql_statements(sql_text: str) -> list[str]:
    statements = [s.strip() for s in re.split(r";\s*\n", sql_text) if s.strip()]
    filtered: list[str] = []

    def is_all_comment(stmt: str) -> bool:
        lines = stmt.splitlines()
        if not lines:
            return True
        for line in lines:
            t = line.strip()
            if not t:
                continue
            if not t.startswith("--"):
                return False
        return True

    for s in statements:
        up = s.lstrip().upper()
        if up.startswith("USE "):
            continue
        if is_all_comment(s):
            continue
        filtered.append(s)
    return filtered


def _already_signed(con) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_269'"
    ).fetchone()
    return row is not None and int(row[0]) > 0


def export_drift_csv(con, path: Path) -> int:
    """Anticipated CPM deltas versus canonical recurrence + catalog ATA rollup."""
    sql_core = """
    WITH rollup_sim AS (
        WITH agg AS (
            SELECT CAST(research_id AS VARCHAR) AS research_id,
                   COALESCE(
                       MAX(CASE WHEN recurrence_type = 'structural' THEN recurrence_type END),
                       MAX(CASE WHEN recurrence_type = 'biochemical' THEN recurrence_type END)
                   ) AS recurrence_type_primary_catalog
            FROM main.recurrence_event_clean_v1
            GROUP BY 1
        ),
        spine AS (
            SELECT CAST(cr.research_id AS VARCHAR) AS research_id,
                   CAST(cr.recurrence_confirmed AS BOOLEAN) AS recurrence_confirmed,
                   CAST(cr.recurrence_date AS DATE) AS first_recurrence_date,
                   CAST(cr.recurrence_type AS VARCHAR) AS recurrence_type_clinical,
                   CAST(cr.recurrence_site AS VARCHAR) AS recurrence_site_clinical
            FROM main.canonical_recurrence_v1 cr
        )
        SELECT s.research_id,
               s.recurrence_confirmed AS sim_recurrence_confirmed,
               COALESCE(
                   NULLIF(TRIM(CAST(a.recurrence_type_primary_catalog AS VARCHAR)), ''),
                   NULLIF(TRIM(s.recurrence_type_clinical), '')
               ) AS sim_recurrence_type_primary,
               NULLIF(TRIM(s.recurrence_site_clinical), '') AS sim_recurrence_site_primary,
               CAST(s.first_recurrence_date AS DATE) AS sim_first_recurrence_date
        FROM spine s
        LEFT JOIN agg a ON a.research_id = s.research_id
    ),
    deltas AS (
        SELECT
          CAST(pm.research_id AS VARCHAR) AS research_id,
          CAST(pm.any_recurrence_flag AS BOOLEAN) AS pm_any_before,
          r.sim_recurrence_confirmed AS pm_any_after_sim,
          CAST(pm.recurrence_confirmed AS BOOLEAN) AS pm_recurrence_confirmed_before,
          r.sim_recurrence_confirmed AS pm_recurrence_confirmed_after_sim,
          CAST(pm.first_recurrence_date AS DATE) AS pm_first_recurrence_date_before,
          r.sim_first_recurrence_date AS pm_first_recurrence_date_after_sim,
          CAST(pm.recurrence_type_primary AS VARCHAR) AS pm_recurrence_type_before,
          CAST(r.sim_recurrence_type_primary AS VARCHAR) AS pm_recurrence_type_after_sim,
          CAST(pm.recurrence_site_primary AS VARCHAR) AS pm_recurrence_site_before,
          CAST(r.sim_recurrence_site_primary AS VARCHAR) AS pm_recurrence_site_after_sim,
          CAST(pm.time_to_recurrence_days AS BIGINT) AS time_to_recurrence_days_before,
          CAST(CASE
            WHEN r.sim_first_recurrence_date IS NULL OR pm.first_surgery_date IS NULL THEN NULL
            WHEN DATE_DIFF('day', CAST(pm.first_surgery_date AS DATE), r.sim_first_recurrence_date) < 0
                THEN NULL
            WHEN pm.overall_survival_days IS NULL THEN DATE_DIFF(
              'day', CAST(pm.first_surgery_date AS DATE), r.sim_first_recurrence_date)
            ELSE LEAST(
              DATE_DIFF('day', CAST(pm.first_surgery_date AS DATE), r.sim_first_recurrence_date),
              CAST(pm.overall_survival_days AS BIGINT)
            )
          END AS BIGINT) AS time_to_recurrence_days_after_sim
        FROM main.canonical_patient_master pm
        INNER JOIN rollup_sim r ON CAST(pm.research_id AS VARCHAR) = r.research_id
        WHERE (
            COALESCE(pm.any_recurrence_flag, FALSE) IS DISTINCT FROM
                COALESCE(r.sim_recurrence_confirmed, FALSE)
            OR CAST(pm.recurrence_confirmed AS BOOLEAN) IS DISTINCT FROM
                COALESCE(r.sim_recurrence_confirmed, FALSE)
            OR CAST(pm.first_recurrence_date AS DATE) IS DISTINCT FROM r.sim_first_recurrence_date
        )
    )
    SELECT * FROM deltas
    """
    sql_order = sql_core + """
    ORDER BY TRY_CAST(research_id AS BIGINT)
    LIMIT 500
    """
    n_total = int(con.execute(f"SELECT COUNT(*) AS n FROM ({sql_core}) AS t").fetchone()[0])
    df = con.execute(sql_order).fetchdf()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return int(n_total)


def append_log(lines: list[str]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description="Apply mig_269 recurrence SSOT to MotherDuck publication DB.")
    p.add_argument("--dry-run", action="store_true", help="Emit drift CSV + metrics; no DDL.")
    p.add_argument("--apply", action="store_true", help="Run migration SQL.")
    p.add_argument(
        "--force-apply",
        action="store_true",
        help="Allow apply even if signoff_migration.mig_id=mig_269 exists.",
    )
    args = p.parse_args()

    if not SQL_PATH.exists():
        sys.stderr.write(f"[ERROR] SQL not found: {SQL_PATH}\n")
        sys.exit(1)

    if args.dry_run == args.apply:
        sys.stderr.write("[ERROR] Pass exactly one of --dry-run OR --apply\n")
        sys.exit(1)

    if args.apply and LOG_PATH.exists():
        LOG_PATH.unlink()

    con = connect_locked()

    if args.apply and _already_signed(con) and not args.force_apply:
        sys.stderr.write(
            "[STOP] mig_269 already present in signoff_migration — pass --force-apply to re-run.\n"
        )
        con.close()
        sys.exit(1)

    if args.apply and args.force_apply and _already_signed(con):
        con.execute("DELETE FROM main.signoff_migration WHERE mig_id = 'mig_269'")
        print("[INFO] Deleted prior signoff row(s) for mig_269 (--force-apply)")

    drift_path = OUTPUT_DIR / "mig_269_dryrun_diff.csv"
    n_drift = export_drift_csv(con, drift_path)

    hdr: list[str] = []
    hdr.append(f"mig_269 drift rows (anticipated CPM deltas): {n_drift}")
    hdr.append(f"exported: {drift_path}")
    hdr.append(
        con.execute(
            """SELECT SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS cpm_any_t,
SUM(CASE WHEN recurrence_confirmed THEN 1 ELSE 0 END) AS cpm_cf_t
FROM canonical_patient_master"""
        )
        .fetchdf()
        .to_string(index=False)
    )
    hdr.append(
        con.execute(
            """SELECT SUM(CASE WHEN recurrence_confirmed THEN 1 ELSE 0 END) AS canonical_recurrence_confirmed_true
FROM canonical_recurrence_v1"""
        )
        .fetchdf()
        .to_string(index=False)
    )
    hdr.append(
        con.execute(
            """SELECT COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_pts
FROM recurrence_event_clean_v1"""
        )
        .fetchdf()
        .to_string(index=False)
    )

    append_log(hdr)
    print("\n".join(hdr))

    if args.dry_run:
        print("[DONE] Dry-run — database unchanged.")
        con.close()
        return

    sql_text = SQL_PATH.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)
    print(f"[INFO] Running {len(statements)} batches from {SQL_PATH.name}")
    for i, stmt in enumerate(statements, 1):
        preview = stmt.replace("\n", " ")[:100]
        print(f"[RUN] {i}/{len(statements)}: {preview} …")
        con.execute(stmt)
        print(f"[OK ] {i}")

    verified: list[str] = []

    verified.append(
        "[POST] canonical_recurrence_events_v1 — "
        + con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM canonical_recurrence_events_v1"
        ).fetchdf().to_string(index=False)
    )
    verified.append(
        "[POST] canonical_recurrence_patient_rollup_v1 — "
        + con.execute(
            """SELECT COUNT(*), COUNT(DISTINCT research_id)
FROM canonical_recurrence_patient_rollup_v1"""
        ).fetchdf().to_string(index=False)
    )
    verified.append(
        "[POST] CPM recurrence alignment vs canonical_recurrence — "
        + con.execute(
            """SELECT
  SUM(CASE WHEN COALESCE(pm.any_recurrence_flag, FALSE)
            IS DISTINCT FROM COALESCE(cr.recurrence_confirmed, FALSE) THEN 1 ELSE 0 END)
            AS mismatch_any_vs_canonical,
  SUM(CASE WHEN COALESCE(pm.recurrence_confirmed, FALSE)
            IS DISTINCT FROM COALESCE(cr.recurrence_confirmed, FALSE) THEN 1 ELSE 0 END)
            AS mismatch_confirmed_vs_canonical,
  SUM(CASE WHEN CAST(pm.first_recurrence_date AS DATE)
            IS DISTINCT FROM CAST(cr.recurrence_date AS DATE) THEN 1 ELSE 0 END)
            AS mismatch_first_date_vs_canonical
FROM canonical_patient_master pm
JOIN canonical_recurrence_v1 cr ON CAST(pm.research_id AS VARCHAR) = CAST(cr.research_id AS VARCHAR)"""
        ).fetchdf().to_string(index=False)
    )
    verified.append(
        "[POST] CPM recurrence KPIs — "
        + con.execute(
            """SELECT
  SUM(CASE WHEN recurrence_confirmed THEN 1 ELSE 0 END) AS flagged_confirmed,
  SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS flagged_any,
  SUM(CASE WHEN first_recurrence_date IS NOT NULL THEN 1 ELSE 0 END) AS with_date,
  SUM(CASE WHEN time_to_recurrence_days IS NOT NULL THEN 1 ELSE 0 END) AS with_ttr
FROM canonical_patient_master"""
        ).fetchdf().to_string(index=False)
    )

    append_log(verified)
    print("\n".join(verified))

    con.close()


if __name__ == "__main__":
    main()
