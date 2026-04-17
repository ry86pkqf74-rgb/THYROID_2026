#!/usr/bin/env python3
"""
Script 266 - triage the 220+ unmapped CPM cols into 3 buckets.

Builds manuscript_workspace.cpm_unmapped_triage_v265 with one row per CPM
column that has no entry in canonical_detail_pointer_v1, classified into:

  - A_deprecated_candidate  (staging / temp / raw / pre prefixes)
  - B_computed_score        (composite scores, _final, _v\\d+, _inferred_negative)
  - C_missing_feeder        (everything else - real registry gaps)

Reports the bucket counts so we know whether bucket C is trivial or a project.

Default dry-run; pass --apply to execute.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ensure_audit_table, make_logger, record_audit, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG = OUT_DIR / "266_run.log"
DECISION_LOG = OUT_DIR / "266_decision_log.json"

SCRIPT_TAG = "Script 266"
SCRIPT_NUM = "266"
RUN_DATE = "2026-04-17"

TRIAGE_TBL = (
    f"{PUBLICATION_DB}.manuscript_workspace.cpm_unmapped_triage_v265"
)
POINTER = f"{PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default dry-run.")
    args = ap.parse_args()
    do_writes = bool(args.apply)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    try:
        log("=" * 78)
        log(f"=== START scripts/266_cpm_unmapped_triage.py "
            f"({'APPLY' if do_writes else 'DRY-RUN'})")
        log(f"started_at: {utc_now()}")
        con = connect_locked()
        log(f"connected to {PUBLICATION_DB}")

        n_unmapped = int(con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns c
            LEFT JOIN (
              SELECT DISTINCT master_column FROM {POINTER}
            ) p ON p.master_column = c.column_name
            WHERE c.table_catalog = '{PUBLICATION_DB}'
              AND c.table_schema = 'main'
              AND c.table_name = 'canonical_patient_master'
              AND p.master_column IS NULL
        """).fetchone()[0])
        log(f"  unmapped CPM cols to triage: {n_unmapped}")

        if do_writes:
            con.execute(f"DROP TABLE IF EXISTS {TRIAGE_TBL}")
            con.execute(f"""
                CREATE TABLE {TRIAGE_TBL} AS
                SELECT
                  c.column_name,
                  c.ordinal_position,
                  c.data_type,
                  CASE
                    WHEN regexp_matches(c.column_name, '^(_|tmp_|stg_|raw_|pre_)')
                      THEN 'A_deprecated_candidate'
                    WHEN regexp_matches(c.column_name, '^(ajcc[78]?_|ames_|macis_|ages_|ata_|sage_|delphian_|rscore_)')
                      THEN 'B_computed_score'
                    WHEN regexp_matches(c.column_name, '_inferred_negative$')
                      THEN 'B_computed_score'
                    WHEN regexp_matches(c.column_name, '_final$')
                      THEN 'B_computed_score'
                    WHEN regexp_matches(c.column_name, '_v[0-9]+$')
                      THEN 'B_computed_score'
                    ELSE 'C_missing_feeder'
                  END AS triage_bucket,
                  current_timestamp AS triaged_at
                FROM information_schema.columns c
                LEFT JOIN (
                  SELECT DISTINCT master_column FROM {POINTER}
                ) p ON p.master_column = c.column_name
                WHERE c.table_catalog = '{PUBLICATION_DB}'
                  AND c.table_schema = 'main'
                  AND c.table_name = 'canonical_patient_master'
                  AND p.master_column IS NULL
            """)
            comment = (
                "Script 266: triage of CPM columns with no detail-table feeder "
                "in canonical_detail_pointer_v1. Bucket A = deprecation "
                "candidates (staging prefixes), Bucket B = composite scores / "
                "computed (AJCC, AMES, MACIS, AGES, _final, _v\\d+, "
                "_inferred_negative), Bucket C = real registry gaps. "
                "See scripts/266_cpm_unmapped_triage.py."
            ).replace("'", "''")
            con.execute(f"COMMENT ON TABLE {TRIAGE_TBL} IS '{comment}'")
            log(f"  built {TRIAGE_TBL}")

        bucket_query = f"""
            SELECT triage_bucket, COUNT(*) AS n FROM {TRIAGE_TBL}
            GROUP BY triage_bucket ORDER BY triage_bucket
        """ if do_writes else f"""
            WITH triaged AS (
              SELECT
                c.column_name,
                CASE
                  WHEN regexp_matches(c.column_name, '^(_|tmp_|stg_|raw_|pre_)')
                    THEN 'A_deprecated_candidate'
                  WHEN regexp_matches(c.column_name, '^(ajcc[78]?_|ames_|macis_|ages_|ata_|sage_|delphian_|rscore_)')
                    THEN 'B_computed_score'
                  WHEN regexp_matches(c.column_name, '_inferred_negative$')
                    THEN 'B_computed_score'
                  WHEN regexp_matches(c.column_name, '_final$')
                    THEN 'B_computed_score'
                  WHEN regexp_matches(c.column_name, '_v[0-9]+$')
                    THEN 'B_computed_score'
                  ELSE 'C_missing_feeder'
                END AS triage_bucket
              FROM information_schema.columns c
              LEFT JOIN (
                SELECT DISTINCT master_column FROM {POINTER}
              ) p ON p.master_column = c.column_name
              WHERE c.table_catalog = '{PUBLICATION_DB}'
                AND c.table_schema = 'main'
                AND c.table_name = 'canonical_patient_master'
                AND p.master_column IS NULL
            )
            SELECT triage_bucket, COUNT(*) AS n FROM triaged
            GROUP BY triage_bucket ORDER BY triage_bucket
        """
        buckets = con.execute(bucket_query).fetchall()
        log("\nBUCKET COUNTS:")
        for b, n in buckets:
            log(f"  {b}: {n}")

        sample_query_C = f"""
            SELECT column_name FROM (
              SELECT
                c.column_name,
                CASE
                  WHEN regexp_matches(c.column_name, '^(_|tmp_|stg_|raw_|pre_)')
                    THEN 'A_deprecated_candidate'
                  WHEN regexp_matches(c.column_name, '^(ajcc[78]?_|ames_|macis_|ages_|ata_|sage_|delphian_|rscore_)')
                    THEN 'B_computed_score'
                  WHEN regexp_matches(c.column_name, '_inferred_negative$')
                    THEN 'B_computed_score'
                  WHEN regexp_matches(c.column_name, '_final$')
                    THEN 'B_computed_score'
                  WHEN regexp_matches(c.column_name, '_v[0-9]+$')
                    THEN 'B_computed_score'
                  ELSE 'C_missing_feeder'
                END AS triage_bucket
              FROM information_schema.columns c
              LEFT JOIN (
                SELECT DISTINCT master_column FROM {POINTER}
              ) p ON p.master_column = c.column_name
              WHERE c.table_catalog = '{PUBLICATION_DB}'
                AND c.table_schema = 'main'
                AND c.table_name = 'canonical_patient_master'
                AND p.master_column IS NULL
            )
            WHERE triage_bucket = 'C_missing_feeder'
            ORDER BY column_name
        """
        c_cols = [r[0] for r in con.execute(sample_query_C).fetchall()]
        log(f"\nBucket C columns ({len(c_cols)}):")
        for c in c_cols[:50]:
            log(f"  {c}")
        if len(c_cols) > 50:
            log(f"  ... +{len(c_cols) - 50} more")

        if do_writes:
            ensure_audit_table(con)
            bucket_dict = {b: int(n) for b, n in buckets}
            record_audit(
                con, SCRIPT_NUM, "cpm_unmapped_triage",
                "bucket_C_missing_feeder",
                count_before=n_unmapped,
                count_after=bucket_dict.get("C_missing_feeder", 0),
                target_after=0,
                status="DOCUMENTED_GAP",
                notes=(f"A={bucket_dict.get('A_deprecated_candidate', 0)} "
                       f"B={bucket_dict.get('B_computed_score', 0)} "
                       f"C={bucket_dict.get('C_missing_feeder', 0)}. "
                       "Bucket C is the actionable backlog."))
            log("  audit row written")

        elapsed = time.time() - t0
        log(f"=== END elapsed={elapsed:.1f}s")
        write_decision_log(DECISION_LOG, {
            "script": SCRIPT_TAG, "run_date": RUN_DATE,
            "do_writes": do_writes, "elapsed_seconds": round(elapsed, 1),
            "n_unmapped": n_unmapped,
            "buckets": {b: int(n) for b, n in buckets},
            "bucket_C_columns": c_cols,
        })
        return 0
    except Exception as e:
        log(f"FATAL: {e!r}")
        import traceback
        log(traceback.format_exc())
        return 1
    finally:
        fh.close()


if __name__ == "__main__":
    sys.exit(main())
