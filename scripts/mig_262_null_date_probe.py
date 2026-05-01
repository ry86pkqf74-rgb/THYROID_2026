#!/usr/bin/env python3
"""Read-only probes for CF-mig262-NULL-DATE-RECOVERY (publication imaging dates).

Writes JSON summary under studies/mig_262_null_date_recovery_20260501/ when --md.

Usage:
  .venv/bin/python scripts/mig_262_null_date_probe.py
  .venv/bin/python scripts/mig_262_null_date_probe.py --md
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))

STUDY_DIR = REPO / "studies" / "mig_262_null_date_recovery_20260501"
LEGACY_INM = '"Thyroid 2026 UPdated".us_legacy_20260421.imaging_nodule_master_v1'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--md", action="store_true", help="Connect to MotherDuck publication DB")
    args = ap.parse_args()
    if not args.md:
        print("Pass --md to run probes (read-only).")
        return 0

    from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

    con = connect_locked()
    db = PUBLICATION_DB
    out: dict = {"probed_at_utc": datetime.now(timezone.utc).isoformat(), "db": db}

    def scalar(sql: str) -> int:
        return int(con.sql(sql).fetchone()[0])

    out["raw_imaging_12_slots_v1_exists"] = scalar(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog = '{db}' AND table_schema = 'main' "
        f"AND table_name = 'raw_imaging_12_slots_v1'"
    ) > 0

    out["imaging_exam_master_v1"] = {
        "null_exam_date": scalar(
            f"SELECT COUNT(*) FROM {db}.main.imaging_exam_master_v1 WHERE exam_date IS NULL"
        ),
        "total": scalar(f"SELECT COUNT(*) FROM {db}.main.imaging_exam_master_v1"),
        "distinct_rid_exam_id_null": scalar(
            f"SELECT COUNT(DISTINCT (research_id, exam_id)) FROM {db}.main.imaging_exam_master_v1 "
            f"WHERE exam_date IS NULL"
        ),
    }

    out["legacy_join_null_exam_to_dated_nodule_same_exam_id"] = scalar(
        f"""
        WITH nu AS (
          SELECT CAST(research_id AS BIGINT) AS rid,
                 TRIM(CAST(exam_id AS VARCHAR)) AS eid,
                 exam_date
          FROM {LEGACY_INM}
          WHERE exam_date IS NOT NULL
        )
        SELECT COUNT_IF(nu.exam_date IS NOT NULL)
        FROM {db}.main.imaging_exam_master_v1 e
        LEFT JOIN nu ON CAST(e.research_id AS BIGINT) = nu.rid
          AND TRIM(CAST(e.exam_id AS VARCHAR)) = nu.eid
        WHERE e.exam_date IS NULL
        """
    )

    out["null_exam_same_rid_any_dated_legacy_nodule"] = scalar(
        f"""
        SELECT COUNT(*) FROM {db}.main.imaging_exam_master_v1 e
        WHERE e.exam_date IS NULL
        AND EXISTS (
          SELECT 1 FROM {LEGACY_INM} n
          WHERE CAST(n.research_id AS BIGINT) = CAST(e.research_id AS BIGINT)
            AND n.exam_date IS NOT NULL
        )
        """
    )

    out["canonical_us_nodule_v2_non_aggregate"] = {
        "null_exam_date": scalar(
            f"SELECT COUNT(*) FROM {db}.main.canonical_us_nodule_v2 "
            f"WHERE is_aggregate_row IS NOT TRUE AND exam_date IS NULL"
        ),
        "null_exam_text_digit_date_hits": scalar(
            f"""
            SELECT COUNT_IF(
              regexp_matches(CAST(tirads_reported_in_text AS VARCHAR),
                '\\d{{1,2}}[/-]\\d{{1,2}}[/-]\\d{{2,4}}')
            )
            FROM {db}.main.canonical_us_nodule_v2
            WHERE is_aggregate_row IS NOT TRUE AND exam_date IS NULL
            """
        ),
    }

    con.close()

    STUDY_DIR.mkdir(parents=True, exist_ok=True)
    path = STUDY_DIR / "probe_results.json"
    path.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
