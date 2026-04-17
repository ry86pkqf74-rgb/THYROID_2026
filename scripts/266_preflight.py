#!/usr/bin/env python3
"""Script 266 preflight — verify live state matches prompt assertions.

Halts (exit 1) on mismatch. Read-only.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

OUT = HERE / "output" / "266_preflight.json"
OUT.parent.mkdir(parents=True, exist_ok=True)

EXPECT = {
    "pub_n_main_tables": 114,
    "pub_n_ws_objects": 81,
    "pub_n_patients": 10871,
    "pub_n_cpm_cols": 1499,
}


def main() -> int:
    con = connect_locked()
    row = con.execute(
        f"""
        SELECT
          current_database() AS connected_to,
          (SELECT COUNT(*) FROM information_schema.tables
             WHERE table_catalog='{PUBLICATION_DB}'
               AND table_schema='main' AND table_type='BASE TABLE') AS pub_n_main_tables,
          (SELECT COUNT(*) FROM information_schema.tables
             WHERE table_catalog='{PUBLICATION_DB}'
               AND table_schema='manuscript_workspace') AS pub_n_ws_objects,
          (SELECT COUNT(*) FROM {PUBLICATION_DB}.main.canonical_patient_master) AS pub_n_patients,
          (SELECT COUNT(*) FROM information_schema.columns
             WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
               AND table_name='canonical_patient_master') AS pub_n_cpm_cols
        """
    ).fetchone()
    cols = ["connected_to", "pub_n_main_tables", "pub_n_ws_objects",
            "pub_n_patients", "pub_n_cpm_cols"]
    actual = dict(zip(cols, row))

    # Workspace breakdown: tables vs views
    ws_breakdown = con.execute(f"""
        SELECT table_type, COUNT(*) FROM information_schema.tables
         WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
         GROUP BY 1 ORDER BY 1
    """).fetchall()
    actual["ws_breakdown"] = {t: n for t, n in ws_breakdown}

    # Confirm archive DB attachable
    try:
        n_archive_cpm = con.execute(
            'SELECT COUNT(*) FROM "Thyroid 2026 UPdated".main.canonical_patient_master'
        ).fetchone()[0]
        actual["archive_cpm_rows"] = n_archive_cpm
        actual["archive_db_reachable"] = True
    except Exception as e:
        actual["archive_db_reachable"] = False
        actual["archive_db_error"] = str(e)

    # View count referencing AJCC / multifocal / n_tumors / stage_migration
    n_ajcc_views = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.views
         WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
           AND (LOWER(view_definition) LIKE '%ajcc%'
                OR LOWER(view_definition) LIKE '%multifocal%'
                OR LOWER(view_definition) LIKE '%n_tumors%'
                OR LOWER(view_definition) LIKE '%stage_migration%')
    """).fetchone()[0]
    actual["ws_ajcc_referencing_views"] = n_ajcc_views

    # Existence check: the conventions table cited in prompt
    try:
        conv_n = con.execute(
            f"SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace.__conventions"
        ).fetchone()[0]
        actual["conventions_rows"] = conv_n
    except Exception as e:
        actual["conventions_rows"] = None
        actual["conventions_error"] = str(e)

    # Existence checks for tables referenced extensively by prompt
    for fq in [
        f"{PUBLICATION_DB}.main.canonical_tumor_characteristics_v1",
        f"{PUBLICATION_DB}.main.tumor_episode_master_v2",
        f"{PUBLICATION_DB}.main.tumor_pathology",
        f"{PUBLICATION_DB}.main.path_synoptics",
        f"{PUBLICATION_DB}.main.ln_master_rollup_v1",
        f"{PUBLICATION_DB}.main.synoptic_tumor_long_v1",
        f"{PUBLICATION_DB}.main.patient_tumor_rollup_v1",
        f"{PUBLICATION_DB}.main.path_size_adjudication_v241",
        f"{PUBLICATION_DB}.main.data_dictionary_v240",
        f"{PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1",
        f"{PUBLICATION_DB}.manuscript_workspace.cpm_unmapped_triage_v265",
        f"{PUBLICATION_DB}.manuscript_workspace.nan_string_audit_v1_1",
    ]:
        key = fq.split(".")[-1]
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
            actual[f"row_count.{key}"] = n
        except Exception as e:
            actual[f"row_count.{key}"] = None
            actual[f"row_count.{key}_error"] = str(e)

    # Triage bucket distribution
    try:
        rows = con.execute(f"""
            SELECT triage_bucket, COUNT(*) FROM
              {PUBLICATION_DB}.manuscript_workspace.cpm_unmapped_triage_v265
             GROUP BY 1 ORDER BY 1
        """).fetchall()
        actual["triage_bucket_distribution"] = {b: n for b, n in rows}
    except Exception as e:
        actual["triage_bucket_distribution_error"] = str(e)

    # Per-tumor AJCC scan
    try:
        ajcc_cols = con.execute(f"""
            SELECT table_schema, table_name, column_name
              FROM information_schema.columns
             WHERE table_catalog='{PUBLICATION_DB}'
               AND table_schema IN ('main','manuscript_workspace')
               AND (column_name ILIKE '%ajcc7%' OR column_name ILIKE '%ajcc8%')
             ORDER BY table_schema, table_name, column_name
        """).fetchall()
        actual["existing_ajcc_columns"] = [
            f"{s}.{t}.{c}" for s, t, c in ajcc_cols
        ]
        actual["existing_ajcc_columns_count"] = len(ajcc_cols)
    except Exception as e:
        actual["existing_ajcc_columns_error"] = str(e)

    # Mismatch detection vs EXPECT
    mismatches = []
    for k, v in EXPECT.items():
        a = actual.get(k)
        if a != v:
            mismatches.append({"key": k, "expected": v, "actual": a})
    actual["mismatches"] = mismatches
    actual["preflight_ts"] = datetime.now(timezone.utc).isoformat()

    OUT.write_text(json.dumps(actual, indent=2, default=str))
    print(json.dumps(actual, indent=2, default=str))

    if mismatches:
        print(f"\nPREFLIGHT FAIL — {len(mismatches)} mismatch(es).", file=sys.stderr)
        return 1
    print("\nPREFLIGHT PASS — all asserted counts match.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
