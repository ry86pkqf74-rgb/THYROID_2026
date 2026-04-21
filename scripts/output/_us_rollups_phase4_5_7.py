#!/usr/bin/env python3
"""Phase 4 + Phase 5 + Phase 7 executor for US rollups -> views refactor.

Phase 4: archive snapshots, drop tables, replace with views in main.
Phase 5: move ultrasound_reports & us_nodules_tirads to raw schema with
         content-hash verify, then drop from main.
Phase 7: final verification queries.

Phase 6 (script updates) is a separate Python step (file edits).
"""
from __future__ import annotations

import argparse
import datetime
import importlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
sys.path.insert(0, str(SCRIPTS))

mod366 = importlib.import_module("366_canonical_us_exam_master_v2")
mod367 = importlib.import_module("367_canonical_us_patient_master_v2")
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "us_legacy_20260421"
ARCHIVE_DB_QUOTED = f'"{ARCHIVE_DB}"'

TBL_EXAM = f"{PUB}.main.canonical_us_exam_master_v2"
TBL_PT = f"{PUB}.main.canonical_us_patient_master_v2"

ARCH_EXAM = (
    f'{ARCHIVE_DB_QUOTED}.{ARCHIVE_SCHEMA}.archived_canonical_us_exam_master_v2'
)
ARCH_PT = (
    f'{ARCHIVE_DB_QUOTED}.{ARCHIVE_SCHEMA}.archived_canonical_us_patient_master_v2'
)

RAW_US_REPORTS = f"{PUB}.raw.ultrasound_reports"
RAW_US_NODULES_TIRADS = f"{PUB}.raw.us_nodules_tirads"
SRC_US_REPORTS = f"{PUB}.main.ultrasound_reports"
SRC_US_NODULES_TIRADS = f"{PUB}.main.us_nodules_tirads"

TIRADS_COL = "acr2017_tirads_category"
TIRADS_PTS_COL = "acr2017_tirads_points"

RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = HERE / f"_us_rollups_phase4_5_7_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def correct_tirads_column(sql: str) -> str:
    out = sql.replace("tirads_category_v2", TIRADS_COL)
    out = out.replace("tirads_score_2017", TIRADS_PTS_COL)
    return out


def to_view_sql_inplace(create_table_sql: str, table_target: str) -> str:
    """Swap CREATE OR REPLACE TABLE to CREATE OR REPLACE VIEW for the same target."""
    out = create_table_sql.replace(
        f"CREATE OR REPLACE TABLE {table_target}",
        f"CREATE OR REPLACE VIEW {table_target}",
    )
    if out == create_table_sql:
        raise SystemExit(
            f"Failed to rewrite CREATE TABLE -> CREATE VIEW for {table_target}"
        )
    return out


def phase4(con) -> dict:
    log("=" * 60)
    log("PHASE 4: archive snapshots, drop tables, create views in main")
    log("=" * 60)
    out: dict = {}

    log(f'  CREATE SCHEMA IF NOT EXISTS {ARCHIVE_DB_QUOTED}.{ARCHIVE_SCHEMA}')
    con.execute(
        f'CREATE SCHEMA IF NOT EXISTS {ARCHIVE_DB_QUOTED}.{ARCHIVE_SCHEMA}'
    )

    log(f"  CREATE TABLE {ARCH_EXAM} AS SELECT * FROM {TBL_EXAM}")
    con.execute(f'DROP TABLE IF EXISTS {ARCH_EXAM}')
    con.execute(f'CREATE TABLE {ARCH_EXAM} AS SELECT * FROM {TBL_EXAM}')
    n_arch_exam = con.execute(f'SELECT COUNT(*) FROM {ARCH_EXAM}').fetchone()[0]
    log(f"    archived rows: {n_arch_exam}")

    log(f"  CREATE TABLE {ARCH_PT} AS SELECT * FROM {TBL_PT}")
    con.execute(f'DROP TABLE IF EXISTS {ARCH_PT}')
    con.execute(f'CREATE TABLE {ARCH_PT} AS SELECT * FROM {TBL_PT}')
    n_arch_pt = con.execute(f'SELECT COUNT(*) FROM {ARCH_PT}').fetchone()[0]
    log(f"    archived rows: {n_arch_pt}")

    out["archive"] = {"exam_rows": n_arch_exam, "pt_rows": n_arch_pt}

    log(f"  DROP TABLE {TBL_EXAM}")
    con.execute(f'DROP TABLE {TBL_EXAM}')
    log(f"  DROP TABLE {TBL_PT}")
    con.execute(f'DROP TABLE {TBL_PT}')

    surg_rows = con.execute(mod366.SURG_COL_PROBE_SQL).fetchall()
    surg_col = surg_rows[0][0] if surg_rows else None
    log(f"  surgery date column on CPM: {surg_col}")
    out["surg_col"] = surg_col

    exam_table_sql = mod366.build_sql(surg_col)
    exam_view_sql = to_view_sql_inplace(
        correct_tirads_column(exam_table_sql), mod366.TARGET
    )
    log(f"  CREATE VIEW {TBL_EXAM}")
    con.execute(exam_view_sql)
    n_view_exam = con.execute(f'SELECT COUNT(*) FROM {TBL_EXAM}').fetchone()[0]
    log(f"    view rows: {n_view_exam}")

    pt_view_sql = to_view_sql_inplace(mod367.BUILD_SQL, mod367.TARGET)
    log(f"  CREATE VIEW {TBL_PT}")
    con.execute(pt_view_sql)
    n_view_pt = con.execute(f'SELECT COUNT(*) FROM {TBL_PT}').fetchone()[0]
    log(f"    view rows: {n_view_pt}")

    out["views"] = {"exam_rows": n_view_exam, "pt_rows": n_view_pt}

    log("  verify table_type via information_schema")
    rows = con.execute(f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = '{PUB}'
          AND table_schema = 'main'
          AND table_name IN ('canonical_us_exam_master_v2',
                             'canonical_us_patient_master_v2')
        ORDER BY table_name
    """).fetchall()
    out["table_types"] = [{"name": r[0], "type": r[1]} for r in rows]
    for n, t in rows:
        log(f"    {n}: {t}")

    return out


def phase5(con) -> dict:
    log("")
    log("=" * 60)
    log("PHASE 5: move raw feeds to raw schema with hash verify")
    log("=" * 60)
    out: dict = {}

    log(f"  CREATE SCHEMA IF NOT EXISTS {PUB}.raw")
    con.execute(f'CREATE SCHEMA IF NOT EXISTS {PUB}.raw')

    pairs = [
        (SRC_US_REPORTS, RAW_US_REPORTS, "ultrasound_reports"),
        (SRC_US_NODULES_TIRADS, RAW_US_NODULES_TIRADS, "us_nodules_tirads"),
    ]
    moves: list[dict] = []
    for src, dst, label in pairs:
        log(f"  CREATE TABLE {dst} AS SELECT * FROM {src}")
        con.execute(f'DROP TABLE IF EXISTS {dst}')
        con.execute(f'CREATE TABLE {dst} AS SELECT * FROM {src}')
        n_src = con.execute(f'SELECT COUNT(*) FROM {src}').fetchone()[0]
        n_dst = con.execute(f'SELECT COUNT(*) FROM {dst}').fetchone()[0]
        log(f"    rows: src={n_src}  dst={n_dst}")
        if n_src != n_dst:
            raise SystemExit(
                f"Row count mismatch on {label}: src={n_src}, dst={n_dst}. Aborting."
            )

        log(f"  hash-compare {label}")
        h_src, h_dst = con.execute(f"""
            SELECT
              (SELECT MD5(STRING_AGG(CAST(t AS VARCHAR),
                                     '||' ORDER BY CAST(t AS VARCHAR)))
                 FROM {src} t),
              (SELECT MD5(STRING_AGG(CAST(t AS VARCHAR),
                                     '||' ORDER BY CAST(t AS VARCHAR)))
                 FROM {dst} t)
        """).fetchone()
        log(f"    src_hash={h_src}")
        log(f"    dst_hash={h_dst}")
        if h_src != h_dst:
            raise SystemExit(
                f"Hash mismatch on {label}: aborting before drop."
            )

        log(f"  hashes match -> DROP TABLE {src}")
        con.execute(f'DROP TABLE {src}')

        moves.append({
            "label": label, "src_rows": n_src, "dst_rows": n_dst,
            "src_hash": h_src, "dst_hash": h_dst,
        })

    out["moves"] = moves
    return out


def phase7(con) -> dict:
    log("")
    log("=" * 60)
    log("PHASE 7: final verification")
    log("=" * 60)
    out: dict = {}

    rows = con.execute(f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = '{PUB}'
          AND table_schema = 'main'
          AND (LOWER(table_name) LIKE '%us%'
               OR LOWER(table_name) LIKE '%tirads%'
               OR LOWER(table_name) LIKE '%ultrasound%'
               OR LOWER(table_name) LIKE '%nodule%')
        ORDER BY table_type, table_name
    """).fetchall()
    out["main_us_objects"] = [{"name": r[0], "type": r[1]} for r in rows]
    log("  main.* objects matching US/tirads/ultrasound/nodule:")
    for n, t in rows:
        log(f"    {t:<12s} {n}")

    rows = con.execute(f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = '{PUB}'
          AND table_schema = 'raw'
        ORDER BY table_name
    """).fetchall()
    out["raw_schema_objects"] = [{"name": r[0], "type": r[1]} for r in rows]
    log("  raw.* objects:")
    for n, t in rows:
        log(f"    {t:<12s} {n}")

    n_exam = con.execute(f"SELECT COUNT(*) FROM {TBL_EXAM}").fetchone()[0]
    n_pt = con.execute(f"SELECT COUNT(*) FROM {TBL_PT}").fetchone()[0]
    out["smoke_counts"] = {"exam_master_rows": n_exam,
                           "patient_master_rows": n_pt}
    log(f"  smoke: exam_master_rows={n_exam}  patient_master_rows={n_pt}")

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true",
                    help="Actually run all destructive operations.")
    args = ap.parse_args()
    log(f"start  commit={args.commit}")
    con = connect_locked()

    if not args.commit:
        log("DRY RUN ONLY (no --commit)")
        return 0

    p4 = phase4(con)
    p5 = phase5(con)
    p7 = phase7(con)

    DECISION_LOG.write_text(json.dumps({
        "run_ts_utc": RUN_TS,
        "phase4": p4,
        "phase5": p5,
        "phase7": p7,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
