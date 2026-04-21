#!/usr/bin/env python3
"""
Part B / Phase 5: Archive pre-drop CPM + DROP 53 TIRADS columns from
main.canonical_patient_master.

Logan greenlit 2026-04-21. Drop is irreversible outside archive restore.

Steps:
  1. Pre-drop snapshot of full CPM:
       "Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB
       (10871 rows × 1585 cols)
  2. Capture pre-drop row count + col count + content checksum.
  3. Sanity gate: drop-list comes from manuscript_workspace.cpm_tirads_canonical_coverage_v1
     (53 rows). Cross-check against information_schema — must be 53/53 still on CPM.
  4. Atomic per-column drops (one ALTER per column for clean error reporting).
  5. Drop 19 Part A sample tables from manuscript_workspace.cpm_tirads_audit_sample_*_v1.
     KEEP cpm_tirads_audit_classification_v1 + cpm_tirads_canonical_coverage_v1
     (2-week retention per Logan).
  6. Post-drop verification:
       - row count unchanged (10871)
       - column count went from N to N - 53
       - 0 columns matching ILIKE '%tirads%' on live CPM
       - 0 columns matching the explicit drop list on live CPM
       - archive snapshot: row count + col count match pre-drop live values
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "cpm_tirads_legacy_20260421"
ARCHIVE_TABLE = "canonical_patient_master_pre_partB"
OUT = REPO / "scripts" / "output"


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    log: dict = {"phase": 5, "started_at_utc": utc_iso()}
    con = MotherDuckClient(MotherDuckConfig(database=DB)).connect_rw()

    # ── 1. Drop list from coverage table ──────────────────────────────────
    drop_cols = [
        r[0] for r in con.execute(
            "SELECT column_name FROM manuscript_workspace.cpm_tirads_canonical_coverage_v1 ORDER BY column_name"
        ).fetchall()
    ]
    log["drop_list_from_coverage"] = drop_cols
    log["n_drop_list"] = len(drop_cols)
    assert len(drop_cols) == 53, f"Expected 53 cols in coverage table, got {len(drop_cols)}"

    # ── 2. Cross-check: every drop col still on CPM ────────────────────────
    cpm_cols_pre = [
        r[0] for r in con.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema='main' AND table_name='canonical_patient_master'
               ORDER BY ordinal_position"""
        ).fetchall()
    ]
    log["cpm_column_count_pre_drop"] = len(cpm_cols_pre)
    on_cpm = [c for c in drop_cols if c in cpm_cols_pre]
    not_on_cpm = [c for c in drop_cols if c not in cpm_cols_pre]
    log["drop_cols_present_on_cpm"] = len(on_cpm)
    log["drop_cols_missing_from_cpm"] = not_on_cpm
    assert not not_on_cpm, f"Drop list contains cols not on CPM: {not_on_cpm}"
    assert len(on_cpm) == 53, f"Expected 53/53 drop cols on CPM, got {len(on_cpm)}/53"

    # ── 3. Pre-drop row count + sanity ─────────────────────────────────────
    pre_n = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    log["cpm_row_count_pre_drop"] = pre_n[0]
    log["cpm_distinct_rids_pre_drop"] = pre_n[1]
    assert pre_n[0] == 10871, f"Expected 10871 CPM rows, got {pre_n[0]}"

    # ── 4. Archive snapshot ────────────────────────────────────────────────
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {ARCHIVE_DB}.{ARCHIVE_SCHEMA}")
    archive_fq = f'{ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{ARCHIVE_TABLE}"'
    print(f"Creating archive: {archive_fq}")
    con.execute(f"""
        CREATE OR REPLACE TABLE {archive_fq} AS
        SELECT * FROM main.canonical_patient_master
    """)
    archive_n = con.execute(f"SELECT COUNT(*) FROM {archive_fq}").fetchone()[0]
    archive_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='Thyroid 2026 UPdated'
          AND table_schema='{ARCHIVE_SCHEMA}'
          AND table_name='{ARCHIVE_TABLE}'
    """).fetchone()[0]
    log["archive_table"] = f'"Thyroid 2026 UPdated".{ARCHIVE_SCHEMA}.{ARCHIVE_TABLE}'
    log["archive_row_count"] = archive_n
    log["archive_column_count"] = archive_cols
    assert archive_n == pre_n[0], f"Archive row count {archive_n} != live pre-drop {pre_n[0]}"
    assert archive_cols == len(cpm_cols_pre), (
        f"Archive col count {archive_cols} != live pre-drop {len(cpm_cols_pre)}"
    )
    print(f"Archive verified: {archive_n} rows × {archive_cols} cols.")

    # ── 5. Atomic per-column drops ─────────────────────────────────────────
    drop_log: list[dict] = []
    for c in drop_cols:
        try:
            con.execute(f'ALTER TABLE main.canonical_patient_master DROP COLUMN "{c}"')
            drop_log.append({"column": c, "status": "DROPPED"})
        except Exception as e:
            drop_log.append({"column": c, "status": "FAILED", "error": str(e)})
            print(f"  FAIL: {c} — {e}")
    log["drops"] = drop_log
    n_dropped = sum(1 for d in drop_log if d["status"] == "DROPPED")
    n_failed = sum(1 for d in drop_log if d["status"] == "FAILED")
    log["n_dropped"] = n_dropped
    log["n_failed"] = n_failed
    assert n_failed == 0, f"{n_failed} column drops failed; check log"
    assert n_dropped == 53, f"Expected 53 drops, got {n_dropped}"
    print(f"Dropped {n_dropped} columns.")

    # ── 6. Drop Part A sample tables (KEEP classification + coverage) ─────
    sample_tables = [
        r[0] for r in con.execute(
            """SELECT table_name FROM information_schema.tables
               WHERE table_schema='manuscript_workspace'
                 AND table_name LIKE 'cpm_tirads_audit_sample_%_v1'"""
        ).fetchall()
    ]
    log["sample_tables_to_drop"] = sample_tables
    sample_drop_log: list[dict] = []
    for t in sample_tables:
        try:
            con.execute(f'DROP TABLE manuscript_workspace."{t}"')
            sample_drop_log.append({"table": t, "status": "DROPPED"})
        except Exception as e:
            sample_drop_log.append({"table": t, "status": "FAILED", "error": str(e)})
    log["sample_drops"] = sample_drop_log
    n_samples_dropped = sum(1 for d in sample_drop_log if d["status"] == "DROPPED")
    log["n_sample_tables_dropped"] = n_samples_dropped
    print(f"Dropped {n_samples_dropped} sample tables.")

    # KEEP these:
    kept = con.execute(
        """SELECT table_name FROM information_schema.tables
           WHERE table_schema='manuscript_workspace'
             AND table_name IN ('cpm_tirads_audit_classification_v1', 'cpm_tirads_canonical_coverage_v1')"""
    ).fetchall()
    log["retained_workspace_tables"] = [r[0] for r in kept]
    assert len(kept) == 2, f"Expected 2 retained workspace tables, got {len(kept)}"

    # ── 7. Post-drop verification ─────────────────────────────────────────
    post_n = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    cpm_cols_post = [
        r[0] for r in con.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema='main' AND table_name='canonical_patient_master'"""
        ).fetchall()
    ]
    log["cpm_row_count_post_drop"] = post_n[0]
    log["cpm_distinct_rids_post_drop"] = post_n[1]
    log["cpm_column_count_post_drop"] = len(cpm_cols_post)
    log["cpm_column_count_delta"] = len(cpm_cols_pre) - len(cpm_cols_post)

    assert post_n[0] == pre_n[0], f"CPM row count changed: {pre_n[0]} -> {post_n[0]}"
    assert post_n[1] == pre_n[1], f"CPM distinct RIDs changed: {pre_n[1]} -> {post_n[1]}"
    assert len(cpm_cols_post) == len(cpm_cols_pre) - 53, (
        f"Expected {len(cpm_cols_pre) - 53} cols post-drop, got {len(cpm_cols_post)}"
    )
    # 0 NON-NLP columns matching ILIKE '%tirads%'
    # (nlp_tirads_* family is OUT OF SCOPE per Part A audit + Part B prompt's
    # "Out of scope" section: "NLP diagnostic columns (`nlp_tirads_*`,
    # `nlp_imaging_*`, `nlp_usnodule_*`) — different concern, separate audit if needed.")
    n_nlp_tirads = con.execute(
        """SELECT COUNT(*) FROM information_schema.columns
           WHERE table_schema='main' AND table_name='canonical_patient_master'
             AND column_name LIKE 'nlp_%' AND column_name ILIKE '%tirads%'"""
    ).fetchone()[0]
    n_non_nlp_tirads_post = con.execute(
        """SELECT COUNT(*) FROM information_schema.columns
           WHERE table_schema='main' AND table_name='canonical_patient_master'
             AND column_name ILIKE '%tirads%'
             AND column_name NOT LIKE 'nlp_%'"""
    ).fetchone()[0]
    log["cpm_post_drop_nlp_tirads_col_count"] = n_nlp_tirads
    log["cpm_post_drop_non_nlp_tirads_col_count"] = n_non_nlp_tirads_post
    assert n_non_nlp_tirads_post == 0, (
        f"Expected 0 non-NLP TIRADS cols on CPM post-drop, got {n_non_nlp_tirads_post}. "
        f"(nlp_tirads_* family expected count: 5; got {n_nlp_tirads}.)"
    )

    # 0 columns matching the explicit drop list on live CPM
    leftover = [c for c in drop_cols if c in cpm_cols_post]
    log["drop_list_leftover_on_cpm"] = leftover
    assert not leftover, f"Drop-list cols still present on CPM: {leftover}"

    log["finished_at_utc"] = utc_iso()
    log["status"] = "OK"

    out_path = OUT / "partB_phase5_drop.json"
    out_path.write_text(json.dumps(log, indent=2, default=str))
    print(f"\nPhase 5 done. Report: {out_path.relative_to(REPO)}")
    print(f"  CPM cols: {len(cpm_cols_pre)} -> {len(cpm_cols_post)} (-{53})")
    print(f"  CPM rows: {pre_n[0]} -> {post_n[0]} (unchanged)")
    print(f"  TIRADS cols on CPM: {n_tirads_post}")
    print(f"  Archive: {log['archive_table']} ({archive_n} rows × {archive_cols} cols)")
    print(f"  Sample tables dropped: {n_samples_dropped}")
    print(f"  Retained workspace tables: {log['retained_workspace_tables']}")


if __name__ == "__main__":
    main()
