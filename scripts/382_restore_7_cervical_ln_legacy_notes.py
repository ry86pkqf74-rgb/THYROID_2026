#!/usr/bin/env python3
"""
382_restore_7_cervical_ln_legacy_notes — restore 7 history_summary cervical-LN rows
dropped between legacy freeze and round-2 Script 382 promotion.

Source: studies/prompt5_legacy_canonical_rowdrop_audit_20260514.md

Steps:
  1. Pull rows from BigQuery pub_legacy_source_20260416.note_entities_llm_cervical_ln_detail
  2. Column-align to live MotherDuck main.note_entities_llm_cervical_ln_detail
  3. DELETE+INSERT restore keys in MD (idempotent)
  4. Re-run Script 382 phases 5–7 (events, rollup, CPM, guards, registry)
  5. Optional --apply-bq: COPY three MD tables to parquet + BigQuery LOAD (WRITE_TRUNCATE)
     — note_entities_llm_cervical_ln_detail, canonical_cervical_ln_clinical_events_v1,
     canonical_cervical_ln_clinical_patient_rollup_v1 — so BQ matches MD post-rollup.

PHI: do not log result_json or evidence payloads — counts and gate messages only.

Run:
  .venv/bin/python scripts/382_restore_7_cervical_ln_legacy_notes.py [--apply-md] [--apply-bq]

Defaults dry-run unless at least one --apply-* flag is set.
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from google.cloud import bigquery  # noqa: E402

from scripts._round2_helpers import (  # noqa: E402
    CANONICAL_DB,
    RunLogger,
    connect_md,
)

SOURCE_TABLE = "note_entities_llm_cervical_ln_detail"

# Full note_row_id hex (legacy freeze).
RESTORE_NOTE_ROW_IDS: tuple[str, ...] = (
    "358b30e0655adf52423f4bcb7d64623379541e51",
    "d6e344eb7c5bb413c0bcad94dbc21c3296f4decc",
    "c40da2bc8d731b073821caafd50740c2fe892d09",
    "1be1ebebf448d78ab2ee961c745a64f22b5b715d",
    "fa948a5c025efa265987bcf1ba5b7d4757d70601",
    "6ea9941d0ef6171c2593b44d71b1fd0b9f21f92b",
    "240a2102c79a6c4221a26aeffddc7d1ad32a2ef8",
)

BQ_PROJECT = "thyroid-canonical-pub-2026"
LEGACY_TABLE = f"{BQ_PROJECT}.pub_legacy_source_20260416.{SOURCE_TABLE}"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "382_restore_7_cervical_ln_run.log"
BQ_PARQUET_SUBDIR = "bq_restore382_7notes"

# MD → BQ full replace (post–phase-7) so downstream clinical-event tables match promoted notes.
BQ_SYNC_TABLES: tuple[str, ...] = (
    SOURCE_TABLE,
    "canonical_cervical_ln_clinical_events_v1",
    "canonical_cervical_ln_clinical_patient_rollup_v1",
)


def _load_382_module():
    path = REPO_ROOT / "scripts" / "382_cervical_ln_clinical_merge_load_rollup.py"
    spec = importlib.util.spec_from_file_location("script_382_cervical_ln", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _md_column_order(con, logger: RunLogger) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_catalog = ?
          AND table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [CANONICAL_DB, SOURCE_TABLE],
    ).fetchall()
    gate = logger.gate
    gate(len(rows) > 0, f"MD main.{SOURCE_TABLE} has columns")
    return [r[0] for r in rows]


def _fetch_legacy_rows(logger: RunLogger) -> pd.DataFrame:
    log = logger.log
    gate = logger.gate
    ids_sql = ", ".join(f"'{x}'" for x in RESTORE_NOTE_ROW_IDS)
    sql = f"""
    SELECT *
    FROM `{LEGACY_TABLE}`
    WHERE note_row_id IN ({ids_sql})
    ORDER BY note_row_id
    """
    log("  Querying BigQuery legacy table …")
    client = bigquery.Client(project=BQ_PROJECT)
    df = client.query(sql).to_dataframe()
    log(f"  Legacy rows fetched: {len(df):,} (expect 7)")
    gate(len(df) == 7, "exactly 7 legacy rows")
    got = set(df["note_row_id"].astype(str).tolist())
    gate(got == set(RESTORE_NOTE_ROW_IDS), "note_row_id set matches restore list")
    return df


def _align_dataframe_to_md(df: pd.DataFrame, md_cols: list[str], logger: RunLogger) -> pd.DataFrame:
    log = logger.log
    gate = logger.gate
    extra = [c for c in df.columns if c not in md_cols]
    if extra:
        log(f"  Dropping legacy-only columns: {extra}")
    missing = [c for c in md_cols if c not in df.columns]
    out = df[[c for c in df.columns if c in md_cols]].copy()
    for c in missing:
        out[c] = None
    out = out[md_cols]
    # MotherDuck uses VARCHAR research_id for this table
    if "research_id" in out.columns:
        out["research_id"] = out["research_id"].astype(str)
    gate(len(out) == 7, "aligned frame rows = 7")
    return out


def _insert_md(con, df: pd.DataFrame, logger: RunLogger) -> None:
    log = logger.log
    ids_sql = ", ".join(f"'{x}'" for x in RESTORE_NOTE_ROW_IDS)
    pre = con.execute(
        f"SELECT COUNT(*) FROM main.{SOURCE_TABLE} WHERE note_row_id IN ({ids_sql})"
    ).fetchone()[0]
    log(f"  Pre-restore rows matching restore IDs in MD: {pre}")
    if pre:
        log(f"  DELETE {pre} existing restore-ID row(s) (idempotent re-run)")
        con.execute(f"DELETE FROM main.{SOURCE_TABLE} WHERE note_row_id IN ({ids_sql})")

    con.register("_restore_stg", df)
    col_list = ", ".join(f'"{c}"' for c in df.columns)
    con.execute(
        f"INSERT INTO main.{SOURCE_TABLE} ({col_list}) SELECT {col_list} FROM _restore_stg"
    )
    post = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
    log(f"  Post-insert main.{SOURCE_TABLE} row count: {post:,}")


def _sync_bq_cervical_ln_from_md(con, logger: RunLogger) -> None:
    """Truncate-replace cervical-LN pub_canonical tables from live MotherDuck."""
    log = logger.log
    gate = logger.gate
    out_dir = OUTPUT_DIR / BQ_PARQUET_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)
    client = bigquery.Client(project=BQ_PROJECT)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    log("  BQ sync: MD → parquet → LOAD TRUNCATE …")
    for t in BQ_SYNC_TABLES:
        path = (out_dir / f"{t}.parquet").resolve()
        p = str(path).replace("'", "''")
        n_local = con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
        con.execute(
            f"COPY (SELECT * FROM main.{t}) TO '{p}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        log(f"    {t}: MD rows={n_local:,} → {path.name}")
        table_ref = f"{BQ_PROJECT}.pub_canonical.{t}"
        with path.open("rb") as f:
            job = client.load_table_from_file(
                f, table_ref, job_config=job_config
            )
        job.result()
        dest_rows = client.get_table(table_ref).num_rows
        gate(dest_rows == n_local, f"BQ {t} row count {dest_rows} == MD {n_local}")
        log(f"    BQ {t}: loaded rows={dest_rows:,}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Restore 7 cervical_ln legacy notes to MD + BQ")
    ap.add_argument("--apply-md", action="store_true", help="Write to MotherDuck + rerollup")
    ap.add_argument("--apply-bq", action="store_true",
                    help="TRUNCATE+load 3 cervical-LN tables from MD → pub_canonical")
    args = ap.parse_args()

    logger = RunLogger(LOG_PATH)
    log = logger.log
    gate = logger.gate

    log("382_restore_7_cervical_ln_legacy_notes")
    log(f"  apply_md={args.apply_md} apply_bq={args.apply_bq}")

    df_legacy = None
    if args.apply_md or (not args.apply_md and not args.apply_bq):
        df_legacy = _fetch_legacy_rows(logger)

    con = None
    try:
        if args.apply_md:
            assert df_legacy is not None
            con = connect_md(logger)
            md_cols = _md_column_order(con, logger)
            df_align = _align_dataframe_to_md(df_legacy, md_cols, logger)
            n_before = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
            ids_sql = ", ".join(f"'{x}'" for x in RESTORE_NOTE_ROW_IDS)
            pre_existing = con.execute(
                f"SELECT COUNT(*) FROM main.{SOURCE_TABLE} WHERE note_row_id IN ({ids_sql})"
            ).fetchone()[0]
            log(
                f"  MD row count before insert: {n_before:,} "
                f"(restore-id rows already present: {pre_existing})"
            )
            _insert_md(con, df_align, logger)
            n_mid = con.execute(f"SELECT COUNT(*) FROM main.{SOURCE_TABLE}").fetchone()[0]
            gate(
                n_mid == n_before - pre_existing + 7,
                f"MD net +7 rows (before {n_before}, after {n_mid}, removed {pre_existing})",
            )

            s382 = _load_382_module()
            log("  Script 382 phase 5 (events + rollup + CPM) …")
            s382.phase_5(con)
            log("  Script 382 phase 6 (invariants) …")
            s382.phase_6(con)
            log("  Script 382 phase 7 (registry + readme) …")
            s382.phase_7(con)
            log("  MotherDuck apply complete.")
        else:
            log("  SKIP MD (no --apply-md)")

        if args.apply_bq:
            if con is None:
                con = connect_md(logger)
            _sync_bq_cervical_ln_from_md(con, logger)
            log("  BigQuery cervical-LN trio sync complete.")
            log("  NOTE: pub_canonical.canonical_patient_master nlp_cervln_* may still")
            log("        differ until the next full CPM publish from MD.")
        else:
            log("  SKIP BQ (no --apply-bq)")
    finally:
        if con is not None:
            con.close()

    logger.flush()
    log("Done.")


if __name__ == "__main__":
    main()
