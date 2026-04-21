"""
Script 339 — Build tier2.patient_tier2_master_v1 + move 12 event tables.

Per Prompt 4 §339:

Part A:
    Full-outer-join the 12 main.*_patient_wide_v1 tables onto the CPM
    cohort (research_id) to produce a single wide rollup at
    tier2.patient_tier2_master_v1. Non-research_id columns are renamed
    with a `<domain>__` prefix derived from the source table name.

Part B:
    Move the 12 main.*_event_v1 tables to tier2.<name> via CTAS +
    archive + drop (DuckDB ALTER TABLE ... SET SCHEMA is unreliable on
    MotherDuck).

Both parts archive sources to "Thyroid 2026 UPdated".archive_pub_v1_0
with `_preSCHEMAREORG_<UTCZ>` suffix and log to
manuscript_workspace.archive_move_log_v1 +
manuscript_workspace.schema_reorg_move_log_v1.

Note: Prompt 4 wrote `parathyroid_detail_patient_wide_v1` but the
actual table name in main is `parathyroid_patient_wide_v1`; sources are
discovered via duckdb_tables() so this resolves automatically.

Usage:
    python 339_build_tier2_master_and_move_events.py            # dry-run
    python 339_build_tier2_master_and_move_events.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "339_build_tier2_master_and_move_events"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"
DB = "thyroid_canonical_publication_v1_0"


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


def ensure_logs(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.archive_move_log_v1 (
            moved_at TIMESTAMP, src_schema VARCHAR, src_table VARCHAR,
            archive_fq VARCHAR, n_rows BIGINT, reason VARCHAR, script VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.schema_reorg_move_log_v1 (
            moved_at TIMESTAMP,
            source_schema VARCHAR,
            source_name VARCHAR,
            dest_schema VARCHAR,
            dest_name VARCHAR,
            action VARCHAR,
            rowcount_src BIGINT,
            rowcount_dest BIGINT,
            script VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.schema_reorg_orphan_references_v1 (
            detected_at TIMESTAMP,
            source_schema VARCHAR,
            source_name VARCHAR,
            ref_database VARCHAR,
            ref_schema VARCHAR,
            ref_name VARCHAR,
            ref_type VARCHAR,
            note VARCHAR,
            script VARCHAR
        )
    """)


def get_columns(con, schema, table):
    return [r[0] for r in con.execute(f"""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='{DB}' AND schema_name='{schema}' AND table_name='{table}'
         ORDER BY column_index
    """).fetchall()]


def list_wide_tables(con):
    rows = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='main'
           AND table_name LIKE '%_patient_wide_v1'
         ORDER BY table_name
    """).fetchall()
    return [r[0] for r in rows]


def list_event_tables(con):
    rows = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='main'
           AND table_name LIKE '%_event_v1'
         ORDER BY table_name
    """).fetchall()
    return [r[0] for r in rows]


def domain_prefix_from_wide(name):
    # 'frozen_section_patient_wide_v1' -> 'frozen_section'
    assert name.endswith("_patient_wide_v1"), name
    return name[:-len("_patient_wide_v1")]


def check_references(con, table_name):
    rows = con.execute(f"""
        SELECT DISTINCT
            database_name || '.' || schema_name || '.' || view_name AS ref,
            database_name, schema_name, view_name
        FROM duckdb_views()
        WHERE sql ILIKE '%{table_name}%'
          AND view_name != '{table_name}'
          AND schema_name != '{ARCHIVE_SCHEMA}'
    """).fetchall()
    return rows


def archive_and_drop(con, table_name, utcz, source_rowcount, reason):
    src_fq = f'main."{table_name}"'
    archive_name = f"{table_name}_preSCHEMAREORG_{utcz}"
    archive_fq = f'{ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{archive_name}"'
    con.execute(f"CREATE TABLE {archive_fq} AS SELECT * FROM {src_fq}")
    dest_n = con.execute(f"SELECT COUNT(*) FROM {archive_fq}").fetchone()[0]
    if dest_n != source_rowcount:
        raise SystemExit(
            f"Archive count mismatch for {table_name}: "
            f"src={source_rowcount} dest={dest_n}"
        )
    con.execute(f"DROP TABLE {src_fq}")
    con.execute("""
        INSERT INTO manuscript_workspace.archive_move_log_v1
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), "main", table_name, archive_fq,
          source_rowcount, reason, SCRIPT])
    return archive_fq


def build_master_sql(con, wide_tables):
    """Build the CTAS SQL for tier2.patient_tier2_master_v1 by introspecting
    each wide table's columns.

    Returns (sql, total_columns_emitted, src_meta).

    src_meta is a list of (table_name, prefix, alias, n_rows, n_nonkey_cols).
    """
    select_parts = ["cpm.research_id"]
    join_parts = []
    src_meta = []

    # Reserve aliases that are short and safe.
    for idx, t in enumerate(wide_tables):
        prefix = domain_prefix_from_wide(t)
        alias = f"src{idx}"
        cols = get_columns(con, "main", t)
        nonkey = [c for c in cols if c != "research_id"]
        n_rows = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
        for c in nonkey:
            new_name = f"{prefix}__{c}"
            select_parts.append(f'{alias}."{c}" AS "{new_name}"')
        join_parts.append(
            f'LEFT JOIN main."{t}" {alias} USING (research_id)'
        )
        src_meta.append((t, prefix, alias, n_rows, len(nonkey)))

    sql = (
        "SELECT\n    "
        + ",\n    ".join(select_parts)
        + "\n  FROM (SELECT DISTINCT research_id "
          "FROM main.canonical_patient_master) cpm\n  "
        + "\n  ".join(join_parts)
    )
    total_cols = len(select_parts)
    return sql, total_cols, src_meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()
    ensure_logs(con)

    log("=" * 72)
    log(f"Script 339 — tier2 master + move events "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # ===================================================================
    # PART A: build tier2.patient_tier2_master_v1
    # ===================================================================
    log("")
    log("=== PART A: build tier2.patient_tier2_master_v1 ===")
    wide_tables = list_wide_tables(con)
    log(f"  Found {len(wide_tables)} *_patient_wide_v1 tables in main")
    if len(wide_tables) != 12:
        log(f"  WARNING: expected 12 wide tables, got {len(wide_tables)}")
    for t in wide_tables:
        log(f"    {t}")

    master_sql, total_cols, src_meta = build_master_sql(con, wide_tables)
    log(f"  Master SELECT will emit {total_cols} columns "
        f"(1 research_id + {sum(m[4] for m in src_meta)} prefixed)")

    if not args.commit:
        # Preview parity
        preview_n = con.execute(
            f"SELECT COUNT(*) FROM ({master_sql}) t"
        ).fetchone()[0]
        log(f"  Preview master rowcount: {preview_n}")
        if preview_n != 10871:
            raise SystemExit(
                f"Preview parity failed: master has {preview_n} rows, "
                f"expected 10871"
            )

        # Per-source coverage check: distinct research_id in source
        # should equal distinct research_id where any of its prefixed
        # cols is non-NULL in master.
        for (t, prefix, _alias, n_rows, _) in src_meta:
            src_distinct = con.execute(
                f'SELECT COUNT(DISTINCT research_id) FROM main."{t}"'
            ).fetchone()[0]
            # Pick the first non-key column to test non-NULL coverage
            cols = get_columns(con, "main", t)
            nonkey = [c for c in cols if c != "research_id"]
            if not nonkey:
                continue
            first_col = nonkey[0]
            preview_match = con.execute(
                f'SELECT COUNT(*) FROM ({master_sql}) t '
                f'WHERE "{prefix}__{first_col}" IS NOT NULL'
            ).fetchone()[0]
            n_nonnull_first_col_src = con.execute(
                f'SELECT COUNT(*) FROM main."{t}" '
                f'WHERE "{first_col}" IS NOT NULL'
            ).fetchone()[0]
            log(f"    {t}: src_distinct_rid={src_distinct}, src_rows={n_rows}, "
                f"non-NULL '{first_col}' src={n_nonnull_first_col_src} "
                f"master={preview_match}")
            if preview_match != n_nonnull_first_col_src:
                log(f"    WARNING: master non-NULL count differs from src "
                    f"non-NULL count for {prefix}__{first_col}")

        cpm_invariants(con, "post-dryrun-A")
    else:
        # COMMIT: build master
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{DB}".tier2')
        con.execute(
            f'CREATE OR REPLACE TABLE tier2.patient_tier2_master_v1 AS '
            f'{master_sql}'
        )
        master_n = con.execute(
            "SELECT COUNT(*) FROM tier2.patient_tier2_master_v1"
        ).fetchone()[0]
        master_rid = con.execute(
            "SELECT COUNT(DISTINCT research_id) FROM tier2.patient_tier2_master_v1"
        ).fetchone()[0]
        master_cols = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_columns()
             WHERE database_name='{DB}' AND schema_name='tier2'
               AND table_name='patient_tier2_master_v1'
        """).fetchone()[0]
        log(f"  tier2.patient_tier2_master_v1: {master_n} rows, "
            f"{master_rid} distinct rid, {master_cols} columns")
        if master_n != 10871 or master_rid != 10871:
            raise SystemExit(
                f"Master parity FAILED: rows={master_n}, distinct_rid={master_rid}"
            )

        # Archive + drop the 12 wide sources
        utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        n_dropped = 0
        n_skipped = 0
        for (t, _prefix, _alias, n_rows, _) in src_meta:
            refs = check_references(con, t)
            external_refs = [r for r in refs
                             if not (r[1] == DB and r[2] == 'main' and r[3] == t)]
            if external_refs:
                log(f"    {t}: REFERENCED by {len(external_refs)} view(s) — "
                    f"logging orphan refs, skipping drop")
                for ref in external_refs:
                    con.execute("""
                        INSERT INTO manuscript_workspace.schema_reorg_orphan_references_v1
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [dt.datetime.utcnow(), "main", t,
                          ref[1], ref[2], ref[3], "view",
                          "References merged-source patient_wide table; "
                          "rewrite to tier2.patient_tier2_master_v1 with "
                          "<domain>__<col> prefix",
                          SCRIPT])
                con.execute("""
                    INSERT INTO manuscript_workspace.schema_reorg_move_log_v1
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [dt.datetime.utcnow(), "main", t,
                      "tier2", "patient_tier2_master_v1",
                      "merge_join_dropskip", n_rows, master_n, SCRIPT])
                n_skipped += 1
                continue
            archive_fq = archive_and_drop(
                con, t, utcz, n_rows,
                "Schema reorg: joined into tier2.patient_tier2_master_v1"
            )
            log(f"    {t}: archived -> {archive_fq}; dropped from main")
            con.execute("""
                INSERT INTO manuscript_workspace.schema_reorg_move_log_v1
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [dt.datetime.utcnow(), "main", t,
                  "tier2", "patient_tier2_master_v1",
                  "merge_join", n_rows, master_n, SCRIPT])
            n_dropped += 1
        log(f"  Wide sources: dropped {n_dropped}/{len(src_meta)}, "
            f"skipped {n_skipped}")

        cpm_invariants(con, "post-A")

    # ===================================================================
    # PART B: move 12 *_event_v1 tables main -> tier2 (CTAS + drop)
    # ===================================================================
    log("")
    log("=== PART B: move *_event_v1 tables to tier2 ===")
    event_tables = list_event_tables(con)
    log(f"  Found {len(event_tables)} *_event_v1 tables in main")
    if len(event_tables) != 12:
        log(f"  WARNING: expected 12 event tables, got {len(event_tables)}")
    for t in event_tables:
        log(f"    {t}")

    if not args.commit:
        # Preview: verify each can be SELECTed and rowcount looks sane
        for t in event_tables:
            n = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
            nc = con.execute(f"""
                SELECT COUNT(*) FROM duckdb_columns()
                 WHERE database_name='{DB}' AND schema_name='main' AND table_name='{t}'
            """).fetchone()[0]
            log(f"    {t}: {n} rows, {nc} cols (would CTAS to tier2)")
        cpm_invariants(con, "post-dryrun-B")
        log("(dry-run — re-run with --commit to apply)")
        return

    # COMMIT: per-table CTAS + drop
    utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    n_moved = 0
    n_skipped = 0
    for t in event_tables:
        n_src = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
        nc_src = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_columns()
             WHERE database_name='{DB}' AND schema_name='main' AND table_name='{t}'
        """).fetchone()[0]

        refs = check_references(con, t)
        external_refs = [r for r in refs
                         if not (r[1] == DB and r[2] == 'main' and r[3] == t)]
        if external_refs:
            log(f"    {t}: REFERENCED by {len(external_refs)} view(s) — "
                f"logging orphan refs, copying to tier2 but NOT dropping main copy")
            for ref in external_refs:
                con.execute("""
                    INSERT INTO manuscript_workspace.schema_reorg_orphan_references_v1
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [dt.datetime.utcnow(), "main", t,
                      ref[1], ref[2], ref[3], "view",
                      f"References *_event_v1 table; rewrite to tier2.{t}",
                      SCRIPT])
        # Always create the tier2 copy first (idempotent via CREATE OR REPLACE)
        con.execute(
            f'CREATE OR REPLACE TABLE tier2."{t}" AS SELECT * FROM main."{t}"'
        )
        n_dst = con.execute(f'SELECT COUNT(*) FROM tier2."{t}"').fetchone()[0]
        nc_dst = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_columns()
             WHERE database_name='{DB}' AND schema_name='tier2' AND table_name='{t}'
        """).fetchone()[0]
        if n_dst != n_src or nc_dst != nc_src:
            raise SystemExit(
                f"Move parity FAILED for {t}: "
                f"src=({n_src}r,{nc_src}c) dst=({n_dst}r,{nc_dst}c)"
            )
        log(f"    {t}: copied to tier2 ({n_dst} rows, {nc_dst} cols)")

        if external_refs:
            con.execute("""
                INSERT INTO manuscript_workspace.schema_reorg_move_log_v1
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [dt.datetime.utcnow(), "main", t,
                  "tier2", t,
                  "move_dropskip", n_src, n_dst, SCRIPT])
            n_skipped += 1
            continue

        archive_fq = archive_and_drop(
            con, t, utcz, n_src,
            "Schema reorg: moved to tier2 schema"
        )
        log(f"    {t}: archived -> {archive_fq}; dropped from main")
        con.execute("""
            INSERT INTO manuscript_workspace.schema_reorg_move_log_v1
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [dt.datetime.utcnow(), "main", t,
              "tier2", t,
              "move", n_src, n_dst, SCRIPT])
        n_moved += 1

    log(f"  Event sources: moved {n_moved}/{len(event_tables)}, "
        f"skipped {n_skipped}")

    # ===================================================================
    # Final invariants
    # ===================================================================
    log("")
    log("=== Final invariants ===")
    n_tier2 = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='tier2'
    """).fetchone()[0]
    log(f"  tier2 table count: {n_tier2}")
    n_main_event = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='main'
           AND table_name LIKE '%_event_v1'
    """).fetchone()[0]
    n_main_wide = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='main'
           AND table_name LIKE '%_patient_wide_v1'
    """).fetchone()[0]
    log(f"  main *_event_v1: {n_main_event}, main *_patient_wide_v1: {n_main_wide}")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 339 complete.")


if __name__ == "__main__":
    main()
