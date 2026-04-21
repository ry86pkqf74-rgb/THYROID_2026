"""
Script 303 — Build genetics_per_patient_master_v1.

Grain: one row per CPM patient (10,871 rows).

Full-outer-joins CPM with genetics_per_test_master_v1 (from Script 302),
rolls up per-gene status with priority logic (any positive → positive;
any negative → negative; else indeterminate), adds link columns to CPM,
and archives superseded tables.

Usage:
    python 303_genetics_per_patient_master_v1.py            # dry-run
    python 303_genetics_per_patient_master_v1.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "303_genetics_per_patient_master_v1"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"

TABLES_TO_ARCHIVE = [
    ("main", "canonical_molecular_tested_v1",
     "Superseded by genetics_per_patient_master_v1 (Script 303)"),
    ("main", "us_nodules_tirads",
     "Superseded by genetics pipeline rebuild (Script 303)"),
]


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


def ensure_log_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.archive_move_log_v1 (
            moved_at TIMESTAMP,
            src_schema VARCHAR,
            src_table VARCHAR,
            archive_fq VARCHAR,
            n_rows BIGINT,
            reason VARCHAR,
            script VARCHAR
        )
    """)


def table_exists(con, schema, table):
    return con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """).fetchone()[0] > 0


def check_references(con, table_name):
    """Check if any view references this table."""
    refs = con.execute(f"""
        SELECT DISTINCT
            database_name || '.' || schema_name || '.' ||
            COALESCE(view_name, 'unknown') AS ref
        FROM duckdb_views()
        WHERE sql ILIKE '%{table_name}%'
          AND view_name != '{table_name}'
    """).fetchall()
    return [r[0] for r in refs]


# ── Priority rollup: any positive → positive; any negative → negative ──

GENE_ROLLUP_EXPR = """
CASE
    WHEN BOOL_OR({col}) = TRUE  THEN TRUE
    WHEN BOOL_OR({col}) = FALSE THEN FALSE
    ELSE NULL
END
"""


PATIENT_MASTER_SQL = """
SELECT
    cpm.research_id,

    (gt.research_id IS NOT NULL)         AS was_tested,
    gt.n_episodes,
    gt.first_test_date,
    gt.last_test_date,
    gt.platforms,

    -- Per-gene rollup: priority = any positive > any negative > NULL
    gt.braf_patient,
    gt.ras_patient,
    gt.tert_patient,
    gt.tp53_patient,
    gt.any_fusion_patient,
    gt.max_n_variants,
    gt.molecular_risk_tier

FROM main.canonical_patient_master cpm
LEFT JOIN (
    SELECT
        research_id,
        COUNT(*)                                             AS n_episodes,
        MIN(test_date)                                       AS first_test_date,
        MAX(test_date)                                       AS last_test_date,
        STRING_AGG(DISTINCT platform, '|')                   AS platforms,

        CASE WHEN BOOL_OR(braf_positive_this_test) = TRUE  THEN TRUE
             WHEN BOOL_OR(braf_positive_this_test) = FALSE THEN FALSE
             ELSE NULL END                                   AS braf_patient,

        CASE WHEN BOOL_OR(ras_positive_this_test) = TRUE   THEN TRUE
             WHEN BOOL_OR(ras_positive_this_test) = FALSE  THEN FALSE
             ELSE NULL END                                   AS ras_patient,

        CASE WHEN BOOL_OR(tert_positive_this_test) = TRUE  THEN TRUE
             WHEN BOOL_OR(tert_positive_this_test) = FALSE THEN FALSE
             ELSE NULL END                                   AS tert_patient,

        CASE WHEN BOOL_OR(tp53_positive_this_test) = TRUE  THEN TRUE
             WHEN BOOL_OR(tp53_positive_this_test) = FALSE THEN FALSE
             ELSE NULL END                                   AS tp53_patient,

        CASE WHEN BOOL_OR(any_fusion_flag) = TRUE          THEN TRUE
             WHEN BOOL_OR(any_fusion_flag) = FALSE         THEN FALSE
             ELSE NULL END                                   AS any_fusion_patient,

        MAX(n_variants)                                      AS max_n_variants,
        (ARRAY_AGG(molecular_risk_tier ORDER BY
            molecular_risk_tier NULLS LAST))[1]              AS molecular_risk_tier

    FROM main.genetics_per_test_master_v1
    GROUP BY research_id
) gt ON gt.research_id = cpm.research_id
"""


def archive_one(con, schema, table_name, reason, commit):
    """Archive one table with reference-safety check. Returns (success, n_rows)."""
    if not table_exists(con, schema, table_name):
        log(f"    {schema}.{table_name}: NOT FOUND — skipping")
        return False, 0

    src_fq = f'"{schema}"."{table_name}"'
    n_rows = con.execute(f"SELECT COUNT(*) FROM {src_fq}").fetchone()[0]

    refs = check_references(con, table_name)
    if refs:
        ref_list = ", ".join(refs[:5])
        log(f"    {schema}.{table_name}: REFERENCED by {ref_list} — SKIPPING")
        return False, n_rows

    utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_name = f"{table_name}_pre303_{utcz}"
    archive_fq = f'{ARCHIVE_DB}.{ARCHIVE_SCHEMA}."{archive_name}"'

    log(f"    {schema}.{table_name}: {n_rows} rows, no refs — archiving")

    if not commit:
        log(f"    (dry-run — would archive to {archive_name})")
        return True, n_rows

    con.execute(f"CREATE TABLE {archive_fq} AS SELECT * FROM {src_fq}")

    dest_count = con.execute(f"SELECT COUNT(*) FROM {archive_fq}").fetchone()[0]
    if dest_count != n_rows:
        raise SystemExit(
            f"Archive count mismatch for {table_name}: "
            f"src={n_rows}, dest={dest_count}"
        )

    con.execute(f"DROP TABLE {src_fq}")
    log(f"    Archived + dropped: {n_rows} rows -> {archive_name}")

    con.execute("""
        INSERT INTO manuscript_workspace.archive_move_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), schema, table_name, archive_fq,
          n_rows, reason, SCRIPT])

    return True, n_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 303 — genetics_per_patient_master_v1 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # ── Preflight: genetics_per_test_master_v1 must exist ───────────────
    if not table_exists(con, "main", "genetics_per_test_master_v1"):
        raise SystemExit(
            "genetics_per_test_master_v1 not found. "
            "Run Script 302 with --commit first."
        )
    n_test = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM main.genetics_per_test_master_v1
    """).fetchone()
    log(f"  genetics_per_test_master_v1: {n_test[0]} rows, {n_test[1]} patients")

    # ── Step 1-3: Build patient-level master ────────────────────────────
    log("Steps 1-3: Building genetics_per_patient_master_v1...")

    preview = con.execute(f"""
        SELECT
            COUNT(*)                                                     AS n_rows,
            COUNT(DISTINCT research_id)                                  AS n_rid,
            SUM(CASE WHEN was_tested THEN 1 ELSE 0 END)                  AS n_tested,
            SUM(CASE WHEN NOT was_tested THEN 1 ELSE 0 END)              AS n_untested,
            SUM(CASE WHEN NOT was_tested
                      AND (braf_patient IS NOT NULL
                           OR ras_patient IS NOT NULL
                           OR tert_patient IS NOT NULL
                           OR tp53_patient IS NOT NULL
                           OR any_fusion_patient IS NOT NULL)
                 THEN 1 ELSE 0 END)                                      AS n_untested_with_gene
        FROM ({PATIENT_MASTER_SQL}) t
    """).fetchone()

    n_rows, n_rid, n_tested, n_untested, n_bad = preview
    log(f"  Preview: rows={n_rows} distinct_rid={n_rid} "
        f"tested={n_tested} untested={n_untested}")

    if n_rows != 10871:
        raise SystemExit(f"Expected 10,871 rows, got {n_rows}")
    if n_rid != 10871:
        raise SystemExit(f"Expected 10,871 distinct RIDs, got {n_rid}")
    if not (1100 <= n_tested <= 1500):
        raise SystemExit(f"Expected ~1,286 tested, got {n_tested}")
    if n_bad > 0:
        raise SystemExit(
            f"Invariant violation: {n_bad} untested patients have "
            "non-NULL gene status columns"
        )
    log("  Invariants passed: row count, tested count, untested-gene-null check")

    # Gene status distribution among tested
    gene_dist = con.execute(f"""
        SELECT
            SUM(CASE WHEN braf_patient = TRUE  THEN 1 ELSE 0 END) AS braf_pos,
            SUM(CASE WHEN braf_patient = FALSE THEN 1 ELSE 0 END) AS braf_neg,
            SUM(CASE WHEN ras_patient  = TRUE  THEN 1 ELSE 0 END) AS ras_pos,
            SUM(CASE WHEN ras_patient  = FALSE THEN 1 ELSE 0 END) AS ras_neg,
            SUM(CASE WHEN tert_patient = TRUE  THEN 1 ELSE 0 END) AS tert_pos,
            SUM(CASE WHEN tert_patient = FALSE THEN 1 ELSE 0 END) AS tert_neg,
            SUM(CASE WHEN tp53_patient = TRUE  THEN 1 ELSE 0 END) AS tp53_pos,
            SUM(CASE WHEN tp53_patient = FALSE THEN 1 ELSE 0 END) AS tp53_neg
        FROM ({PATIENT_MASTER_SQL}) t
        WHERE was_tested
    """).fetchone()
    log("  Gene positivity (among tested):")
    log(f"    BRAF: pos={gene_dist[0]} neg={gene_dist[1]}")
    log(f"    RAS:  pos={gene_dist[2]} neg={gene_dist[3]}")
    log(f"    TERT: pos={gene_dist[4]} neg={gene_dist[5]}")
    log(f"    TP53: pos={gene_dist[6]} neg={gene_dist[7]}")

    if not args.commit:
        log("  (dry-run — no CREATE TABLE, no CPM ALTER, no archive)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    # ── Create the table ────────────────────────────────────────────────
    log("  Creating genetics_per_patient_master_v1...")
    con.execute(f"""
        CREATE OR REPLACE TABLE main.genetics_per_patient_master_v1
        AS {PATIENT_MASTER_SQL}
    """)

    n_final = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM main.genetics_per_patient_master_v1
    """).fetchone()
    log(f"  Created: {n_final[0]} rows, {n_final[1]} patients")

    # ── Step 4: Add link columns to CPM ─────────────────────────────────
    log("Step 4: Adding CPM link columns...")

    for col_name, col_type in [
        ("genetics_master_v1_link_flag", "BOOLEAN"),
        ("genetics_master_v1_episode_count", "INTEGER"),
    ]:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master'
              AND column_name = '{col_name}'
        """).fetchone()[0]
        if col_exists == 0:
            con.execute(f"""
                ALTER TABLE main.canonical_patient_master
                ADD COLUMN "{col_name}" {col_type}
            """)
            log(f"    Added column: {col_name} ({col_type})")
        else:
            log(f"    Column already exists: {col_name}")

    con.execute("""
        UPDATE main.canonical_patient_master AS c
           SET genetics_master_v1_link_flag    = g.was_tested,
               genetics_master_v1_episode_count = CAST(g.n_episodes AS INTEGER)
          FROM main.genetics_per_patient_master_v1 AS g
         WHERE c.research_id = g.research_id
    """)

    link_check = con.execute("""
        SELECT
            SUM(CASE WHEN genetics_master_v1_link_flag = TRUE  THEN 1 ELSE 0 END),
            SUM(CASE WHEN genetics_master_v1_link_flag = FALSE THEN 1 ELSE 0 END),
            SUM(CASE WHEN genetics_master_v1_link_flag IS NULL THEN 1 ELSE 0 END),
            AVG(genetics_master_v1_episode_count)
                FILTER (WHERE genetics_master_v1_link_flag = TRUE)
        FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM link columns: tested={link_check[0]} "
        f"untested={link_check[1]} null={link_check[2]} "
        f"avg_episodes={link_check[3]:.2f}" if link_check[3] else
        f"  CPM link columns: tested={link_check[0]} "
        f"untested={link_check[1]} null={link_check[2]}")

    cpm_invariants(con, "post-step4")

    # ── Step 5: Archive superseded tables ───────────────────────────────
    log("Step 5: Archiving superseded tables...")

    for schema, table_name, reason in TABLES_TO_ARCHIVE:
        archive_one(con, schema, table_name, reason, args.commit)

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 303 complete.")


if __name__ == "__main__":
    main()
