"""
Script 337 — Build verify.concordance_master_v1 (merge 12 summary tables).

Per Prompt 4: UNION ALL the 12 main.verify_*_summary_v1 tables into a single
verify.concordance_master_v1 with a `domain` column. Map each source's actual
columns to a canonical schema (NULL where source lacks a column, with `notes`
flagging the gap). Reference-safe drop afterwards, with archive snapshot to
"Thyroid 2026 UPdated".archive_pub_v1_0.

The "expected canonical" schema in the prompt is aspirational — the
real Prompt-2 summaries only carry:
    domain, field_name, n_agree, n_disagree, n_excel_only, n_llm_only, pct_agree
and 2 of the 12 (pathology_synoptics, us_nodule) additionally carry:
    n_total, n_both_null, n_excel_nonnull, n_llm_nonnull
We map those to the canonical names where the math is unambiguous and
otherwise leave NULL.

Usage:
    python 337_build_verify_concordance_master.py            # dry-run
    python 337_build_verify_concordance_master.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "337_build_verify_concordance_master"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"
DB = "thyroid_canonical_publication_v1_0"

# domain string derived from table_name = 'verify_<domain>_summary_v1'
# the 12 source tables are enumerated dynamically; this is just for sanity-checks
EXPECTED_DOMAINS = {
    "airway_invasion", "frozen_section", "genetics_per_test", "labs", "ln",
    "operative", "parathyroid", "pathology_synoptics", "rai", "recurrence",
    "us_nodule", "vascular_invasion",
}


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


def list_summary_tables(con):
    rows = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='main'
           AND table_name LIKE 'verify_%_summary_v1'
         ORDER BY table_name
    """).fetchall()
    return [r[0] for r in rows]


def domain_from_summary_name(name):
    # 'verify_<domain>_summary_v1' -> '<domain>'
    assert name.startswith("verify_") and name.endswith("_summary_v1"), name
    return name[len("verify_"):-len("_summary_v1")]


def get_columns(con, schema, table):
    return [r[0] for r in con.execute(f"""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='{DB}' AND schema_name='{schema}' AND table_name='{table}'
         ORDER BY column_index
    """).fetchall()]


def build_select_for_source(table_name, cols):
    """Map a source verify_*_summary_v1's columns onto the canonical schema.

    Canonical columns (in order):
      domain, field_name, n_rows_evaluated, n_excel_populated,
      n_llm_populated, n_both_populated, n_concordant,
      n_discordant_excel_only, n_discordant_llm_only, n_value_mismatch,
      concordance_pct_both_populated, concordance_pct_of_excel,
      notes, built_at, source_table
    """
    domain = domain_from_summary_name(table_name)
    has_total = "n_total" in cols
    has_both_null = "n_both_null" in cols
    has_excel_nn = "n_excel_nonnull" in cols
    has_llm_nn = "n_llm_nonnull" in cols

    # n_rows_evaluated: prefer n_total; else sum of agree+disagree+excel_only+llm_only(+both_null)
    if has_total:
        n_rows_evaluated_expr = "CAST(n_total AS BIGINT)"
    else:
        parts = ["COALESCE(n_agree,0)", "COALESCE(n_disagree,0)",
                 "COALESCE(n_excel_only,0)", "COALESCE(n_llm_only,0)"]
        if has_both_null:
            parts.append("COALESCE(n_both_null,0)")
        n_rows_evaluated_expr = f"CAST({' + '.join(parts)} AS BIGINT)"

    # n_excel_populated: prefer n_excel_nonnull; else agree + disagree + excel_only
    if has_excel_nn:
        n_excel_populated_expr = "CAST(n_excel_nonnull AS BIGINT)"
    else:
        n_excel_populated_expr = ("CAST(COALESCE(n_agree,0) + COALESCE(n_disagree,0) "
                                  "+ COALESCE(n_excel_only,0) AS BIGINT)")

    if has_llm_nn:
        n_llm_populated_expr = "CAST(n_llm_nonnull AS BIGINT)"
    else:
        n_llm_populated_expr = ("CAST(COALESCE(n_agree,0) + COALESCE(n_disagree,0) "
                                "+ COALESCE(n_llm_only,0) AS BIGINT)")

    # n_both_populated: agree + disagree (rows where both excel and llm have a value)
    n_both_populated_expr = "CAST(COALESCE(n_agree,0) + COALESCE(n_disagree,0) AS BIGINT)"

    # concordance_pct_of_excel = n_agree / (n_agree + n_disagree + n_excel_only)
    concordance_pct_of_excel_expr = (
        "CASE WHEN (COALESCE(n_agree,0) + COALESCE(n_disagree,0) "
        "+ COALESCE(n_excel_only,0)) > 0 "
        "THEN CAST(COALESCE(n_agree,0) AS DOUBLE) / "
        "(COALESCE(n_agree,0) + COALESCE(n_disagree,0) + COALESCE(n_excel_only,0)) "
        "ELSE NULL END"
    )

    # notes: track what we synthesized
    note_parts = []
    if not has_total:
        note_parts.append("n_total derived")
    if not has_both_null:
        note_parts.append("n_both_null absent")
    if not has_excel_nn:
        note_parts.append("n_excel_populated derived")
    if not has_llm_nn:
        note_parts.append("n_llm_populated derived")
    notes_str = "; ".join(note_parts) if note_parts else ""

    # Escape single quotes in notes_str (none expected, but defensive)
    notes_str_sql = notes_str.replace("'", "''")

    sql = f"""SELECT
        '{domain}' AS domain,
        CAST(field_name AS VARCHAR) AS field_name,
        {n_rows_evaluated_expr} AS n_rows_evaluated,
        {n_excel_populated_expr} AS n_excel_populated,
        {n_llm_populated_expr} AS n_llm_populated,
        {n_both_populated_expr} AS n_both_populated,
        CAST(COALESCE(n_agree,0) AS BIGINT) AS n_concordant,
        CAST(COALESCE(n_excel_only,0) AS BIGINT) AS n_discordant_excel_only,
        CAST(COALESCE(n_llm_only,0) AS BIGINT) AS n_discordant_llm_only,
        CAST(COALESCE(n_disagree,0) AS BIGINT) AS n_value_mismatch,
        CAST(pct_agree AS DOUBLE) AS concordance_pct_both_populated,
        {concordance_pct_of_excel_expr} AS concordance_pct_of_excel,
        '{notes_str_sql}' AS notes,
        CURRENT_TIMESTAMP AS built_at,
        '{table_name}' AS source_table
        FROM main."{table_name}"
    """
    return sql


def check_references(con, table_name):
    """Return list of view fq-names that reference this table (excluding self
    and the archive schema)."""
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


def archive_and_drop(con, table_name, utcz, source_rowcount):
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
          source_rowcount,
          "Schema reorg: merged into verify.concordance_master_v1",
          SCRIPT])
    return archive_fq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()
    ensure_logs(con)

    log("=" * 72)
    log(f"Script 337 — verify.concordance_master_v1 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # 1. Enumerate sources
    sources = list_summary_tables(con)
    log(f"  Found {len(sources)} verify_*_summary_v1 tables")
    if len(sources) != 12:
        log(f"  WARNING: expected 12 summary tables, got {len(sources)}")
    domains = {domain_from_summary_name(s) for s in sources}
    missing = EXPECTED_DOMAINS - domains
    extra = domains - EXPECTED_DOMAINS
    if missing:
        log(f"  WARNING: missing expected domains: {sorted(missing)}")
    if extra:
        log(f"  WARNING: unexpected domains found: {sorted(extra)}")

    # 2. Per-source rowcount + canonical SELECT
    src_rowcounts = {}
    select_sqls = []
    for t in sources:
        n = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
        src_rowcounts[t] = n
        cols = get_columns(con, "main", t)
        sql = build_select_for_source(t, cols)
        select_sqls.append(sql)
        log(f"    {t}: {n} rows, cols={cols}")

    src_total = sum(src_rowcounts.values())
    log(f"  Source row total: {src_total}")

    union_sql = "\nUNION ALL\n".join(select_sqls)

    # 3. Preview + ensure schema
    if not args.commit:
        preview = con.execute(f"SELECT COUNT(*) FROM ({union_sql}) t").fetchone()[0]
        log(f"  Preview merged rowcount: {preview}")
        if preview != src_total:
            raise SystemExit(
                f"Preview parity failed: {preview} merged != {src_total} source sum"
            )
        # Distinct domains
        d = con.execute(f"SELECT COUNT(DISTINCT domain) FROM ({union_sql}) t").fetchone()[0]
        log(f"  Distinct domains in preview: {d}")
        cpm_invariants(con, "post-dryrun")
        log("(dry-run — re-run with --commit to apply)")
        return

    # 4. COMMIT path
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{DB}".verify')

    con.execute(f'CREATE OR REPLACE TABLE verify.concordance_master_v1 AS {union_sql}')
    merged_n = con.execute(
        "SELECT COUNT(*) FROM verify.concordance_master_v1"
    ).fetchone()[0]
    log(f"  verify.concordance_master_v1: {merged_n} rows")

    if merged_n != src_total:
        raise SystemExit(
            f"Parity check FAILED: merged={merged_n}, source sum={src_total}"
        )

    merged_domains = [r[0] for r in con.execute(
        "SELECT DISTINCT domain FROM verify.concordance_master_v1 ORDER BY domain"
    ).fetchall()]
    log(f"  Domains in merged table ({len(merged_domains)}): {merged_domains}")

    # 5. Reference-safety + archive + drop each source
    utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    n_dropped = 0
    n_skipped_referenced = 0
    for t in sources:
        n = src_rowcounts[t]
        refs = check_references(con, t)
        # Filter out self-references (shouldn't happen for tables, but defensive)
        external_refs = [r for r in refs
                         if not (r[1] == DB and r[2] == 'main' and r[3] == t)]
        if external_refs:
            log(f"    {t}: REFERENCED by {len(external_refs)} view(s) — logging "
                f"orphan refs, skipping drop")
            for ref in external_refs:
                con.execute("""
                    INSERT INTO manuscript_workspace.schema_reorg_orphan_references_v1
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [dt.datetime.utcnow(), "main", t,
                      ref[1], ref[2], ref[3], "view",
                      "References merged-source summary table; rewrite to "
                      "verify.concordance_master_v1 WHERE domain='...'",
                      SCRIPT])
            n_skipped_referenced += 1
            # Still log a merge_union row but with "skipped" note
            con.execute("""
                INSERT INTO manuscript_workspace.schema_reorg_move_log_v1
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [dt.datetime.utcnow(), "main", t,
                  "verify", "concordance_master_v1",
                  "merge_union_dropskip", n, merged_n, SCRIPT])
            continue

        archive_fq = archive_and_drop(con, t, utcz, n)
        log(f"    {t}: archived -> {archive_fq}; dropped from main")
        con.execute("""
            INSERT INTO manuscript_workspace.schema_reorg_move_log_v1
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [dt.datetime.utcnow(), "main", t,
              "verify", "concordance_master_v1",
              "merge_union", n, merged_n, SCRIPT])
        n_dropped += 1

    log(f"  Dropped {n_dropped}/{len(sources)} sources; "
        f"{n_skipped_referenced} skipped due to external view references")

    # 6. Final invariant: no verify_*_summary_v1 left in main (unless skipped)
    remaining = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='main'
           AND table_name LIKE 'verify_%_summary_v1'
         ORDER BY table_name
    """).fetchall()
    log(f"  Remaining verify_*_summary_v1 in main: {len(remaining)} "
        f"{'(all due to view refs)' if remaining else ''}")
    for r in remaining:
        log(f"    {r[0]}")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 337 complete.")


if __name__ == "__main__":
    main()
