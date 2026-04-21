"""
Script 338 — Build verify.verify_long_v1 (melt 12 detail tables).

Per Prompt 4: melt 12 main.verify_<domain>_v1 detail tables (NOT
*_summary_v1) into a single long-format verify.verify_long_v1 with
columns:
    research_id, domain, field_name,
    excel_value, llm_value,
    source_text, source_note_ref, source_note_date, concordance_status,
    built_at

Reality (per inspection): the actual detail tables only carry the
3-suffix pattern (`_excel`, `_llm`, `_concordance`) — none have
`_source_text`, `_source_note_ref`, `_source_note_date` columns.
We still emit those columns in the long table (always NULL) so the
canonical schema is honored and any future detail tables that DO carry
source-text columns can be melted with the same logic.

Drops `both_null` rows (excel_value, llm_value, source_text,
concordance_status all NULL) — these add no audit value.

Usage:
    python 338_build_verify_long.py            # dry-run
    python 338_build_verify_long.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "338_build_verify_long"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "archive_pub_v1_0"
DB = "thyroid_canonical_publication_v1_0"

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


def list_detail_tables(con):
    rows = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='main'
           AND table_name LIKE 'verify_%_v1'
           AND table_name NOT LIKE 'verify_%_summary_v1'
         ORDER BY table_name
    """).fetchall()
    return [r[0] for r in rows]


def domain_from_detail_name(name):
    # 'verify_<domain>_v1' -> '<domain>'
    assert name.startswith("verify_") and name.endswith("_v1"), name
    return name[len("verify_"):-len("_v1")]


SUFFIXES = ["_excel", "_llm", "_source_text",
            "_source_note_ref", "_source_note_date", "_concordance"]


def get_columns(con, schema, table):
    return [r[0] for r in con.execute(f"""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='{DB}' AND schema_name='{schema}' AND table_name='{table}'
         ORDER BY column_index
    """).fetchall()]


def discover_base_fields(cols):
    """Return dict {base_field: set(present_suffixes)}."""
    out = {}
    for c in cols:
        for suf in SUFFIXES:
            if c.endswith(suf) and len(c) > len(suf):
                base = c[:-len(suf)]
                out.setdefault(base, set()).add(suf)
                break
    return out


def build_select_for_field(table_name, domain, base_field, present_suffixes):
    """One SELECT for one (domain, field) — emits canonical 9-col long row."""
    def col_or_null(suf, cast_to=None):
        if suf in present_suffixes:
            col = f'"{base_field}{suf}"'
            if cast_to:
                return f"CAST({col} AS {cast_to})"
            return col
        if cast_to:
            return f"CAST(NULL AS {cast_to})"
        return "NULL"

    excel_expr = col_or_null("_excel", "VARCHAR")
    llm_expr = col_or_null("_llm", "VARCHAR")
    src_text_expr = col_or_null("_source_text", "VARCHAR")
    src_note_ref_expr = col_or_null("_source_note_ref", "VARCHAR")
    src_note_date_expr = col_or_null("_source_note_date", "DATE")
    conc_expr = col_or_null("_concordance", "VARCHAR")

    return f"""SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        '{domain}' AS domain,
        '{base_field}' AS field_name,
        {excel_expr} AS excel_value,
        {llm_expr} AS llm_value,
        {src_text_expr} AS source_text,
        {src_note_ref_expr} AS source_note_ref,
        {src_note_date_expr} AS source_note_date,
        {conc_expr} AS concordance_status,
        CURRENT_TIMESTAMP AS built_at
        FROM main."{table_name}"
    """


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
          "Schema reorg: melted into verify.verify_long_v1",
          SCRIPT])
    return archive_fq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()
    ensure_logs(con)

    log("=" * 72)
    log(f"Script 338 — verify.verify_long_v1 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    sources = list_detail_tables(con)
    log(f"  Found {len(sources)} verify_*_v1 detail tables")
    if len(sources) != 12:
        log(f"  WARNING: expected 12 detail tables, got {len(sources)}")
    domains = {domain_from_detail_name(s) for s in sources}
    missing = EXPECTED_DOMAINS - domains
    extra = domains - EXPECTED_DOMAINS
    if missing:
        log(f"  WARNING: missing expected domains: {sorted(missing)}")
    if extra:
        log(f"  WARNING: unexpected domains found: {sorted(extra)}")

    src_meta = []  # list of (table_name, domain, rowcount, base_fields_dict)
    select_sqls = []
    expected_long_rows_no_dedup = 0
    for t in sources:
        domain = domain_from_detail_name(t)
        cols = get_columns(con, "main", t)
        if "research_id" not in cols:
            log(f"    {t}: SKIP (no research_id column)")
            continue
        n = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
        bases = discover_base_fields(cols)
        if not bases:
            log(f"    {t}: SKIP (no per-field suffix columns found)")
            continue
        log(f"    {t}: {n} rows, {len(bases)} base field(s): {sorted(bases.keys())}")
        for base_field, present in sorted(bases.items()):
            select_sqls.append(
                build_select_for_field(t, domain, base_field, present)
            )
            expected_long_rows_no_dedup += n
        src_meta.append((t, domain, n, bases))

    log(f"  Total field-emissions: {len(select_sqls)}")
    log(f"  Expected long rows (pre-null-drop): "
        f"{expected_long_rows_no_dedup}")

    # Wrap UNION ALL in a CTE so we can count + filter in one pass.
    # Per Prompt 4: drop "both-null" rows that add no audit value.
    # In the actual schema, these are rows where excel_value AND llm_value
    # are both NULL (the underlying tables tag these with concordance_status
    # = 'both_null' as a string). We additionally require source_text IS NULL
    # so any future detail tables that carry source-text but no excel/llm
    # value are preserved.
    union_sql = "\nUNION ALL\n".join(select_sqls)
    final_sql = f"""WITH melted AS (
        {union_sql}
    )
    SELECT * FROM melted
     WHERE NOT (excel_value IS NULL
                AND llm_value IS NULL
                AND source_text IS NULL)
    """

    if not args.commit:
        # Preview parity
        all_n = con.execute(
            f"SELECT COUNT(*) FROM ({union_sql}) t"
        ).fetchone()[0]
        kept_n = con.execute(
            f"SELECT COUNT(*) FROM ({final_sql}) t"
        ).fetchone()[0]
        dropped_n = all_n - kept_n
        log(f"  Preview: melted_rows={all_n}, kept={kept_n}, dropped_null={dropped_n}")
        if all_n != expected_long_rows_no_dedup:
            raise SystemExit(
                f"Pre-melt parity failed: melted_rows={all_n} "
                f"!= expected={expected_long_rows_no_dedup}"
            )
        d = con.execute(
            f"SELECT COUNT(DISTINCT domain) FROM ({final_sql}) t"
        ).fetchone()[0]
        log(f"  Distinct domains in kept rows: {d}")
        cpm_invariants(con, "post-dryrun")
        log("(dry-run — re-run with --commit to apply)")
        return

    # COMMIT path
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{DB}".verify')
    con.execute(f'CREATE OR REPLACE TABLE verify.verify_long_v1 AS {final_sql}')

    long_n = con.execute(
        "SELECT COUNT(*) FROM verify.verify_long_v1"
    ).fetchone()[0]
    log(f"  verify.verify_long_v1: {long_n} rows")

    # Domain coverage check
    long_domains = [r[0] for r in con.execute(
        "SELECT DISTINCT domain FROM verify.verify_long_v1 ORDER BY domain"
    ).fetchall()]
    log(f"  Distinct domains in verify_long_v1 ({len(long_domains)}): "
        f"{long_domains}")

    # Per-(domain, field) coverage — must match source field count
    per_domain_field_counts = con.execute("""
        SELECT domain, COUNT(DISTINCT field_name)
          FROM verify.verify_long_v1
         GROUP BY domain
         ORDER BY domain
    """).fetchall()
    log("  Per-domain distinct field counts (post-melt):")
    for d, cnt in per_domain_field_counts:
        expected_cnt = sum(len(b) for (_, dd, _, b) in src_meta if dd == d)
        log(f"    {d}: {cnt} fields (source had {expected_cnt})")

    # Total dropped (info)
    melted_n = con.execute(
        f"SELECT COUNT(*) FROM ({union_sql}) t"
    ).fetchone()[0]
    log(f"  Pre-null-filter melted rows: {melted_n}")
    log(f"  Dropped both-null rows: {melted_n - long_n}")

    # Cross-check against verify.concordance_master_v1
    try:
        cm_pairs = set(con.execute("""
            SELECT domain, field_name FROM verify.concordance_master_v1
        """).fetchall())
        long_pairs = set(con.execute("""
            SELECT DISTINCT domain, field_name FROM verify.verify_long_v1
             WHERE concordance_status IS NOT NULL
        """).fetchall())
        missing = cm_pairs - long_pairs
        if missing:
            log(f"  WARNING: {len(missing)} (domain,field) pairs in concordance_master "
                f"but absent from verify_long with concordance_status NOT NULL:")
            for d, f in sorted(missing):
                log(f"    {d} / {f}")
        else:
            log(f"  Coverage check PASS: all {len(cm_pairs)} (domain,field) pairs "
                f"in concordance_master_v1 present in verify_long_v1")
    except Exception as e:
        log(f"  Coverage cross-check ERROR: {e}")

    # Reference-safety + archive + drop
    utcz = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    n_dropped = 0
    n_skipped = 0
    for (t, _domain, n, _bases) in src_meta:
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
                      "References melted-source detail; rewrite to "
                      "verify.verify_long_v1 WHERE domain='...' AND field_name='...'",
                      SCRIPT])
            con.execute("""
                INSERT INTO manuscript_workspace.schema_reorg_move_log_v1
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [dt.datetime.utcnow(), "main", t,
                  "verify", "verify_long_v1",
                  "merge_melt_dropskip", n, long_n, SCRIPT])
            n_skipped += 1
            continue

        archive_fq = archive_and_drop(con, t, utcz, n)
        log(f"    {t}: archived -> {archive_fq}; dropped from main")
        con.execute("""
            INSERT INTO manuscript_workspace.schema_reorg_move_log_v1
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [dt.datetime.utcnow(), "main", t,
              "verify", "verify_long_v1",
              "merge_melt", n, long_n, SCRIPT])
        n_dropped += 1

    log(f"  Dropped {n_dropped}/{len(src_meta)} sources; {n_skipped} skipped")

    remaining = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
         WHERE database_name='{DB}' AND schema_name='main'
           AND table_name LIKE 'verify_%_v1'
           AND table_name NOT LIKE 'verify_%_summary_v1'
         ORDER BY table_name
    """).fetchall()
    log(f"  Remaining verify_*_v1 detail in main: {len(remaining)}")
    for r in remaining:
        log(f"    {r[0]}")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 338 complete.")


if __name__ == "__main__":
    main()
