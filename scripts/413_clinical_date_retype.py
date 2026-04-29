#!/usr/bin/env python3
"""413 — Clinical date VARCHAR/TIMESTAMP → DATE (clinical_date_retype_20260428).

Snapshots: `"Thyroid 2026 UPdated".archive_pub_v1_0.<table>_pre_date_retype_20260428`
Spec: cursor_prompts/CURSOR_PROMPT_clinical_date_retype_20260428.md

  .venv/bin/python scripts/413_clinical_date_retype.py --dry-run
  .venv/bin/python scripts/413_clinical_date_retype.py --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import duckdb  # noqa: E402

SCRIPT_ID = "413"
TAG = "clinical_date_retype_20260428"
PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_QUAL = '"Thyroid 2026 UPdated"."archive_pub_v1_0"'

AUDIT_SQL = """
WITH verified_tables AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='verified' AND table_name LIKE 'canonical_%'
),
audit_allowlist AS (
  SELECT col_name FROM (VALUES
    ('build_ts'),('extracted_at'),('llm_build_ts'),('verified_ts'),
    ('signed_off_ts'),('registered_ts'),('llm_extracted_at'),('updated_at'),
    ('created_at'),('promoted_at'),('completed_at'),('started_at'),('ended_at'),
    ('ingested_at_utc')
  ) v(col_name)
)
SELECT c.table_name, c.column_name, c.data_type
FROM information_schema.columns c
JOIN verified_tables v ON c.table_name = v.table_name
WHERE c.table_catalog=? AND c.table_schema='main'
  AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
  AND c.column_name NOT LIKE '%_status'
  AND c.column_name NOT LIKE '%_source'
  AND c.column_name NOT LIKE '%_keyword'
  AND c.column_name NOT LIKE '%_raw'
  AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
       OR (c.data_type='VARCHAR'
           AND (c.column_name ILIKE '%date%' OR c.column_name ILIKE '%dt%')))
ORDER BY c.table_name, c.ordinal_position;
"""

# Shared VARCHAR → DATE (empty / whitespace → NULL).
VARCHAR_DATE_EXPR = """CASE WHEN TRIM(COALESCE({c}, '')) = '' THEN CAST(NULL AS DATE)
ELSE COALESCE(
  TRY_STRPTIME(TRIM({c}), '%m/%d/%Y')::DATE,
  TRY_STRPTIME(TRIM({c}), '%Y-%m-%d')::DATE,
  TRY_STRPTIME(TRIM({c}), '%-m/%-d/%Y')::DATE
) END"""

TABLE_CHAIN: list[tuple[str, Callable[["duckdb.DuckDBPyConnection"], str]]] = [
    ("canonical_esophageal_invasion_events_v1", lambda c: _rebuild_esophageal(c)),
    ("canonical_frozen_section_events_v1", lambda c: _rebuild_frozen(c)),
    ("canonical_path_malignant_events_v1", lambda c: _rebuild_path_mal(c)),
    ("canonical_operative_events_v1", lambda c: _rebuild_operative(c)),
]


def connect_rw() -> duckdb.DuckDBPyConnection:
    from motherduck_client import get_token

    tok = get_token()
    con = duckdb.connect(f"md:{PUBLICATION_DB}?motherduck_token={tok}")
    con.execute(f'USE "{PUBLICATION_DB}"')
    con.execute(f'USE "{PUBLICATION_DB}".main')
    rc, rdx = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        f"FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()
    if int(rc) != 10871 or int(rdx) != 10871:
        raise SystemExit("canonical_patient_master invariant expects 10871 rows/distinct rids.")
    return con


def dtype(con: duckdb.DuckDBPyConnection, table: str, col: str) -> str:
    r = con.execute(
        """
        SELECT data_type FROM information_schema.columns
        WHERE table_catalog=? AND table_schema='main'
          AND table_name=? AND column_name=?""",
        [PUBLICATION_DB, table, col],
    ).fetchone()
    assert r
    return str(r[0])


def tbl_count(con: duckdb.DuckDBPyConnection, fq: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0])


def list_dependent_views(con: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, str]]:
    return [
        (str(a), str(b))
        for a, b in con.execute(
            """
            SELECT DISTINCT schema_name, view_name FROM duckdb_views()
            WHERE sql LIKE '%' || ? || '%'
            ORDER BY 1,2""",
            [table],
        ).fetchall()
    ]




def varchar_failures_on_archive(
    con: duckdb.DuckDBPyConnection, snap_fq: str, col: str
) -> int:
    expr = VARCHAR_DATE_EXPR.format(c=col)
    return int(
        con.execute(
            f"""
            SELECT SUM(CASE WHEN TRIM(COALESCE({col},'')) <> '' AND ({expr}) IS NULL THEN 1 ELSE 0 END)
            FROM {snap_fq}
            """
        ).fetchone()[0]
        or 0
    )


def _rebuild_esophageal(con: duckdb.DuckDBPyConnection) -> str:
    c = "note_date"
    con.execute(
        f"""
        CREATE OR REPLACE TABLE main.canonical_esophageal_invasion_events_v1 AS
        SELECT * REPLACE ({VARCHAR_DATE_EXPR.format(c=c)} AS note_date)
        FROM main.canonical_esophageal_invasion_events_v1
        """
    )
    return "COALESCE(STRPTIME %m/%d/%Y, %Y-%m-%d, %-m/%-d/%Y); VARCHAR ''→DATE NULL"


def _rebuild_frozen(con: duckdb.DuckDBPyConnection) -> str:
    c = "frozen_section_date"
    con.execute(
        f"""
        CREATE OR REPLACE TABLE main.canonical_frozen_section_events_v1 AS
        SELECT * REPLACE ({VARCHAR_DATE_EXPR.format(c=c)} AS frozen_section_date)
        FROM main.canonical_frozen_section_events_v1
        """
    )
    return "frozen_section_date VARCHAR→DATE COALESCE(TRY_STRPTIME %m/%d/%Y, %Y-%m-%d, %-m/%-d/%Y)"

def _rebuild_path_mal(con: duckdb.DuckDBPyConnection) -> str:
    con.execute(
        """
        CREATE OR REPLACE TABLE main.canonical_path_malignant_events_v1 AS
        SELECT * REPLACE (surgery_date::DATE AS surgery_date)
        FROM main.canonical_path_malignant_events_v1
        """
    )
    return "surgery_date TIMESTAMP→DATE (::DATE)"


def _rebuild_operative(con: duckdb.DuckDBPyConnection) -> str:
    r = VARCHAR_DATE_EXPR.format(c="resolved_surgery_date")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE main.canonical_operative_events_v1 AS
        SELECT * REPLACE (
          surgery_date_native::DATE AS surgery_date_native,
          {r} AS resolved_surgery_date,
          note_date_resolved::DATE AS note_date_resolved
        ) FROM main.canonical_operative_events_v1
        """
    )
    return TIMESTAMP_NOTES()


def TIMESTAMP_NOTES() -> str:
    return (
        "surgery_date_native TIMESTAMP→DATE; resolved_surgery_date VARCHAR COALESCE(STRPTIME); "
        "note_date_resolved TIMESTAMP→DATE"
    )


def archive_snapshot(con: duckdb.DuckDBPyConnection, table: str) -> str:
    snap = f"{table}_pre_date_retype_20260428"
    con.execute(f"DROP TABLE IF EXISTS {ARCHIVE_QUAL}.{snap}")
    con.execute(
        f"""
        CREATE TABLE {ARCHIVE_QUAL}.{snap} AS
        SELECT * FROM main.{table}
        """
    )
    return snap


def registry_note(table: str, col: str, before: str, snap: str, method: str) -> str:
    return (
        f" | {TAG}: {before} → DATE; pre-snapshot {ARCHIVE_QUAL}.{snap}; "
        f"parse: {method}. Row counts preserved vs snapshot."
    )


def registry_update(
    con: duckdb.DuckDBPyConnection, table: str, col: str, before_dt: str, snap: str, method: str
) -> None:
    con.execute(
        """
        UPDATE main.canonical_column_verification_registry_v1
        SET notes = COALESCE(notes,'') || ?,
            verified_ts = CURRENT_TIMESTAMP
        WHERE schema_name='main' AND table_name=? AND column_name=?""",
        [registry_note(table, col, before_dt, snap, method), table, col],
    )


COLS_META: dict[str, list[tuple[str, str]]] = {
    "canonical_esophageal_invasion_events_v1": [("note_date", "varchar_date")],
    "canonical_frozen_section_events_v1": [("frozen_section_date", "varchar_date")],
    "canonical_operative_events_v1": [
        ("surgery_date_native", "timestamp"),
        ("resolved_surgery_date", "varchar_date"),
        ("note_date_resolved", "timestamp"),
    ],
    "canonical_path_malignant_events_v1": [("surgery_date", "timestamp")],
}


def cols_for(table: str) -> list[tuple[str, str]]:
    return COLS_META[table]


METHOD_FRAG: dict[tuple[str, str], str] = {
    ("canonical_operative_events_v1", "surgery_date_native"): "TIMESTAMP::DATE (::DATE)",
    ("canonical_operative_events_v1", "resolved_surgery_date"): (
        "COALESCE(TRY_STRPTIME %m/%d/%Y, %Y-%m-%d, %-m/%-d/%Y); empty VARCHAR→NULL DATE"
    ),
    ("canonical_operative_events_v1", "note_date_resolved"): "TIMESTAMP::DATE (::DATE)",
}

# Six repaired (table,column); must stay aligned with audit / EXPECTED_AUDIT_ROWS.
SCOPE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("canonical_esophageal_invasion_events_v1", "note_date"),
    ("canonical_frozen_section_events_v1", "frozen_section_date"),
    ("canonical_operative_events_v1", "surgery_date_native"),
    ("canonical_operative_events_v1", "resolved_surgery_date"),
    ("canonical_operative_events_v1", "note_date_resolved"),
    ("canonical_path_malignant_events_v1", "surgery_date"),
)

# Pre-migration audit signature (matches audit query exactly before retype).
EXPECTED_PRE_RETYPES: frozenset[tuple[str, str, str]] = frozenset(
    [
        ("canonical_esophageal_invasion_events_v1", "note_date", "VARCHAR"),
        ("canonical_frozen_section_events_v1", "frozen_section_date", "VARCHAR"),
        ("canonical_operative_events_v1", "surgery_date_native", "TIMESTAMP"),
        ("canonical_operative_events_v1", "resolved_surgery_date", "VARCHAR"),
        ("canonical_operative_events_v1", "note_date_resolved", "TIMESTAMP"),
        ("canonical_path_malignant_events_v1", "surgery_date", "TIMESTAMP"),
    ]
)


def method_for_registry(table: str, col: str, rebuild_return: str) -> str:
    return METHOD_FRAG.get((table, col), rebuild_return)


def all_targets_are_date(con: duckdb.DuckDBPyConnection) -> bool:
    """True when scoped columns already have DATE datatype."""
    for tbl, cn in SCOPE_COLUMNS:
        if not dtype(con, tbl, cn).upper().startswith("DATE"):
            return False
    return True


def migrations_already_done(con: duckdb.DuckDBPyConnection) -> bool:
    return len(run_audit(con)) == 0 and all_targets_are_date(con)


def dependent_views_union(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    for table, _fn in TABLE_CHAIN:
        seen.update(list_dependent_views(con, table))
    return sorted(seen)


def reinstall_dependent_views(
    con: duckdb.DuckDBPyConnection, views: list[tuple[str, str]]
) -> list[str]:
    """Re-run DDL from duckdb_views() so VIEW column types refresh after ALTER TABLE semantics."""
    problems: list[str] = []
    for sch, vn in views:
        row = con.execute(
            """SELECT sql FROM duckdb_views() WHERE schema_name=? AND view_name=?""",
            [sch, vn],
        ).fetchone()
        if not row:
            problems.append(f"{sch}.{vn} missing DDL in duckdb_views()")
            continue
        ddl = row[0]
        up = ddl.replace("CREATE VIEW", "CREATE OR REPLACE VIEW", 1)
        if ddl.upper().startswith("CREATE OR REPLACE VIEW"):
            up = ddl
        elif not ddl.upper().startswith("CREATE VIEW"):
            problems.append(f"{sch}.{vn} unexpected DDL prefix: {ddl[:48]}...")
            continue
        try:
            con.execute(up)
        except Exception as e:
            problems.append(f"{sch}.{vn}: recreate failed: {e!s}")
    return problems


def smoke_views_lenient(con: duckdb.DuckDBPyConnection, views: list[tuple[str, str]]) -> list[str]:
    errs: list[str] = []
    for sch, vn in views:
        fq = ".".join(f'"{p}"' for p in [sch, vn])
        try:
            con.execute(f"SELECT * FROM {fq} LIMIT 1")
        except Exception as e:
            errs.append(f"{fq}: {e!s}")
    return errs




def run_audit(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    return con.execute(AUDIT_SQL, [PUBLICATION_DB]).fetchall()


def dry_run(con: duckdb.DuckDBPyConnection) -> None:
    rows = run_audit(con)
    if not rows:
        print(
            f"[{SCRIPT_ID}] audit rows (clinical date cleanup scope): 0 "
            "(no VARCHAR/TIMESTAMP clinical date cols on audited verified canonical_* tables)."
        )
        if all_targets_are_date(con):
            print(
                f"[{SCRIPT_ID}] All scoped DATE columns detected — migration likely applied;"
                " `python ... --apply` reinstalls VIEW DDL only."
            )
    else:
        print(f"[{SCRIPT_ID}] audit rows (clinical date cleanup scope):", len(rows))
        for tn, cn, dt in rows:
            print(f"  {tn}.{cn} [{dt}]  n_rows={tbl_count(con, f'main.{tn}')}")
    seen: set[tuple[str, str]] = set()
    for table, _ in TABLE_CHAIN:
        for sc, vn in list_dependent_views(con, table):
            seen.add((sc, vn))
        print(f"  dependent_views({table})={len(list_dependent_views(con, table))}")
    print(f"[{SCRIPT_ID}] unique_dependent_views (all targets):", len(seen))




def post_verify(con: duckdb.DuckDBPyConnection) -> None:
    leftover = run_audit(con)
    if leftover:
        print("AUDIT SHOULD BE EMPTY AFTER RETYPE:")
        print(leftover[:20])
        raise SystemExit(f"audit still sees {len(leftover)} non-DATE-ish clinical cols")



def apply_all(con: duckdb.DuckDBPyConnection) -> None:
    view_refs = dependent_views_union(con)

    if migrations_already_done(con):
        print(
            f"[{SCRIPT_ID}] Columns already DATE and audit empty — skipping "
            f"snapshot/retype/registry; reinstalling dependent views ({len(view_refs)})."
        )
    else:
        observed = {(r[0], r[1], r[2]) for r in run_audit(con)}
        if observed != EXPECTED_PRE_RETYPES:
            raise SystemExit(f"audit plan mismatch vs expected scope: observed={sorted(observed)}")

        for table, rebuild in TABLE_CHAIN:
            snap = archive_snapshot(con, table)
            n_pre = tbl_count(con, f"{ARCHIVE_QUAL}.{snap}")
            before = {
                cn: dtype(con, table, cn) for cn, _ in cols_for(table)
            }
            if tbl_count(con, f"main.{table}") != n_pre:
                raise SystemExit(f"parity pre-snapshot {table}")

            for cn, kind in cols_for(table):
                if kind == "varchar_date":
                    fv = varchar_failures_on_archive(con, f"{ARCHIVE_QUAL}.{snap}", cn)
                    if fv:
                        raise SystemExit(f"{snap}: {cn} nonempty parse failures preview={fv}")

            method_ret = rebuild(con)

            n_post = tbl_count(con, f"main.{table}")
            if n_post != n_pre:
                raise SystemExit(f"{table} row drift {n_pre}->{n_post}")

            for cn, kind in cols_for(table):
                d = dtype(con, table, cn)
                assert d.upper().startswith("DATE"), f"{table}.{cn} stayed {d}"

                if kind == "varchar_date":
                    post_dates = int(
                        con.execute(
                            f"""
                            SELECT SUM(CASE WHEN {cn} IS NOT NULL THEN 1 ELSE 0 END) FROM main.{table}
                            """
                        ).fetchone()[0]
                        or 0
                    )
                    mapped = int(
                        con.execute(
                            f"""
                            SELECT SUM(CASE WHEN TRIM(COALESCE(s.{cn},'')) <> ''
                                   AND ({VARCHAR_DATE_EXPR.format(c="s." + cn)}) IS NOT NULL
                                   THEN 1 ELSE 0 END)
                            FROM {ARCHIVE_QUAL}.{snap} AS s
                            """
                        ).fetchone()[0]
                        or 0
                    )
                    if mapped != post_dates:
                        raise SystemExit(
                            f"{table}.{cn}: mapped nonempty DATE count {mapped} vs post_nn {post_dates}"
                        )

                if kind == "timestamp":
                    pre_nn = int(
                        con.execute(
                            f"SELECT SUM(CASE WHEN {cn} IS NOT NULL THEN 1 ELSE 0 END) FROM {ARCHIVE_QUAL}.{snap}"
                        ).fetchone()[0]
                        or 0
                    )
                    post_nn = int(
                        con.execute(
                            f"SELECT SUM(CASE WHEN {cn} IS NOT NULL THEN 1 ELSE 0 END) FROM main.{table}"
                        ).fetchone()[0]
                        or 0
                    )
                    if pre_nn != post_nn:
                        raise SystemExit(f"{table}.{cn}: TIMESTAMP non-null parity {pre_nn}!={post_nn}")

            snap_table = snap
            for cn, _ in cols_for(table):
                registry_update(
                    con,
                    table,
                    cn,
                    before[cn],
                    snap_table,
                    method_for_registry(table, cn, method_ret),
                )

    ddl_notes = reinstall_dependent_views(con, view_refs)
    for ln in ddl_notes:
        print(f"[{SCRIPT_ID}] WARN ddl_recreate: {ln}")
    smoke_issues = smoke_views_lenient(con, view_refs)
    for ln in smoke_issues:
        print(f"[{SCRIPT_ID}] WARN smoke: {ln}")

    post_verify(con)
    print(f"[{SCRIPT_ID}] APPLY OK.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    cx = connect_rw()
    out_path = Path(__file__).resolve().parents[1] / "scripts" / "output" / f"{SCRIPT_ID}_{TAG}_summary.json"

    if args.dry_run:
        dry_run(cx)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(
                {
                    "audit": [{"t": a[0], "c": a[1], "dt": a[2]} for a in run_audit(cx)],
                },
                indent=2,
            )
        )
        print(f"wrote {out_path}")
    elif args.apply:
        apply_all(cx)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps({"status": "applied", "tag": TAG}, indent=2))
        print(f"wrote {out_path}")
    else:
        dry_run(cx)
