"""
Script 346 — Archive 5 extracted_*_v1 legacy pipeline outputs.

Tables archived (lineage content-verified 2026-04-21):

  extracted_braf_recovery_v1            -> canonical_molecular_tested_v1 + molecular_test_episode_v2.braf_*
  extracted_ete_subgraded_v1            -> CPM ete_* columns
  extracted_fna_bethesda_v1             -> fna_episode_master_v2.bethesda_*
  extracted_postop_labs_expanded_v1     -> note_entities_llm_labs (superset)
  extracted_ras_patient_summary_v1      -> molecular_test_episode_v2.ras_*

Deferred (NOT archived in this script):
  extracted_tirads_validated_v1         -> awaits RunPod tirads_granular re-extraction (Prompt 7)

Pattern per table:
  1. Pre-snapshot row+col count to prompt6_prestate_v1
  2. CTAS to "Thyroid 2026 UPdated".archive_pub_v1_0.<name>_pre346_<UTCZ>
  3. Assert archive row count == source row count (RAISE on mismatch)
  4. INSERT to archive_move_log_v1 with reason citing lineage
  5. DROP source table
  6. Post-snapshot (count of `main` objects)

Final assertions (RAISE on failure):
  - 5 rows added to archive_move_log_v1 with script='346_archive_extracted_legacy'
  - `main` table count decreased by exactly 5
  - extracted_tirads_validated_v1 still present in main
  - All 5 archive tables exist in archive_pub_v1_0 with matching row counts
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
ARCH_DB = '"Thyroid 2026 UPdated"'
SCRIPT_NUM = 346
SCRIPT_TAG = "346_archive_extracted_legacy"
UTC = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

ARCHIVE_PLAN: list[tuple[str, str]] = [
    ("extracted_braf_recovery_v1",
     "data preserved in canonical_molecular_tested_v1 + molecular_test_episode_v2.braf_*"),
    ("extracted_ete_subgraded_v1",
     "data preserved in canonical_patient_master ete_* columns (ete_original_grade, ete_op_note_grade, ete_refined_grade, ete_subgrade_method)"),
    ("extracted_fna_bethesda_v1",
     "data preserved in fna_episode_master_v2 (bethesda_category, bethesda_raw)"),
    ("extracted_postop_labs_expanded_v1",
     "data preserved in note_entities_llm_labs (superset of source notes; 1051/1051 rid coverage)"),
    ("extracted_ras_patient_summary_v1",
     "data preserved in molecular_test_episode_v2.ras_* (320/321 rid coverage; 1-rid gap noted in archive)"),
]

DEFERRED = ["extracted_tirads_validated_v1"]


def header(s):
    print()
    print("=" * 78)
    print(s)
    print("=" * 78)


def main_object_count() -> int:
    return con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main'
    """).fetchone()[0]


def table_present(schema: str, name: str, db: str = DB) -> bool:
    db_clean = db.strip('"')
    r = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name=? AND schema_name=? AND table_name=?
    """, [db_clean, schema, name]).fetchone()[0]
    return r > 0


def col_count(schema: str, name: str) -> int:
    return con.execute(f"""
        SELECT COUNT(*) FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name=? AND table_name=?
    """, [schema, name]).fetchone()[0]


def row_count_main(name: str) -> int:
    return con.execute(f'SELECT COUNT(*) FROM {DB}.main."{name}"').fetchone()[0]


# 0. Pre-state object count
header("0. Pre-state")
pre_main_count = main_object_count()
print(f"  main object count: {pre_main_count}")
for name, _ in ARCHIVE_PLAN:
    assert table_present("main", name), f"PRECONDITION FAIL: main.{name} missing"
    print(f"  present: main.{name} (rows={row_count_main(name)}, cols={col_count('main', name)})")
for name in DEFERRED:
    assert table_present("main", name), f"DEFERRED table main.{name} missing — unexpected"

# Snapshot prestate
for name, _ in ARCHIVE_PLAN:
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_prestate_v1
        VALUES (?, 'main', ?, ?, ?, NOW())
    """, [SCRIPT_NUM, name, row_count_main(name), col_count("main", name)])
print(f"  inserted {len(ARCHIVE_PLAN)} pre-state rows")


# 1. Per-table archive
header("1. Archive sequence")
for name, reason in ARCHIVE_PLAN:
    src_rows = row_count_main(name)
    archive_table = f"{name}_pre346_{UTC}"
    archive_fq = f'"Thyroid 2026 UPdated".archive_pub_v1_0."{archive_table}"'
    print()
    print(f"  -> {name} (src_rows={src_rows})")

    # CTAS
    con.execute(f"""
        CREATE TABLE {ARCH_DB}.archive_pub_v1_0."{archive_table}"
        AS SELECT * FROM {DB}.main."{name}"
    """)

    # Row-count parity assertion
    arc_rows = con.execute(
        f'SELECT COUNT(*) FROM {ARCH_DB}.archive_pub_v1_0."{archive_table}"'
    ).fetchone()[0]
    if arc_rows != src_rows:
        raise SystemExit(
            f"ARCHIVE PARITY FAIL: {name} src={src_rows} archive={arc_rows}"
        )
    print(f"     archive parity OK ({arc_rows} rows)")

    # Log
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.archive_move_log_v1
        (moved_at, src_schema, src_table, archive_fq, n_rows, reason, script)
        VALUES (NOW(), 'main', ?, ?, ?, ?, ?)
    """, [name, archive_fq, src_rows, f"Prompt 6 cleanup — {reason}", SCRIPT_TAG])

    # Drop
    con.execute(f'DROP TABLE {DB}.main."{name}"')
    print(f"     dropped main.{name}")


# 2. Final assertions
header("2. Final assertions")
post_main_count = main_object_count()
print(f"  main object count: pre={pre_main_count} post={post_main_count} delta={post_main_count - pre_main_count}")
if post_main_count != pre_main_count - len(ARCHIVE_PLAN):
    raise SystemExit(
        f"OBJECT-COUNT ASSERTION FAIL: expected delta -{len(ARCHIVE_PLAN)}, "
        f"got {post_main_count - pre_main_count}"
    )

logged = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.manuscript_workspace.archive_move_log_v1
     WHERE script = ?
""", [SCRIPT_TAG]).fetchone()[0]
print(f"  archive_move_log_v1 rows for {SCRIPT_TAG}: {logged}")
if logged != len(ARCHIVE_PLAN):
    raise SystemExit(
        f"LOG ASSERTION FAIL: expected {len(ARCHIVE_PLAN)} rows, got {logged}"
    )

for name in DEFERRED:
    assert table_present("main", name), (
        f"DEFER ASSERTION FAIL: main.{name} unexpectedly removed"
    )
    print(f"  deferred-still-present: main.{name}")

for name, _ in ARCHIVE_PLAN:
    assert not table_present("main", name), (
        f"DROP ASSERTION FAIL: main.{name} still present after drop"
    )

# Post-snapshot of main object count (single summary row, table_name='__main_object_count__')
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_poststate_v1
    VALUES (?, 'main', '__main_object_count__', ?, NULL, NOW())
""", [SCRIPT_NUM, post_main_count])

print()
print(f"DONE. Script 346 archived {len(ARCHIVE_PLAN)} extracted_*_v1 tables.")
print(f"      Deferred: {DEFERRED}")
print(f"      UTC stamp: {UTC}")
